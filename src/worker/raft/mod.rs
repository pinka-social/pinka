mod log_entry;
mod replicate;
mod rpc;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::ops::Deref;
use std::time::Duration;

pub(crate) use self::log_entry::{LogEntry, LogEntryList, LogEntryValue};
use self::replicate::{ReplicateArgs, ReplicateMsg, ReplicateWorker};
pub(crate) use self::rpc::PinkaSerDe;
pub(super) use self::rpc::{
    AdvanceCommitIndexMsg, AppendEntriesAsk, AppendEntriesReply, PeerId, RequestVoteAsk,
    RequestVoteReply,
};

use anyhow::{Context, Result, anyhow};
use fjall::{KvSeparationOptions, PartitionCreateOptions, PartitionHandle};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent, pg};
use ractor_cluster::RactorClusterMessage;
use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::block_in_place;
use tokio::time::{Instant, sleep};
use tracing::{debug, info, trace, warn};

use crate::config::{RuntimeConfig, ServerConfig};

pub(super) struct RaftWorker;

#[derive(RactorClusterMessage)]
pub(super) enum RaftMsg {
    ElectionTimeout,
    UpdateTerm(u32),
    AdvanceCommitIndex(AdvanceCommitIndexMsg),
    #[rpc]
    AppendEntries(AppendEntriesAsk, RpcReplyPort<AppendEntriesReply>),
    RequestVote(RequestVoteAsk),
    RequestVoteResponse(RequestVoteReply),
    // TODO: add status code
    #[rpc]
    ClientRequest(LogEntryValue, RpcReplyPort<bool>),
}

/// Role played by the worker.
#[derive(Debug, Clone, Copy)]
pub(super) enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy)]
struct RaftShared {
    /// Latest term this worker has seen (initialized to 0 on first boot,
    /// increases monotonically).
    ///
    /// Updated on stable storage before responding to RPCs.
    current_term: u32,

    /// Volatile state. Index of highest log entry known to be committed
    /// (initialized to 0, increases monotonically).
    commit_index: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RaftSaved {
    /// Latest term this worker has seen (initialized to 0 on first boot,
    /// increases monotonically).
    ///
    /// Updated on stable storage before responding to RPCs.
    current_term: u32,

    /// CandidateId that received vote in current term (or None if none).
    ///
    /// Updated on stable storage before responding to RPCs.
    voted_for: Option<PeerId>,
}

impl<'de> PinkaSerDe<'de> for RaftSaved {}

pub(super) struct RaftState {
    /// Actor reference
    myself: ActorRef<RaftMsg>,

    /// Cluster config
    config: RuntimeConfig,

    /// State restore partition
    restore: PartitionHandle,

    /// Latest term this worker has seen (initialized to 0 on first boot,
    /// increases monotonically).
    ///
    /// Updated on stable storage before responding to RPCs.
    current_term: u32,

    /// Role played by the worker.
    role: RaftRole,

    /// CandidateId that received vote in current term (or None if none).
    ///
    /// Updated on stable storage before responding to RPCs.
    voted_for: Option<PeerId>,

    /// Volatile state on candidates. At most one record for each peer.
    votes_received: BTreeSet<PeerId>,

    /// Volatile state on leaders. For each peer, index of the highest log
    /// entry known to be replicated on server (initialized to 0, increases
    /// monotonically).
    match_index: BTreeMap<PeerId, usize>,

    /// Raft log partition
    log: PartitionHandle,

    /// Volatile state. Index of highest log entry known to be committed
    /// (initialized to 0, increases monotonically).
    commit_index: usize,

    /// Last known remote leader
    leader_id: Option<PeerId>,

    /// Volatile state. Term of the last log entry appended. Initialized from
    /// log and updated after each append.
    last_log_term: u32,

    /// Volatile state. Index of the last log entry appended. Initialized from
    /// log and updated after each append.
    last_log_index: usize,

    /// Volatile state. Index of highest log entry applied to the state machine
    /// (initialized to 0, increases monotonically).
    _last_applied: usize,

    /// Keeps track of outstanding start election timer.
    election_timer: Option<Sender<Duration>>,

    /// Peers, workaround bug in ractor
    replicate_workers: BTreeMap<PeerId, ActorRef<ReplicateMsg>>,
}

impl Deref for RaftState {
    type Target = ActorRef<RaftMsg>;

    fn deref(&self) -> &Self::Target {
        &self.myself
    }
}

impl RaftWorker {
    pub(crate) fn pg_name() -> String {
        "raft_worker".into()
    }
}

impl Actor for RaftWorker {
    type Msg = RaftMsg;
    type State = RaftState;
    type Arguments = RuntimeConfig;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let config = args;

        let log = block_in_place(|| {
            config.keyspace.open_partition(
                "raft_log",
                PartitionCreateOptions::default()
                    .compression(fjall::CompressionType::Lz4)
                    .manual_journal_persist(true)
                    .with_kv_separation(KvSeparationOptions::default()),
            )
        })?;

        let restore = block_in_place(|| {
            config.keyspace.open_partition(
                "raft_restore",
                PartitionCreateOptions::default()
                    .compression(fjall::CompressionType::Lz4)
                    .manual_journal_persist(true),
            )
        })?;

        let mut state = RaftState::new(myself, config, log, restore);
        state.restore_state().await?;

        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "raft_worker started");

        pg::join_scoped("raft".into(), RaftWorker::pg_name(), vec![
            myself.get_cell(),
        ]);
        pg::monitor_scope("raft".into(), myself.get_cell());
        info!(target: "lifecycle", "joined raft process group");

        if !matches!(state.role, RaftRole::Leader) {
            state.set_election_timer();
        }

        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        use RaftMsg::*;

        match message {
            RequestVote(request) => {
                if state.config.server.observer {
                    return Ok(());
                }
                state.handle_request_vote(request).await?;
            }
            RequestVoteResponse(reply) => {
                if state.config.server.observer {
                    return Ok(());
                }
                state.handle_request_vote_response(reply).await?;
            }
            AppendEntries(request, reply) => {
                state.handle_append_entries(request, reply).await?;
            }
            ElectionTimeout => {
                if state.config.server.observer {
                    return Ok(());
                }
                state.start_new_election().await?;
            }
            AdvanceCommitIndex(peer_info) => {
                if state.config.server.observer {
                    return Ok(());
                }
                state.advance_commit_index(peer_info);
            }
            UpdateTerm(new_term) => {
                state.update_term(new_term).await?;
            }
            ClientRequest(request, reply) => {
                state.handle_client_request(request, reply).await?;
            }
        }

        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorFailed(_, _) => {
                myself.stop(Some("replication failed".into()));
            }
            SupervisionEvent::ProcessGroupChanged(change) => {
                if change.get_scope() != "raft" {
                    return Ok(());
                }
                if matches!(state.role, RaftRole::Leader) {
                    match change {
                        pg::GroupChangeMessage::Join(_, _, members) => {
                            for server in members {
                                let server_name = server.get_name().unwrap();
                                if !state.replicate_workers.contains_key(&server_name) {
                                    info!(target: "raft", peer = server_name, "resume replicating");
                                    state.spawn_one_replicate_worker(server.into()).await?;
                                }
                            }
                        }
                        pg::GroupChangeMessage::Leave(_, _, members) => {
                            for server in members {
                                let server_name = server.get_name().unwrap();
                                if let Some(worker) = state.replicate_workers.remove(&server_name) {
                                    info!(target: "raft", peer = server_name, "stop replicating");
                                    worker.stop(Some("remote server disconnected".into()));
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn election_timer(myself: ActorRef<RaftMsg>, timeout: Duration) -> Sender<Duration> {
    let (tx, mut rx) = channel(1);
    let mut sleep = Box::pin(sleep(timeout));

    tokio::spawn(async move {
        loop {
            select! {
                new_timeout = rx.recv() => {
                    match new_timeout {
                        Some(timeout) => sleep.as_mut().reset(Instant::now() + timeout),
                        None => break,
                    }
                }
                _ = &mut sleep => {
                    if let Err(ref error) = ractor::cast!(myself, RaftMsg::ElectionTimeout) {
                        warn!(
                            target: "raft",
                            myself = %myself.get_id(),
                            %error,
                            "failed to send election timeout"
                        );
                    }
                    break;
                }
            }
        }
    });

    tx
}

impl RaftState {
    fn new(
        myself: ActorRef<RaftMsg>,
        config: RuntimeConfig,
        log: PartitionHandle,
        restore: PartitionHandle,
    ) -> RaftState {
        Self {
            myself,
            config,
            restore,
            current_term: 1,
            role: RaftRole::Follower,
            voted_for: None,
            votes_received: BTreeSet::new(),
            match_index: BTreeMap::new(),
            log,
            commit_index: 0,
            leader_id: None,
            last_log_term: 0,
            last_log_index: 0,
            _last_applied: 0,
            election_timer: None,
            replicate_workers: BTreeMap::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        self.get_name()
            .expect("raft_worker should have name=server_name")
    }

    async fn restore_state(&mut self) -> Result<()> {
        let saved = block_in_place(|| match self.restore.get("raft_saved") {
            Ok(Some(value)) => RaftSaved::from_bytes(&value),
            _ => Ok(RaftSaved::default()),
        })?;
        info!(
            target: "raft",
            current_term = saved.current_term,
            voted_for = saved.voted_for,
            "restored from state"
        );
        self.current_term = saved.current_term;
        self.voted_for = saved.voted_for;

        if let Ok(last_log) = self.get_last_log_entry() {
            info!(
                target: "raft",
                last_log_index = last_log.index,
                last_log_term = last_log.term,
                "restored from log"
            );
            self.last_log_index = last_log.index;
            self.last_log_term = last_log.term;
        }

        Ok(())
    }

    async fn persist_state(&mut self) -> Result<()> {
        let saved = RaftSaved {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
        };
        block_in_place(|| {
            saved
                .to_bytes()
                .context("Failed to serialize raft_saved state")
                .and_then(|value| {
                    self.restore
                        .insert("raft_saved", value.as_slice())
                        .context("Failed to update raft_saved state")
                })
                .and_then(|_| {
                    self.config
                        .keyspace
                        .persist(fjall::PersistMode::SyncAll)
                        .context("Failed to persist change")
                })
        })?;
        Ok(())
    }

    async fn spawn_replicate_workers(&mut self) -> Result<()> {
        assert!(self.replicate_workers.is_empty());

        for server in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            if server.get_name() == self.get_name() {
                continue;
            }
            self.spawn_one_replicate_worker(server.into()).await?;
        }
        Ok(())
    }

    async fn spawn_one_replicate_worker(&mut self, server: ActorRef<RaftMsg>) -> Result<()> {
        if server.get_name() == self.get_name() {
            return Ok(());
        }
        let server_name = server.get_name().unwrap();
        let observer = self
            .server_config_for(&server_name)
            .with_context(|| format!("server {server_name} is not defined in config"))?
            .observer;

        info!(target: "raft", peer = server_name, observer, "spawn replicate worker");
        let args = ReplicateArgs {
            config: self.config.clone(),
            raft: RaftShared {
                current_term: self.current_term,
                commit_index: self.commit_index,
            },
            name: self.peer_id(),
            parent: self.myself.clone(),
            peer: server.into(),
            log: self.log.clone(),
            last_log_index: self.last_log_index,
            observer,
        };
        let (peer, _) = Actor::spawn_linked(None, ReplicateWorker, args, self.get_cell()).await?;
        self.replicate_workers.insert(server_name, peer);
        Ok(())
    }

    fn min_quorum_match_index(&self) -> usize {
        if self.match_index.is_empty() {
            return 0;
        }
        let mut values = self.match_index.values().collect::<Vec<_>>();
        values.sort_unstable();
        *values[(values.len() - 1) / 2]
    }

    fn voted_has_quorum(&self) -> bool {
        let cluster_size = self
            .config
            .init
            .cluster
            .servers
            .iter()
            .filter(|s| !s.observer)
            .count();
        if cluster_size == 1 {
            return true;
        }
        self.votes_received.len() >= cluster_size / 2 + 1
    }

    fn set_election_timer(&mut self) {
        let duration = Duration::from_millis(rand::thread_rng().gen_range(
            self.config.init.raft.min_election_ms..=self.config.init.raft.max_election_ms,
        ));

        debug_assert!(matches!(
            self.role,
            RaftRole::Follower | RaftRole::Candidate
        ));
        debug!(target: "raft", "will start election in {:?}", duration);

        match &self.election_timer {
            Some(timer) if !timer.is_closed() => {
                let _ = timer.try_send(duration);
            }
            _ => {
                self.election_timer = Some(election_timer(self.myself.clone(), duration));
            }
        }
    }

    fn unset_election_timer(&mut self) {
        info!(target: "raft", "unset election timer");
        self.election_timer = None;
    }

    async fn start_new_election(&mut self) -> Result<()> {
        if matches!(self.role, RaftRole::Leader) {
            warn!(target: "raft", "starting a election as a leader");
        }
        if let Some(leader_id) = &self.leader_id {
            info!(
                target: "raft",
                prev_leader_id = %leader_id,
                new_term = self.current_term + 1,
                "running for election (unresponsive leader)"
            );
        } else if matches!(self.role, RaftRole::Candidate) {
            info!(
                target: "raft",
                new_term = self.current_term + 1,
                "running for election (previous candidacy timed out)"
            );
        } else {
            info!(
                target: "raft",
                new_term = self.current_term + 1,
                "running for election (there was no leader)"
            );
        }
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = None;
        self.votes_received.clear();
        self.leader_id = None;
        self.persist_state().await?;
        self.set_election_timer();

        self.request_vote();

        Ok(())
    }

    fn request_vote(&self) {
        assert!(matches!(self.role, RaftRole::Candidate));
        for peer in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            let peer: ActorRef<RaftMsg> = peer.into();
            if self
                .server_config_for(&peer.get_name().unwrap())
                .unwrap()
                .observer
            {
                continue;
            }

            let request = RequestVoteAsk {
                term: self.current_term,
                candidate_name: self.peer_id(),
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
            };

            info!(
                target: "raft",
                to = %peer.get_name().unwrap(),
                term = request.term,
                "request_vote"
            );

            let call_result = ractor::cast!(peer, RaftMsg::RequestVote(request));
            if let Err(error) = call_result {
                warn!(target: "rpc", %error, "request_vote failed");
            }
        }
    }

    fn advance_commit_index(&mut self, peer_info: AdvanceCommitIndexMsg) {
        if thread_rng().gen_bool(1.0 / 10000.0) {
            panic!("simulated crash");
        }
        if !matches!(self.role, RaftRole::Leader) {
            warn!(target: "raft", "advance_commit_index called as {:?}", self.role);
            return;
        }
        if let Some(peer_id) = peer_info.peer_id {
            self.match_index.insert(peer_id, peer_info.match_index);
        }
        let new_commit_index = self.min_quorum_match_index();
        if self.commit_index >= new_commit_index {
            return;
        }
        // At least one log entry must be from the current term to guarantee
        // that no server without them can be elected.
        let log_entry = self.get_log_entry(new_commit_index);
        if let Ok(entry) = log_entry {
            if entry.term == self.current_term {
                self.commit_index = new_commit_index;
                trace!(target: "raft", "new commit_index: {}", self.commit_index);
                self.notify_state_change();
            }
        }
    }

    async fn handle_request_vote(&mut self, request: RequestVoteAsk) -> Result<()> {
        info!(
            target: "raft",
            candidate = request.candidate_name,
            current_term = self.current_term,
            request_term = request.term,
            "received request for vote"
        );
        // TODO ignore distrubing request_vote
        self.update_term(request.term).await?;

        let log_ok = request.last_log_term > self.last_log_term
            || (request.last_log_term == self.last_log_term
                && request.last_log_index == self.last_log_index);

        if request.term == self.current_term && log_ok && self.voted_for.is_none() {
            info!(
                target: "raft",
                candidate = request.candidate_name,
                "voted"
            );
            self.voted_for = Some(request.candidate_name.clone());
            self.persist_state().await?;
        }

        for server in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            if server.get_name().as_ref() == Some(&request.candidate_name) {
                let response = RequestVoteReply {
                    term: self.current_term,
                    vote_granted: self.voted_for == Some(request.candidate_name.clone()),
                    vote_from: self.peer_id(),
                };
                let server: ActorRef<RaftMsg> = server.into();
                if let Err(error) = ractor::cast!(server, RaftMsg::RequestVoteResponse(response)) {
                    warn!(
                        candidate = request.candidate_name,
                        %error,
                        "sending request vote reply failed"
                    );
                }
                break;
            }
        }

        Ok(())
    }

    async fn handle_append_entries(
        &mut self,
        request: AppendEntriesAsk,
        reply: RpcReplyPort<AppendEntriesReply>,
    ) -> Result<()> {
        self.update_term(request.term).await?;

        if self.leader_id.is_some() {
            self.set_election_timer();
        }

        assert!(request.term <= self.current_term);

        let log_ok = request.prev_log_index == 0
            || (request.prev_log_index > 0
                && request.prev_log_index <= self.last_log_index
                && request.prev_log_term == self.get_log_entry(request.prev_log_index)?.term);

        let mut response = AppendEntriesReply {
            term: self.current_term,
            success: false,
        };
        if request.term < self.current_term
            || (request.term == self.current_term
                && matches!(self.role, RaftRole::Follower)
                && !log_ok)
        {
            trace!(
                target: "raft",
                server = request.leader_id,
                term = request.term,
                "discard stale append_entries request from server {} in term {} (this server's term was {}",
                request.leader_id,
                request.term,
                self.get_id()
            );
            // reject request
            if let Err(error) = reply.send(response) {
                warn!(target: "rpc", %error, "send response to append_entries failed");
            }
            return Ok(());
        }

        self.recognize_new_leader(&request.leader_id).await?;

        let index = request.prev_log_index + 1;
        if request.entries.is_empty()
            || (self.last_log_index >= index
                && self.get_log_entry(index)?.term == request.entries[0].term)
        {
            // already done with request
            self.commit_index = request.commit_index;
            response.success = true;
            self.set_election_timer();

            if let Err(error) = reply.send(response) {
                warn!(target: "rpc", %error, "send response to append_entries failed");
            }
            return Ok(());
        }
        if !request.entries.is_empty()
            && self.last_log_index >= index
            && self.get_log_entry(index)?.term != request.entries[0].term
        {
            // conflict: remove 1 entry
            self.remove_last_log_entry()?;
            self.set_election_timer();

            if let Err(error) = reply.send(response) {
                warn!(target: "rpc", %error, "send response to append_entries failed");
            }
            return Ok(());
        }
        if !request.entries.is_empty() && self.last_log_index == request.prev_log_index {
            self.replicate_log_entries(&request.entries)?;
            response.success = true;
            self.set_election_timer();
            if let Err(error) = reply.send(response) {
                warn!(target: "rpc", %error, "send response to append_entries failed");
            }
            return Ok(());
        }

        Ok(())
    }

    async fn handle_request_vote_response(&mut self, response: RequestVoteReply) -> Result<()> {
        self.update_term(response.term).await?;

        if response.term < self.current_term {
            warn!(target: "raft", peer = response.vote_from, "discard stale vote response");
            return Ok(());
        }

        if response.term == self.current_term {
            if response.vote_granted {
                info!(
                    target: "raft",
                    peer = response.vote_from,
                    peer_term = response.term,
                    current_term = self.current_term,
                    "got one vote",
                );
                if !matches!(self.role, RaftRole::Candidate) {
                    if matches!(self.role, RaftRole::Follower) {
                        warn!(
                            target: "raft",
                            peer = response.vote_from,
                            current_role = ?self.role,
                            "received vote but not a candidate"
                        );
                    }
                    return Ok(());
                }
                self.votes_received.insert(response.vote_from);
                info!(target: "raft", result = ?self.votes_received, "election poll");
                if self.voted_has_quorum() {
                    self.become_leader().await?;
                }
            } else {
                info!(
                    target: "raft",
                    peer = response.vote_from,
                    peer_term = response.term,
                    current_term = self.current_term,
                    "vote was denied",
                );
            }
        }
        Ok(())
    }

    async fn become_leader(&mut self) -> Result<()> {
        assert!(matches!(self.role, RaftRole::Candidate));
        info!(target: "raft", "received quorum, becoming leader");
        self.role = RaftRole::Leader;
        self.leader_id = None;
        self.voted_for = None;
        self.persist_state().await?;
        self.unset_election_timer();
        self.reset_match_index();
        self.append_log(LogEntryValue::NewTermStarted)?;
        self.spawn_replicate_workers().await?;
        Ok(())
    }

    async fn recognize_new_leader(&mut self, peer_id: &PeerId) -> Result<()> {
        if self.leader_id.as_ref() != Some(peer_id) {
            self.leader_id = Some(peer_id.to_owned());
            info!(
                target: "raft",
                leader = self.leader_id,
                term = self.current_term,
                "recognized new leader",
            );
        }
        Ok(())
    }

    async fn update_term(&mut self, new_term: u32) -> Result<()> {
        if new_term <= self.current_term {
            return Ok(());
        }

        let was_leader = matches!(self.role, RaftRole::Leader);

        self.current_term = new_term;
        self.voted_for = None;
        self.role = RaftRole::Follower;
        self.stop_children(None);
        self.replicate_workers.clear();
        self.persist_state().await?;
        self.set_election_timer();

        if was_leader || self.election_timer.is_none() {
            info!(target: "raft", "stepping down");
        }

        Ok(())
    }

    async fn handle_client_request(
        &mut self,
        request: LogEntryValue,
        reply: RpcReplyPort<bool>,
    ) -> Result<()> {
        if matches!(self.role, RaftRole::Leader) {
            self.append_log(request)?;
            reply.send(true)?;
            return Ok(());
        }
        // Forward to leader
        if let Some(leader) = self.get_leader() {
            reply.send(ractor::call!(leader, RaftMsg::ClientRequest, request)?)?;
            return Ok(());
        }
        reply.send(false)?;
        Ok(())
    }

    fn server_config_for<'a>(&'a self, name: &str) -> Option<&'a ServerConfig> {
        self.config
            .init
            .cluster
            .servers
            .iter()
            .find(|&s| s.name == name)
    }

    fn get_leader(&self) -> Option<ActorRef<RaftMsg>> {
        if matches!(self.role, RaftRole::Leader) {
            return Some(self.myself.clone());
        }
        if let Some(leader_id) = &self.leader_id {
            for server in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
                if server.get_name().as_ref() == Some(leader_id) {
                    return Some(server.into());
                }
            }
        }
        None
    }

    fn get_last_log_entry(&self) -> Result<LogEntry> {
        block_in_place(|| {
            self.log
                .last_key_value()
                .context("get log entry failed")?
                .ok_or_else(|| anyhow!("index out of range"))
                .and_then(|(_, slice)| {
                    LogEntry::from_bytes(&slice).context("failed to deserialize log entry")
                })
        })
    }

    fn get_log_entry(&self, index: usize) -> Result<LogEntry> {
        block_in_place(|| {
            self.log
                .get(&index.to_be_bytes())
                .context("get log entry failed")?
                .ok_or_else(|| anyhow!("log entry index {index} does not exist"))
                .and_then(|slice| {
                    LogEntry::from_bytes(&slice).context("failed to deserialize log entry")
                })
        })
    }

    fn append_log(&mut self, value: LogEntryValue) -> Result<()> {
        let index = self.last_log_index + 1;
        let new_log_entry = LogEntry {
            index,
            term: self.current_term,
            value,
        };
        let value_bytes = new_log_entry.to_bytes()?;
        block_in_place(|| -> Result<()> {
            self.log.insert(&index.to_be_bytes(), value_bytes)?;
            self.config.keyspace.persist(fjall::PersistMode::SyncAll)?;
            Ok(())
        })?;
        self.last_log_index = index;
        self.last_log_term = self.current_term;
        Ok(())
    }

    fn replicate_log_entries(&mut self, entries: &[LogEntry]) -> Result<()> {
        block_in_place(|| -> Result<()> {
            for entry in entries {
                let value_bytes = entry.to_bytes()?;
                self.log.insert(&entry.index.to_be_bytes(), value_bytes)?;
                self.last_log_index = entry.index;
                self.last_log_term = entry.term;
            }
            self.config.keyspace.persist(fjall::PersistMode::SyncAll)?;
            Ok(())
        })?;
        Ok(())
    }

    fn remove_last_log_entry(&mut self) -> Result<()> {
        block_in_place(|| self.log.remove(&self.last_log_index.to_be_bytes()))?;
        self.last_log_index -= 1;
        self.last_log_term = self.get_log_entry(self.last_log_index)?.term;
        Ok(())
    }

    fn reset_match_index(&mut self) {
        self.match_index.clear();
        for server in &self.config.init.cluster.servers {
            if !server.observer {
                self.match_index.insert(server.name.clone(), 0);
            }
        }
    }

    fn notify_state_change(&self) {
        let raft = RaftShared {
            current_term: self.current_term,
            commit_index: self.commit_index,
        };
        for worker in self.replicate_workers.values() {
            if let Err(ref err) = worker.cast(ReplicateMsg::NotifyStateChange(raft)) {
                warn!(
                    error = err as &dyn Error,
                    peer = worker.get_id().to_string(),
                    "notify_state_change failed",
                );
            }
        }
    }
}
