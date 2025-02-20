mod client;
mod log_entry;
mod replicate;
mod rpc;
mod state;
mod state_machine;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::time::Duration;

pub(crate) use self::client::{get_raft_local_client, ClientResult, RaftClientMsg};
use self::log_entry::RaftLog;
pub(crate) use self::log_entry::{LogEntry, LogEntryList, LogEntryValue};
use self::replicate::{ReplicateArgs, ReplicateMsg, ReplicateWorker};
use self::rpc::RaftSerDe;
use self::rpc::{
    AdvanceCommitIndexMsg, AppendEntriesAsk, AppendEntriesReply, PeerId, RequestVoteAsk,
    RequestVoteReply,
};
use self::state::RaftSaved;
pub(crate) use self::state_machine::{get_raft_applied, RaftAppliedMsg, StateMachineMsg};

use anyhow::{Context, Error, Result};
use fjall::{KvSeparationOptions, PartitionCreateOptions, PartitionHandle, PersistMode};
use ractor::{pg, Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};
use ractor_cluster::{RactorClusterMessage, RactorMessage};
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::spawn_blocking;
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, trace, warn};

use crate::config::{RuntimeConfig, ServerConfig};

pub(super) struct RaftServer;
#[derive(RactorMessage)]
pub(super) enum RaftServerMsg {}

impl Actor for RaftServer {
    type Msg = RaftServerMsg;
    type State = RuntimeConfig;
    type Arguments = RuntimeConfig;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Actor::spawn_linked(
            Some(args.server.name.clone()),
            RaftWorker,
            args.clone(),
            myself.get_cell(),
        )
        .await?;
        Ok(args)
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        if let SupervisionEvent::ActorFailed(_, error) = message {
            error!("{:?}", error);
            info!("raft worker crashed, restarting...");
            Actor::spawn_linked(
                Some(state.server.name.clone()),
                RaftWorker,
                state.clone(),
                myself.get_cell(),
            )
            .await?;
        }
        Ok(())
    }
}

struct RaftWorker;

#[derive(RactorClusterMessage)]
enum RaftMsg {
    ElectionTimeout,
    UpdateTerm(u32),
    AdvanceCommitIndex(AdvanceCommitIndexMsg),
    #[rpc]
    AppendEntries(AppendEntriesAsk, RpcReplyPort<AppendEntriesReply>),
    RequestVote(RequestVoteAsk),
    RequestVoteResponse(RequestVoteReply),
    // TODO: add status code
    #[rpc]
    ClientRequest(LogEntryValue, RpcReplyPort<ClientResult>),
    AppliedLog(u64, ClientResult),
}

/// Role played by the worker.
#[derive(Debug, Clone, Copy)]
enum RaftRole {
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
    commit_index: u64,
}

struct RaftState {
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
    match_index: BTreeMap<PeerId, u64>,

    /// Raft log
    log: RaftLog,

    /// Volatile state. Index of highest log entry known to be committed
    /// (initialized to 0, increases monotonically).
    commit_index: u64,

    /// Last known remote leader
    leader_id: Option<PeerId>,

    /// Volatile state. Term of the last log entry appended. Initialized from
    /// log and updated after each append.
    last_log_term: u32,

    /// Volatile state. Index of the last log entry appended. Initialized from
    /// log and updated after each append.
    last_log_index: u64,

    /// Volatile state. Index of the last log entry enqueued for the state
    /// machine to avoid sending duplicated log entries to the state machine.
    /// However the state machine might still receive repeated logs after a
    /// crash restore.
    ///
    /// Restored from last_applied.
    last_queued: u64,

    /// Index of highest log entry applied to the state machine (initialized to
    /// 0, increases monotonically).
    ///
    /// Updated on stable storage after state machine has applied an entry.
    last_applied: u64,

    /// Keeps track of outstanding start election timer.
    election_timer: Option<Sender<Duration>>,

    /// Peers, workaround bug in ractor
    replicate_workers: BTreeMap<PeerId, ActorRef<ReplicateMsg>>,

    /// Volatile state on leaders. Outstanding client requests mapped by log index.
    /// TODO: add Effect
    pending_responses: BTreeMap<u64, RpcReplyPort<ClientResult>>,
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

        let keyspace = config.keyspace.clone();
        let log = spawn_blocking(move || {
            keyspace.open_partition(
                "raft_log",
                PartitionCreateOptions::default()
                    .with_kv_separation(KvSeparationOptions::default()),
            )
        })
        .await?
        .context("Failed to open raft_log")?;

        let keyspace = config.keyspace.clone();
        let restore = spawn_blocking(move || {
            keyspace.open_partition("raft_restore", PartitionCreateOptions::default())
        })
        .await?
        .context("Failed to open raft_restore state")?;

        let mut state = RaftState::new(myself, config, log, restore);
        state
            .restore_state()
            .await
            .context("Failed to restore raft state")?;

        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("raft_worker started");

        pg::join_scoped(
            "raft".into(),
            RaftWorker::pg_name(),
            vec![myself.get_cell()],
        );
        pg::monitor_scope("raft".into(), myself.get_cell());
        info!("raft_worker joined raft process group and start monitoring changes");

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
                if state.config.server.readonly_replica {
                    return Ok(());
                }
                state
                    .handle_request_vote(request)
                    .await
                    .context("Failed to handle RequestVote")?;
            }
            RequestVoteResponse(reply) => {
                if state.config.server.readonly_replica {
                    return Ok(());
                }
                state
                    .handle_request_vote_response(reply)
                    .await
                    .context("Failed to handle RequestVoteResponse")?;
            }
            AppendEntries(request, reply) => {
                state
                    .handle_append_entries(request, reply)
                    .await
                    .context("Failed to handle AppendEntries")?;
            }
            ElectionTimeout => {
                if state.config.server.readonly_replica {
                    return Ok(());
                }
                state
                    .start_new_election()
                    .await
                    .context("Failed to start a new election")?;
            }
            AdvanceCommitIndex(peer_info) => {
                if state.config.server.readonly_replica {
                    return Ok(());
                }
                state
                    .advance_commit_index(peer_info)
                    .await
                    .context("Failed to advance commit index")?;
            }
            UpdateTerm(new_term) => {
                state.update_term(new_term).await?;
            }
            ClientRequest(request, reply) => {
                state
                    .handle_client_request(request, reply)
                    .await
                    .context("Failed to handle ClientRequest")?;
            }
            AppliedLog(last_applied, result) => {
                state
                    .handle_applied_log(last_applied, result)
                    .await
                    .context("Failed to handle AppliedLog")?;
            }
        }

        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorFailed(_, error) => {
                error!("replication failed");
                return Err(error);
            }
            SupervisionEvent::ProcessGroupChanged(change) => {
                if change.get_scope() != "raft" {
                    return Ok(());
                }
                if !matches!(state.role, RaftRole::Leader) {
                    return Ok(());
                }
                match change {
                    pg::GroupChangeMessage::Join(_, _, members) => {
                        for server in members {
                            let server_name =
                                server.get_name().expect("raft server should have name");
                            if !state.replicate_workers.contains_key(&server_name) {
                                info!(peer = server_name, "peer joined, resume replication");
                                state
                                    .spawn_one_replicate_worker(server.into())
                                    .await
                                    .context("Failed to spawn replication worker")?;
                            }
                        }
                    }
                    pg::GroupChangeMessage::Leave(_, _, members) => {
                        for server in members {
                            let server_name =
                                server.get_name().expect("raft server should have name");
                            if let Some(worker) = state.replicate_workers.remove(&server_name) {
                                info!(peer = server_name, "peer left, stop replication");
                                worker.stop(Some("remote server disconnected".into()));
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
                        warn!(%error, "failed to send election timeout");
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
            log: RaftLog::new(log),
            commit_index: 0,
            leader_id: None,
            last_log_term: 0,
            last_log_index: 0,
            last_queued: 0,
            last_applied: 0,
            election_timer: None,
            replicate_workers: BTreeMap::new(),
            pending_responses: BTreeMap::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        self.get_name()
            .expect("raft_worker should have name=server_name")
    }

    async fn restore_state(&mut self) -> Result<()> {
        let restore = self.restore.clone();
        let saved = spawn_blocking(move || match restore.get("raft_saved") {
            Ok(Some(value)) => RaftSaved::from_bytes(&value),
            _ => Ok(RaftSaved::default()),
        })
        .await?
        .context("Failed to decode saved raft state")?;

        let RaftSaved {
            current_term,
            voted_for,
            last_applied,
        } = saved;

        info!(voted_for, current_term, last_applied, "restored from state");

        self.current_term = current_term;
        self.voted_for = voted_for;
        self.last_queued = last_applied;
        self.last_applied = last_applied;

        if let Some(last_log) = self.log.get_last_log_entry().await? {
            info!(last_log.term, last_log.index, "restored from raft_log");
            self.last_log_index = last_log.index;
            self.last_log_term = last_log.term;
        }

        if self.last_applied > self.last_log_index {
            error!(
                last_applied = self.last_applied,
                last_log_index = self.last_log_index,
                "detected inconsistent state, last_applied is greater than last_log_index"
            );
        }

        Ok(())
    }

    async fn persist_state(&mut self) -> Result<()> {
        let saved = RaftSaved {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            last_applied: self.last_applied,
        };
        let mut batch = self
            .config
            .keyspace
            .batch()
            .durability(Some(PersistMode::SyncAll));
        let restore = self.restore.clone();
        spawn_blocking(move || {
            saved.to_bytes().and_then(|value| {
                batch.insert(&restore, "raft_saved", value);
                batch.commit()?;
                Ok(())
            })
        })
        .await?
        .context("Failed to persist raft state")
    }

    async fn spawn_replicate_workers(&mut self) -> Result<()> {
        assert!(self.replicate_workers.is_empty());

        for server in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            if server.get_name() == self.get_name() {
                continue;
            }
            self.spawn_one_replicate_worker(server.into())
                .await
                .context("Failed to spawn replication worker")?;
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
            .with_context(|| format!("Server {server_name} is not defined in config"))?
            .readonly_replica;

        info!(peer = server_name, observer, "spawn replication worker");
        let args = ReplicateArgs {
            config: self.config.clone(),
            raft: RaftShared {
                current_term: self.current_term,
                commit_index: self.commit_index,
            },
            name: self.peer_id(),
            parent: self.myself.clone(),
            peer: server,
            log: self.log.clone(),
            last_log_index: self.last_log_index,
            observer,
        };
        let (peer, _) = Actor::spawn_linked(None, ReplicateWorker, args, self.get_cell()).await?;
        self.replicate_workers.insert(server_name, peer);
        Ok(())
    }

    fn min_quorum_match_index(&self) -> u64 {
        let server_count = self.active_server_count();
        if self.match_index.is_empty() {
            return 0;
        }
        let mut values = self.match_index.values().collect::<Vec<_>>();
        assert_eq!(server_count, values.len());
        values.sort_unstable();
        // Leader is always in position 0 with value 0 so we can use 1-index
        // Quorum pos = majority
        //            = (N / 2 + 1)
        // For example in 5 server cluster we need to look at index 3
        //         3 = 5 / 2 + 1
        // For example in 4 server cluster we need to look at index 3
        //         3 = 4 / 2 + 1
        // For example in 3 server cluster we need to look at index 2
        //         2 = 3 / 2 + 1
        *values[server_count / 2 + 1]
    }

    fn voted_has_quorum(&self) -> bool {
        let server_count = self.active_server_count();
        if server_count == 1 {
            return true;
        }
        // Quorum = N / 2 + 1 (we need to count leader because we always vote for ourselves)
        // For example in 5 server cluster we should receive 3 votes
        //      3 > 5 / 2
        // For example in 4 server cluster we should also receive 3 votes
        //      3 > 4 / 2
        // For example in 3 server cluster we should receive 2 votes
        //      2 > 3 / 2
        self.votes_received.len() > server_count / 2
    }

    fn set_election_timer(&mut self) {
        let duration = Duration::from_millis(rand::rng().random_range(
            self.config.init.raft.min_election_ms..=self.config.init.raft.max_election_ms,
        ));

        debug_assert!(matches!(
            self.role,
            RaftRole::Follower | RaftRole::Candidate
        ));
        debug!("will start election in {:?}", duration);

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
        debug!("unset election timer");
        self.election_timer = None;
    }

    async fn start_new_election(&mut self) -> Result<()> {
        if matches!(self.role, RaftRole::Leader) {
            warn!("starting a election as a leader");
        }
        let new_term = self.current_term + 1;
        if let Some(prev_leader_id) = &self.leader_id {
            info!(
                prev_leader_id,
                new_term, "running for election (unresponsive leader)"
            );
        } else if matches!(self.role, RaftRole::Candidate) {
            info!(
                new_term,
                "running for election (previous candidacy timed out)"
            );
        } else {
            info!(new_term, "running for election (there was no leader)");
        }
        self.role = RaftRole::Candidate;
        self.current_term = new_term;
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

        info!(term = self.current_term, "requesting votes");
        for peer in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            let peer: ActorRef<RaftMsg> = peer.into();
            let Some(peer_name) = peer.get_name() else {
                error!(remote_actor = ?peer.get_id(), "peer has no name, skipped");
                continue;
            };
            let Some(server_config) = self.server_config_for(&peer_name) else {
                error!(peer = peer_name, "peer has no server config, skipped");
                continue;
            };
            if server_config.readonly_replica {
                continue;
            }

            let request = RequestVoteAsk {
                term: self.current_term,
                candidate_name: self.peer_id(),
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
            };

            info!(to = peer_name, term = request.term, "request_vote");

            if let Err(error) = ractor::cast!(peer, RaftMsg::RequestVote(request)) {
                warn!(%error, "request_vote failed");
            }
        }
    }

    async fn advance_commit_index(&mut self, peer_info: AdvanceCommitIndexMsg) -> Result<()> {
        trace!(?peer_info, "received advance_commit_index");
        if !matches!(self.role, RaftRole::Leader) {
            warn!("advance_commit_index called as {:?}", self.role);
            return Ok(());
        }
        if let Some(peer_id) = peer_info.peer_id {
            let prev_match_index = self
                .match_index
                .insert(peer_id.clone(), peer_info.match_index);
            if let Some(prev_match_index) = prev_match_index {
                if prev_match_index > peer_info.match_index {
                    warn!(
                        peer = peer_id,
                        prev_match_index, peer_info.match_index, "match_index did not advance"
                    );
                }
            }
        }
        trace!(
            match_index = ?self.match_index,
            commit_index = self.commit_index,
            "current match_index"
        );
        let new_commit_index = self.min_quorum_match_index();
        if self.commit_index >= new_commit_index {
            return Ok(());
        }
        // At least one log entry must be from the current term to guarantee
        // that no server without them can be elected.
        let log_entry = self.log.get_log_entry(new_commit_index).await?;
        if log_entry.term == self.current_term {
            info!("new commit_index: {new_commit_index}");
            self.commit_index = new_commit_index;
            self.notify_state_change();
            self.apply_log_entries().await?;
        }

        Ok(())
    }

    async fn handle_request_vote(&mut self, request: RequestVoteAsk) -> Result<()> {
        info!(
            candidate = request.candidate_name,
            current_term = self.current_term,
            request_term = request.term,
            "received request for vote"
        );
        // TODO ignore distrubing request_vote
        self.update_term(request.term).await?;

        let log_ok = request.last_log_term > self.last_log_term
            || (request.last_log_term == self.last_log_term
                && request.last_log_index >= self.last_log_index);
        let grant = request.term == self.current_term && log_ok && self.voted_for.is_none();

        if grant {
            info!(candidate = request.candidate_name, "voted for candidate");
            self.voted_for = Some(request.candidate_name.clone());
            self.persist_state().await?;
        } else {
            info!(
                candidate = request.candidate_name,
                term_ok = (request.term == self.current_term),
                log_ok,
                voted_for = self.voted_for,
                "rejected vote request"
            );
        }

        let server = pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name())
            .into_iter()
            .find(|server| server.get_name().as_ref() == Some(&request.candidate_name));
        if let Some(server) = server {
            let response = RequestVoteReply {
                term: self.current_term,
                vote_granted: grant,
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
        } else {
            warn!(candidate = request.candidate_name, "candidate not found");
        }

        Ok(())
    }

    async fn handle_append_entries(
        &mut self,
        request: AppendEntriesAsk,
        reply: RpcReplyPort<AppendEntriesReply>,
    ) -> Result<()> {
        trace!(?request, "received append_entries");
        self.update_term(request.term).await?;

        if self.leader_id.is_some() {
            self.set_election_timer();
        }

        assert!(request.term <= self.current_term);

        let log_ok = request.prev_log_index == 0
            || (request.prev_log_index > 0
                && request.prev_log_index <= self.last_log_index
                && request.prev_log_term
                    == self.log.get_log_entry(request.prev_log_index).await?.term);

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
                server = request.leader_id,
                term = request.term,
                "discard stale append_entries request from server {} in term {} (this server's term was {}",
                request.leader_id,
                request.term,
                self.current_term
            );
            // reject request
            if let Err(error) = reply.send(response) {
                warn!(%error, "send response to append_entries failed");
            }
            return Ok(());
        }

        self.recognize_new_leader(&request.leader_id);

        let index = request.prev_log_index + 1;
        if request.entries.is_empty()
            || (self.last_log_index >= index
                && self.log.get_log_entry(index).await?.term == request.entries[0].term)
        {
            // already done with request
            self.commit_index = request.commit_index;
            response.success = true;

            trace!(?response, "done with request");
            if let Err(error) = reply.send(response) {
                warn!(%error, "send response to append_entries failed");
            }
            self.apply_log_entries().await?;
            self.set_election_timer();
            return Ok(());
        }
        if !request.entries.is_empty()
            && self.last_log_index >= index
            && self.log.get_log_entry(index).await?.term != request.entries[0].term
        {
            // conflict: remove 1 entry
            let batch = self
                .config
                .keyspace
                .batch()
                .durability(Some(PersistMode::SyncAll));
            self.log
                .remove_last_log_entry(batch, self.last_log_index)
                .await?;

            trace!(?response, "conflict, remove 1 entry from our log");
            if let Err(error) = reply.send(response) {
                warn!(%error, "send response to append_entries failed");
            }
            self.set_election_timer();
            return Ok(());
        }
        if !request.entries.is_empty() && self.last_log_index == request.prev_log_index {
            // Is there a better way to handle timeout? Just use Instant and a
            // regular interval to check?
            self.unset_election_timer();
            self.replicate_log_entries(request.entries).await?;
            response.success = true;

            trace!(?response, "replicated some log entries");
            if let Err(error) = reply.send(response) {
                warn!(%error, "send response to append_entries failed");
            }
            self.apply_log_entries().await?;
            self.set_election_timer();
            return Ok(());
        }

        Ok(())
    }

    async fn handle_request_vote_response(&mut self, response: RequestVoteReply) -> Result<()> {
        self.update_term(response.term).await?;

        if response.term < self.current_term {
            warn!(peer = response.vote_from, "discard stale vote response");
            return Ok(());
        }

        if response.term == self.current_term {
            if response.vote_granted {
                info!(
                    peer = response.vote_from,
                    peer_term = response.term,
                    current_term = self.current_term,
                    "got one vote",
                );
                if !matches!(self.role, RaftRole::Candidate) {
                    if matches!(self.role, RaftRole::Follower) {
                        warn!(
                            peer = response.vote_from,
                            current_role = ?self.role,
                            "received vote but not a candidate"
                        );
                    }
                    return Ok(());
                }
                self.votes_received.insert(response.vote_from);
                info!(result = ?self.votes_received, "election poll");
                if self.voted_has_quorum() {
                    self.become_leader()
                        .await
                        .context("Failed to become leader")?;
                }
            } else {
                info!(
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
        info!("received quorum, becoming leader");
        self.role = RaftRole::Leader;
        self.leader_id = None;
        self.voted_for = None;
        self.persist_state().await?;
        self.unset_election_timer();
        self.reset_match_index();
        self.append_log(LogEntryValue::NewTermStarted).await?;
        self.spawn_replicate_workers().await?;
        Ok(())
    }

    fn recognize_new_leader(&mut self, peer_id: &PeerId) {
        if self.leader_id.as_ref() != Some(peer_id) {
            self.leader_id = Some(peer_id.to_owned());
            info!(
                leader = self.leader_id,
                term = self.current_term,
                "recognized new leader",
            );
        }
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
        self.pending_responses.clear();
        self.persist_state()
            .await
            .context("Failed to update current term")?;
        self.set_election_timer();

        if was_leader || self.election_timer.is_none() {
            info!("stepping down");
        }

        Ok(())
    }

    async fn handle_client_request(
        &mut self,
        request: LogEntryValue,
        reply: RpcReplyPort<ClientResult>,
    ) -> Result<()> {
        if matches!(self.role, RaftRole::Leader) {
            info!("received a new client request");
            let log_index = self.append_log(request).await?;
            self.pending_responses.insert(log_index, reply);
            return Ok(());
        }
        // Forward to leader
        info!("received a new client request, forwarding to leader");
        if let Some(leader) = self.get_leader() {
            // DEADLOCK HAZARD: Leader needs our vote to confirm quorum so we
            // should not block our actor thread.
            tokio::spawn(async move {
                // TODO: add timeout?
                reply
                    .send(
                        ractor::call!(leader, RaftMsg::ClientRequest, request)
                            .expect("client_request forwarding failed"),
                    )
                    .expect("unable to reply to client");
            });
            return Ok(());
        }
        Ok(())
    }

    async fn handle_applied_log(&mut self, last_applied: u64, result: ClientResult) -> Result<()> {
        debug_assert!(self.last_applied <= last_applied);

        self.last_applied = last_applied;
        self.persist_state().await?;

        // Avoid flooded apply message caused election timeout
        if !matches!(self.role, RaftRole::Leader) {
            self.set_election_timer();
        }

        info!(
            last_queued = self.last_queued,
            last_applied = self.last_applied,
            "applied log entry"
        );

        if let Some(reply) = self.pending_responses.remove(&self.last_applied) {
            debug!("index {last_applied} applied, reply to client");
            if let Err(error) = reply.send(result) {
                info!(%error, "failed to reply client request");
            }
        }
        Ok(())
    }

    async fn apply_log_entries(&mut self) -> Result<()> {
        debug_assert!(self.last_queued >= self.last_applied);

        // TODO configurable machine name
        async {
            if let Some(machine) = ActorRef::where_is("state_machine".into()) {
                // TODO avoid message pile up
                for log_entry in self
                    .log
                    .log_entry_range(self.last_queued + 1..=self.commit_index)
                    .await?
                {
                    ractor::cast!(machine, StateMachineMsg::Apply(log_entry))?;
                }
                self.last_queued = u64::max(self.last_queued, self.commit_index);
            } else {
                warn!("unable to apply log entries because state_machine is not running");
            }
            trace!(
                last_queued = self.last_queued,
                last_applied = self.last_applied,
                "queued log entries to apply"
            );
            Ok::<_, Error>(())
        }
        .await
        .context("Failed to apply log entries")
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

    async fn append_log(&mut self, value: LogEntryValue) -> Result<u64> {
        let index = self.last_log_index + 1;
        let new_log_entry = LogEntry {
            index,
            term: self.current_term,
            value,
        };
        let batch = self
            .config
            .keyspace
            .batch()
            .durability(Some(PersistMode::SyncAll));
        self.log.insert(batch, new_log_entry).await?;
        self.last_log_index = index;
        self.last_log_term = self.current_term;

        // special case single server mode
        if self.config.init.cluster.servers.len() == 1 {
            debug!("commit immediately for single server cluster");
            self.advance_commit_index(AdvanceCommitIndexMsg {
                peer_id: Some(self.peer_id()),
                match_index: index,
            })
            .await
            .context("Failed to advance commit index (single server mode)")?;
        }

        Ok(self.last_log_index)
    }

    async fn replicate_log_entries(&mut self, entries: Vec<LogEntry>) -> Result<()> {
        let Some((last_log_index, last_log_term)) =
            entries.last().map(|entry| (entry.index, entry.term))
        else {
            return Ok(());
        };
        let batch = self
            .config
            .keyspace
            .batch()
            .durability(Some(PersistMode::SyncAll));
        self.log.insert_all(batch, entries).await?;
        self.last_log_index = last_log_index;
        self.last_log_term = last_log_term;
        Ok(())
    }

    fn active_server_count(&self) -> usize {
        self.config
            .init
            .cluster
            .servers
            .iter()
            .filter(|s| !s.readonly_replica)
            .count()
    }

    fn reset_match_index(&mut self) {
        self.match_index.clear();
        for server in &self.config.init.cluster.servers {
            if !server.readonly_replica {
                self.match_index.insert(server.name.clone(), 0);
            }
        }
    }

    fn notify_state_change(&self) {
        let raft = RaftShared {
            current_term: self.current_term,
            commit_index: self.commit_index,
        };
        info!(
            current_term = self.current_term,
            commit_index = self.commit_index,
            "notify state change to replication workers"
        );
        for (id, worker) in self.replicate_workers.iter() {
            if let Err(error) = ractor::cast!(worker, ReplicateMsg::NotifyStateChange(raft)) {
                warn!(
                    %error,
                    peer = id,
                    "notify_state_change failed",
                );
            }
        }
    }
}
