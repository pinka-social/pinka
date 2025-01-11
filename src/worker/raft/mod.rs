mod replicate;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::ops::Deref;
use std::time::Duration;

use self::replicate::{ReplicateArgs, ReplicateMsg, ReplicateWorker};
use self::rpc::{
    AppendEntriesAsk, AppendEntriesReply, PeerId, RequestVoteAsk, RequestVoteReply,
    TryAdvanceCommitIndexMsg,
};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent, pg};
use ractor_cluster::RactorClusterMessage;
use rand::distributions::{Alphanumeric, DistString};
use rand::{Rng, thread_rng};
use tokio::select;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Instant, sleep};
use tracing::{info, trace, warn};

use crate::config::Config;

pub(crate) struct RaftWorker;

#[derive(RactorClusterMessage)]
pub(crate) enum RaftMsg {
    ElectionTimeout,
    TryAdvanceCommitIndex(TryAdvanceCommitIndexMsg),
    #[rpc]
    AppendEntries(AppendEntriesAsk, RpcReplyPort<AppendEntriesReply>),
    RequestVote(RequestVoteAsk),
    RequestVoteResponse(RequestVoteReply),
}

/// Role played by the worker.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RaftRole {
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

#[derive(Debug)]
pub(crate) struct RaftState {
    /// Actor reference
    myself: ActorRef<RaftMsg>,

    /// Cluster config
    config: Config,

    /// Role played by the worker.
    role: RaftRole,

    /// Latest term this worker has seen (initialized to 0 on first boot,
    /// increases monotonically).
    ///
    /// Updated on stable storage before responding to RPCs.
    current_term: u32,

    /// CandidateId that received vote in current term (or None if none).
    ///
    /// Updated on stable storage before responding to RPCs.
    voted_for: Option<PeerId>,

    /// Last known remote leader
    leader_id: Option<PeerId>,

    /// Volatile state. Index of highest log entry known to be committed
    /// (initialized to 0, increases monotonically).
    commit_index: usize,

    /// Volatile state. Index of highest log entry applied to the state machine
    /// (initialized to 0, increases monotonically).
    last_applied: usize,

    /// Volatile state on leaders. For each peer, index of the next log entry
    /// to send to that peer (initialized to leader's last log index + 1).
    next_index: BTreeMap<PeerId, usize>,

    /// Volatile state on leaders. For each peer, index of the highest log
    /// entry known to be replicated on server (initialized to 0, increases
    /// monotonically).
    match_index: BTreeMap<PeerId, usize>,

    /// Volatile state on candidates. At most onne record for each peer.
    votes: BTreeSet<PeerId>,

    /// Keeps track of outstanding start election timer.
    election_timer: Option<Sender<Duration>>,

    /// Peers, workaround bug in ractor
    replicate_workers: Vec<ActorRef<ReplicateMsg>>,
}

impl Deref for RaftState {
    type Target = ActorRef<RaftMsg>;

    fn deref(&self) -> &Self::Target {
        &self.myself
    }
}

impl RaftWorker {
    pub(crate) fn gen_name() -> String {
        Alphanumeric.sample_string(&mut thread_rng(), 6)
    }

    pub(crate) fn pg_name() -> String {
        "raft_worker".into()
    }
}

impl Actor for RaftWorker {
    type Msg = RaftMsg;
    type State = RaftState;
    type Arguments = (bool, Config);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (bootstrap, config) = args;
        let mut state = RaftState::new(config, myself);

        if bootstrap {
            info!(target: "spawn", "in bootstrap mode, assume leader's role");
            state.role = RaftRole::Leader;
        }

        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "started");

        pg::join_scoped("raft".into(), RaftWorker::pg_name(), vec![
            myself.get_cell(),
        ]);
        pg::monitor_scope("raft".into(), myself.get_cell());
        info!(target: "lifecycle", "joined process group");

        if !matches!(state.role, RaftRole::Leader) {
            state.step_down(state.current_term);
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
                state.handle_request_vote(request);
            }
            AppendEntries(request, reply) => {
                state.handle_append_entries(request, reply);
            }
            ElectionTimeout => {
                state.start_new_election().await?;
            }
            TryAdvanceCommitIndex(peer_info) => {
                state.try_advance_commit_index(peer_info);
            }
            RequestVoteResponse(reply) => {
                state.received_vote(reply).await?;
            }
        }

        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<RaftMsg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorStarted(_) => {}
            SupervisionEvent::ActorTerminated(_, _, _) => {}
            SupervisionEvent::ActorFailed(_, _) => {}
            SupervisionEvent::ProcessGroupChanged(group_change_message) => {
                // match group_change_message {
                //     pg::GroupChangeMessage::Join(scope, group, actors) => {
                //         if scope == "raft" && group == RaftWorker::pg_name() {
                //             for p in actors {
                //                 let peer_arg = PeerArgs {
                //                     config: state.config,
                //                     raft: RaftShared {
                //                         role: state.role,
                //                         current_term: state.current_term,
                //                         commit_index: state.commit_index,
                //                     },
                //                     parent: state.myself.clone(),
                //                     peer: p.into(),
                //                     last_log_index: 0,
                //                 };
                //                 let (peer, _) =
                //                     Actor::spawn_linked(None, Peer, peer_arg, state.get_cell())
                //                         .await?;
                //                 state.peers.push(peer);
                //             }
                //         }
                //     }
                //     pg::GroupChangeMessage::Leave(scope, group, actors) => {
                //         if scope == "raft" && group == RaftWorker::pg_name() {
                //             let removed = actors
                //                 .into_iter()
                //                 .map(|p| p.get_id())
                //                 .collect::<BTreeSet<_>>();
                //             state.peers = state
                //                 .peers
                //                 .iter()
                //                 .filter(|p| removed.contains(&p.get_id()))
                //                 .cloned()
                //                 .collect();
                //         }
                //     }
                // }
            }
            SupervisionEvent::PidLifecycleEvent(pid_lifecycle_event) => {}
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
                    if let Err(ref err) = ractor::cast!(myself, RaftMsg::ElectionTimeout) {
                        warn!(
                            target: "raft",
                            myself = %myself.get_id(),
                            error = err as &dyn Error,
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
    fn new(config: Config, myself: ActorRef<RaftMsg>) -> RaftState {
        Self {
            myself,
            config,
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            votes: BTreeSet::new(),
            election_timer: None,
            replicate_workers: vec![],
        }
    }

    fn peer_id(&self) -> PeerId {
        self.get_name().unwrap()
    }

    async fn spawn_replicate_workers(&mut self) -> Result<(), ActorProcessingErr> {
        assert!(self.replicate_workers.is_empty());

        for server in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            if server.get_id() == self.get_id() {
                continue;
            }
            let args = ReplicateArgs {
                config: self.config.clone(),
                raft: RaftShared {
                    current_term: self.current_term,
                    commit_index: self.commit_index,
                },
                parent: self.myself.clone(),
                peer: server.into(),
                last_log_index: 0,
            };
            let (peer, _) =
                Actor::spawn_linked(None, ReplicateWorker, args, self.get_cell()).await?;
            self.replicate_workers.push(peer);
        }
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
        let cluster_size = self.config.cluster.servers.len();
        if cluster_size == 1 {
            return true;
        }
        self.votes.len() >= cluster_size / 2 + 1
    }

    fn set_election_timer(&mut self) {
        let duration = Duration::from_millis(
            rand::thread_rng()
                .gen_range(self.config.raft.min_election_ms..=self.config.raft.max_election_ms),
        );

        match self.election_timer {
            Some(ref timer) if !timer.is_closed() => {
                let _ = timer.try_send(duration);
            }
            _ => self.election_timer = Some(election_timer(self.myself.clone(), duration)),
        }
    }

    fn unset_election_timer(&mut self) {
        self.election_timer = None;
    }

    async fn start_new_election(&mut self) -> Result<(), ActorProcessingErr> {
        if let Some(ref leader_id) = self.leader_id {
            info!(
                target: "raft",
                myself = %self.get_id(),
                term = self.current_term + 1,
                prev_leader_id = %leader_id,
                "running for election (unresponsive leader)"
            );
        } else if matches!(self.role, RaftRole::Candidate) {
            info!(
                target: "raft",
                myself = %self.get_id(),
                term = self.current_term + 1,
                prev_term = self.current_term,
                "running for election (previous candidacy timed out)"
            );
        } else {
            info!(
                target: "raft",
                myself = %self.get_id(),
                term = self.current_term + 1,
                "running for election"
            );
        }
        self.current_term += 1;
        self.role = RaftRole::Candidate;
        self.leader_id = None;
        self.voted_for = Some(self.peer_id());
        self.votes.clear();
        self.votes.insert(self.peer_id());
        self.set_election_timer();

        // if we are the only server, this election is already done
        if self.voted_has_quorum() {
            self.become_leader().await?;
            return Ok(());
        }

        self.request_vote();

        Ok(())
    }

    fn request_vote(&mut self) {
        for peer in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            if peer.get_id() == self.get_id() {
                continue;
            }

            let peer: ActorRef<RaftMsg> = peer.into();

            let request = RequestVoteAsk {
                term: self.current_term,
                candidate_name: self.get_name().unwrap(),
                last_log_index: 0,
                last_log_term: 0,
            };

            info!(
                target: "raft",
                from = %self.peer_id(),
                to = %peer.get_name().unwrap(),
                "request_vote"
            );

            let call_result = ractor::cast!(peer, RaftMsg::RequestVote(request));
            if let Err(ref err) = call_result {
                warn!(target: "rpc", error = err as &dyn Error, "request_vote failed");
            }
        }
    }

    fn try_advance_commit_index(&mut self, peer_info: TryAdvanceCommitIndexMsg) {
        if thread_rng().gen_bool(1.0 / 10000.0) {
            panic!("simulated crash");
        }
        if !matches!(self.role, RaftRole::Leader) {
            warn!(target: "raft", "try_advance_commit_index called as {:?}", self.role);
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
        // TODO
        self.commit_index = new_commit_index;
        trace!(target: "raft", "new commit_index: {}", self.commit_index);
        self.notify_state_change();
    }

    fn handle_request_vote(&mut self, request: RequestVoteAsk) {
        info!(
            target: "raft",
            from = %request.candidate_name,
            myself = %self.peer_id(),
            current_term = self.current_term,
            "received request for vote"
        );
        // TODO verify log completeness
        // TODO ignore distrubing request_vote
        if request.term > self.current_term {
            self.step_down(request.term);
        }
        if request.term == self.current_term && self.voted_for.is_none() {
            info!(
                target: "raft",
                myself = %self.peer_id(),
                peer = %request.candidate_name,
                current_term = self.current_term,
                "voted"
            );
            self.step_down(self.current_term);
            self.set_election_timer();
            self.voted_for = Some(request.candidate_name.clone());
            // TODO persist votes
        }

        for server in pg::get_scoped_members(&"raft".into(), &RaftWorker::pg_name()) {
            if server.get_id() == self.get_id() {
                continue;
            }
            if server.get_name().as_ref() == Some(&request.candidate_name) {
                let response = RequestVoteReply {
                    term: self.current_term,
                    vote_granted: self.voted_for == Some(request.candidate_name.clone()),
                    vote_from: self.peer_id(),
                };
                let server: ActorRef<RaftMsg> = server.into();
                if let Err(ref err) = ractor::cast!(server, RaftMsg::RequestVoteResponse(response))
                {
                    warn!(
                        error = err as &dyn Error,
                        peer = %request.candidate_name,
                        "sending request vote reply failed"
                    );
                }
                break;
            }
        }
    }

    fn handle_append_entries(
        &mut self,
        request: AppendEntriesAsk,
        reply: RpcReplyPort<AppendEntriesReply>,
    ) {
        let mut response = AppendEntriesReply {
            term: self.current_term,
            success: false,
        };
        if request.term < self.current_term {
            trace!(
                target: "raft",
                "received stale request from server {} in term {} (this server's term was {}",
                request.leader_id,
                request.term,
                self.get_id()
            );
            return;
        }
        if request.term > self.current_term {
            info!(
                target: "raft",
                "received append_entries request from server {} in term {} (this server's term was {})",
                request.leader_id,
                request.term,
                self.current_term,
            );
            response.term = request.term;
        }
        self.step_down(request.term);
        self.set_election_timer();

        if self.leader_id.is_none() {
            self.leader_id = Some(request.leader_id.clone());
            info!(target: "raft", "recognized new leader {} for term {}", request.leader_id, self.current_term);
        } else {
            debug_assert_eq!(self.leader_id, Some(request.leader_id));
        }

        // TODO verify log completness
        response.success = true;

        self.set_election_timer();

        if let Err(ref err) = reply.send(response) {
            warn!(target: "rpc", error = err as &dyn Error, "send response to append_entries failed");
        }
    }

    async fn received_vote(&mut self, reply: RequestVoteReply) -> Result<(), ActorProcessingErr> {
        if reply.term > self.current_term {
            info!(
                target: "raft",
                peer = %reply.vote_from,
                peer_term = reply.term,
                current_term = self.current_term,
                "received request_vote response",
            );
            self.step_down(reply.term);
        } else {
            if reply.vote_granted {
                info!(
                    target: "raft",
                    myself = %self.peer_id(),
                    peer = %reply.vote_from,
                    peer_term = reply.term,
                    current_term = self.current_term,
                    "got one vote",
                );
                if !matches!(self.role, RaftRole::Candidate) {
                    if matches!(self.role, RaftRole::Follower) {
                        warn!(
                            target: "raft",
                            peer = %reply.vote_from,
                            myself = %self.get_id(),
                            role = ?self.role,
                            "received vote but not a candidate"
                        );
                    }
                    return Ok(());
                }
                self.votes.insert(reply.vote_from);
                if self.voted_has_quorum() {
                    self.become_leader().await?;
                }
            } else {
                info!(
                    target: "raft",
                    peer = %reply.vote_from,
                    peer_term = reply.term,
                    current_term = self.current_term,
                    "vote was denied",
                );
            }
        }
        Ok(())
    }

    async fn become_leader(&mut self) -> Result<(), ActorProcessingErr> {
        assert!(matches!(self.role, RaftRole::Candidate));
        info!(target: "raft", "received quorum, becoming leader");
        self.role = RaftRole::Leader;
        self.unset_election_timer();
        self.reset_match_index();
        self.spawn_replicate_workers().await?;
        Ok(())
        // TODO append no-op log
    }

    fn step_down(&mut self, new_term: u32) {
        assert!(self.current_term <= new_term);

        let was_leader = matches!(self.role, RaftRole::Leader);

        if self.current_term < new_term {
            self.current_term = new_term;
            self.leader_id = None;
            self.voted_for = None;
        }
        self.role = RaftRole::Follower;
        self.stop_children(None);
        self.replicate_workers.clear();

        if was_leader || self.election_timer.is_none() {
            warn!("stepping down");
            self.set_election_timer();
        }
    }

    fn reset_match_index(&mut self) {
        for index in self.match_index.values_mut() {
            *index = 0;
        }
    }

    fn notify_state_change(&self) {
        let raft = RaftShared {
            current_term: self.current_term,
            commit_index: self.commit_index,
        };
        for worker in &self.replicate_workers {
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

mod rpc {
    use ractor::BytesConvertable;
    use serde::{Deserialize, Serialize};

    pub(super) type PeerId = String;

    #[derive(Serialize, Deserialize, Default)]
    pub(super) struct TryAdvanceCommitIndexMsg {
        pub(super) peer_id: Option<PeerId>,
        pub(super) match_index: usize,
    }

    #[derive(Serialize, Deserialize)]
    pub(super) struct AppendEntriesAsk {
        /// Leader's term
        pub(super) term: u32,
        /// Leader's id, so followers can redirect clients
        pub(super) leader_id: PeerId,
        /// Index of log entry immediately preceding new ones
        pub(super) prev_log_index: usize,
        /// Term of prev_log_index entry
        pub(super) prev_log_term: u32,
        /// Log entries to store (empty for heartbeat; may send more than one for
        /// efficiency)
        pub(super) entries: Vec<()>,
        /// Leader's commit index
        pub(super) commit_index: usize,
    }

    #[derive(Serialize, Deserialize)]
    pub(super) struct AppendEntriesReply {
        /// Current term, for leader to update itself
        pub(super) term: u32,
        /// True if follower contained entry matching prev_log_index and
        /// prev_log_term
        pub(super) success: bool,
    }

    #[derive(Serialize, Deserialize)]
    pub(super) struct RequestVoteAsk {
        /// Candidate's term
        pub(super) term: u32,
        /// Candidate's unique name
        pub(super) candidate_name: String,
        /// Index of candidate's last log entry
        pub(super) last_log_index: usize,
        /// Term of candidate's last log entry
        pub(super) last_log_term: u32,
    }

    #[derive(Serialize, Deserialize)]
    pub(super) struct RequestVoteReply {
        /// Current term, for the candidate to update itself
        pub(super) term: u32,
        /// True means candidate received and granted vote
        pub(super) vote_granted: bool,
        /// Follower's unique name
        pub(super) vote_from: String,
    }

    macro_rules! impl_bytes_convertable_for_serde {
        ($t:ty) => {
            impl BytesConvertable for $t {
                fn into_bytes(self) -> Vec<u8> {
                    postcard::to_stdvec(&self).expect(stringify!(unable to serialize $t))
                }

                fn from_bytes(bytes: Vec<u8>) -> Self {
                    postcard::from_bytes(&bytes).expect(stringify!(unable to deserialize $t))
                }
            }
        };
    }

    impl_bytes_convertable_for_serde!(TryAdvanceCommitIndexMsg);
    impl_bytes_convertable_for_serde!(AppendEntriesAsk);
    impl_bytes_convertable_for_serde!(AppendEntriesReply);
    impl_bytes_convertable_for_serde!(RequestVoteAsk);
    impl_bytes_convertable_for_serde!(RequestVoteReply);
}
