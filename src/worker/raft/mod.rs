mod peer;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::ops::Deref;
use std::time::Duration;

use self::peer::{Peer, PeerArgs, PeerMsg};
use self::rpc::{
    AppendEntriesAsk, AppendEntriesReply, PeerId, RequestVoteAsk, RequestVoteReply,
    TryAdvanceCommitIndexMsg,
};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, pg};
use ractor_cluster::RactorClusterMessage;
use rand::Rng;
use tokio::task::AbortHandle;
use tracing::{info, trace, warn};

use crate::Mode;
use crate::config::Config;

pub(crate) struct RaftWorker;

#[derive(RactorClusterMessage)]
pub(crate) enum RaftMsg {
    ElectionTimeout,
    ReceivedVote(PeerId),
    TryAdvanceCommitIndex(TryAdvanceCommitIndexMsg),
    #[rpc]
    AppendEntries(AppendEntriesAsk, RpcReplyPort<AppendEntriesReply>),
    #[rpc]
    RequestVote(RequestVoteAsk, RpcReplyPort<RequestVoteReply>),
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
    /// Role played by the worker.
    role: RaftRole,

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

    /// Volatile state. The size of the current cluster.
    cluster_size: usize,

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
    election_abort_handle: Option<AbortHandle>,

    /// Peers, workaround bug in ractor
    peers: Vec<ActorRef<PeerMsg>>,
}

impl Deref for RaftState {
    type Target = ActorRef<RaftMsg>;

    fn deref(&self) -> &Self::Target {
        &self.myself
    }
}

impl RaftWorker {
    pub(crate) fn name() -> Option<String> {
        Some("raft_worker".into())
    }

    pub(crate) fn pg_name() -> String {
        "raft_worker".into()
    }
}

#[ractor::async_trait]
impl Actor for RaftWorker {
    type Msg = RaftMsg;
    type State = RaftState;
    type Arguments = (Mode, Config);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (mode, config) = args;
        let mut state = RaftState::new(config, myself);

        if matches!(mode, Mode::Bootstrap) {
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

        pg::join(RaftWorker::pg_name(), vec![myself.get_cell()]);
        info!(target: "lifecycle", "joined process group");

        state.spawn_peers().await?;

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
            RequestVote(request, reply) => {
                state.handle_request_vote(request, reply);
            }
            AppendEntries(request, reply) => {
                state.handle_append_entries(request, reply);
            }
            ElectionTimeout => {
                state.start_new_election();
            }
            TryAdvanceCommitIndex(peer_info) => {
                state.try_advance_commit_index(peer_info);
            }
            ReceivedVote(peer_id) => {
                state.received_vote(peer_id);
            }
        }

        Ok(())
    }
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
            cluster_size: 0,
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            votes: BTreeSet::new(),
            election_abort_handle: None,
            peers: vec![],
        }
    }

    async fn spawn_peers(&mut self) -> Result<(), ActorProcessingErr> {
        let members = pg::get_members(&RaftWorker::pg_name());

        self.cluster_size = members.len();
        self.peers.clear();

        for server in members {
            if server.get_id() == self.get_id() {
                continue;
            }
            let peer_arg = PeerArgs {
                config: self.config,
                raft: RaftShared {
                    role: self.role,
                    current_term: self.current_term,
                    commit_index: self.commit_index,
                },
                parent: self.myself.clone(),
                peer: server.into(),
                last_log_index: 0,
            };
            let (peer, _) = Actor::spawn_linked(None, Peer, peer_arg, self.get_cell()).await?;
            self.peers.push(peer);
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
        if self.cluster_size == 1 {
            return true;
        }
        self.votes.len() >= self.cluster_size / 2 + 1
    }

    fn set_election_timer(&mut self) {
        // TODO simplify timer. move timer to a task and only keep a channel to
        // update the timer.
        self.unset_election_timer();

        let duration = rand::thread_rng()
            .gen_range(self.config.raft.min_election_ms..=self.config.raft.max_election_ms);

        // info!("will start election in {} ms", duration);
        self.election_abort_handle = Some(
            self.send_after(Duration::from_millis(duration), || RaftMsg::ElectionTimeout)
                .abort_handle(),
        );
    }

    fn unset_election_timer(&mut self) {
        if let Some(ref abort_handle) = self.election_abort_handle {
            abort_handle.abort();
        }
        self.election_abort_handle = None;
    }

    fn start_new_election(&mut self) {
        if let Some(leader_id) = self.leader_id {
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
        self.voted_for = Some(self.get_id().into());
        self.votes.clear();
        self.votes.insert(self.get_id().into());
        self.set_election_timer();
        self.notify_role_change();

        // if we are the only server, this election is already done
        if self.voted_has_quorum() {
            self.become_leader();
        }
    }

    fn try_advance_commit_index(&mut self, peer_info: TryAdvanceCommitIndexMsg) {
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

    fn handle_request_vote(
        &mut self,
        request: RequestVoteAsk,
        reply: RpcReplyPort<RequestVoteReply>,
    ) {
        info!(
            target: "raft",
            from = %request.candidate_id,
            myself = %self.get_id(),
            current_term = self.current_term,
            "received request for vote"
        );
        // TODO verify log completeness
        // TODO ignore distrubing request_vote
        if request.term > self.current_term {
            self.step_down(request.term);
        }
        if request.term == self.current_term {
            info!(
                target: "raft",
                myself = %self.get_id(),
                peer = %request.candidate_id,
                current_term = self.current_term,
                "voted"
            );
            self.step_down(self.current_term);
            self.set_election_timer();
            self.voted_for = Some(request.candidate_id);
            // TODO persist votes
        }

        let response = RequestVoteReply {
            term: self.current_term,
            vote_granted: self.voted_for == Some(request.candidate_id),
        };
        if let Err(ref err) = reply.send(response) {
            warn!(
                error = err as &dyn Error,
                peer = %request.candidate_id,
                "sending request vote reply failed"
            );
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
            self.leader_id = Some(request.leader_id);
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

    fn received_vote(&mut self, peer_id: PeerId) {
        if !matches!(self.role, RaftRole::Candidate) {
            warn!(
                target: "raft",
                peer = %peer_id,
                myself = %self.get_id(),
                "received vote but not a candidate"
            );
            return;
        }
        self.votes.insert(peer_id);
        if self.voted_has_quorum() {
            info!(target: "raft", "received quorum, becoming leader");
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        assert!(matches!(self.role, RaftRole::Candidate));
        self.role = RaftRole::Leader;
        self.unset_election_timer();
        self.reset_match_index();
        self.notify_role_change();
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

        if was_leader || self.election_abort_handle.is_none() {
            self.set_election_timer();
        }

        self.notify_role_change();
    }

    fn reset_match_index(&mut self) {
        for index in self.match_index.values_mut() {
            *index = 0;
        }
    }

    fn notify_role_change(&self) {
        let raft = RaftShared {
            role: self.role,
            current_term: self.current_term,
            commit_index: self.commit_index,
        };
        for peer in &self.peers {
            if let Err(ref err) = peer.cast(PeerMsg::NotifyRoleChange(raft)) {
                warn!(
                    error = err as &dyn Error,
                    peer = peer.get_id().to_string(),
                    "notify_role_change failed",
                );
            }
        }
    }

    fn notify_state_change(&self) {
        let raft = RaftShared {
            role: self.role,
            current_term: self.current_term,
            commit_index: self.commit_index,
        };
        for peer in &self.peers {
            if let Err(ref err) = peer.cast(PeerMsg::NotifyStateChange(raft)) {
                warn!(
                    error = err as &dyn Error,
                    peer = peer.get_id().to_string(),
                    "notify_state_change failed",
                );
            }
        }
    }
}

mod rpc {
    use std::fmt::Display;

    use ractor::{ActorId, BytesConvertable};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Copy, Serialize, Deserialize)]
    pub(super) struct PeerId {
        pub(super) node_id: u64,
        pub(super) pid: u64,
    }

    impl Display for PeerId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}.{}", self.node_id, self.pid)
        }
    }

    impl From<ActorId> for PeerId {
        fn from(value: ActorId) -> Self {
            match value {
                ActorId::Local(pid) => PeerId { node_id: 0, pid },
                ActorId::Remote { node_id, pid } => PeerId { node_id, pid },
            }
        }
    }

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
        /// Candidate's id requesting vote
        pub(super) candidate_id: PeerId,
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

    impl_bytes_convertable_for_serde!(PeerId);
    impl_bytes_convertable_for_serde!(TryAdvanceCommitIndexMsg);
    impl_bytes_convertable_for_serde!(AppendEntriesAsk);
    impl_bytes_convertable_for_serde!(AppendEntriesReply);
    impl_bytes_convertable_for_serde!(RequestVoteAsk);
    impl_bytes_convertable_for_serde!(RequestVoteReply);
}
