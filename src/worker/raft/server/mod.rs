mod peer;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Duration;

use self::peer::{Peer, PeerArgs, PeerMsg};
use self::rpc::{
    AppendEntriesAsk, AppendEntriesReply, PeerId, RequestVoteAsk, RequestVoteReply,
    TryAdvanceCommitIndexMsg,
};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent, pg};
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
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (mode, config) = args;
        let mut state = RaftState {
            config,
            ..Default::default()
        };

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

        let members = pg::get_members(&RaftWorker::pg_name());

        state.cluster_size = members.len();

        for server in members {
            if server.get_id() == myself.get_id() {
                continue;
            }
            let peer_arg = PeerArgs {
                config: state.config,
                raft: RaftShared {
                    role: state.role,
                    current_term: state.current_term,
                    commit_index: state.commit_index,
                },
                parent_id: myself.get_id().into(),
                peer: server.into(),
                last_log_index: 0,
            };
            let (peer, _) = Actor::spawn_linked(None, Peer, peer_arg, myself.get_cell()).await?;
            state.peers.push(peer);
        }

        if !matches!(state.role, RaftRole::Leader) {
            state.step_down(myself, state.current_term);
        }

        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        use RaftMsg::*;

        match message {
            RequestVote(request, reply) => {
                state.handle_request_vote(myself, request, reply);
            }
            AppendEntries(request, reply) => {
                state.handle_append_entries(myself, request, reply);
            }
            ElectionTimeout => {
                state.start_new_election(myself);
            }
            TryAdvanceCommitIndex(peer_info) => {
                state.try_advance_commit_index(myself, peer_info);
            }
            ReceivedVote(peer_id) => {
                state.received_vote(myself, peer_id);
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
        info!("{:?}", message);
        Ok(())
    }
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            config: Default::default(),
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
}

impl RaftState {
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

    fn set_election_timer(&mut self, myself: ActorRef<RaftMsg>) {
        self.unset_election_timer();

        let duration = rand::thread_rng()
            .gen_range(self.config.raft.min_election_ms..=self.config.raft.max_election_ms);

        // info!("will start election in {} ms", duration);
        self.election_abort_handle = Some(
            myself
                .send_after(Duration::from_millis(duration), || RaftMsg::ElectionTimeout)
                .abort_handle(),
        );
    }

    fn unset_election_timer(&mut self) {
        if let Some(ref abort_handle) = self.election_abort_handle {
            abort_handle.abort();
        }
        self.election_abort_handle = None;
    }

    fn start_new_election(&mut self, myself: ActorRef<RaftMsg>) {
        if let Some(leader_id) = self.leader_id {
            info!(
                target: "raft",
                term = self.current_term + 1,
                prev_leader_id = tracing::field::display(leader_id),
                "running for election (unresponsive leader)"
            );
        } else if matches!(self.role, RaftRole::Candidate) {
            info!(
                target: "raft",
                term = self.current_term + 1,
                prev_term = self.current_term,
                "running for election (previous candidacy timed out)"
            );
        } else {
            info!(
                target: "raft",
                term = self.current_term + 1,
                "running for election"
            );
        }
        self.current_term += 1;
        self.role = RaftRole::Candidate;
        self.leader_id = None;
        self.voted_for = Some(myself.get_id().into());
        self.votes.clear();
        self.votes.insert(myself.get_id().into());
        self.set_election_timer(myself.clone());
        self.notify_role_change(myself.clone());

        // if we are the only server, this election is already done
        if self.voted_has_quorum() {
            self.become_leader(myself);
        }
    }

    fn try_advance_commit_index(
        &mut self,
        myself: ActorRef<RaftMsg>,
        peer_info: TryAdvanceCommitIndexMsg,
    ) {
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
        self.notify_state_change(myself);
    }

    fn handle_request_vote(
        &mut self,
        myself: ActorRef<RaftMsg>,
        request: RequestVoteAsk,
        reply: RpcReplyPort<RequestVoteReply>,
    ) {
        info!(
            target: "raft",
            server = tracing::field::display(myself.get_id()),
            peer = tracing::field::display(request.candidate_id),
            current_term = self.current_term,
            "received request for vote"
        );
        // TODO verify log completeness
        // TODO ignore distrubing request_vote
        if request.term > self.current_term {
            self.step_down(myself.clone(), request.term);
        }
        if request.term == self.current_term {
            info!(
                target: "raft",
                peer = tracing::field::display(request.candidate_id),
                current_term = self.current_term,
                "voted"
            );
            self.step_down(myself.clone(), self.current_term);
            self.set_election_timer(myself.clone());
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
                peer = tracing::field::display(request.candidate_id),
                "sending request vote reply failed"
            );
        }
    }

    fn handle_append_entries(
        &mut self,
        myself: ActorRef<RaftMsg>,
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
                myself.get_id()
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
        self.step_down(myself.clone(), request.term);
        self.set_election_timer(myself.clone());

        if self.leader_id.is_none() {
            self.leader_id = Some(request.leader_id);
            info!(target: "raft", "recognized new leader {} for term {}", request.leader_id, self.current_term);
        } else {
            debug_assert_eq!(self.leader_id, Some(request.leader_id));
        }

        // TODO verify log completness
        response.success = true;

        self.set_election_timer(myself);

        if let Err(ref err) = reply.send(response) {
            warn!(target: "rpc", error = err as &dyn Error, "send response to append_entries failed");
        }
    }

    fn received_vote(&mut self, myself: ActorRef<RaftMsg>, peer_id: PeerId) {
        if !matches!(self.role, RaftRole::Candidate) {
            warn!(
                target: "raft",
                peer = tracing::field::display(peer_id),
                "received vote but not a candidate"
            );
            return;
        }
        self.votes.insert(peer_id);
        if self.voted_has_quorum() {
            info!(target: "raft", "received quorum, becoming leader");
            self.become_leader(myself);
        }
    }

    fn become_leader(&mut self, myself: ActorRef<RaftMsg>) {
        assert!(matches!(self.role, RaftRole::Candidate));
        self.role = RaftRole::Leader;
        self.unset_election_timer();
        self.reset_match_index();
        self.notify_role_change(myself);
        // TODO append no-op log
    }

    fn step_down(&mut self, myself: ActorRef<RaftMsg>, new_term: u32) {
        assert!(self.current_term <= new_term);

        // let was_leader = matches!(self.role, RaftRole::Leader);

        if self.current_term < new_term {
            self.current_term = new_term;
            self.leader_id = None;
            self.voted_for = None;
        }
        self.role = RaftRole::Follower;

        // if was_leader {
        self.set_election_timer(myself.clone());
        // }

        self.notify_role_change(myself);
    }

    fn reset_match_index(&mut self) {
        for index in self.match_index.values_mut() {
            *index = 0;
        }
    }

    fn notify_role_change(&self, myself: ActorRef<RaftMsg>) {
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

    fn notify_state_change(&self, myself: ActorRef<RaftMsg>) {
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
