pub(crate) use self::rpc::{
    AppendEntriesAsk, AppendEntriesReply, RequestVoteAsk, RequestVoteReply,
};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use ractor_cluster::RactorClusterMessage;
use tracing::info;

use super::Mode;

pub(crate) struct RaftWorker;

#[derive(RactorClusterMessage)]
pub(crate) enum RaftMsg {
    #[rpc]
    AppendEntries(AppendEntriesAsk, RpcReplyPort<AppendEntriesReply>),
    #[rpc]
    RequestVote(RequestVoteAsk, RpcReplyPort<RequestVoteReply>),
}

/// Role played by the worker.
#[derive(Debug)]
pub(crate) enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub(crate) struct RaftState {
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
    voted_for: Option<u32>,

    /// Volatile state. Index of highest log entry known to be committed
    /// (initialized to 0, increases monotonically).
    commit_idx: usize,

    /// Volatile state. Index of highest log entry applied to the state machine
    /// (initialized to 0, increases monotonically).
    last_applied: usize,

    /// Volatile state on leaders. For each worker, index of the next log entry
    /// to send to that worker (initialized to leader's last log index + 1).
    next_index: Vec<usize>,

    /// Volatile state on leaders. For each worker, index of the highest log
    /// entry known to be replicated on server (initialized to 0, increases
    /// monotonically).
    match_index: Vec<usize>,
}

impl RaftWorker {
    pub(crate) fn name() -> Option<String> {
        Some("raft_worker".into())
    }
}

#[ractor::async_trait]
impl Actor for RaftWorker {
    type Msg = RaftMsg;
    type State = RaftState;
    type Arguments = Mode;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut state = RaftState::default();

        if matches!(args, Mode::Bootstrap) {
            info!(target: "spawn", "in bootstrap mode, assume leader's role");
            state.role = RaftRole::Leader;
        }

        Ok(RaftState::default())
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "started");
        Ok(())
    }
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            commit_idx: 0,
            last_applied: 0,
            next_index: vec![],
            match_index: vec![],
        }
    }
}

mod rpc {
    use ractor::BytesConvertable;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub(crate) struct AppendEntriesAsk {
        /// Leader's term
        term: u32,
        /// Leader's pid, so followers can redirect clients
        leader_id: u32,
        /// Index of log entry immediately preceding new ones
        prev_log_index: usize,
        /// Term of prev_log_index entry
        prev_log_term: u32,
        /// Log entries to store (empty for heartbeat; may send more than one for
        /// efficiency)
        entries: Vec<()>,
        /// Leader's commit index
        leader_commit: usize,
    }

    #[derive(Serialize, Deserialize)]
    pub(crate) struct AppendEntriesReply {
        /// Current term, for leader to update itself
        term: u32,
        /// True if follower contained entry matching prev_log_index and
        /// prev_log_term
        success: bool,
    }

    #[derive(Serialize, Deserialize)]
    pub(crate) struct RequestVoteAsk {
        /// Candidate's term
        term: u32,
        /// Candidate's pid requesting vote
        candidate_id: u32,
        /// Index of candidate's last log entry
        last_log_index: usize,
        /// Term of candidate's last log entry
        last_log_term: u32,
    }

    #[derive(Serialize, Deserialize)]
    pub(crate) struct RequestVoteReply {
        /// Current term, for the candidate to update itself
        term: u32,
        /// True means candidate received and granted vote
        vote_granted: bool,
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

    impl_bytes_convertable_for_serde!(AppendEntriesAsk);
    impl_bytes_convertable_for_serde!(AppendEntriesReply);
    impl_bytes_convertable_for_serde!(RequestVoteAsk);
    impl_bytes_convertable_for_serde!(RequestVoteReply);
}
