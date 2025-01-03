use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorClusterMessage;
use tracing::info;

use super::Mode;

pub(crate) struct RaftWorker;

#[derive(RactorClusterMessage)]
pub(crate) enum RaftMsg {}

#[derive(Debug)]
pub(crate) struct RaftState {
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
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
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
            current_term: 0,
            voted_for: None,
            commit_idx: 0,
            last_applied: 0,
            next_index: vec![],
            match_index: vec![],
        }
    }
}
