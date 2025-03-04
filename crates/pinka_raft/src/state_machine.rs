use anyhow::{Result, bail};
use ractor::{ActorRef, DerivedActorRef};
use ractor_cluster::RactorMessage;

use super::{ClientResult, LogEntry, RaftMsg, RaftWorker};

#[derive(RactorMessage)]
pub enum StateMachineMsg {
    Apply(LogEntry),
}

#[derive(RactorMessage)]
pub enum RaftAppliedMsg {
    Applied(u64, ClientResult),
}

impl From<RaftAppliedMsg> for RaftMsg {
    fn from(value: RaftAppliedMsg) -> Self {
        match value {
            RaftAppliedMsg::Applied(index, result) => RaftMsg::AppliedLog(index, result),
        }
    }
}

impl From<RaftMsg> for RaftAppliedMsg {
    fn from(value: RaftMsg) -> Self {
        match value {
            RaftMsg::AppliedLog(index, result) => RaftAppliedMsg::Applied(index, result),
            _ => panic!("unsupported RaftAppliedMsg conversion"),
        }
    }
}

pub fn get_raft_applied() -> Result<DerivedActorRef<RaftAppliedMsg>> {
    if let Some(cell) =
        ractor::pg::get_scoped_local_members(&"raft".into(), &RaftWorker::pg_name()).first()
    {
        let worker: ActorRef<RaftMsg> = cell.clone().into();
        return Ok(worker.get_derived());
    }
    bail!("raft service is not available")
}
