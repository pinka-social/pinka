use anyhow::{Context, Result};
use ractor::{ActorRef, DerivedActorRef, RpcReplyPort};
use ractor_cluster::RactorClusterMessage;

use super::{LogEntryValue, RaftMsg};

#[derive(RactorClusterMessage)]
pub(crate) enum RaftClientMsg {
    // TODO: add status code
    #[rpc]
    ClientRequest(LogEntryValue, RpcReplyPort<bool>),
}

impl From<RaftClientMsg> for RaftMsg {
    fn from(value: RaftClientMsg) -> Self {
        match value {
            RaftClientMsg::ClientRequest(value, reply) => RaftMsg::ClientRequest(value, reply),
        }
    }
}

impl From<RaftMsg> for RaftClientMsg {
    fn from(value: RaftMsg) -> Self {
        match value {
            RaftMsg::ClientRequest(value, reply) => RaftClientMsg::ClientRequest(value, reply),
            _ => panic!("unsupported RaftClientMsg conversion"),
        }
    }
}

pub(crate) fn get_raft_client(name: &str) -> Result<DerivedActorRef<RaftClientMsg>> {
    let raft_worker: ActorRef<RaftMsg> =
        ActorRef::where_is(name.into()).context("raft_worker is not running")?;
    Ok(raft_worker.get_derived())
}
