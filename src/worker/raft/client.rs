use anyhow::{Context, Result, bail};
use ractor::{ActorRef, DerivedActorRef, RpcReplyPort};
use ractor_cluster::RactorClusterMessage;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::worker::raft::RaftWorker;

use super::{LogEntryValue, RaftMsg};

#[derive(RactorClusterMessage)]
pub(crate) enum RaftClientMsg {
    // TODO: add status code
    #[rpc]
    ClientRequest(LogEntryValue, RpcReplyPort<ClientResult>),
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ClientResult {
    Ok(ByteBuf),
    Err(ByteBuf),
}

impl ClientResult {
    pub(crate) fn ok() -> ClientResult {
        ClientResult::Ok(ByteBuf::new())
    }
    pub(crate) fn is_ok(&self) -> bool {
        matches!(self, ClientResult::Ok(_))
    }
}

impl From<ByteBuf> for ClientResult {
    fn from(value: ByteBuf) -> Self {
        ClientResult::Ok(value)
    }
}

pub(crate) fn get_raft_client(name: &str) -> Result<DerivedActorRef<RaftClientMsg>> {
    let raft_worker: ActorRef<RaftMsg> =
        ActorRef::where_is(name.into()).context("raft_worker is not running")?;
    Ok(raft_worker.get_derived())
}

pub(crate) fn get_raft_local_client() -> Result<DerivedActorRef<RaftClientMsg>> {
    for cell in ractor::pg::get_scoped_local_members(&"raft".into(), &RaftWorker::pg_name()) {
        let worker: ActorRef<RaftMsg> = cell.into();
        return Ok(worker.get_derived());
    }
    bail!("no local raft_worker")
}
