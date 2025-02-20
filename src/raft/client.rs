use anyhow::{Result, bail};
use minicbor::{Decode, Encode};
use ractor::{ActorRef, DerivedActorRef, RpcReplyPort};
use ractor_cluster::RactorClusterMessage;

use super::RaftWorker;
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

#[derive(Debug, Encode, Decode)]
pub(crate) enum ClientResult {
    #[n(0)]
    Ok(#[cbor(n(0), with = "minicbor::bytes")] Vec<u8>),
    #[n(1)]
    Err(#[cbor(n(0), with = "minicbor::bytes")] Vec<u8>),
}

impl ClientResult {
    pub(crate) fn ok() -> ClientResult {
        ClientResult::Ok(vec![])
    }
}

impl From<Vec<u8>> for ClientResult {
    fn from(value: Vec<u8>) -> Self {
        ClientResult::Ok(value)
    }
}

pub(crate) fn get_raft_local_client() -> Result<DerivedActorRef<RaftClientMsg>> {
    if let Some(cell) =
        ractor::pg::get_scoped_local_members(&"raft".into(), &RaftWorker::pg_name()).first()
    {
        let worker: ActorRef<RaftMsg> = cell.clone().into();
        return Ok(worker.get_derived());
    }
    bail!("raft service is not available")
}
