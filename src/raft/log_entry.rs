use minicbor::{Decode, Encode};

use super::rpc::RaftSerDe;

#[derive(Debug, Encode, Decode)]
pub(crate) struct LogEntry {
    #[n(0)]
    pub(crate) index: u64,
    #[n(1)]
    pub(crate) term: u32,
    #[n(2)]
    pub(crate) value: LogEntryValue,
}

#[derive(Debug, Encode, Decode)]
pub(crate) enum LogEntryValue {
    /// New leader has been elected
    #[n(0)]
    NewTermStarted,
    /// Raft cluster wide message
    #[n(1)]
    ClusterMessage(#[n(0)] String),
    /// Raw bytes for application payload
    #[n(2)]
    Command(#[cbor(n(0), with = "minicbor::bytes")] Vec<u8>),
}

#[derive(Debug, Encode, Decode)]
pub(crate) struct LogEntryList {
    #[n(0)]
    pub(crate) items: Vec<LogEntry>,
}

impl RaftSerDe for LogEntry {}
