use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LogEntry {
    pub(crate) index: u64,
    pub(crate) term: u32,
    pub(crate) value: LogEntryValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum LogEntryValue {
    /// New leader has been elected
    NewTermStarted,
    /// Raft cluster wide message
    ClusterMessage(String),
    /// Raw bytes for application payload
    Bytes(ByteBuf),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LogEntryList {
    pub(crate) items: Vec<LogEntry>,
}
