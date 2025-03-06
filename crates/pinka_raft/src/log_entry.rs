use std::fmt::Debug;
use std::ops::RangeBounds;

use anyhow::{Context, Error, Result};
use blocking::unblock;
use fjall::{Batch, PartitionHandle};
use minicbor::{Decode, Encode};

use super::rpc::RaftSerDe;

#[derive(Debug, Encode, Decode)]
pub struct LogEntry {
    #[n(0)]
    pub index: u64,
    #[n(1)]
    pub term: u32,
    #[n(2)]
    pub value: LogEntryValue,
}

#[derive(Encode, Decode)]
pub enum LogEntryValue {
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

impl Debug for LogEntryValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewTermStarted => write!(f, "NewTermStarted"),
            Self::ClusterMessage(arg0) => f.debug_tuple("ClusterMessage").field(arg0).finish(),
            Self::Command(arg0) => write!(f, "Command([u8; {}])", arg0.len()),
        }
    }
}

impl RaftSerDe for LogEntry {}

#[derive(Clone)]
pub(super) struct RaftLog {
    log: PartitionHandle,
}

impl RaftLog {
    pub(super) fn new(log: PartitionHandle) -> RaftLog {
        RaftLog { log }
    }

    pub(super) async fn insert(&self, mut b: Batch, entry: LogEntry) -> Result<()> {
        let log = self.log.clone();
        unblock(move || {
            let key = entry.index.to_be_bytes();
            let value = entry.to_bytes()?;
            b.insert(&log, key, value);
            b.commit().context("Failed to write log entry")
        })
        .await
        .context("Failed to insert log entry")
    }

    pub(super) async fn insert_all(&self, mut b: Batch, entries: Vec<LogEntry>) -> Result<()> {
        let log = self.log.clone();
        unblock(move || {
            for entry in entries {
                let key = entry.index.to_be_bytes();
                let value = entry.to_bytes()?;
                b.insert(&log, key, value);
            }
            b.commit().context("Failed to write log entries")
        })
        .await
        .context("Failed to insert log entries")
    }

    pub(super) async fn get_last_log_entry(&self) -> Result<Option<LogEntry>> {
        let log = self.log.clone();
        unblock(move || {
            log.last_key_value()?
                .map(|(_, value)| {
                    LogEntry::from_bytes(&value).context("Failed to deserialize log entry")
                })
                .transpose()
        })
        .await
        .context("Failed to get last log entry")
    }

    pub(super) async fn get_log_entry(&self, index: u64) -> Result<LogEntry> {
        let log = self.log.clone();
        unblock(move || {
            log.get(index.to_be_bytes())
                .map_err(Error::new)
                .and_then(|slice| {
                    let value = slice
                        .with_context(|| format!("log entry with index {index} does not exist"))?;
                    LogEntry::from_bytes(&value).context("failed to deserialize log entry")
                })
        })
        .await
        .context("Failed to get log entry")
    }

    pub(super) async fn log_entry_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<LogEntry>> {
        let log = self.log.clone();
        let range = (
            range.start_bound().map(|b| b.to_be_bytes()),
            range.end_bound().map(|b| b.to_be_bytes()),
        );
        unblock(move || {
            log.range(range)
                .map(|r| {
                    r.map_err(Error::new).and_then(|(_, slice)| {
                        LogEntry::from_bytes(&slice).context("failed to deserialize log entry")
                    })
                })
                .collect::<Result<Vec<LogEntry>>>()
        })
        .await
        .context("Failed to get log entries")
    }

    pub(super) async fn remove_last_log_entry(
        &mut self,
        mut b: Batch,
        last_log_index: u64,
    ) -> Result<()> {
        let log = self.log.clone();
        unblock(move || {
            b.remove(&log, last_log_index.to_be_bytes());
            b.commit().context("Failed to remove last log entry")
        })
        .await
        .context("Failed to remove last log entry")
    }
}
