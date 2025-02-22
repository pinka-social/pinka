use std::ops::RangeBounds;

use anyhow::{Context, Error, Result};
use fjall::{Batch, PartitionHandle};
use minicbor::{Decode, Encode};
use tokio::task::spawn_blocking;

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
        spawn_blocking(move || {
            let key = entry.index.to_be_bytes();
            let value = entry.to_bytes()?;
            b.insert(&log, key, value);
            b.commit().context("Failed to write log entry")
        })
        .await
        .context("Failed to insert log entry")?
    }

    pub(super) async fn insert_all(&self, mut b: Batch, entries: Vec<LogEntry>) -> Result<()> {
        let log = self.log.clone();
        spawn_blocking(move || {
            for entry in entries {
                let key = entry.index.to_be_bytes();
                let value = entry.to_bytes()?;
                b.insert(&log, key, value);
            }
            b.commit().context("Failed to write log entries")
        })
        .await
        .context("Failed to insert log entries")?
    }

    pub(super) async fn get_last_log_entry(&self) -> Result<Option<LogEntry>> {
        let log = self.log.clone();
        spawn_blocking(move || {
            log.last_key_value()?
                .map(|(_, value)| {
                    LogEntry::from_bytes(&value).context("Failed to deserialize log entry")
                })
                .transpose()
        })
        .await
        .context("Failed to get last log entry")?
    }

    pub(super) async fn get_log_entry(&self, index: u64) -> Result<LogEntry> {
        let log = self.log.clone();
        spawn_blocking(move || {
            log.get(index.to_be_bytes())
                .map_err(Error::new)
                .and_then(|slice| {
                    let value = slice
                        .with_context(|| format!("log entry with index {index} does not exist"))?;
                    LogEntry::from_bytes(&value).context("failed to deserialize log entry")
                })
        })
        .await
        .context("Failed to get log entry")?
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
        spawn_blocking(move || {
            log.range(range)
                .map(|r| {
                    r.map_err(Error::new).and_then(|(_, slice)| {
                        LogEntry::from_bytes(&slice).context("failed to deserialize log entry")
                    })
                })
                .collect()
        })
        .await
        .context("Failed to get log entries")?
    }

    pub(super) async fn remove_last_log_entry(
        &mut self,
        mut b: Batch,
        last_log_index: u64,
    ) -> Result<()> {
        let log = self.log.clone();
        spawn_blocking(move || {
            b.remove(&log, last_log_index.to_be_bytes());
            b.commit().context("Failed to remove last log entry")
        })
        .await
        .context("Failed to remove last log entry")?
    }
}
