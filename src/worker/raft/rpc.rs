use anyhow::{Context, Result};
use ractor::BytesConvertable;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{LogEntry, LogEntryList, LogEntryValue};

pub(crate) trait RaftSerDe {
    fn to_bytes(&self) -> Result<Vec<u8>>
    where
        Self: Serialize,
    {
        let header = vec![];
        let payload =
            postcard::to_extend(&Header::V_1, header).context("unable to serialize RPC header")?;
        let result = postcard::to_extend(&self, payload).context("unable to serialize payload")?;
        Ok(result)
    }
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: DeserializeOwned,
    {
        let (header, payload): (Header, _) =
            postcard::take_from_bytes(bytes).context("unable to deserialize RPC header")?;
        if header != Header::V_1 {
            tracing::error!(target: "rpc", ?header, "invalid RPC header version");
        }
        Ok(postcard::from_bytes(&payload).context("unable to deserialize payload")?)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub(super) struct Header {
    version: u32,
}

impl Header {
    const V_1: Header = Header { version: 1 };
}

pub(super) type PeerId = String;

#[derive(Debug, Serialize, Deserialize, Default)]
pub(super) struct AdvanceCommitIndexMsg {
    pub(super) peer_id: Option<PeerId>,
    pub(super) match_index: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct AppendEntriesAsk {
    /// Leader's term
    pub(super) term: u32,
    /// Leader's id, so followers can redirect clients
    pub(super) leader_id: PeerId,
    /// Index of log entry immediately preceding new ones
    pub(super) prev_log_index: usize,
    /// Term of prev_log_index entry
    pub(super) prev_log_term: u32,
    /// Log entries to store (empty for heartbeat; may send more than one for
    /// efficiency)
    pub(super) entries: Vec<LogEntry>,
    /// Leader's commit index
    pub(super) commit_index: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct AppendEntriesReply {
    /// Current term, for leader to update itself
    pub(super) term: u32,
    /// True if follower contained entry matching prev_log_index and
    /// prev_log_term
    pub(super) success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RequestVoteAsk {
    /// Candidate's term
    pub(super) term: u32,
    /// Candidate's unique name
    pub(super) candidate_name: String,
    /// Index of candidate's last log entry
    pub(super) last_log_index: usize,
    /// Term of candidate's last log entry
    pub(super) last_log_term: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RequestVoteReply {
    /// Current term, for the candidate to update itself
    pub(super) term: u32,
    /// True means candidate received and granted vote
    pub(super) vote_granted: bool,
    /// Follower's unique name
    pub(super) vote_from: String,
}

macro_rules! impl_bytes_convertable_for_serde {
    ($t:ty) => {
        impl RaftSerDe for $t {}
        impl BytesConvertable for $t {
            fn into_bytes(self) -> Vec<u8> {
                RaftSerDe::to_bytes(&self).unwrap()
            }

            fn from_bytes(bytes: Vec<u8>) -> Self {
                RaftSerDe::from_bytes(&bytes).unwrap()
            }
        }
    };
}

impl_bytes_convertable_for_serde!(AdvanceCommitIndexMsg);
impl_bytes_convertable_for_serde!(AppendEntriesAsk);
impl_bytes_convertable_for_serde!(AppendEntriesReply);
impl_bytes_convertable_for_serde!(RequestVoteAsk);
impl_bytes_convertable_for_serde!(RequestVoteReply);
impl_bytes_convertable_for_serde!(LogEntry);
impl_bytes_convertable_for_serde!(LogEntryValue);
impl_bytes_convertable_for_serde!(LogEntryList);
