use anyhow::{Context, Result};
use minicbor::{Decode, Encode};
use ractor::BytesConvertable;

use super::client::ClientResult;
use super::{LogEntry, LogEntryList, LogEntryValue};

pub(crate) trait RaftSerDe {
    fn to_bytes(&self) -> Result<Vec<u8>>
    where
        Self: Encode<()>,
    {
        let result = minicbor::to_vec(&self).context("unable to serialize payload")?;
        Ok(result)
    }
    fn from_bytes<'b>(bytes: &'b [u8]) -> Result<Self>
    where
        Self: Decode<'b, ()>,
    {
        Ok(minicbor::decode(&bytes).context("unable to deserialize payload")?)
    }
}

pub(super) type PeerId = String;

#[derive(Debug, Default, Encode, Decode)]
pub(super) struct AdvanceCommitIndexMsg {
    #[n(0)]
    pub(super) peer_id: Option<PeerId>,
    #[n(1)]
    pub(super) match_index: u64,
}

#[derive(Debug, Encode, Decode)]
pub(super) struct AppendEntriesAsk {
    /// Leader's term
    #[n(0)]
    pub(super) term: u32,
    /// Leader's id, so followers can redirect clients
    #[n(1)]
    pub(super) leader_id: PeerId,
    /// Index of log entry immediately preceding new ones
    #[n(2)]
    pub(super) prev_log_index: u64,
    /// Term of prev_log_index entry
    #[n(3)]
    pub(super) prev_log_term: u32,
    /// Log entries to store (empty for heartbeat; may send more than one for
    /// efficiency)
    #[n(4)]
    pub(super) entries: Vec<LogEntry>,
    /// Leader's commit index
    #[n(5)]
    pub(super) commit_index: u64,
}

#[derive(Debug, Encode, Decode)]
pub(super) struct AppendEntriesReply {
    /// Current term, for leader to update itself
    #[n(0)]
    pub(super) term: u32,
    /// True if follower contained entry matching prev_log_index and
    /// prev_log_term
    #[n(1)]
    pub(super) success: bool,
}

#[derive(Debug, Encode, Decode)]
pub(super) struct RequestVoteAsk {
    /// Candidate's term
    #[n(0)]
    pub(super) term: u32,
    /// Candidate's unique name
    #[n(1)]
    pub(super) candidate_name: String,
    /// Index of candidate's last log entry
    #[n(2)]
    pub(super) last_log_index: u64,
    /// Term of candidate's last log entry
    #[n(3)]
    pub(super) last_log_term: u32,
}

#[derive(Debug, Encode, Decode)]
pub(super) struct RequestVoteReply {
    /// Current term, for the candidate to update itself
    #[n(0)]
    pub(super) term: u32,
    /// True means candidate received and granted vote
    #[n(1)]
    pub(super) vote_granted: bool,
    /// Follower's unique name
    #[n(2)]
    pub(super) vote_from: String,
}

macro_rules! impl_bytes_convertable_for_serde {
    ($t:ident) => {
        impl RaftSerDe for $t {}
        impl BytesConvertable for $t {
            fn into_bytes(self) -> Vec<u8> {
                self.to_bytes().unwrap()
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
impl_bytes_convertable_for_serde!(LogEntryValue);
impl_bytes_convertable_for_serde!(LogEntryList);
impl_bytes_convertable_for_serde!(ClientResult);
