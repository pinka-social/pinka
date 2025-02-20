use minicbor::{Decode, Encode};

use super::PeerId;
use super::rpc::RaftSerDe;

#[derive(Debug, Default, Clone, Encode, Decode)]
pub(super) struct RaftSaved {
    /// Latest term this worker has seen (initialized to 0 on first boot,
    /// increases monotonically).
    ///
    /// Updated on stable storage before responding to RPCs.
    #[n(0)]
    pub(super) current_term: u32,

    /// CandidateId that received vote in current term (or None if none).
    ///
    /// Updated on stable storage before responding to RPCs.
    #[n(1)]
    pub(super) voted_for: Option<PeerId>,

    /// Last applied log entry index
    #[n(2)]
    pub(super) last_applied: u64,
}

impl RaftSerDe for RaftSaved {}
