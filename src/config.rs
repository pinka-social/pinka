use serde::Deserialize;

#[derive(Clone, Copy, Default, Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) raft: RaftConfig,
}

#[derive(Clone, Copy, Default, Debug, Deserialize)]
pub(crate) struct RaftConfig {
    pub(crate) cluster_size: usize,
    pub(crate) heartbeat_ms: u64,
    pub(crate) min_election_ms: u64,
    pub(crate) max_election_ms: u64,
}
