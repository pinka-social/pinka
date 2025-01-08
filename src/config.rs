use std::path::PathBuf;

use serde::Deserialize;

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub(crate) raft: RaftConfig,
    pub(crate) cluster: ClusterConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct RaftConfig {
    pub(crate) heartbeat_ms: u64,
    pub(crate) min_election_ms: u64,
    pub(crate) max_election_ms: u64,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct ClusterConfig {
    pub(crate) auth_cookie: String,
    pub(crate) use_mtls: bool,
    pub(crate) cert_dir: Option<PathBuf>,
    pub(crate) disable_system_root_store: bool,
    pub(crate) servers: Vec<ServerConfig>,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct ServerConfig {
    pub(crate) name: String,
    pub(crate) hostname: String,
    pub(crate) port: u16,
    pub(crate) ca_cert_names: Vec<PathBuf>,
    pub(crate) server_cert_name: Option<PathBuf>,
    pub(crate) server_priv_name: Option<PathBuf>,
    pub(crate) client_sert_name: Option<PathBuf>,
    pub(crate) client_priv_name: Option<PathBuf>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 100,
            min_election_ms: 150,
            max_election_ms: 300,
        }
    }
}
