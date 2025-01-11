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
    pub(crate) pem_dir: Option<PathBuf>,
    pub(crate) ca_certs: Vec<PathBuf>,
    pub(crate) servers: Vec<ServerConfig>,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct ServerConfig {
    pub(crate) name: String,
    pub(crate) hostname: String,
    pub(crate) port: u16,
    pub(crate) server_ca_certs: Vec<PathBuf>,
    pub(crate) server_cert_chain: Vec<PathBuf>,
    pub(crate) server_key: Option<PathBuf>,
    pub(crate) client_ca_certs: Vec<PathBuf>,
    pub(crate) client_cert_chain: Vec<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
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
