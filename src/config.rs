use std::fmt::Debug;
use std::path::PathBuf;

use fjall::Keyspace;
use serde::Deserialize;

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub(crate) raft: RaftConfig,
    pub(crate) cluster: ClusterConfig,
    pub(crate) database: DatabaseConfig,
    pub(crate) activity_pub: ActivityPubConfig,
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
    pub(crate) manholes: Vec<ManholeConfig>,
    pub(crate) reconnect_timeout_ms: u64,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct ServerConfig {
    pub(crate) name: String,
    pub(crate) hostname: String,
    pub(crate) port: u16,
    pub(crate) observer: bool,
    pub(crate) server_ca_certs: Vec<PathBuf>,
    pub(crate) server_cert_chain: Vec<PathBuf>,
    pub(crate) server_key: Option<PathBuf>,
    pub(crate) client_ca_certs: Vec<PathBuf>,
    pub(crate) client_cert_chain: Vec<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct DatabaseConfig {
    pub(crate) path: PathBuf,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct ActivityPubConfig {
    pub(crate) base_url: String,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct ManholeConfig {
    pub(crate) server_name: String,
    pub(crate) auth_cookie: String,
    pub(crate) port: u16,
    pub(crate) enable: bool,
}

#[derive(Clone)]
pub(crate) struct RuntimeConfig {
    pub(crate) init: Config,
    pub(crate) server: ServerConfig,
    pub(crate) keyspace: Keyspace,
}

#[derive(Clone)]
pub(crate) struct ReplConfig {
    pub(crate) init: Config,
    pub(crate) server: ServerConfig,
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

impl Debug for RuntimeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeConfig")
            .field("init", &self.init)
            .field("keyspace", &"[..]")
            .finish()
    }
}
