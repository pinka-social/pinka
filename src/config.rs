use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use fjall::Keyspace;
use secrecy::SecretString;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub(crate) admin: AdminConfig,
    pub(crate) raft: RaftConfig,
    pub(crate) cluster: ClusterConfig,
    pub(crate) database: DatabaseConfig,
    pub(crate) activity_pub: ActivityPubConfig,
    pub(crate) feeds: BTreeMap<String, FeedSlurpConfig>,
}

impl Config {
    pub(super) fn open<P>(path: P) -> Result<Config>
    where
        P: AsRef<Path>,
    {
        let config_text = fs::read_to_string(path)?;
        Ok(toml::from_str(&config_text)?)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AdminConfig {
    pub(crate) password: SecretString,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            password: Uuid::new_v4().to_string().into(),
        }
    }
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
    pub(crate) servers: BTreeMap<String, ServerConfig>,
    pub(crate) reconnect_timeout_ms: u64,
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct ServerConfig {
    pub(crate) hostname: String,
    pub(crate) port: u16,
    pub(crate) readonly_replica: bool,
    pub(crate) server_ca_certs: Vec<PathBuf>,
    pub(crate) server_cert_chain: Vec<PathBuf>,
    pub(crate) server_key: Option<PathBuf>,
    pub(crate) client_ca_certs: Vec<PathBuf>,
    pub(crate) client_cert_chain: Vec<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) http: HttpConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct HttpConfig {
    pub(crate) listen: bool,
    pub(crate) address: String,
    pub(crate) port: u16,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            listen: true,
            address: "[::1]".to_string(),
            port: 8080,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(default)]
pub(crate) struct DatabaseConfig {
    pub(crate) path: PathBuf,
}

#[derive(Clone, Default, Debug, Deserialize)]
pub(crate) struct ActivityPubConfig {
    pub(crate) base_url: String,
    pub(crate) webfinger_at_host: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FeedSlurpConfig {
    /// The user id to attribute to as author.
    pub(crate) uid: String,
    /// The feed URL. Feed can use RSS, Atom, or JSON format.
    pub(crate) feed_url: String,
    /// The base URL to resolve relative URLs in the feed.
    pub(crate) base_url: String,
    /// Only fetch the last N items from the feed if set.
    pub(crate) items: Option<usize>,
    /// Customize the minijinja template used to render the ingested item.
    pub(crate) template: Option<String>,
    /// Only logging the input/output without producing real activity.
    pub(crate) dry_run: Option<bool>,
}

#[derive(Clone)]
pub(crate) struct RuntimeConfig {
    pub(crate) init: Config,
    pub(crate) server_name: String,
    pub(crate) server: ServerConfig,
    pub(crate) keyspace: Keyspace,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 100,
            min_election_ms: 1000,
            max_election_ms: 2000,
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
