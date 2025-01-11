mod config;
mod flags;
mod worker;

use std::process::exit;

use anyhow::Result;
use ractor::Actor;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

use self::config::{
    ClusterConfig, Config, DatabaseConfig, RaftConfig, RuntimeConfig, ServerConfig,
};
use self::flags::Flags;
use self::worker::Supervisor;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let flags = Flags::from_env_or_exit();

    let config = Config {
        raft: RaftConfig {
            heartbeat_ms: 250,
            min_election_ms: 500,
            max_election_ms: 1000,
        },
        cluster: ClusterConfig {
            auth_cookie: "foobar".to_string(),
            use_mtls: true,
            pem_dir: Some("devcerts".into()),
            ca_certs: vec!["ca_cert.pem".into()],
            servers: vec![
                ServerConfig {
                    name: "s1".into(),
                    hostname: "localhost".into(),
                    port: 8001,
                    server_cert_chain: vec!["s1.pem".into()],
                    server_key: Some("s1.key".into()),
                    client_cert_chain: vec!["s1.pem".into()],
                    client_key: Some("s1.key".into()),
                    ..Default::default()
                },
                ServerConfig {
                    name: "s2".into(),
                    hostname: "localhost".into(),
                    port: 8002,
                    server_cert_chain: vec!["s2.pem".into()],
                    server_key: Some("s2.key".into()),
                    client_cert_chain: vec!["s2.pem".into()],
                    client_key: Some("s2.key".into()),
                    ..Default::default()
                },
                ServerConfig {
                    name: "s3".into(),
                    hostname: "localhost".into(),
                    port: 8003,
                    server_cert_chain: vec!["s3.pem".into()],
                    server_key: Some("s3.key".into()),
                    client_cert_chain: vec!["s3.pem".into()],
                    client_key: Some("s3.key".into()),
                    ..Default::default()
                },
            ],
        },
        database: DatabaseConfig {
            path: "devdb".into(),
        },
    };

    if config.cluster.servers.len() < flags.server.unwrap_or_default() {
        eprintln!(
            "invalid server number {}, config only defined {} servers.",
            flags.server.unwrap_or_default(),
            config.cluster.servers.len()
        );
        exit(1);
    }

    let keyspace = fjall::Config::new(
        config
            .database
            .path
            .join(&config.cluster.servers[flags.server.unwrap_or_default()].name),
    )
    .open()?;

    let config = RuntimeConfig {
        init: config,
        keyspace,
    };

    let (supervisor, actor_handle) =
        Actor::spawn(Some("supervisor".into()), Supervisor, (flags, config)).await?;

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received the terminate signal; stopping");
                break;
            }
            _ = sigint.recv() => {
                info!("Received the interrupt signal; stopping");
                break;
            }
        }
    }

    supervisor.stop(None);
    actor_handle.await?;

    Ok(())
}
