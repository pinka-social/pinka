mod config;
mod flags;
mod worker;

use std::process::exit;

use anyhow::Result;
use ractor::Actor;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

use self::config::{ClusterConfig, Config, RaftConfig, ServerConfig};
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
            use_mtls: false,
            cert_dir: None,
            disable_system_root_store: false,
            servers: vec![
                ServerConfig {
                    name: "s1".into(),
                    hostname: "localhost".into(),
                    port: 8001,
                    ..Default::default()
                },
                ServerConfig {
                    name: "s2".into(),
                    hostname: "localhost".into(),
                    port: 8002,
                    ..Default::default()
                },
                ServerConfig {
                    name: "s3".into(),
                    hostname: "localhost".into(),
                    port: 8003,
                    ..Default::default()
                },
            ],
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
