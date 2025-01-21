mod config;
mod flags;
mod repl;
mod worker;

use std::fs::File;
use std::process::exit;

use anyhow::Result;
use fd_lock::RwLock;
use fjall::{KvSeparationOptions, PartitionCreateOptions};
use ractor::Actor;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

use self::config::{
    ClusterConfig, Config, DatabaseConfig, ManholeConfig, RaftConfig, ReplConfig, RuntimeConfig,
    ServerConfig,
};
use self::flags::{Dump, Pinka, PinkaCmd, RaftCmd, Serve};
use self::worker::Supervisor;
use self::worker::raft::{LogEntry, RaftSerDe};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let flags = Pinka::from_env_or_exit();

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
                ServerConfig {
                    name: "s4".into(),
                    hostname: "localhost".into(),
                    port: 8004,
                    observer: true,
                    server_cert_chain: vec!["s4.pem".into()],
                    server_key: Some("s4.key".into()),
                    client_cert_chain: vec!["s4.pem".into()],
                    client_key: Some("s4.key".into()),
                    ..Default::default()
                },
            ],
            manholes: vec![
                ManholeConfig {
                    server_name: "s1".into(),
                    auth_cookie: "".into(),
                    port: 9001,
                    enable: true,
                },
                ManholeConfig {
                    server_name: "s2".into(),
                    auth_cookie: "".into(),
                    port: 9002,
                    enable: true,
                },
                ManholeConfig {
                    server_name: "s3".into(),
                    auth_cookie: "".into(),
                    port: 9003,
                    enable: true,
                },
                ManholeConfig {
                    server_name: "s4".into(),
                    auth_cookie: "".into(),
                    port: 9004,
                    enable: true,
                },
            ],
            reconnect_timeout_ms: 10_000,
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

    let server = config.cluster.servers[flags.server.unwrap_or_default()].clone();

    if matches!(flags.subcommand, PinkaCmd::Repl(_)) {
        let repl_config = ReplConfig {
            init: config,
            server,
        };
        repl::run(repl_config).await?;
        return Ok(());
    }

    let keyspace_name = config.database.path.join(&server.name);
    let mut keyspace_lock = RwLock::new(File::create(keyspace_name.join("lock"))?);
    let write_guard = match keyspace_lock.try_write() {
        Ok(guard) => guard,
        Err(_) => {
            tracing::error!(
                "Database '{}' cannot be accessed because it is locked by another process.",
                keyspace_name.display()
            );
            tracing::error!(
                "If you are certain no other process is using this database, delete '{}' to remove the lock file.",
                keyspace_name.join("lock").display()
            );
            exit(1);
        }
    };

    let keyspace = fjall::Config::new(config.database.path.join(&server.name)).open()?;

    let config = RuntimeConfig {
        init: config,
        server,
        keyspace,
    };

    match flags.subcommand {
        PinkaCmd::Serve(flags) => serve(config, flags).await?,
        PinkaCmd::Raft(raft) => match raft.subcommand {
            RaftCmd::Dump(flags) => {
                raft_dump(config, flags)?;
            }
        },
        _ => {}
    }

    drop(write_guard);

    Ok(())
}

async fn serve(config: RuntimeConfig, flags: Serve) -> Result<()> {
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

fn raft_dump(config: RuntimeConfig, flags: Dump) -> Result<()> {
    let log = config.keyspace.open_partition(
        "raft_log",
        PartitionCreateOptions::default()
            .compression(fjall::CompressionType::Lz4)
            .manual_journal_persist(true)
            .with_kv_separation(KvSeparationOptions::default()),
    )?;
    info!("Dump raft log entries");
    info!("=====================");
    for entry in log.iter().skip(flags.from.unwrap_or_default()) {
        let (key, value) = entry.unwrap();
        let value = LogEntry::from_bytes(&value)?;
        info!(
            "key = {}, value = {:?}",
            usize::from_be_bytes(key.as_ref().try_into().unwrap()),
            value
        );
    }
    Ok(())
}
