mod activity_pub;
mod cluster;
mod config;
mod feed_slurp;
mod flags;
mod http;
mod raft;
mod supervisor;

use std::fs::{self, File};
use std::process::exit;

use anyhow::Result;
use fd_lock::RwLock;
use fjall::PartitionCreateOptions;
use ractor::Actor;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info};

use self::config::{ActivityPubConfig, Config, FeedSlurpConfig, RuntimeConfig};
use self::flags::{Dump, Pinka, PinkaCmd, RaftCmd, Serve};
use self::raft::{LogEntry, RaftSerDe};
use self::supervisor::Supervisor;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let flags = Pinka::from_env_or_exit();

    let config_text = fs::read_to_string(flags.config)?;
    let config: Config = toml::from_str(&config_text)?;

    if config.cluster.servers.len() < flags.server.unwrap_or_default() {
        eprintln!(
            "invalid server number {}, config only defined {} servers.",
            flags.server.unwrap_or_default(),
            config.cluster.servers.len()
        );
        exit(1);
    }

    let server = config.cluster.servers[flags.server.unwrap_or_default()].clone();

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

    let keyspace = fjall::Config::new(config.database.path.join(&server.name))
        .manual_journal_persist(true)
        .open()?;

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
    let (supervisor, actor_handle) = Actor::spawn(
        Some("supervisor".into()),
        Supervisor,
        (flags, config.clone()),
    )
    .await?;

    let http = http::serve(&config);
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    tokio::select! {
        _ = http => {
            error!("HTTP server crashed");
        }
        _ = sigterm.recv() => {
            info!("Received the terminate signal; stopping");
        }
        _ = sigint.recv() => {
            info!("Received the interrupt signal; stopping");
        }
    }

    supervisor.stop(None);
    actor_handle.await?;

    Ok(())
}

fn raft_dump(config: RuntimeConfig, flags: Dump) -> Result<()> {
    let log = config
        .keyspace
        .open_partition("raft_log", PartitionCreateOptions::default())?;
    info!("Dump raft log entries");
    info!("=====================");
    for entry in log.range(flags.from.unwrap_or_default().to_be_bytes()..) {
        let (key, value) = entry.unwrap();
        let value = LogEntry::from_bytes(&value)?;
        info!(
            "key = {}, value = {:?}",
            u64::from_be_bytes(key.as_ref().try_into().unwrap()),
            value
        );
    }
    Ok(())
}
