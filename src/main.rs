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
use ractor::Actor;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info};

use self::config::{ActivityPubConfig, Config, RuntimeConfig};
use self::flags::{Pinka, PinkaCmd};
use self::supervisor::Supervisor;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

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
    if !keyspace_name.exists() {
        fs::create_dir_all(&keyspace_name)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perm = keyspace_name.metadata()?.permissions();
            perm.set_mode(0o700);
            fs::set_permissions(&keyspace_name, perm)?;
        }
    }
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
        PinkaCmd::Serve(_) => serve(config).await?,
    }

    drop(write_guard);

    Ok(())
}

async fn serve(config: RuntimeConfig) -> Result<()> {
    let (supervisor, actor_handle) =
        Actor::spawn(Some("supervisor".into()), Supervisor, config.clone()).await?;

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
