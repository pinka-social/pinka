mod activity_pub;
mod cluster;
mod config;
mod feed_slurp;
mod flags;
mod http;
mod raft;
mod supervisor;

use std::fs::{self, File};
use std::path::Path;
use std::process::exit;

use anyhow::{Context, Result};
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

    let config = Config::open(&flags.config)
        .with_context(|| format!("Failed to read config file {}", flags.config.display()))?;

    let server_id = flags.server.unwrap_or_default();
    if config.cluster.servers.len() <= server_id {
        eprintln!(
            "Error: Invalid server index {server_id}, config only defined {} servers.",
            config.cluster.servers.len()
        );
        exit(1);
    }
    let server = config.cluster.servers[server_id].clone();

    let keyspace_name = config.database.path.join(&server.name);
    if !keyspace_name.exists() {
        create_keyspace_folder(&keyspace_name).context("Failed to create database folder")?;
    }
    let lock_file =
        File::create(keyspace_name.join("lock")).context("Failed to create lock file")?;
    let mut keyspace_lock = RwLock::new(lock_file);
    let write_guard = match keyspace_lock.try_write() {
        Ok(guard) => guard,
        Err(_) => {
            eprintln!(
                "Database '{}' cannot be accessed because it is locked by another process.",
                keyspace_name.display()
            );
            eprintln!(
                "If you are certain no other process is using this database, delete '{}' to remove the lock file.",
                keyspace_name.join("lock").display()
            );
            exit(1);
        }
    };

    let keyspace = fjall::Config::new(keyspace_name)
        .manual_journal_persist(true)
        .open()
        .context("Failed to open database")?;

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

fn create_keyspace_folder(keyspace_name: &Path) -> Result<()> {
    fs::create_dir_all(keyspace_name)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = keyspace_name.metadata()?.permissions();
        perm.set_mode(0o700);
        fs::set_permissions(keyspace_name, perm)?;
    }
    Ok(())
}

async fn serve(config: RuntimeConfig) -> Result<()> {
    let (supervisor, actor_handle) =
        Actor::spawn(Some("supervisor".into()), Supervisor, config.clone())
            .await
            .context("Failed to spawn supervisor")?;

    let http = http::serve(&config);
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    tokio::select! {
        _ = http => {
            error!("HTTP thread crashed");
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
