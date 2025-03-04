mod activity_pub;
mod cluster;
mod config;
mod feed_slurp;
mod flags;
mod http;
mod supervisor;

use std::fs::{self, File};
use std::path::Path;
use std::process::exit;

use anyhow::{Context, Result, bail};
use fd_lock::RwLock;
use ractor::Actor;
use tokio::signal::unix::{SignalKind, signal};
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

    if flags.server.is_none() && config.cluster.servers.len() != 1 {
        eprintln!("Error: Server name is required for config with multiple servers.");
        exit(1);
    }

    let server_name = flags.server.unwrap_or_else(|| {
        config
            .cluster
            .servers
            .first_key_value()
            .unwrap()
            .0
            .to_owned()
    });
    if !config.cluster.servers.contains_key(&server_name) {
        eprintln!(
            "Error: Invalid server index {server_name}, config only defined {} servers.",
            config.cluster.servers.len()
        );
        exit(1);
    }
    let server = config.cluster.servers[&server_name].clone();

    let keyspace_name = config.database.path.join(&server_name);
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
        server_name,
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
    let (supervisor, mut actor_handle) =
        Actor::spawn(Some("supervisor".into()), Supervisor, config.clone())
            .await
            .context("Failed to spawn supervisor")?;

    let http = http::serve(&config);
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    tokio::select! {
        _ = &mut actor_handle => {
            error!("Supervisor thread crashed");
            bail!("Supervisor thread crashed");
        }
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
