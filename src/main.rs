mod config;
mod worker;

use anyhow::Result;
use ractor::Actor;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

use crate::config::{Config, RaftConfig};
use crate::worker::{Mode, Supervisor};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mode = if false {
        Mode::Bootstrap
    } else {
        Mode::Restart
    };
    let config = Config {
        raft: RaftConfig {
            cluster_size: 5,
            heartbeat_ms: 100,
            min_election_ms: 150,
            max_election_ms: 300,
        },
    };

    let (supervisor, actor_handle) =
        Actor::spawn(Some("supervisor".into()), Supervisor, (mode, config)).await?;

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
