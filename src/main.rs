mod worker;

use anyhow::Result;
use ractor::Actor;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

use crate::worker::{Mode, Supervisor};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mode = if true { Mode::Bootstrap } else { Mode::Restart };

    let (supervisor, actor_handle) =
        Actor::spawn(Some("supervisor".into()), Supervisor, mode).await?;

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
