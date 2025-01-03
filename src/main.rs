use anyhow::Result;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

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

    Ok(())
}
