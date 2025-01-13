use anyhow::{Context, Result, bail};
use ractor::{Actor, ActorRef};
use ractor_cluster::NodeServer;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin, stdout};
use tokio::task::yield_now;
use tracing::info;

use crate::config::ReplConfig;
use crate::worker::ManholeMsg;

pub(super) async fn run(repl_config: ReplConfig) -> Result<()> {
    let manhole = repl_config
        .init
        .cluster
        .manholes
        .iter()
        .find(|m| m.server_name == repl_config.server.name && m.enable)
        .context("cannot find enabled manhole config for server")?;
    let node_server = NodeServer::new(
        0,
        manhole.auth_cookie.clone(),
        format!("repl/{}", repl_config.server.name),
        "localhost".into(),
        None,
        None,
    );
    let (server, _) = Actor::spawn(None, node_server, ()).await?;
    ractor_cluster::client_connect(&server, ("localhost", manhole.port)).await?;

    while let Err(_) = manhole_ref(&repl_config.server.name) {
        yield_now().await;
    }

    let mut stdout = stdout();
    let reader = BufReader::new(stdin());
    let mut lines = reader.lines();

    stdout.write_all(b"\n> ").await?;
    stdout.flush().await?;
    while let Some(line) = lines.next_line().await? {
        if line.starts_with("dump") {
            let args: Vec<&str> = line.split_ascii_whitespace().collect();
            let from = args.get(1).unwrap_or(&"0").parse()?;
            let list = ractor::call!(
                manhole_ref(&repl_config.server.name)?,
                ManholeMsg::GetRaftLogEntries,
                from
            )?;
            info!("Dump raft log entries");
            info!("=====================");
            for entry in list.items {
                info!("index = {}, value = {:?}", entry.index, entry.value);
            }
        }
        if line.starts_with("log") {
            let (_, msg) = line.split_once(' ').context("log <message>")?;
            ractor::cast!(
                manhole_ref(&repl_config.server.name)?,
                ManholeMsg::AppendRaftClusterMessage(msg.into())
            )?;
        }
        stdout.write_all(b"> ").await?;
        stdout.flush().await?;
    }

    Ok(())
}

fn manhole_ref(server_name: &str) -> Result<ActorRef<ManholeMsg>> {
    for member in ractor::pg::get_scoped_members(&"manhole".into(), &server_name.into()) {
        return Ok(member.into());
    }
    bail!("unable to find enabled manhole for server");
}
