use anyhow::{Context, Result};
use fjall::{KvSeparationOptions, PartitionCreateOptions};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent, pg};
use ractor_cluster::{NodeServer, RactorClusterMessage};
use tracing::warn;

use crate::config::{RuntimeConfig, ServerConfig};
use crate::worker::raft::{LogEntry, LogEntryValue};

use super::raft::{LogEntryList, RaftMsg};

pub(super) struct Manhole;

#[derive(RactorClusterMessage)]
pub(crate) enum ManholeMsg {
    #[rpc]
    GetRaftLogEntries(u64, RpcReplyPort<LogEntryList>),
    AppendRaftClusterMessage(String),
}

pub(super) struct ManholeState {
    server: ServerConfig,
    config: RuntimeConfig,
}

impl Actor for Manhole {
    type Msg = ManholeMsg;
    type State = ManholeState;
    type Arguments = (ServerConfig, RuntimeConfig);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let (server, config) = args;
        Ok(ManholeState { server, config })
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        pg::join_scoped("manhole".into(), state.server.name.clone(), vec![
            myself.get_cell(),
        ]);
        state.spawn_node_server(myself).await?;
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ManholeMsg::GetRaftLogEntries(from, reply) => {
                state.handle_get_raft_log_entries(from as usize, reply)?;
            }
            ManholeMsg::AppendRaftClusterMessage(msg) => {
                state.handle_append_raft_cluster_message(msg).await?;
            }
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorFailed(_actor_cell, error) => {
                warn!(error, "manhole_node_server crashed, respawning...");
                state.spawn_node_server(myself).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl ManholeState {
    async fn spawn_node_server(&self, myself: ActorRef<ManholeMsg>) -> Result<()> {
        for manhole in &self.config.init.cluster.manholes {
            if manhole.server_name == self.server.name && manhole.enable {
                let node_server = NodeServer::new(
                    manhole.port,
                    manhole.auth_cookie.clone(),
                    format!("{}/manhole", self.server.name),
                    "localhost".into(),
                    None,
                    None,
                );
                Actor::spawn_linked(
                    Some("manhole_node_server".into()),
                    node_server,
                    (),
                    myself.get_cell(),
                )
                .await?;
                break;
            }
        }
        Ok(())
    }
    fn handle_get_raft_log_entries(
        &self,
        from: usize,
        reply: RpcReplyPort<LogEntryList>,
    ) -> Result<()> {
        // TODO: abstract db operation
        let log = self.config.keyspace.open_partition(
            "raft_log",
            PartitionCreateOptions::default()
                .compression(fjall::CompressionType::Lz4)
                .manual_journal_persist(true)
                .with_kv_separation(KvSeparationOptions::default()),
        )?;
        let mut items = vec![];
        for entry in log.iter().skip(from) {
            let (_, value) = entry.unwrap();
            let item: LogEntry = postcard::from_bytes(&value)?;
            items.push(item);
        }
        reply.send(LogEntryList { items })?;
        Ok(())
    }
    async fn handle_append_raft_cluster_message(&self, msg: String) -> Result<()> {
        let raft: ActorRef<RaftMsg> =
            ActorRef::where_is(self.server.name.clone()).context("unable to find raft_worker")?;
        if !ractor::call!(
            raft,
            RaftMsg::ClientRequest,
            LogEntryValue::ClusterMessage(msg)
        )? {
            warn!(target: "manhole", "unable to append raft cluster message");
        }
        Ok(())
    }
}
