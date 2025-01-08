use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::node::NodeConnectionMode;
use ractor_cluster::{NodeServer, NodeServerMessage, RactorMessage};
use tokio::time::timeout;
use tracing::{info, warn};

use crate::config::{Config, ServerConfig};
use crate::flags::Flags;

use super::raft::{RaftMsg, RaftWorker};

pub(crate) struct Supervisor;

#[derive(RactorMessage)]
pub(crate) enum SupervisorMsg {}

pub(crate) struct SupervisorState {
    server: ServerConfig,
    config: Config,
    myself: ActorRef<SupervisorMsg>,
    node_ref: ActorRef<NodeServerMessage>,
}

impl Actor for Supervisor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = (Flags, Config);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let flags = &args.0;
        let config = args.1.clone();
        let server = config.cluster.servers[flags.server.unwrap_or_default()].clone();

        Actor::spawn_linked(
            Some(server.name.clone()),
            RaftWorker,
            (args.0.bootstrap, config.clone()),
            myself.get_cell(),
        )
        .await?;

        let node = NodeServer::new(
            server.port,
            config.cluster.auth_cookie.clone(),
            server.name.clone(),
            server.hostname.clone(),
            None,
            Some(NodeConnectionMode::Isolated),
        );
        let (node_ref, _) =
            Actor::spawn_linked(Some("node".into()), node, (), myself.get_cell()).await?;

        let mut state = SupervisorState {
            server,
            config,
            myself,
            node_ref,
        };

        if flags.connect {
            state.connect_peers().await?;
        }

        Ok(state)
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "started");
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        use SupervisionEvent::*;

        match message {
            ActorStarted(_) => {}
            ActorTerminated(_, _, _) => {}
            ActorFailed(actor_cell, _error) => {
                if matches!(
                    actor_cell.is_message_type_of::<NodeServerMessage>(),
                    Some(true)
                ) {
                    info!(target: "supervision", "node_server crashed, restarting...");
                    state.spawn_node_server().await?;
                    state.connect_peers().await?;
                }
                if matches!(actor_cell.is_message_type_of::<RaftMsg>(), Some(true)) {
                    info!(target: "supervision", "raft_worker crashed, restarting...");
                    Actor::spawn_linked(
                        Some(state.server.name.clone()),
                        RaftWorker,
                        (false, state.config.clone()),
                        myself.into(),
                    )
                    .await?;
                }
            }
            ProcessGroupChanged(_) => {}
            PidLifecycleEvent(_) => {}
        }

        Ok(())
    }
}

impl SupervisorState {
    async fn spawn_node_server(&mut self) -> Result<(), ActorProcessingErr> {
        let node = NodeServer::new(
            self.server.port,
            self.config.cluster.auth_cookie.clone(),
            self.server.name.clone(),
            self.server.hostname.clone(),
            None,
            Some(NodeConnectionMode::Isolated),
        );
        let (node_ref, _) =
            Actor::spawn_linked(Some("node".into()), node, (), self.myself.get_cell()).await?;
        self.node_ref = node_ref;
        Ok(())
    }

    async fn connect_peers(&mut self) -> Result<(), ActorProcessingErr> {
        for peer in self.config.cluster.servers.iter().cloned() {
            if peer.name == self.server.name {
                continue;
            }
            let node_server = self.node_ref.clone();
            ractor::concurrency::spawn(async move {
                info!(target: "raft", "connecting to {}@{}:{}", peer.name, peer.hostname, peer.port);
                if let Err(_) = ractor_cluster::client_connect(
                    &node_server,
                    (peer.hostname.as_str(), peer.port),
                )
                .await
                {
                    warn!(target: "raft", "unable to connect to {}@{}:{}", peer.name, peer.hostname, peer.port);
                }
            });
        }
        Ok(())
    }
}
