use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::{
    RactorMessage,
    node::{
        NodeConnectionMode, NodeEventSubscription, NodeServer, NodeServerMessage,
        NodeServerSessionInformation,
    },
};
use tracing::{error, info, warn};

pub(super) struct ClusterMaint;

#[derive(RactorMessage, Debug)]
pub(super) enum ClusterMaintMsg {
    CheckConnection,
    ServerConnected(String),
    ServerDisconnected(String),
}

#[derive(Debug)]
pub(super) struct ClusterConfig {
    pub(super) server_name: String,
    pub(super) servers: Vec<String>,
}

#[derive(Debug)]
pub(super) struct ClusterState {
    config: ClusterConfig,
    server_status: BTreeMap<String, ServerStatus>,
    myself: ActorRef<ClusterMaintMsg>,
}

#[derive(Debug)]
enum ServerStatus {
    Connected,
    Disconnected,
}

impl Actor for ClusterMaint {
    type Msg = ClusterMaintMsg;
    type State = ClusterState;
    type Arguments = ClusterConfig;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        config: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut server_status = BTreeMap::new();
        for server_name in config.servers.iter() {
            if server_name == &config.server_name {
                continue;
            }
            server_status.insert(server_name.clone(), ServerStatus::Disconnected);
        }

        Ok(ClusterState {
            config,
            server_status,
            myself,
        })
    }

    async fn post_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("cluster_maint started, setting up connections");
        myself.send_interval(Duration::from_millis(10_000), || {
            ClusterMaintMsg::CheckConnection
        });
        state.spawn_node_server().await?;
        state.connect_peers().await?;
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let _ = state;
        match message {
            ClusterMaintMsg::CheckConnection => {
                state.connect_peers().await?;
            }
            ClusterMaintMsg::ServerConnected(name) => {
                info!(name, "server connected");
                state.server_status.insert(name, ServerStatus::Connected);
            }
            ClusterMaintMsg::ServerDisconnected(name) => {
                info!(name, "server disconnected");
                state.server_status.insert(name, ServerStatus::Disconnected);
            }
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorStarted(_actor_cell) => {}
            SupervisionEvent::ActorTerminated(_actor_cell, _boxed_statee, _) => {}
            SupervisionEvent::ActorFailed(_actor_cell, error) => {
                error!("{error:#}");
                error!("node server crashed, restarting...");
                state.spawn_node_server().await?;
                state.connect_peers().await?;
            }
            SupervisionEvent::ProcessGroupChanged(_group_change_message) => {}
            SupervisionEvent::PidLifecycleEvent(_pid_lifecycle_event) => {}
        }
        Ok(())
    }
}

impl ClusterState {
    async fn spawn_node_server(&self) -> Result<ActorRef<NodeServerMessage>> {
        let node = NodeServer::new(
            8001,
            "supersecureauthcookie".to_string(),
            self.config.server_name.clone(),
            self.config.server_name.clone(),
            None,
            Some(NodeConnectionMode::Isolated),
        );
        let (node_server, _) =
            Actor::spawn_linked(Some("node_server".into()), node, (), self.myself.get_cell())
                .await?;
        node_server.cast(NodeServerMessage::SubscribeToEvents {
            id: "cluster_maint".into(),
            subscription: Box::new(NodeEventListener),
        })?;
        Ok(node_server)
    }

    async fn connect_peers(&self) -> Result<()> {
        let node_server = match ActorRef::where_is("node_server".into()) {
            Some(node_server) => node_server,
            None => self.spawn_node_server().await?,
        };
        for peer_name in self.config.servers.clone().into_iter() {
            if peer_name == self.config.server_name {
                continue;
            }
            if let Some(ServerStatus::Connected) = self.server_status.get(&peer_name) {
                continue;
            }
            let node_server = node_server.clone();
            ractor::concurrency::spawn(async move {
                info!("connecting to {}:{}", peer_name, 8001);
                if let Err(error) =
                    ractor_cluster::client_connect(&node_server, (peer_name.as_str(), 8001)).await
                {
                    warn!("Error: {}", error);
                    warn!("unable to connect to {}:{}", peer_name, 8001);
                }
            });
        }
        Ok(())
    }
}

struct NodeEventListener;

impl NodeEventSubscription for NodeEventListener {
    fn node_session_opened(&self, ses: NodeServerSessionInformation) {
        let _ = ses;
    }

    fn node_session_disconnected(&self, ses: NodeServerSessionInformation) {
        if let Some(peer_name) = ses.peer_name {
            if let Some(cluster_maint) = ActorRef::where_is("cluster_maint".into()) {
                if let Some((server_name, _hostname)) = peer_name.name.split_once('@') {
                    ractor::cast!(
                        cluster_maint,
                        ClusterMaintMsg::ServerDisconnected(server_name.to_string())
                    )
                    .expect("unable to send message to cluster_maint");
                }
            }
        }
    }

    fn node_session_authenicated(&self, ses: NodeServerSessionInformation) {
        if let Some(peer_name) = ses.peer_name {
            if let Some(cluster_maint) = ActorRef::where_is("cluster_maint".into()) {
                if let Some((server_name, _hostname)) = peer_name.name.split_once('@') {
                    ractor::cast!(
                        cluster_maint,
                        ClusterMaintMsg::ServerConnected(server_name.to_string())
                    )
                    .expect("unable to send message to cluster_maint");
                }
            }
        }
    }
}
