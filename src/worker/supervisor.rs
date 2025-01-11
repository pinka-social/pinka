use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::RactorMessage;
use tracing::info;

use crate::config::{RuntimeConfig, ServerConfig};
use crate::flags::Flags;
use crate::worker::cluster::ClusterMaintMsg;

use super::cluster::ClusterMaint;
use super::raft::{RaftMsg, RaftWorker};

pub(crate) struct Supervisor;

#[derive(RactorMessage)]
pub(crate) enum SupervisorMsg {}

pub(crate) struct SupervisorState {
    server: ServerConfig,
    config: RuntimeConfig,
    #[allow(unused)]
    myself: ActorRef<SupervisorMsg>,
}

impl Actor for Supervisor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = (Flags, RuntimeConfig);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let flags = &args.0;
        let config = args.1.clone();
        let server = config.init.cluster.servers[flags.server.unwrap_or_default()].clone();

        Actor::spawn_linked(
            Some("cluster_maint".into()),
            ClusterMaint,
            (server.clone(), config.clone()),
            myself.get_cell(),
        )
        .await?;

        Actor::spawn_linked(
            Some(server.name.clone()),
            RaftWorker,
            (args.0.bootstrap, config.clone()),
            myself.get_cell(),
        )
        .await?;

        Ok(SupervisorState {
            server,
            config,
            myself,
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
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
            ActorFailed(actor_cell, error) => {
                if matches!(
                    actor_cell.is_message_type_of::<ClusterMaintMsg>(),
                    Some(true)
                ) {
                    info!(target: "supervision", error, "cluster_maint crashed, restarting...");
                    Actor::spawn_linked(
                        Some("cluster_maint".into()),
                        ClusterMaint,
                        (state.server.clone(), state.config.clone()),
                        myself.get_cell(),
                    )
                    .await?;
                }
                if matches!(actor_cell.is_message_type_of::<RaftMsg>(), Some(true)) {
                    info!(target: "supervision", error, "raft_worker crashed, restarting...");
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
