use std::thread;
use std::time::Duration;

use anyhow::Result;
use fjall::GarbageCollection;
use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::RactorMessage;
use tracing::info;

use crate::activity_pub::delivery::{DeliveryWorker, DeliveryWorkerInit, DeliveryWorkerMsg};
use crate::activity_pub::machine::{ActivityPubMachine, ActivityPubMachineInit};
use crate::cluster::{ClusterMaint, ClusterMaintMsg};
use crate::config::RuntimeConfig;
use crate::feed_slurp::{FeedSlurpMsg, FeedSlurpWorker, FeedSlurpWorkerInit};
use crate::raft::{RaftServer, RaftServerMsg, StateMachineMsg};

pub(crate) struct Supervisor;

#[derive(RactorMessage)]
pub(crate) enum SupervisorMsg {
    KeyspaceMaint,
}

pub(crate) struct SupervisorState {
    config: RuntimeConfig,
    myself: ActorRef<SupervisorMsg>,
}

impl Actor for Supervisor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = RuntimeConfig;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        config: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(SupervisorState { config, myself })
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "started");

        state.spawn_cluster_maint().await?;
        state.spawn_raft_server().await?;
        state.spawn_state_machine().await?;
        state.spawn_delivery_worker().await?;
        state.spawn_feed_slurp().await?;

        myself.send_interval(Duration::from_secs(24 * 60 * 60), || {
            SupervisorMsg::KeyspaceMaint
        });
        state.gc_keyspace();

        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // TODO move to module scope
        state.gc_keyspace();
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        use SupervisionEvent::*;

        let is_true = |x| -> bool { x };

        match message {
            ActorStarted(_) => {}
            ActorTerminated(_, _, _) => {}
            ActorFailed(actor_cell, error) => {
                if actor_cell
                    .is_message_type_of::<ClusterMaintMsg>()
                    .is_some_and(is_true)
                {
                    info!(target: "supervision", error, "cluster_maint crashed, restarting...");
                    state.spawn_cluster_maint().await?;
                }
                if actor_cell
                    .is_message_type_of::<RaftServerMsg>()
                    .is_some_and(is_true)
                {
                    info!(target: "supervision", error, "raft server crashed, restarting...");
                    state.spawn_raft_server().await?;
                }
                if actor_cell
                    .is_message_type_of::<StateMachineMsg>()
                    .is_some_and(is_true)
                {
                    info!(target: "supervision", error, "state machine crashed, restarting...");
                    state.spawn_state_machine().await?;
                }
                if actor_cell
                    .is_message_type_of::<DeliveryWorkerMsg>()
                    .is_some_and(is_true)
                {
                    info!(target: "supervision", error, "delivery worker crashed, restarting...");
                    state.spawn_delivery_worker().await?;
                }
                if actor_cell
                    .is_message_type_of::<FeedSlurpMsg>()
                    .is_some_and(is_true)
                {
                    info!(target: "supervision", error, "feed slurp worker crashed, restarting...");
                    state.spawn_feed_slurp().await?;
                }
            }
            ProcessGroupChanged(_) => {}
            PidLifecycleEvent(_) => {}
        }

        Ok(())
    }
}

impl SupervisorState {
    fn gc_keyspace(&self) {
        let keyspace = self.config.keyspace.clone();
        thread::spawn(move || {
            let raft_log = keyspace
                .open_partition("raft_log", Default::default())
                .expect("failed to open raft_log partition");
            raft_log
                .gc_with_staleness_threshold(0.5)
                .expect("failed to garbage collect raft_log");
            let objects = keyspace
                .open_partition("objects", Default::default())
                .expect("failed to open objects partition");
            objects
                .gc_with_staleness_threshold(0.5)
                .expect("failed to garbage collect objects");
        });
    }
    async fn spawn_cluster_maint(&self) -> Result<()> {
        Actor::spawn_linked(
            Some("cluster_maint".into()),
            ClusterMaint,
            (self.config.server.clone(), self.config.clone()),
            self.myself.get_cell(),
        )
        .await?;
        Ok(())
    }
    async fn spawn_raft_server(&self) -> Result<()> {
        Actor::spawn_linked(
            None,
            RaftServer,
            self.config.clone(),
            self.myself.get_cell(),
        )
        .await?;
        Ok(())
    }
    async fn spawn_state_machine(&self) -> Result<()> {
        Actor::spawn_linked(
            Some("state_machine".into()),
            ActivityPubMachine,
            ActivityPubMachineInit {
                apub: self.config.init.activity_pub.clone(),
                keyspace: self.config.keyspace.clone(),
            },
            self.myself.get_cell(),
        )
        .await?;
        Ok(())
    }
    async fn spawn_delivery_worker(&self) -> Result<()> {
        Actor::spawn_linked(
            None,
            DeliveryWorker,
            DeliveryWorkerInit {
                config: self.config.clone(),
            },
            self.myself.get_cell(),
        )
        .await?;
        Ok(())
    }
    async fn spawn_feed_slurp(&self) -> Result<()> {
        Actor::spawn_linked(
            Some("feed_slurp".to_string()),
            FeedSlurpWorker,
            FeedSlurpWorkerInit {
                apub: self.config.init.activity_pub.clone(),
            },
            self.myself.get_cell(),
        )
        .await?;
        Ok(())
    }
}
