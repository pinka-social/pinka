use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::RactorMessage;
use tracing::info;

use crate::config::Config;

use super::{RaftMsg, RaftWorker};

pub(crate) struct Supervisor;

#[derive(RactorMessage)]
pub(crate) enum SupervisorMsg {}

pub(crate) struct SupervisorState {
    config: Config,
}

pub(crate) enum Mode {
    Bootstrap,
    Restart,
}

#[ractor::async_trait]
impl Actor for Supervisor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = (Mode, Config);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let config = args.1.clone();
        // simulate a cluster of size 3
        Actor::spawn_linked(None, RaftWorker, (Mode::Restart, config), myself.get_cell()).await?;
        Actor::spawn_linked(None, RaftWorker, (Mode::Restart, config), myself.get_cell()).await?;
        Actor::spawn_linked(None, RaftWorker, (Mode::Restart, config), myself.get_cell()).await?;
        Actor::spawn_linked(None, RaftWorker, (Mode::Restart, config), myself.get_cell()).await?;
        Actor::spawn_linked(None, RaftWorker, args, myself.get_cell()).await?;
        Ok(SupervisorState { config })
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
            ActorFailed(actor_cell, _error) => {
                if matches!(actor_cell.is_message_type_of::<RaftMsg>(), Some(true)) {
                    info!(target: "supervision", "raft_worker crashed, restarting...");

                    Actor::spawn_linked(
                        None,
                        RaftWorker,
                        (Mode::Restart, state.config.clone()),
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
