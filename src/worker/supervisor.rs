use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::RactorMessage;
use tracing::info;

use super::{RaftMsg, RaftWorker};

pub(crate) struct Supervisor;

#[derive(RactorMessage)]
pub(crate) enum SupervisorMsg {}

pub(crate) struct SupervisorState {}

pub(crate) enum Mode {
    Bootstrap,
    Restart,
}

#[ractor::async_trait]
impl Actor for Supervisor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = Mode;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Actor::spawn_linked(RaftWorker::name(), RaftWorker, args, myself.into()).await?;
        Ok(SupervisorState {})
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
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        use SupervisionEvent::*;

        match message {
            ActorStarted(_) => {}
            ActorTerminated(_, _, _) => {}
            ActorFailed(actor_cell, _error) => {
                if matches!(actor_cell.is_message_type_of::<RaftMsg>(), Some(true)) {
                    info!(target: "supervision", "raft_worker crashed, restarting...");

                    Actor::spawn_linked(
                        RaftWorker::name(),
                        RaftWorker,
                        Mode::Restart,
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
