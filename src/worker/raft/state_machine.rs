use std::time::Duration;

use anyhow::Result;
use fjall::PartitionHandle;
use futures::future::BoxFuture;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use ractor_cluster::RactorMessage;
use tracing::info;

use crate::RaftSerDe;

use super::{LogEntry, RaftMsg, RaftRole};

#[derive(RactorMessage)]
pub(crate) enum StateMachineMsg {
    Apply(LogEntry, RpcReplyPort<Option<Effect>>),
}

pub(crate) type Effect = BoxFuture<'static, ()>;

pub(super) struct StateDriver;

#[derive(RactorMessage)]
pub(super) enum StateDriverMsg {
    Tick,
    Committed(usize),
    RoleChanged(RaftRole),
}

pub(super) struct StateDriverState {
    logs: PartitionHandle,
    machine: ActorRef<StateMachineMsg>,
    last_commit_index: usize,
    last_applied: usize,
    parent: ActorRef<RaftMsg>,
    parent_role: RaftRole,
}

impl Actor for StateDriver {
    type Msg = StateDriverMsg;
    type State = StateDriverState;
    type Arguments = StateDriverState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "raft", "state_driver started");
        ractor::cast!(myself, StateDriverMsg::Tick)?;
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            StateDriverMsg::Tick => {
                let next_apply = state.last_applied + 1;
                if next_apply <= state.last_commit_index {
                    if let Some(slice) = state.logs.get(next_apply.to_be_bytes())? {
                        let log_entry = LogEntry::from_bytes(&slice)?;
                        if let Some(effect) =
                            ractor::call!(state.machine, StateMachineMsg::Apply, log_entry)?
                        {
                            if matches!(state.parent_role, RaftRole::Leader) {
                                // TODO: retry?
                                effect.await;
                            }
                        }
                        state.last_applied = next_apply;
                        ractor::cast!(state.parent, RaftMsg::Applied(state.last_applied as u64))?;
                        ractor::cast!(myself, StateDriverMsg::Tick)?;
                        return Ok(());
                    }
                }
                // no new logs, sleep for some time
                myself.send_after(Duration::from_secs(1), || StateDriverMsg::Tick);
            }
            StateDriverMsg::Committed(last_commit_index) => {
                state.last_commit_index = last_commit_index;
            }
            StateDriverMsg::RoleChanged(role) => {
                state.parent_role = role;
            }
        }
        Ok(())
    }
}
