use anyhow::{Context, Result};
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tracing::info;

use crate::worker::raft::{
    ClientResult, LogEntryValue, RaftAppliedMsg, StateMachineMsg, get_raft_applied,
};

use super::object_serde::NodeValue;

pub(crate) struct ActivityPubMachine;

pub(crate) struct State {}

impl Actor for ActivityPubMachine {
    type Msg = StateMachineMsg;
    type State = State;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(State {})
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let reply = get_raft_applied()?;
        match message {
            StateMachineMsg::Apply(log_entry) => match log_entry.value {
                LogEntryValue::Command(byte_buf) => {
                    let command = ActivityPubCommand::from_bytes(&byte_buf)?;
                    let result = state.handle_command(command).await?;
                    ractor::cast!(reply, RaftAppliedMsg::Applied(log_entry.index, result))?;
                }
                LogEntryValue::NewTermStarted | LogEntryValue::ClusterMessage(_) => {
                    ractor::cast!(
                        reply,
                        RaftAppliedMsg::Applied(log_entry.index, ClientResult::ok())
                    )?;
                }
            },
        }
        Ok(())
    }
}

#[derive(Debug, Encode, Decode)]
pub(crate) enum ActivityPubCommand {
    /// Client to Server - Create Activity
    #[n(0)]
    Create(#[n(0)] NodeValue),
}

impl ActivityPubCommand {
    fn into_bytes(self) -> Result<Vec<u8>> {
        Ok(minicbor::to_vec(&self).context("unable to serialize apub command")?)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(minicbor::decode(bytes).context("unable to deserialize apub command")?)
    }
}

impl From<ActivityPubCommand> for LogEntryValue {
    fn from(value: ActivityPubCommand) -> Self {
        LogEntryValue::Command(value.into_bytes().unwrap().into())
    }
}

impl State {
    async fn handle_command(&mut self, command: ActivityPubCommand) -> Result<ClientResult> {
        info!(target: "apub", ?command, "received command");
        Ok(ClientResult::ok())
    }
}
