use anyhow::{Context, Result};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_json::json;
use tracing::info;

use crate::worker::raft::{
    ClientResult, LogEntryValue, RaftAppliedMsg, StateMachineMsg, get_raft_applied,
};

use super::model::Object;
use super::object_serde::{Header, NodeValue};

pub(crate) struct ActivityPubMachine;

pub(crate) struct State {}

impl Actor for ActivityPubMachine {
    type Msg = StateMachineMsg;
    type State = State;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(State {})
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let reply = get_raft_applied()?;
        match message {
            StateMachineMsg::Apply(log_entry) => match log_entry.value {
                LogEntryValue::Bytes(byte_buf) => {
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

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum ActivityPubCommand {
    /// Client to Server - Create Activity
    Create(Object),
}

impl ActivityPubCommand {
    fn into_bytes(self) -> Result<Vec<u8>> {
        let header = vec![];
        let payload =
            postcard::to_extend(&Header::V_1, header).context("unable to serialize RPC header")?;
        let result = postcard::to_extend(&self, payload).context("unable to serialize payload")?;
        Ok(result)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (header, payload): (Header, _) =
            postcard::take_from_bytes(bytes).context("unable to deserialize RPC header")?;
        if header != Header::V_1 {
            tracing::error!(target: "rpc", ?header, "invalid RPC header version");
        }
        Ok(postcard::from_bytes(&payload).context("unable to deserialize payload")?)
    }
}

impl From<ActivityPubCommand> for LogEntryValue {
    fn from(value: ActivityPubCommand) -> Self {
        LogEntryValue::Bytes(value.into_bytes().unwrap().into())
    }
}

impl State {
    async fn handle_command(&mut self, command: ActivityPubCommand) -> Result<ClientResult> {
        info!(target: "apub", ?command, "received command");
        Ok(ClientResult::ok())
    }
}
