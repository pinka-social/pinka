use anyhow::{Context, Result};
use fjall::Keyspace;
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde_json::Value;
use tracing::info;

use crate::worker::raft::{
    ClientResult, LogEntryValue, RaftAppliedMsg, StateMachineMsg, get_raft_applied,
};

use super::UserIndex;
use super::model::{Actor as AsActor, Create, JsonLdValue, Object};
use super::object_serde::NodeValue;
use super::repo::{OutboxIndex, base62_uuid};

pub(crate) struct ActivityPubMachine;

pub(crate) struct State {
    keyspace: Keyspace,
}

pub(crate) struct ActivityPubMachineInit {
    pub(crate) keyspace: Keyspace,
}

impl Actor for ActivityPubMachine {
    type Msg = StateMachineMsg;
    type State = State;
    type Arguments = ActivityPubMachineInit;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let ActivityPubMachineInit { keyspace } = args;
        Ok(State { keyspace })
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
    #[n(0)]
    UpdateUser(#[n(0)] String, #[n(1)] NodeValue),
    /// Client to Server - Create Activity
    #[n(1)]
    Create(#[n(0)] String, #[n(1)] NodeValue),
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
        LogEntryValue::Command(value.into_bytes().unwrap())
    }
}

impl State {
    async fn handle_command(&mut self, command: ActivityPubCommand) -> Result<ClientResult> {
        info!(target: "apub", ?command, "received command");

        match command {
            ActivityPubCommand::UpdateUser(uid, node_value) => {
                self.handle_new_user(uid, node_value).await?;
            }
            ActivityPubCommand::Create(uid, node_value) => {
                self.handle_create(uid, node_value).await?;
            }
        }

        Ok(ClientResult::ok())
    }
    async fn handle_new_user(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let value = Value::from(node_value);
        let object = Object::try_from(value)?;
        let user = AsActor::try_from(object)?;

        let user_index = UserIndex::new(self.keyspace.clone())?;
        user_index.insert(uid, user)?;

        Ok(())
    }
    // TODO effects
    async fn handle_create(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let value = Value::from(node_value);
        let object = Object::try_from(value)?;
        let mut create = Create::try_from(object)?.with_actor("https://example.com/users/john");
        let act_id = format!("pinka-activity:{}", base62_uuid());
        create.as_mut().set_id(&act_id);

        let outbox = OutboxIndex::new(self.keyspace.clone())?;
        outbox.insert_create(uid, create)?;

        Ok(())
    }
}
