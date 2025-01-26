use anyhow::{Context, Result};
use fjall::Keyspace;
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tracing::info;

use crate::worker::raft::{
    ClientResult, LogEntryValue, RaftAppliedMsg, StateMachineMsg, get_raft_applied,
};

use super::model::{Create, JsonLdValue};
use super::object_serde::NodeValue;
use super::repo::{ActivityRepo, ObjectRepo, base62_uuid};

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
        LogEntryValue::Command(value.into_bytes().unwrap())
    }
}

impl State {
    async fn handle_command(&mut self, command: ActivityPubCommand) -> Result<ClientResult> {
        info!(target: "apub", ?command, "received command");

        match command {
            ActivityPubCommand::Create(node_value) => self.handle_create(node_value).await?,
        }

        Ok(ClientResult::ok())
    }

    async fn handle_create(&mut self, node_value: NodeValue) -> Result<()> {
        let mut create = Create::try_from(node_value)?.with_actor("https://example.com/users/john");
        let act_id = format!("pinka-activity:{}", base62_uuid());
        create.set_id(&act_id);

        let object = create.to_inner();
        let iri = object.id().expect("object should have id");
        let obj_repo = ObjectRepo::new(self.keyspace.clone())?;
        obj_repo.insert(&iri, object)?;

        let act_repo = ActivityRepo::new(self.keyspace.clone())?;
        act_repo.insert(&act_id, create.into())?;
        Ok(())
    }
}
