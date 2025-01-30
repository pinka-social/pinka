use anyhow::{Context, Result};
use fjall::Keyspace;
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde_json::Value;
use tracing::info;

use crate::worker::raft::{
    ClientResult, LogEntryValue, RaftAppliedMsg, StateMachineMsg, get_raft_applied,
};

use super::model::{Actor as AsActor, Create, JsonLdValue, Object};
use super::object_serde::NodeValue;
use super::repo::{ContextIndex, OutboxIndex, base62_uuid, make_object_key};
use super::{ObjectRepo, UserIndex};

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
    C2sCreate(#[n(0)] String, #[n(1)] NodeValue),
    #[n(2)]
    S2sCreate(#[n(0)] String, #[n(1)] NodeValue),
    #[n(3)]
    S2sDelete(#[n(0)] String, #[n(1)] NodeValue),
    #[n(4)]
    S2sLike(#[n(0)] String, #[n(1)] NodeValue),
    #[n(5)]
    S2sDislike(#[n(0)] String, #[n(1)] NodeValue),
    #[n(6)]
    S2sFollow(#[n(0)] String, #[n(1)] NodeValue),
    #[n(7)]
    S2sUndo(#[n(0)] String, #[n(1)] NodeValue),
    #[n(8)]
    S2sUpdate(#[n(0)] String, #[n(1)] NodeValue),
    #[n(9)]
    S2sAnnounce(#[n(0)] String, #[n(1)] NodeValue),
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
            ActivityPubCommand::C2sCreate(uid, node_value) => {
                self.handle_c2s_create(uid, node_value).await?;
            }
            ActivityPubCommand::S2sCreate(uid, node_value) => {
                self.handle_s2s_create(uid, node_value).await?;
            }
            ActivityPubCommand::S2sDelete(uid, node_value) => {
                self.handle_s2s_delete(uid, node_value).await?;
            }
            ActivityPubCommand::S2sLike(uid, node_value) => {
                self.handle_s2s_like(uid, node_value).await?;
            }
            ActivityPubCommand::S2sDislike(uid, node_value) => {
                self.handle_s2s_dislike(uid, node_value).await?;
            }
            ActivityPubCommand::S2sFollow(uid, node_value) => {
                self.handle_s2s_follow(uid, node_value).await?;
            }
            ActivityPubCommand::S2sUndo(uid, node_value) => {
                self.handle_s2s_undo(uid, node_value).await?;
            }
            ActivityPubCommand::S2sUpdate(uid, node_value) => {
                self.handle_s2s_update(uid, node_value).await?;
            }
            ActivityPubCommand::S2sAnnounce(uid, node_value) => {
                self.handle_s2s_announce(uid, node_value).await?;
            }
        }

        Ok(ClientResult::ok())
    }
    async fn handle_new_user(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let value = Value::from(node_value);
        let object = Object::try_from(value)?;
        let user = AsActor::try_from(object)?;

        let mut b = self.keyspace.batch();
        let user_index = UserIndex::new(self.keyspace.clone())?;

        user_index.insert(&mut b, uid, user)?;
        b.commit()?;
        Ok(())
    }
    // TODO effects
    async fn handle_c2s_create(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let value = Value::from(node_value);
        let object = Object::try_from(value)?;
        let mut create = Create::try_from(object)?.with_actor("https://example.com/users/john");
        let act_id = format!("pinka-activity:{}", base62_uuid());
        create.as_mut().set_id(&act_id);

        let mut b = self.keyspace.batch();
        let outbox = OutboxIndex::new(self.keyspace.clone())?;

        outbox.insert_create(&mut b, uid, create)?;
        b.commit()?;
        Ok(())
    }
    async fn handle_s2s_create(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let _ = uid;
        let value = Value::from(node_value);
        if value.has_props(&["context"]) {
            // currently we only care activities mentioning our object
            // TODO verify context
            let Some(Value::String(iri)) = value.get("context") else {
                return Ok(());
            };
            let iri = iri.to_string();
            let object = Object::try_from(value)?;
            // TODO let create = Create::try_from(object)?;
            let obj_key = make_object_key();

            let mut b = self.keyspace.batch();
            let obj_repo = ObjectRepo::new(self.keyspace.clone())?;
            let ctx_index = ContextIndex::new(self.keyspace.clone())?;
            obj_repo.insert(&mut b, obj_key, object)?;
            ctx_index.insert(&mut b, iri, obj_key)?;

            b.commit()?;
        }
        Ok(())
    }
    async fn handle_s2s_delete(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let _ = uid;
        let _ = node_value;
        // TODO
        Ok(())
    }
    async fn handle_s2s_like(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let _ = uid;
        let value = Value::from(node_value);
        if value.has_props(&["object"]) {
            let Some(iri) = value.object_iri() else {
                return Ok(());
            };
            let iri = iri.to_string();
            let obj_key = make_object_key();

            let mut b = self.keyspace.batch();
            let obj_repo = ObjectRepo::new(self.keyspace.clone())?;
            let ctx_index = ContextIndex::new(self.keyspace.clone())?;
            obj_repo.insert(&mut b, obj_key, value)?;
            ctx_index.insert_likes(&mut b, iri, obj_key)?;

            b.commit()?;
        }
        Ok(())
    }
    async fn handle_s2s_dislike(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let _ = uid;
        let _ = node_value;
        // TODO
        Ok(())
    }
    async fn handle_s2s_follow(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let value = Value::from(node_value);
        if value.has_props(&["object"]) {
            let obj_key = make_object_key();

            let mut b = self.keyspace.batch();
            let obj_repo = ObjectRepo::new(self.keyspace.clone())?;
            let user_index = UserIndex::new(self.keyspace.clone())?;
            obj_repo.insert(&mut b, obj_key, value)?;
            user_index.insert_follower(&mut b, uid, obj_key)?;

            b.commit()?;
            // TODO send Accept or Reject back
        }
        Ok(())
    }
    async fn handle_s2s_undo(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let _ = uid;
        let _ = node_value;
        // TODO
        Ok(())
    }
    async fn handle_s2s_update(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let _ = uid;
        let value = Value::from(node_value);
        if value.has_props(&["object"]) {
            // let Some(iri) = value.object_iri() else {
            //     return Ok(());
            // };
            // let iri = iri.to_string();
            // let object = Object::try_from(value)?;
            // let create = Create::try_from(object)?;
            // let obj_key = make_object_key();

            // let mut b = self.keyspace.batch();
            // let obj_repo = ObjectRepo::new(self.keyspace.clone())?;
            // let ctx_index = ContextIndex::new(self.keyspace.clone())?;
            // let obj_key = ctx_index.find_activity_by_context(iri)?;
            // obj_repo.insert(&mut b, obj_key, object)?;
            // ctx_index.insert(&mut b, iri, obj_key)?;

            // b.commit()?;
        }
        Ok(())
    }
    async fn handle_s2s_announce(&mut self, uid: String, node_value: NodeValue) -> Result<()> {
        let _ = uid;
        let value = Value::from(node_value);
        if value.has_props(&["object"]) {
            let Some(iri) = value.object_iri() else {
                return Ok(());
            };
            let iri = iri.to_string();
            let obj_key = make_object_key();

            let mut b = self.keyspace.batch();
            let obj_repo = ObjectRepo::new(self.keyspace.clone())?;
            let ctx_index = ContextIndex::new(self.keyspace.clone())?;
            obj_repo.insert(&mut b, obj_key, value)?;
            ctx_index.insert_shares(&mut b, iri, obj_key)?;

            b.commit()?;
        }
        Ok(())
    }
}
