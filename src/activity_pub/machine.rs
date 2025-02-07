use anyhow::{Context, Result};
use fjall::{Keyspace, PersistMode};
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::task::spawn_blocking;
use tracing::{error, info, warn};
use uuid::Bytes;

use crate::raft::{get_raft_applied, ClientResult, LogEntryValue, RaftAppliedMsg, StateMachineMsg};

use super::model::{Actor as AsActor, Create, Object, Update};
use super::repo::{ContextIndex, OutboxIndex};
use super::simple_queue::SimpleQueue;
use super::{IriIndex, ObjectKey, ObjectRepo, UserIndex};

pub(crate) struct ActivityPubMachine;

pub(crate) struct State {
    keyspace: Keyspace,
    user_index: UserIndex,
    outbox_index: OutboxIndex,
    ctx_index: ContextIndex,
    iri_index: IriIndex,
    obj_repo: ObjectRepo,
    queue: SimpleQueue,
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
        spawn_blocking(move || {
            let user_index = UserIndex::new(keyspace.clone())?;
            let outbox_index = OutboxIndex::new(keyspace.clone())?;
            let ctx_index = ContextIndex::new(keyspace.clone())?;
            let iri_index = IriIndex::new(keyspace.clone())?;
            let obj_repo = ObjectRepo::new(keyspace.clone())?;
            let queue = SimpleQueue::new(keyspace.clone())?;
            Ok(State {
                keyspace,
                user_index,
                outbox_index,
                ctx_index,
                iri_index,
                obj_repo,
                queue,
            })
        })
        .await?
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
    UpdateUser(#[n(0)] String, #[n(1)] Object<'static>),
    /// Client to Server - Create Activity
    #[n(1)]
    C2sCreate(#[n(0)] C2sCommand),
    #[n(2)]
    S2sCreate(#[n(0)] S2sCommand),
    #[n(3)]
    S2sDelete(#[n(0)] S2sCommand),
    #[n(4)]
    S2sLike(#[n(0)] S2sCommand),
    #[n(5)]
    S2sDislike(#[n(0)] S2sCommand),
    #[n(6)]
    S2sFollow(#[n(0)] S2sCommand),
    #[n(7)]
    S2sUndo(#[n(0)] S2sCommand),
    #[n(8)]
    S2sUpdate(#[n(0)] S2sCommand),
    #[n(9)]
    S2sAnnounce(#[n(0)] S2sCommand),
    #[n(10)]
    QueueDelivery(#[n(0)] Bytes, #[n(1)] ObjectKey),
    #[n(11)]
    ReceiveDelivery(#[n(0)] Bytes, #[n(1)] u64, #[n(2)] u64),
    #[n(12)]
    AckDelivery(#[n(0)] Bytes, #[n(1)] Bytes),
}

#[derive(Debug, Encode, Decode)]
pub(crate) struct C2sCommand {
    #[n(0)]
    pub(crate) uid: String,
    #[n(1)]
    pub(crate) act_key: ObjectKey,
    #[n(2)]
    pub(crate) obj_key: ObjectKey,
    #[n(3)]
    pub(crate) object: Object<'static>,
}

#[derive(Debug, Encode, Decode)]
pub(crate) struct S2sCommand {
    #[n(0)]
    pub(crate) uid: String,
    #[n(1)]
    pub(crate) obj_key: ObjectKey,
    #[n(2)]
    pub(crate) object: Object<'static>,
}

impl ActivityPubCommand {
    fn into_bytes(self) -> Result<Vec<u8>> {
        minicbor::to_vec(&self).context("unable to serialize apub command")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        minicbor::decode(bytes).context("unable to deserialize apub command")
    }
}

impl From<ActivityPubCommand> for LogEntryValue {
    fn from(value: ActivityPubCommand) -> Self {
        LogEntryValue::Command(value.into_bytes().unwrap())
    }
}

const MAILBOX: &str = "mailbox";

impl State {
    async fn handle_command(&mut self, command: ActivityPubCommand) -> Result<ClientResult> {
        info!(target: "apub", ?command, "received command");

        match command {
            ActivityPubCommand::UpdateUser(uid, node_value) => {
                self.handle_new_user(uid, node_value).await?;
            }
            ActivityPubCommand::C2sCreate(cmd) => {
                self.handle_c2s_create(cmd).await?;
            }
            ActivityPubCommand::S2sCreate(cmd) => {
                self.handle_s2s_create(cmd).await?;
            }
            ActivityPubCommand::S2sDelete(cmd) => {
                self.handle_s2s_delete(cmd).await?;
            }
            ActivityPubCommand::S2sLike(cmd) => {
                self.handle_s2s_like(cmd).await?;
            }
            ActivityPubCommand::S2sDislike(cmd) => {
                self.handle_s2s_dislike(cmd).await?;
            }
            ActivityPubCommand::S2sFollow(cmd) => {
                self.handle_s2s_follow(cmd).await?;
            }
            ActivityPubCommand::S2sUndo(cmd) => {
                self.handle_s2s_undo(cmd).await?;
            }
            ActivityPubCommand::S2sUpdate(cmd) => {
                self.handle_s2s_update(cmd).await?;
            }
            ActivityPubCommand::S2sAnnounce(cmd) => {
                self.handle_s2s_announce(cmd).await?;
            }
            ActivityPubCommand::QueueDelivery(key, object_key) => {
                let queue = self.queue.clone();
                spawn_blocking(move || queue.send_message(MAILBOX, key, object_key.as_ref()))
                    .await??;
            }
            ActivityPubCommand::ReceiveDelivery(receipt_handle, now, visibility_timeout) => {
                let queue = self.queue.clone();
                if let Some(res) = spawn_blocking(move || {
                    queue.receive_message(MAILBOX, receipt_handle, now, visibility_timeout)
                })
                .await??
                {
                    return Ok(ClientResult::Ok(res.to_bytes()?));
                }
            }
            ActivityPubCommand::AckDelivery(key, receipt_handle) => {
                let queue = self.queue.clone();
                spawn_blocking(move || queue.delete_message(MAILBOX, key, receipt_handle))
                    .await??;
            }
        }

        Ok(ClientResult::ok())
    }
    async fn handle_new_user(&mut self, uid: String, object: Object<'static>) -> Result<()> {
        let user = AsActor::from(object);
        let keyspace = self.keyspace.clone();
        let user_index = self.user_index.clone();

        spawn_blocking(move || -> Result<()> {
            let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
            user_index.insert(&mut b, &uid, user)?;
            b.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
    async fn handle_c2s_create(&mut self, cmd: C2sCommand) -> Result<()> {
        let C2sCommand {
            uid,
            act_key,
            obj_key,
            object,
        } = cmd;
        let create = match Create::try_from(object) {
            Ok(create) => create,
            Err(error) => {
                error!(target: "apub", %error, "invalid object");
                return Ok(());
            }
        };
        let keyspace = self.keyspace.clone();
        let iri_index = self.iri_index.clone();
        let obj_repo = self.obj_repo.clone();
        let outbox_index = self.outbox_index.clone();

        spawn_blocking(move || -> Result<()> {
            let create: Object = create.into();
            if let Some(iri) = create.get_node_iri("object") {
                if let Some(object) = iri_index
                    .find_one(iri)?
                    .and_then(|obj_key| obj_repo.find_one(obj_key).transpose())
                    .transpose()?
                {
                    let Some(update) = create.get_node_object("object") else {
                        error!(target: "apub", "activity should have an object");
                        return Ok(());
                    };
                    if update
                        .get_str("updated")
                        .or_else(|| update.get_str("published"))
                        == object
                            .get_str("updated")
                            .or_else(|| object.get_str("published"))
                    {
                        // skip
                        return Ok(());
                    }
                    let update = Update::try_from(update)?;
                    let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                    outbox_index.insert_update(&mut b, uid, act_key, update.into())?;
                    b.commit()?;
                } else {
                    let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                    outbox_index.insert_create(&mut b, uid, act_key, obj_key, create)?;
                    b.commit()?;
                }
            }
            Ok(())
        })
        .await??;

        Ok(())
    }
    async fn handle_s2s_create(&mut self, cmd: S2sCommand) -> Result<()> {
        let S2sCommand {
            obj_key, object, ..
        } = cmd;
        if object.has_props(&["context"]) {
            // currently we only care activities mentioning our object
            // TODO verify context
            let Some(iri) = object.get_str("context") else {
                return Ok(());
            };
            let iri = iri.to_string();
            // TODO let create = Create::try_from(object)?;
            // TODO save the activity and the object

            let keyspace = self.keyspace.clone();
            let obj_repo = self.obj_repo.clone();
            let ctx_index = self.ctx_index.clone();

            spawn_blocking(move || -> Result<()> {
                let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                obj_repo.insert(&mut b, obj_key, object)?;
                ctx_index.insert(&mut b, &iri, obj_key)?;
                b.commit()?;
                Ok(())
            })
            .await??;
        }
        Ok(())
    }
    async fn handle_s2s_delete(&mut self, cmd: S2sCommand) -> Result<()> {
        let _ = cmd;
        // TODO
        Ok(())
    }
    async fn handle_s2s_like(&mut self, cmd: S2sCommand) -> Result<()> {
        let S2sCommand {
            obj_key, object, ..
        } = cmd;
        if object.has_props(&["object"]) {
            let Some(iri) = object.get_node_iri("object") else {
                return Ok(());
            };
            let iri = iri.to_string();
            let keyspace = self.keyspace.clone();
            let iri_index = self.iri_index.clone();
            let obj_repo = self.obj_repo.clone();
            let ctx_index = self.ctx_index.clone();

            spawn_blocking(move || -> Result<()> {
                let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                if let Some(activity_iri) = object.id() {
                    iri_index.insert(&mut b, activity_iri, obj_key)?;
                }
                obj_repo.insert(&mut b, obj_key, object)?;
                ctx_index.insert_likes(&mut b, &iri, obj_key)?;
                b.commit()?;
                Ok(())
            })
            .await??;
        }
        Ok(())
    }
    async fn handle_s2s_dislike(&mut self, cmd: S2sCommand) -> Result<()> {
        let _ = cmd;
        Ok(())
    }
    async fn handle_s2s_follow(&mut self, cmd: S2sCommand) -> Result<()> {
        let S2sCommand {
            uid,
            obj_key,
            object,
        } = cmd;
        if object.has_props(&["object"]) {
            let keyspace = self.keyspace.clone();
            let iri_index = self.iri_index.clone();
            let obj_repo = self.obj_repo.clone();
            let user_index = self.user_index.clone();
            spawn_blocking(move || -> Result<()> {
                let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                if let Some(activity_iri) = object.id() {
                    iri_index.insert(&mut b, activity_iri, obj_key)?;
                }
                obj_repo.insert(&mut b, obj_key, object)?;
                user_index.insert_follower(&mut b, &uid, obj_key)?;
                b.commit()?;
                Ok(())
            })
            .await??;
            // TODO send Accept or Reject back
        }
        Ok(())
    }
    /// Undo previous activity.
    ///
    /// References:
    /// * <https://www.w3.org/TR/activitystreams-vocabulary/#inverse>
    /// * <https://www.w3.org/wiki/ActivityPub/Primer/Referring_to_activities>
    async fn handle_s2s_undo(&mut self, cmd: S2sCommand) -> Result<()> {
        let S2sCommand {
            uid, object: undo, ..
        } = cmd;
        let keyspace = self.keyspace.clone();
        let iri_index = self.iri_index.clone();
        let obj_repo = self.obj_repo.clone();
        let ctx_index = self.ctx_index.clone();
        let user_index = self.user_index.clone();
        spawn_blocking(move || {
            // We can undo Follow and Like
            // FIXME abstraction
            // Find the obj_key of the activity we should undo
            let mut undo_obj_key = None;
            if let Some(iri) = undo.get_node_iri("object") {
                // We have an ID, but do we know this ID?
                if let Some(slice) = iri_index.find_one(iri)? {
                    undo_obj_key = Some(ObjectKey::try_from(slice.as_ref())?);
                } else {
                    warn!(target: "apub", "unknown activity id {iri} mentioned in Undo");
                }
                if undo_obj_key.is_none() {
                    // The object does not have a known IRI.
                    // TODO do our best to find the most recent activity from the actor
                }
            }
            if let Some(undo_obj_key) = undo_obj_key {
                if let Some(activity) = obj_repo.find_one(undo_obj_key)? {
                    if let Some(object_iri) = activity.get_node_iri("object") {
                        if activity.type_is("Like") {
                            // Undo Like
                            let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                            ctx_index.remove_likes(&mut b, object_iri, undo_obj_key)?;
                            b.commit()?;
                        }
                        if activity.type_is("Follow") {
                            // Undo Follow
                            let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                            user_index.remove_follower(&mut b, &uid, undo_obj_key)?;
                            b.commit()?;
                        }
                    }
                } else {
                    warn!(target: "apub", "unknown obj_key {undo_obj_key} when trying to Undo");
                }
            }
            Ok(())
        })
        .await?
    }
    async fn handle_s2s_update(&mut self, cmd: S2sCommand) -> Result<()> {
        let S2sCommand { object: update, .. } = cmd;
        if update.has_props(&["object"]) {
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
    async fn handle_s2s_announce(&mut self, cmd: S2sCommand) -> Result<()> {
        let S2sCommand {
            obj_key,
            object: announce,
            ..
        } = cmd;
        if announce.has_props(&["object"]) {
            let Some(iri) = announce.get_node_iri("object") else {
                return Ok(());
            };
            let iri = iri.to_string();
            let keyspace = self.keyspace.clone();
            let obj_repo = self.obj_repo.clone();
            let ctx_index = self.ctx_index.clone();

            spawn_blocking(move || -> Result<()> {
                let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                obj_repo.insert(&mut b, obj_key, announce)?;
                ctx_index.insert_shares(&mut b, &iri, obj_key)?;
                b.commit()?;
                Ok(())
            })
            .await??;
        }
        Ok(())
    }
}
