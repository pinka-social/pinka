use anyhow::{Context, Result};
use fjall::{Keyspace, PersistMode};
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use raft::{ClientResult, LogEntryValue, RaftAppliedMsg, StateMachineMsg, get_raft_applied};
use tokio::task::spawn_blocking;
use tracing::{error, info, warn};
use uuid::Bytes;

use crate::ActivityPubConfig;

use super::delivery::DeliveryQueueItem;
use super::model::{Actor as AsActor, Create, Object, Update};
use super::repo::{ContextIndex, CryptoRepo, KeyMaterial, OutboxIndex};
use super::simple_queue::SimpleQueue;
use super::{IriIndex, ObjectKey, ObjectRepo, UserIndex};

pub(crate) struct ActivityPubMachine;

pub(crate) struct State {
    apub: ActivityPubConfig,
    keyspace: Keyspace,
    user_index: UserIndex,
    outbox_index: OutboxIndex,
    ctx_index: ContextIndex,
    iri_index: IriIndex,
    obj_repo: ObjectRepo,
    crypto_repo: CryptoRepo,
    queue: SimpleQueue,
}

pub(crate) struct ActivityPubMachineInit {
    pub(crate) apub: ActivityPubConfig,
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
        let ActivityPubMachineInit { apub, keyspace } = args;
        spawn_blocking(move || {
            let user_index = UserIndex::new(keyspace.clone())?;
            let outbox_index = OutboxIndex::new(keyspace.clone())?;
            let ctx_index = ContextIndex::new(keyspace.clone())?;
            let iri_index = IriIndex::new(keyspace.clone())?;
            let obj_repo = ObjectRepo::new(keyspace.clone())?;
            let crypto_repo = CryptoRepo::new(keyspace.clone())?;
            let queue = SimpleQueue::new(keyspace.clone())?;
            Ok(State {
                apub,
                keyspace,
                user_index,
                outbox_index,
                ctx_index,
                iri_index,
                obj_repo,
                crypto_repo,
                queue,
            })
        })
        .await
        .context("Failed to create ActivityPubMachine")?
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
                    let result = match ActivityPubCommand::from_bytes(&byte_buf) {
                        Ok(command) => state.handle_command(command).await?,
                        Err(error) => {
                            error!("failed to deserialize command: {error:#}");
                            ClientResult::err()
                        }
                    };
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
    // ===== 0..10 queue commands =====
    #[n(0)]
    QueueDelivery(#[n(0)] Bytes, #[n(1)] DeliveryQueueItem),
    #[n(1)]
    ReceiveDelivery(#[n(0)] Bytes, #[n(1)] u64, #[n(2)] u64),
    #[n(2)]
    AckDelivery(#[n(0)] Bytes, #[n(1)] Bytes),

    // ===== 10..32 server to server interactions =====
    #[n(10)]
    S2sCreate(#[n(0)] S2sCommand),
    #[n(11)]
    S2sDelete(#[n(0)] S2sCommand),
    #[n(12)]
    S2sLike(#[n(0)] S2sCommand),
    #[n(13)]
    S2sDislike(#[n(0)] S2sCommand),
    #[n(14)]
    S2sFollow(#[n(0)] S2sCommand),
    #[n(15)]
    S2sUndo(#[n(0)] S2sCommand),
    #[n(16)]
    S2sUpdate(#[n(0)] S2sCommand),
    #[n(17)]
    S2sAnnounce(#[n(0)] S2sCommand),

    // ===== 32..100 reserved =====

    // ===== 100..200 admin commands =====
    #[n(100)]
    UpdateUser(
        #[n(0)] String,
        #[n(1)] Object<'static>,
        #[n(2)] Option<KeyMaterial>,
    ),
    #[n(101)]
    GetFeedSlurpLock(#[n(0)] u64),

    // ===== 200..256 client to server interactions =====
    /// Client to Server - Create Activity
    #[n(200)]
    C2sCreate(#[n(0)] C2sCommand),
    /// Client to Server - Add Activity
    #[n(201)]
    C2sAccept(#[n(0)] C2sCommand),
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
        minicbor::to_vec(&self).context("Unable to serialize apub command")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        minicbor::decode(bytes).context("Unable to deserialize apub command")
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
        // TODO refine logging
        info!(?command, "received command");

        match command {
            ActivityPubCommand::UpdateUser(uid, object, key_material) => {
                self.handle_update_user(uid, object, key_material)
                    .await
                    .context("Failed to handle UpdateUser command")?;
            }
            ActivityPubCommand::C2sCreate(cmd) => {
                return self
                    .handle_c2s_create(cmd)
                    .await
                    .context("Failed to handle C2sCreate command");
            }
            ActivityPubCommand::C2sAccept(cmd) => {
                self.handle_c2s_accept(cmd)
                    .await
                    .context("Failed to handle C2sAccept command")?;
            }
            ActivityPubCommand::S2sCreate(cmd) => {
                self.handle_s2s_create(cmd)
                    .await
                    .context("Failed to handle S2sCreate command")?;
            }
            ActivityPubCommand::S2sDelete(cmd) => {
                self.handle_s2s_delete(cmd)
                    .await
                    .context("Failed to handle S2sDelete command")?;
            }
            ActivityPubCommand::S2sLike(cmd) => {
                self.handle_s2s_like(cmd)
                    .await
                    .context("Failed to handle S2sLike command")?;
            }
            ActivityPubCommand::S2sDislike(cmd) => {
                self.handle_s2s_dislike(cmd)
                    .await
                    .context("Failed to handle S2sDislike command")?;
            }
            ActivityPubCommand::S2sFollow(cmd) => {
                self.handle_s2s_follow(cmd)
                    .await
                    .context("Failed to handle S2sFollow command")?;
            }
            ActivityPubCommand::S2sUndo(cmd) => {
                self.handle_s2s_undo(cmd)
                    .await
                    .context("Failed to handle S2sUndo command")?;
            }
            ActivityPubCommand::S2sUpdate(cmd) => {
                self.handle_s2s_update(cmd)
                    .await
                    .context("Failed to handle S2sUpdate command")?;
            }
            ActivityPubCommand::S2sAnnounce(cmd) => {
                self.handle_s2s_announce(cmd)
                    .await
                    .context("Failed to handle S2sAnnounce command")?;
            }
            ActivityPubCommand::QueueDelivery(key, item) => {
                let queue = self.queue.clone();
                let bytes = item.to_bytes()?;
                spawn_blocking(move || queue.send_message(MAILBOX, key, bytes))
                    .await
                    .context("Failed to handle QueueDelivery command")??;
            }
            ActivityPubCommand::ReceiveDelivery(receipt_handle, now, visibility_timeout) => {
                let queue = self.queue.clone();
                if let Some(res) = spawn_blocking(move || {
                    queue.receive_message(MAILBOX, receipt_handle, now, visibility_timeout)
                })
                .await
                .context("Failed to handle ReceiveDelivery command")??
                {
                    return Ok(ClientResult::Ok(res.to_bytes()?));
                }
            }
            ActivityPubCommand::AckDelivery(key, receipt_handle) => {
                let queue = self.queue.clone();
                spawn_blocking(move || queue.delete_message(MAILBOX, key, receipt_handle))
                    .await
                    .context("Failed to handle AckDelivery command")??;
            }
            ActivityPubCommand::GetFeedSlurpLock(now) => {
                let queue_name = "feed_slurp_lock";
                let lock_key = [255u8; 16];
                let queue = self.queue.clone();
                return spawn_blocking(move || {
                    if !queue.has_message(queue_name, lock_key)? {
                        queue.send_message(queue_name, lock_key, [])?;
                    }
                    if queue
                        .receive_message(queue_name, lock_key, now, 5 * 60)?
                        .is_some()
                    {
                        return Ok(ClientResult::ok());
                    }
                    return Ok(ClientResult::err());
                })
                .await?;
            }
        }

        Ok(ClientResult::ok())
    }
    async fn handle_update_user(
        &mut self,
        uid: String,
        object: Object<'static>,
        key_material: Option<KeyMaterial>,
    ) -> Result<()> {
        let user = AsActor::from(object);
        let keyspace = self.keyspace.clone();
        let user_index = self.user_index.clone();
        let crypto_repo = self.crypto_repo.clone();

        spawn_blocking(move || -> Result<()> {
            let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
            user_index.insert(&mut b, &uid, user)?;
            if let Some(key_pair) = key_material {
                crypto_repo.insert(&mut b, &uid, &key_pair);
            }
            b.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
    async fn handle_c2s_create(&mut self, cmd: C2sCommand) -> Result<ClientResult> {
        let C2sCommand {
            uid,
            act_key,
            obj_key,
            object,
        } = cmd;
        let create = match Create::try_from(object) {
            Ok(create) => create,
            Err(error) => {
                error!(?error, "invalid object");
                return Ok(ClientResult::err());
            }
        };
        let base_url = self.apub.base_url.clone();
        let keyspace = self.keyspace.clone();
        let iri_index = self.iri_index.clone();
        let obj_repo = self.obj_repo.clone();
        let outbox_index = self.outbox_index.clone();

        spawn_blocking(move || {
            let create: Object = create.into();
            if let Some(iri) = create.get_node_iri("object") {
                if let Some(obj_key) = iri_index.find_one(iri)? {
                    info!(%iri, "activity mentions an existing object");
                    let object = obj_repo
                        .find_one(obj_key)?
                        .context("Failed to load existing object")?;
                    let Some(update) = create.get_node_object("object") else {
                        error!("activity should have an object");
                        return Ok(ClientResult::err());
                    };
                    if update
                        .get_str("updated")
                        .or_else(|| update.get_str("published"))
                        == object
                            .get_str("updated")
                            .or_else(|| object.get_str("published"))
                    {
                        info!("activity is not newer than the object, skipping");
                        return Ok(ClientResult::err());
                    }
                    // FIXME where should we ensure id and actor?
                    let update = Update::try_from(update)?
                        .ensure_id(format!("{}/as/objects/{act_key}", base_url))
                        .with_actor(format!("{}/users/{uid}", base_url));
                    let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                    outbox_index.insert_update(&mut b, uid, act_key, update.into())?;
                    b.commit()?;
                } else {
                    info!(%iri, "activity mentions a new object");
                    let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                    outbox_index.insert_create(&mut b, uid, act_key, obj_key, create)?;
                    b.commit()?;
                }
            }
            Ok(ClientResult::ok())
        })
        .await?
    }
    async fn handle_c2s_accept(&mut self, cmd: C2sCommand) -> Result<()> {
        let C2sCommand {
            uid: _,
            act_key,
            obj_key: _,
            object,
        } = cmd;
        let mut batch = self.keyspace.batch().durability(Some(PersistMode::SyncAll));
        let obj_repo = self.obj_repo.clone();
        spawn_blocking(move || -> Result<()> {
            obj_repo.insert(&mut batch, act_key, object)?;
            batch.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
    async fn handle_s2s_create(&mut self, cmd: S2sCommand) -> Result<()> {
        let S2sCommand {
            obj_key, object, ..
        } = cmd;
        let Some(object) = object.get_node_object("object") else {
            warn!("c2s activity should have an object but it does not");
            return Ok(());
        };
        // currently we only care activities mentioning our object
        let mut contexts = vec![];
        for ctx_node in ["context", "conversation", "inReplyTo"] {
            if let Some(iri) = object.get_node_iri(ctx_node) {
                if iri.starts_with(self.apub.base_url.as_str()) {
                    contexts.push(iri.to_string());
                }
            }
        }
        if contexts.is_empty() {
            return Ok(());
        }

        let object = object.into_owned();
        let keyspace = self.keyspace.clone();
        let obj_repo = self.obj_repo.clone();
        let ctx_index = self.ctx_index.clone();

        spawn_blocking(move || -> Result<()> {
            let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
            obj_repo.insert(&mut b, obj_key, object)?;
            for iri in contexts {
                ctx_index.insert(&mut b, &iri, obj_key);
            }
            b.commit()?;
            Ok(())
        })
        .await??;
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
                    iri_index.insert(&mut b, activity_iri, obj_key);
                }
                obj_repo.insert(&mut b, obj_key, object)?;
                ctx_index.insert_likes(&mut b, &iri, obj_key);
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
            // TODO verify object is the actor IRI
            let keyspace = self.keyspace.clone();
            let iri_index = self.iri_index.clone();
            let obj_repo = self.obj_repo.clone();
            let user_index = self.user_index.clone();
            spawn_blocking(move || -> Result<()> {
                let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                if let Some(activity_iri) = object.id() {
                    iri_index.insert(&mut b, activity_iri, obj_key);
                }
                obj_repo.insert(&mut b, obj_key, object)?;
                user_index.insert_follower(&mut b, &uid, obj_key);
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
                    warn!("unknown activity id {iri} mentioned in Undo");
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
                            ctx_index.remove_likes(&mut b, object_iri, undo_obj_key);
                            b.commit()?;
                        }
                        if activity.type_is("Follow") {
                            // Undo Follow
                            let mut b = keyspace.batch().durability(Some(PersistMode::SyncAll));
                            user_index.remove_follower(&mut b, &uid, undo_obj_key);
                            b.commit()?;
                        }
                    }
                } else {
                    warn!("unknown obj_key {undo_obj_key} when trying to Undo");
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
                ctx_index.insert_shares(&mut b, &iri, obj_key);
                b.commit()?;
                Ok(())
            })
            .await??;
        }
        Ok(())
    }
}
