use std::time::Duration;

use anyhow::{Context, Result};
use aws_lc_rs::rsa::KeyPair;
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use secrecy::ExposeSecret;
use tokio::task::{JoinSet, spawn_blocking};
use tracing::{error, info, warn};

use crate::RuntimeConfig;
use crate::activity_pub::uuidgen;
use crate::raft::{ClientResult, LogEntryValue, RaftClientMsg, get_raft_local_client};

use super::machine::ActivityPubCommand;
use super::mailman::Mailman;
use super::model::Object;
use super::simple_queue::{ReceiveResult, SimpleQueue};
use super::{CryptoRepo, ObjectKey, ObjectRepo, hs2019};

pub(crate) struct DeliveryWorker;

#[derive(RactorMessage)]
pub(crate) enum DeliveryWorkerMsg {
    RunLoop,
}

pub(crate) struct DeliveryWorkerInit {
    pub(crate) config: RuntimeConfig,
}

pub(crate) struct DeliveryWorkerState {
    obj_repo: ObjectRepo,
    crypto_repo: CryptoRepo,
    queue: SimpleQueue,
    mailman: Mailman,
}

impl Actor for DeliveryWorker {
    type Msg = DeliveryWorkerMsg;
    type State = DeliveryWorkerState;
    type Arguments = DeliveryWorkerInit;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let DeliveryWorkerInit { config } = args;
        let keyspace = config.keyspace.clone();
        spawn_blocking(move || {
            let obj_repo = ObjectRepo::new(keyspace.clone())?;
            let crypto_repo = CryptoRepo::new(keyspace.clone())?;
            let queue = SimpleQueue::new(keyspace.clone())?;
            let mailman = Mailman::new();

            Ok(DeliveryWorkerState {
                obj_repo,
                crypto_repo,
                queue,
                mailman,
            })
        })
        .await
        .context("Failed to create DeliveryWorker")?
    }
    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        myself.send_after(RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
        Ok(())
    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            DeliveryWorkerMsg::RunLoop => {
                match state
                    .handle_delivery()
                    .await
                    .context("Failed to handle delivery")
                {
                    // FIXME use enum
                    Ok(true) => {
                        // There might be more work to do, immediately schedule next loop
                        ractor::cast!(myself, DeliveryWorkerMsg::RunLoop)?;
                    }
                    Ok(false) => {
                        myself.send_after(RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
                    }
                    Err(error) => {
                        warn!("{error:#}");
                        warn!("delivery failed, will retry in {:?}", RETRY_TIMEOUT);
                        myself.send_after(RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
                    }
                }
            }
        }
        Ok(())
    }
}

const RETRY_TIMEOUT: Duration = Duration::from_secs(30);

impl DeliveryWorkerState {
    async fn handle_delivery(&mut self) -> Result<bool> {
        // Sleep if our local replicated queue is empty
        if self.queue.is_empty()? {
            return Ok(false);
        }
        // Pull new work
        let raft_client = get_raft_local_client()?;
        let receipt_handle = uuidgen();
        let command = ActivityPubCommand::ReceiveDelivery(receipt_handle, SimpleQueue::now(), 30);
        let client_result = ractor::call!(
            raft_client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )?;
        let ClientResult::Ok(bytes) = client_result else {
            return Ok(false);
        };
        if bytes.is_empty() {
            return Ok(false);
        }
        let result = ReceiveResult::from_bytes(&bytes)?;

        // Retry limited times
        // TODO: make this configurable
        let retry_count = result.message.approximate_receive_count;
        if retry_count > 10 {
            warn!("retried {retry_count} times, giving up");
            let command = ActivityPubCommand::AckDelivery(result.key, receipt_handle);
            let _ = ractor::call!(
                raft_client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )?;
            return Ok(false);
        }

        let message = result.message;
        let item = DeliveryQueueItem::from_bytes(&message.body)?;

        // Load signing key
        let uid = item.uid.clone();
        let crypto_repo = self.crypto_repo.clone();
        let Some(key_material) = spawn_blocking(move || crypto_repo.find_one(&uid)).await?? else {
            return Ok(false);
        };

        let obj_repo = self.obj_repo.clone();
        if let Some(object) = spawn_blocking(move || obj_repo.find_one(item.act_key)).await?? {
            // Get actor IRI
            let Some(actor_iri) = object.get_node_iri("actor") else {
                warn!(?object, "cannot deliver activity without actor property");
                let command = ActivityPubCommand::AckDelivery(result.key, receipt_handle);
                let _ = ractor::call!(
                    raft_client,
                    RaftClientMsg::ClientRequest,
                    LogEntryValue::from(command)
                )?;
                return Ok(false);
            };
            // Collect recipients
            let mut recipients = vec![];
            for target in ["to", "bto", "cc", "bcc", "audience"] {
                if let Some(iri_array) = object.get_str_array(target) {
                    iri_array.iter().for_each(|&iri| recipients.push(iri));
                    continue;
                }
                if let Some(iri) = object.get_node_iri(target) {
                    recipients.push(iri);
                }
            }
            // Convert to inbox
            let mut inboxes = vec![];
            for iri in recipients {
                // 5.6 Skip public addressing
                if iri == "https://www.w3.org/ns/activitystreams#Public"
                    || iri == "as:Public"
                    || iri == "Public"
                {
                    continue;
                }
                let value = self.mailman.fetch(iri).await?;
                let object = Object::from(value);
                if object.type_is("Collection") || object.type_is("OrderedCollection") {
                    inboxes.extend(
                        self.discover_inboxes(&object)
                            .await
                            .context("Failed to discover inboxes")?,
                    );
                    continue;
                }
                if let Some(inbox) = object.get_str("inbox") {
                    inboxes.push(inbox.to_string());
                }
            }

            // De-duplicate the final recipient list
            inboxes.sort();
            inboxes.dedup();

            // Remove self and attributedTo and origin actor
            // TODO

            // Deliver
            let mut join_set = JoinSet::new();
            for inbox in inboxes {
                let body = object.to_string();
                let actor_iri = actor_iri.to_string();
                let key_pair = KeyPair::from_pkcs8(key_material.expose_secret())?;
                let mailman = self.mailman.clone();
                join_set.spawn(async move {
                    info!(%actor_iri, %inbox, "delivering activity");
                    let headers = hs2019::post_headers(&actor_iri, &inbox, &body, &key_pair)
                        .expect("unable to sign http request");
                    mailman.post(&inbox, headers, &body).await
                });
            }
            let mut success = true;
            for result in join_set.join_all().await {
                if let Err(error) = result {
                    error!(?error, "failed to deliver activity");
                    success = false;
                }
            }
            if !success {
                return Ok(false);
            }
        } else {
            error!(obj_key=%item.act_key, "cannot find object");
        }
        // Ack
        // TODO always ack in case of unrecoverable error
        let command = ActivityPubCommand::AckDelivery(result.key, receipt_handle);
        let _ = ractor::call!(
            raft_client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )?;

        Ok(true)
    }

    async fn discover_inboxes(&self, object: &Object<'_>) -> Result<Vec<String>> {
        let mut next = object.get_str("first").map(str::to_string);

        let mut result_set = JoinSet::new();
        while let Some(iri) = next {
            let value = self.mailman.fetch(&iri).await?;
            let page = Object::from(value);
            let items = page
                .get_str_array("items")
                .or_else(|| page.get_str_array("orderedItems"));
            if let Some(items) = items {
                for item in items {
                    let mailman = self.mailman.clone();
                    let iri = item.to_string();
                    result_set.spawn(async move {
                        if let Ok(value) = mailman.fetch(&iri).await {
                            let object = Object::from(value);
                            // skip nested collections
                            object
                                .get_endpoint("sharedInbox")
                                .or_else(|| object.get_str("inbox"))
                                .map(str::to_string)
                        } else {
                            None
                        }
                    });
                }
            }
            next = page.get_str("next").map(str::to_string);
        }
        let result = result_set.join_all().await.into_iter().flatten().collect();
        Ok(result)
    }
}

#[derive(Debug, Encode, Decode)]
pub(crate) struct DeliveryQueueItem {
    #[n(0)]
    pub(crate) uid: String,
    #[n(1)]
    pub(crate) act_key: ObjectKey,
}

impl DeliveryQueueItem {
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        minicbor::to_vec(self).context("Failed to encode DeliveryQueueItem")
    }
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        minicbor::decode(bytes).context("Failed to decode DeliveryQueueItem")
    }
}
