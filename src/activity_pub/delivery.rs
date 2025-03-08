use std::time::Duration;

use anyhow::{Context, Error, Result, bail};
use aws_lc_rs::rsa::KeyPair;
use minicbor::{Decode, Encode};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use raft::{ClientResult, LogEntryValue, RaftClientMsg, get_raft_local_client};
use secrecy::ExposeSecret;
use tokio::task::{JoinSet, spawn_blocking};
use tracing::{debug, info, warn};

use crate::RuntimeConfig;
use crate::activity_pub::uuidgen;

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
                    Ok(schedule) => {
                        myself.send_after(schedule, || DeliveryWorkerMsg::RunLoop);
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

const IMMEDIATE: Duration = Duration::ZERO;
const RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u64 = 10;

impl DeliveryWorkerState {
    /// Returns next scheduled time
    async fn handle_delivery(&mut self) -> Result<Duration> {
        // Sleep if our local replicated queue is empty
        let queue = self.queue.clone();
        if spawn_blocking(move || queue.is_empty()).await?? {
            debug!("queue is empty, sleeping for {RETRY_TIMEOUT:?}");
            return Ok(RETRY_TIMEOUT);
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
            bail!("Failed to receive delivery, raft client returned error");
        };
        if bytes.is_empty() {
            // No work to do
            debug!("no work to do, sleeping for {RETRY_TIMEOUT:?}");
            return Ok(RETRY_TIMEOUT);
        }
        let result = ReceiveResult::from_bytes(&bytes)?;

        // Retry limited times
        // TODO: make this configurable
        let retry_count = result.message.approximate_receive_count;
        if retry_count > MAX_RETRIES {
            warn!("retried {retry_count} times, giving up");
            let command = ActivityPubCommand::AckDelivery(result.key, receipt_handle);
            let _ = ractor::call!(
                raft_client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )?;
            debug!("giving up, sleeping for {RETRY_TIMEOUT:?}");
            return Ok(RETRY_TIMEOUT);
        }

        let message = result.message;
        let item = DeliveryQueueItem::from_bytes(&message.body)?;

        // Load signing key
        let uid = item.uid.clone();
        let crypto_repo = self.crypto_repo.clone();
        let Some(key_material) = spawn_blocking(move || crypto_repo.find_one(&uid)).await?? else {
            bail!("cannot find key material for {}", item.uid);
        };
        // Load activity
        let obj_repo = self.obj_repo.clone();
        let Some(object) = spawn_blocking(move || obj_repo.find_one(item.act_key)).await?? else {
            bail!("cannot find object {}", item.act_key);
        };
        // Get actor IRI
        let Some(actor_iri) = object.get_node_iri("actor") else {
            warn!(
                ?object,
                "cannot deliver activity without actor property, skipping"
            );
            let command = ActivityPubCommand::AckDelivery(result.key, receipt_handle);
            let _ = ractor::call!(
                raft_client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )?;
            debug!("skipping, sleeping for {RETRY_TIMEOUT:?}");
            return Ok(RETRY_TIMEOUT);
        };
        // Collect recipients
        let recipients = match &item.retry_targets {
            Some(recipients) => {
                let recipients = recipients
                    .iter()
                    .filter_map(|target| match target {
                        RetryTarget::Recipient(iri) => Some(iri.to_string()),
                        RetryTarget::Inbox(_) => None,
                    })
                    .collect();
                info!(%actor_iri, ?recipients, "retrying delivery to recipients");
                recipients
            }
            None => {
                let mut recipients = vec![];
                for target in ["to", "bto", "cc", "bcc", "audience"] {
                    if let Some(iri_array) = object.get_str_array(target) {
                        iri_array
                            .iter()
                            .for_each(|&iri| recipients.push(iri.to_string()));
                        continue;
                    }
                    if let Some(iri) = object.get_node_iri(target) {
                        recipients.push(iri.to_string());
                    }
                }
                recipients
            }
        };
        // Convert to inbox
        let mut inboxes = match &item.retry_targets {
            Some(targets) => {
                let inboxes = targets
                    .iter()
                    .filter_map(|target| match target {
                        RetryTarget::Inbox(inbox) => Some(inbox.to_string()),
                        RetryTarget::Recipient(_) => None,
                    })
                    .collect();
                info!(%actor_iri, ?inboxes, "retrying delivery to inboxes");
                inboxes
            }
            None => vec![],
        };
        let mut failed_targets = vec![];
        for iri in recipients {
            // 5.6 Skip public addressing
            if iri == "https://www.w3.org/ns/activitystreams#Public"
                || iri == "as:Public"
                || iri == "Public"
            {
                continue;
            }
            let value = match self.mailman.fetch(&iri).await {
                Ok(value) => value,
                Err(error) => {
                    warn!("failed to fetch remote object: {error:#}");
                    failed_targets.push(RetryTarget::Recipient(iri.to_string()));
                    continue;
                }
            };
            let object = Object::from(value);
            if object.type_is("Collection") || object.type_is("OrderedCollection") {
                let new_inboxes = match self.discover_inboxes(&object).await {
                    Ok(inboxes) => inboxes,
                    Err(error) => {
                        warn!("failed to discover inboxes: {error:#}");
                        failed_targets.push(RetryTarget::Recipient(iri.to_string()));
                        continue;
                    }
                };
                inboxes.extend(new_inboxes);
                continue;
            }
            if let Some(inbox) = object
                .get_endpoint("sharedInbox")
                .or_else(|| object.get_str("inbox"))
            {
                inboxes.push(inbox.to_string());
                continue;
            }
            warn!("no inbox found for {}, skipping", iri);
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
                mailman
                    .post(&inbox, headers, &body)
                    .await
                    .map_err(|error| DeliveryError { inbox, error })
            });
        }
        for result in join_set.join_all().await {
            if let Err(delivery_error) = result {
                warn!(
                    "failed to deliver activity to {inbox}: {error:#}",
                    inbox = delivery_error.inbox,
                    error = delivery_error.error
                );
                failed_targets.push(RetryTarget::Inbox(delivery_error.inbox))
            }
        }
        let mut schedule = IMMEDIATE;
        if !failed_targets.is_empty() {
            schedule = RETRY_TIMEOUT;
            let command = ActivityPubCommand::QueueDelivery(
                uuidgen(),
                DeliveryQueueItem {
                    uid: item.uid,
                    act_key: item.act_key,
                    retry_targets: Some(failed_targets),
                },
            );
            let _ = ractor::call!(
                raft_client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )?;
        }
        // Ack
        let command = ActivityPubCommand::AckDelivery(result.key, receipt_handle);
        let _ = ractor::call!(
            raft_client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )?;
        debug!("delivery handled, sleeping for {schedule:?}");
        Ok(schedule)
    }

    async fn discover_inboxes(&self, object: &Object<'_>) -> Result<Vec<String>> {
        let mut next = object.get_str("first").map(str::to_string);

        let mut result_set = JoinSet::new();
        while let Some(iri) = next {
            info!(%iri, "fetching collection");
            let value = self.mailman.fetch(&iri).await?;
            let page = Object::from(value);
            let items = page
                .get_str_array("items")
                .or_else(|| page.get_str_array("orderedItems"));
            let Some(items) = items else {
                break;
            };
            if items.is_empty() {
                break;
            }
            info!(?items, "found items");
            for item in items {
                let mailman = self.mailman.clone();
                let iri = item.to_string();
                result_set.spawn(async move {
                    let value = mailman.fetch(&iri).await?;
                    let object = Object::from(value);
                    // skip nested collections
                    Ok(object
                        .get_endpoint("sharedInbox")
                        .or_else(|| object.get_str("inbox"))
                        .map(str::to_string))
                });
            }
            next = page.get_str("next").map(str::to_string);
        }
        let mut inboxes = Vec::new();
        for res in result_set.join_all().await {
            match res {
                Ok(Some(inbox)) => inboxes.push(inbox),
                Ok(None) => continue,
                Err(err) => return Err(err),
            }
        }
        info!(?inboxes, "discovered inboxes");
        Ok(inboxes)
    }
}

#[derive(Debug, Encode, Decode)]
pub(crate) struct DeliveryQueueItem {
    #[n(0)]
    pub(crate) uid: String,
    #[n(1)]
    pub(crate) act_key: ObjectKey,
    #[n(2)]
    pub(crate) retry_targets: Option<Vec<RetryTarget>>,
}

#[derive(Debug, Encode, Decode)]
pub(crate) enum RetryTarget {
    #[n(0)]
    Recipient(#[n(0)] String),
    #[n(1)]
    Inbox(#[n(1)] String),
}

impl DeliveryQueueItem {
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        minicbor::to_vec(self).context("Failed to encode DeliveryQueueItem")
    }
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        minicbor::decode(bytes).context("Failed to decode DeliveryQueueItem")
    }
}

struct DeliveryError {
    inbox: String,
    error: Error,
}
