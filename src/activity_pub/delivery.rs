use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tokio::task::{block_in_place, spawn_blocking, JoinSet};
use tracing::warn;

use crate::activity_pub::uuidgen;
use crate::raft::{get_raft_local_client, ClientResult, LogEntryValue, RaftClientMsg};
use crate::RuntimeConfig;

use super::machine::ActivityPubCommand;
use super::mailman::Mailman;
use super::model::Object;
use super::simple_queue::{ReceiveResult, SimpleQueue};
use super::ObjectRepo;

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
        block_in_place(|| {
            let obj_repo = ObjectRepo::new(config.keyspace.clone())?;
            let queue = SimpleQueue::new(config.keyspace.clone())?;
            let mailman = Mailman::new();

            Ok(DeliveryWorkerState {
                obj_repo,
                queue,
                mailman,
            })
        })
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
                match state.handle_delivery().await {
                    Ok(true) => {
                        // There might be more work to do, immediately schedule next loop
                        ractor::cast!(myself, DeliveryWorkerMsg::RunLoop)?;
                    }
                    Ok(false) => {
                        myself.send_after(RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
                    }
                    Err(error) => {
                        warn!(target: "apub", %error, "delivery loop failed");
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
        let message = result.message;
        let obj_key = message.body;
        let obj_repo = self.obj_repo.clone();
        if let Some(object) = spawn_blocking(move || obj_repo.find_one(obj_key)).await?? {
            // Collect recipients
            let mut recipients = vec![];
            for target in ["to", "bto", "cc", "bcc", "audience"] {
                if let Some(iri_array) = object.get_str_array(&target) {
                    iri_array.iter().for_each(|&iri| recipients.push(iri));
                    continue;
                }
                if let Some(iri) = object.get_node_iri(&target) {
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
                    inboxes.extend(self.discover_inboxes(&object).await?);
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
            let object = Arc::new(object);
            let mut join_set = JoinSet::new();
            for inbox in inboxes {
                let mailman = self.mailman.clone();
                let object = object.clone();
                join_set.spawn(async move { mailman.post(&inbox, object.as_ref()).await });
            }
            join_set.join_all().await;
        }
        // Ack
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
