use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tokio::task::block_in_place;
use tracing::{info, warn};

use crate::activity_pub::uuidgen;
use crate::worker::raft::{get_raft_local_client, ClientResult, LogEntryValue, RaftClientMsg};
use crate::RuntimeConfig;

use super::machine::ActivityPubCommand;
use super::simple_queue::{ReceiveResult, SimpleQueue};
use super::{IriIndex, ObjectRepo};

pub(crate) struct DeliveryWorker;

#[derive(RactorMessage)]
pub(crate) enum DeliveryWorkerMsg {
    RunLoop,
}

pub(crate) struct DeliveryWorkerInit {
    pub(crate) config: RuntimeConfig,
}

pub(crate) struct DeliveryWorkerState {
    iri_index: IriIndex,
    obj_repo: ObjectRepo,
    queue: SimpleQueue,
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
            let iri_index = IriIndex::new(config.keyspace.clone())?;
            let obj_repo = ObjectRepo::new(config.keyspace.clone())?;
            let queue = SimpleQueue::new(config.keyspace.clone())?;

            Ok(DeliveryWorkerState {
                iri_index,
                obj_repo,
                queue,
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
        let command = ActivityPubCommand::ReceiveDelivery(receipt_handle, 30);
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

        info!(target: "apub", ?result, "simulate delivery");

        // Ack
        let command = ActivityPubCommand::AckDelivery(result.key, receipt_handle);
        let _ = ractor::call!(
            raft_client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )?;

        Ok(true)
    }
}
