use std::time::Duration;

use anyhow::Result;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tokio::task::block_in_place;
use tracing::info;

use crate::activity_pub::uuidgen;
use crate::worker::raft::{get_raft_local_client, ClientResult, LogEntryValue, RaftClientMsg};
use crate::RuntimeConfig;

use super::machine::ActivityPubCommand;
use super::simple_queue::ReceiveResult;
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

            Ok(DeliveryWorkerState {
                iri_index,
                obj_repo,
            })
        })
    }
    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        myself.send_after(Self::State::RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
        Ok(())
    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            DeliveryWorkerMsg::RunLoop => state.handle_delivery(myself).await?,
        }
        Ok(())
    }
}

impl DeliveryWorkerState {
    const RETRY_TIMEOUT: Duration = Duration::from_secs(30);
    async fn handle_delivery(&mut self, myself: ActorRef<DeliveryWorkerMsg>) -> Result<()> {
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
            myself.send_after(Self::RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
            return Ok(());
        };
        if bytes.is_empty() {
            myself.send_after(Self::RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
            return Ok(());
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

        // Next loop
        myself.send_after(Self::RETRY_TIMEOUT, || DeliveryWorkerMsg::RunLoop);
        Ok(())
    }
}
