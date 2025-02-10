use anyhow::Result;
use feed_rs::model::Entry;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use serde_json::json;

use crate::activity_pub::delivery::DeliveryQueueItem;
use crate::activity_pub::machine::{ActivityPubCommand, C2sCommand};
use crate::activity_pub::model::Object;
use crate::activity_pub::{uuidgen, ObjectKey};
use crate::raft::{get_raft_local_client, LogEntryValue, RaftClientMsg};
use crate::ActivityPubConfig;

pub(crate) struct FeedSlurpWorker;

pub(crate) struct FeedSlurpWorkerInit {
    pub(crate) apub: ActivityPubConfig,
}

pub(crate) struct FeedSlurpWorkerState {
    apub: ActivityPubConfig,
}

#[derive(RactorMessage)]
pub(crate) enum FeedSlurpMsg {
    IngestFeed {
        uid: String,
        base_url: String,
        feed_url: String,
    },
}

impl Actor for FeedSlurpWorker {
    type Msg = FeedSlurpMsg;
    type State = FeedSlurpWorkerState;
    type Arguments = FeedSlurpWorkerInit;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let FeedSlurpWorkerInit { apub } = args;
        Ok(FeedSlurpWorkerState { apub })
    }
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            FeedSlurpMsg::IngestFeed {
                uid,
                base_url,
                feed_url,
            } => state.handle_ingest_feed(&uid, &base_url, &feed_url).await?,
        }
        Ok(())
    }
}

impl FeedSlurpWorkerState {
    async fn handle_ingest_feed(&self, uid: &str, base_url: &str, feed_url: &str) -> Result<()> {
        let response = reqwest::get(feed_url).await?;
        let feed_text = response.bytes().await?;
        let feed = {
            let feed_parser = feed_rs::parser::Builder::new()
                .base_uri(Some(base_url))
                .build();
            feed_parser.parse(feed_text.as_ref())?
        };
        let client = get_raft_local_client()?;
        for entry in feed.entries.iter().rev() {
            let object = object_from_feed_entry(&self.apub.base_url, uid, entry);
            let act_key = ObjectKey::new();
            let obj_key = ObjectKey::new();
            let command = ActivityPubCommand::C2sCreate(C2sCommand {
                uid: uid.to_string(),
                act_key,
                obj_key,
                object,
            });
            ractor::call!(
                client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )?;
            let command = ActivityPubCommand::QueueDelivery(
                uuidgen(),
                DeliveryQueueItem {
                    uid: uid.to_string(),
                    act_key,
                },
            );
            ractor::call!(
                client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )?;
        }
        Ok(())
    }
}

fn object_from_feed_entry(apub_base_url: &str, uid: &str, entry: &Entry) -> Object<'static> {
    let mut object = Object::from(json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        // TODO config as Note or Article
        "type": "Note",
        "actor": format!("{apub_base_url}/users/{uid}")
    }));

    if entry.id.starts_with("https") {
        object = object.augment("id", entry.id.clone().into());
    } else if !entry.links.is_empty() {
        object = object.augment("id", entry.links[0].href.clone().into());
    }

    // TODO use tera template for formatting
    let mut content = String::new();
    content.push_str(
        entry
            .title
            .as_ref()
            .map(|txt| txt.content.as_str())
            .unwrap_or(""),
    );
    content.push_str("<br/><br/>");

    if let Some(summary) = &entry.summary {
        content.push_str(&summary.content);
    } else if let Some(entry_content) = &entry.content {
        if let Some(body) = &entry_content.body {
            content.push_str(body);
        }
    }

    object = object.augment("content", content.into());
    // TODO convert category to tags

    if let Some(published) = entry.published {
        let timestamp = published.to_rfc3339();
        object = object.augment("published", timestamp.into());
    }

    object = object.augment(
        "url",
        entry
            .links
            .iter()
            .map(|link| link.href.clone())
            .collect::<Vec<_>>()
            .into(),
    );

    object = object.augment(
        "to",
        vec![
            format!("{apub_base_url}/users/{uid}/followers"),
            "https://www.w3.org/ns/activitystreams#Public".to_string(),
        ]
        .into(),
    );

    object
}
