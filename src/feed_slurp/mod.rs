mod filters;

use anyhow::{Context, Result};
use feed_rs::model::Entry;
use minijinja::{Environment, Template};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use raft::{LogEntryValue, RaftClientMsg, get_raft_local_client};
use serde::Deserialize;
use serde_json::json;
use tracing::info;

use crate::ActivityPubConfig;
use crate::activity_pub::delivery::DeliveryQueueItem;
use crate::activity_pub::machine::{ActivityPubCommand, C2sCommand};
use crate::activity_pub::model::{Create, Object};
use crate::activity_pub::{ObjectKey, uuidgen};

use self::filters::{excerpt, to_text};

pub(crate) struct FeedSlurpWorker;

pub(crate) struct FeedSlurpWorkerInit {
    pub(crate) apub: ActivityPubConfig,
}

pub(crate) struct FeedSlurpWorkerState {
    apub: ActivityPubConfig,
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub(crate) struct IngestFeed {
    pub(crate) uid: String,
    pub(crate) base_url: String,
    pub(crate) feed_url: String,
    pub(crate) last: Option<usize>,
    pub(crate) template: Option<String>,
    pub(crate) dry_run: bool,
}

#[derive(RactorMessage)]
pub(crate) enum FeedSlurpMsg {
    IngestFeed(IngestFeed),
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
            FeedSlurpMsg::IngestFeed(ingest_feed) => state
                .handle_ingest_feed(&ingest_feed)
                .await
                .context("Failed to ingest feed")?,
        }
        Ok(())
    }
}

impl FeedSlurpWorkerState {
    async fn handle_ingest_feed(&self, ingest_feed: &IngestFeed) -> Result<()> {
        let IngestFeed {
            uid,
            base_url,
            feed_url,
            last,
            template,
            dry_run,
        } = ingest_feed;
        let response = reqwest::get(feed_url).await?;
        let feed_text = response.bytes().await?;
        let feed = {
            let feed_parser = feed_rs::parser::Builder::new()
                .base_uri(Some(base_url))
                .build();
            feed_parser.parse(feed_text.as_ref())?
        };
        let client = get_raft_local_client()?;
        let template = template.as_deref().unwrap_or(DEFAULT_TEMPLATE);
        let mut env = Environment::new();
        env.add_filter("to_text", to_text);
        env.add_filter("excerpt", excerpt);
        let jinja = env.template_from_str(template)?;
        for entry in feed.entries.iter().take(last.unwrap_or(usize::MAX)).rev() {
            let object = object_from_feed_entry(&self.apub.base_url, uid, entry, &jinja);
            let act_key = ObjectKey::new();
            let obj_key = ObjectKey::new();
            let iri = format!("{}/as/objects/{obj_key}", &self.apub.base_url);
            let object = object
                .ensure_id(iri.clone())
                .augment("context", iri.clone().into())
                .augment("conversation", iri.clone().into());
            let create = Create::try_from(object)?
                .ensure_id(format!("{}/as/objects/{act_key}", &self.apub.base_url))
                .with_actor(format!("{}/users/{uid}", &self.apub.base_url));
            if *dry_run {
                info!(
                    "dry-run ingest entry {}",
                    serde_json::to_string_pretty(&create.to_value())?
                );
                continue;
            }
            let command = ActivityPubCommand::C2sCreate(C2sCommand {
                uid: uid.to_string(),
                act_key,
                obj_key,
                object: create.into(),
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
                    retry_targets: None,
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

static DEFAULT_TEMPLATE: &str = r#"<p>{{ title.content|to_text }}</p>
{% if summary %}
<p>{{ summary.content|to_text|excerpt(400) }}</p>
{% else %}
<p>{{ content.body|to_text|excerpt(400) }}</p>
{% endif %}
{% if id is startingwith "http" %}
<p><a href="{{ id }}">{{ id }}</a></p>
{% elif links %}
<p><a href="{{ links[0].href }}">{{ links[0].href }}</a></p>
{% endif %}"#;

fn object_from_feed_entry(
    apub_base_url: &str,
    uid: &str,
    entry: &Entry,
    template: &Template,
) -> Object<'static> {
    let mut object = Object::from(json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        // TODO config as Note or Article
        "type": "Note",
    }));

    if entry.id.starts_with("http") {
        object = object.augment("id", entry.id.clone().into());
    } else if !entry.links.is_empty() {
        object = object.augment("id", entry.links[0].href.clone().into());
    }

    let content = template.render(entry).unwrap();

    object = object.augment("content", content.into());
    // TODO convert category to tags

    if let Some(published) = entry.published {
        let timestamp = published.to_rfc3339();
        object = object.augment("published", timestamp.into());
    }

    object
        .augment(
            "url",
            entry
                .links
                .iter()
                .map(|link| link.href.clone())
                .collect::<Vec<_>>()
                .into(),
        )
        .augment(
            "to",
            vec![
                format!("{apub_base_url}/users/{uid}/followers"),
                "https://www.w3.org/ns/activitystreams#Public".to_string(),
            ]
            .into(),
        )
}
