mod filters;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use feed_rs::model::Entry;
use minijinja::{Environment, Template};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use raft::{LogEntryValue, RaftClientMsg, get_raft_local_client};
use serde_json::json;
use tracing::info;

use crate::ActivityPubConfig;
use crate::activity_pub::delivery::DeliveryQueueItem;
use crate::activity_pub::machine::{ActivityPubCommand, C2sCommand};
use crate::activity_pub::model::{Create, Object};
use crate::activity_pub::{ObjectKey, uuidgen};
use crate::config::FeedSlurpConfig;

use self::filters::{excerpt, to_text};

pub(crate) struct FeedSlurpWorker;

pub(crate) struct FeedSlurpWorkerInit {
    pub(crate) apub: ActivityPubConfig,
    pub(crate) feeds: BTreeMap<String, FeedSlurpConfig>,
}

pub(crate) struct FeedSlurpWorkerState {
    apub: ActivityPubConfig,
    pub(crate) feeds: BTreeMap<String, FeedSlurpConfig>,
}

#[derive(RactorMessage)]
pub(crate) enum FeedSlurpMsg {
    CheckFeed,
    IngestFeed(FeedSlurpConfig),
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
        let FeedSlurpWorkerInit { apub, feeds } = args;
        Ok(FeedSlurpWorkerState { apub, feeds })
    }
    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("started");
        const FEED_CHECK_INTERVAL: u64 = 60;
        myself.send_interval(Duration::from_secs(FEED_CHECK_INTERVAL), || {
            FeedSlurpMsg::CheckFeed
        });
        Ok(())
    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            FeedSlurpMsg::CheckFeed => {
                info!("Checking feeds");
                let client = get_raft_local_client()?;
                let command = ActivityPubCommand::GetFeedSlurpLock(now());
                let result = ractor::call!(
                    client,
                    RaftClientMsg::ClientRequest,
                    LogEntryValue::from(command)
                )?;
                if result.is_ok() {
                    info!("grabbed lock, checking feeds...");
                    for feed in state.feeds.values().cloned() {
                        ractor::cast!(myself, FeedSlurpMsg::IngestFeed(feed))?;
                    }
                }
            }
            FeedSlurpMsg::IngestFeed(ingest_feed) => state
                .handle_ingest_feed(&ingest_feed)
                .await
                .context("Failed to ingest feed")?,
        }
        Ok(())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after unix epoch")
        .as_secs()
}

impl FeedSlurpWorkerState {
    async fn handle_ingest_feed(&self, config: &FeedSlurpConfig) -> Result<()> {
        let FeedSlurpConfig {
            uid,
            base_url,
            feed_url,
            items,
            template,
            dry_run,
        } = config;
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
        for entry in feed.entries.iter().take(items.unwrap_or(usize::MAX)).rev() {
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
            if matches!(dry_run, Some(true)) {
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
