mod auth;
mod content_type;

use std::str::FromStr;

use anyhow::{Context, Result};
use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rsa::{KeySize, PrivateDecryptingKey};
use axum::extract::{Path, Query, State};
use axum::http::{Method, StatusCode, Uri};
use axum::middleware::from_fn;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use pem_rfc7468::{LineEnding, encode_string as pem_encode};
use ractor::ActorRef;
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::spawn_blocking;
use tracing::info;
use uuid::Uuid;

use crate::activity_pub::delivery::DeliveryQueueItem;
use crate::activity_pub::machine::{ActivityPubCommand, C2sCommand, S2sCommand};
use crate::activity_pub::model::{Actor, Create, Object, OrderedCollection};
use crate::activity_pub::{
    ContextIndex, CryptoRepo, IriIndex, KeyMaterial, ObjectKey, ObjectRepo, OutboxIndex, UserIndex,
    uuidgen, validate_request,
};
use crate::config::RuntimeConfig;
use crate::feed_slurp::FeedSlurpMsg;
use crate::raft::{LogEntryValue, RaftClientMsg, get_raft_local_client};

use self::auth::admin_basic_auth;
use self::content_type::ActivityStreamsJson;

#[derive(Debug, Deserialize)]
struct PageParams {
    before: Option<String>,
    after: Option<String>,
    first: Option<u64>,
    last: Option<u64>,
}

impl PageParams {
    fn has_page(&self) -> bool {
        self.after.is_some() || self.before.is_some()
    }
    fn to_query(&self) -> String {
        let mut query = vec![];
        if let Some(before) = &self.before {
            query.push(format!("before={before}"));
        }
        if let Some(after) = &self.after {
            query.push(format!("after={after}"));
        }
        if let Some(first) = &self.first {
            query.push(format!("first={first}"));
        }
        if let Some(last) = &self.last {
            query.push(format!("last={last}"));
        }
        query.join("&")
    }
}

pub(crate) async fn serve(config: &RuntimeConfig) -> Result<()> {
    if !config.server.http.listen {
        info!(target: "http", "http API server is disabled");
        return Ok(());
    }
    let app = Router::new()
        .route("/.well-known/webfinger", get(get_webfinger))
        .route("/users/{id}", get(get_actor))
        .route(
            "/users/{id}",
            post(post_actor).layer(from_fn(admin_basic_auth)),
        )
        .route("/users/{id}/outbox", get(get_outbox))
        .route(
            "/users/{id}/outbox",
            post(post_outbox).layer(from_fn(admin_basic_auth)),
        )
        .route(
            "/users/{id}/inbox",
            post(post_inbox).layer(from_fn(validate_request)),
        )
        .route("/users/{id}/followers", get(get_followers))
        .route("/as/objects/{obj_key}", get(get_object_by_id))
        .route("/as/objects/{obj_key}/{prop}", get(get_object_likes_shares))
        .route(
            "/as/admin/ingest_feed",
            post(post_ingest_feed).layer(from_fn(admin_basic_auth)),
        )
        .fallback(get_object_by_iri)
        .layer(Extension(config.init.admin.clone()))
        .with_state(config.clone());
    let listener = TcpListener::bind(format!(
        "{}:{}",
        config.server.http.address, config.server.http.port
    ))
    .await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_object_by_id(
    State(config): State<RuntimeConfig>,
    Path(obj_key): Path<String>,
) -> Result<ActivityStreamsJson<Value>, StatusCode> {
    info!(%obj_key, "handle get object by ID request");
    spawn_blocking(move || {
        let obj_key = ObjectKey::from_str(&obj_key)
            .context("invalid UUID")
            .map_err(invalid)?;
        blocking_get_object(&config, obj_key)
    })
    .await
    .context("task failed")
    .map_err(ise)?
}

async fn get_object_by_iri(
    State(config): State<RuntimeConfig>,
    method: Method,
    uri: Uri,
) -> Result<ActivityStreamsJson<Value>, StatusCode> {
    info!(%uri, "handle get object by IRI request");
    if !matches!(method, Method::GET) {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    spawn_blocking(move || {
        let iri_index = IriIndex::new(config.keyspace.clone()).map_err(ise)?;
        let iri = format!("{}{}", config.init.activity_pub.base_url, uri.path());
        let obj_key = iri_index
            .find_one(&iri)
            .map_err(ise)?
            .context("unknown IRI")
            .map_err(invalid)?;
        let obj_key = ObjectKey::try_from(obj_key.as_ref())
            .context("invalid UUID")
            .map_err(invalid)?;
        blocking_get_object(&config, obj_key)
    })
    .await
    .context("task failed")
    .map_err(ise)?
}

fn blocking_get_object(
    config: &RuntimeConfig,
    obj_key: ObjectKey,
) -> Result<ActivityStreamsJson<Value>, StatusCode> {
    let ctx_index = ContextIndex::new(config.keyspace.clone()).map_err(ise)?;
    let obj_repo = ObjectRepo::new(config.keyspace.clone()).map_err(ise)?;
    info!(%obj_key, "loading object");
    if let Some(object) = obj_repo.find_one(obj_key).map_err(ise)? {
        if let Some(iri) = object.id() {
            let likes = ctx_index.count_likes(iri);
            let shares = ctx_index.count_shares(iri);
            let object = object.augment(
                "likes",
                json!({
                    "id": format!("{}/as/objects/{obj_key}/likes", config.init.activity_pub.base_url),
                    "type": "Collection",
                    "totalItems": likes
                }),
            ).augment(
                "shares",
                json!({
                    "id": format!("{}/as/objects/{obj_key}/shares", config.init.activity_pub.base_url),
                    "type": "Collection",
                    "totalItems": shares
                }),
            );
            return Ok(ActivityStreamsJson(Json(object.into())));
        }
        return Ok(ActivityStreamsJson(Json(object.into())));
    }
    Err(StatusCode::NOT_FOUND)
}

async fn get_object_likes_shares(
    State(config): State<RuntimeConfig>,
    Path((obj_key, prop)): Path<(String, String)>,
) -> Result<ActivityStreamsJson<Value>, StatusCode> {
    info!(%obj_key, %prop, "handle get object likes/shares request");
    if prop != "likes" && prop != "shares" {
        return Err(StatusCode::NOT_FOUND);
    }
    spawn_blocking(move || {
        let ctx_index = ContextIndex::new(config.keyspace.clone()).map_err(ise)?;
        let obj_key = ObjectKey::from_str(&obj_key)
            .context("invalid UUID")
            .map_err(invalid)?;
        let iri = format!("{}/as/objects/{obj_key}", config.init.activity_pub.base_url);
        let count = match prop.as_str() {
            "likes" => ctx_index.count_likes(&iri),
            "shares" => ctx_index.count_shares(&iri),
            _ => unreachable!(),
        };
        Ok(ActivityStreamsJson(Json(json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": format!("{}/as/objects/{obj_key}/{prop}", config.init.activity_pub.base_url),
            "type": "Collection",
            "totalItems": count
        }))))
    })
    .await
    .context("task failed")
    .map_err(ise)?
}

#[derive(Deserialize)]
struct WebFingerParams {
    resource: String,
}

async fn get_webfinger(
    State(config): State<RuntimeConfig>,
    Query(params): Query<WebFingerParams>,
) -> Result<impl IntoResponse, StatusCode> {
    info!(%params.resource, "handle webfinger request");
    spawn_blocking(move || {
        if !params.resource.starts_with("acct:") {
            return Err(StatusCode::BAD_REQUEST);
        }
        let subject = params.resource.strip_prefix("acct:").unwrap();
        let Some(uid) = subject.strip_suffix(&config.init.activity_pub.webfinger_at_host) else {
            return Err(StatusCode::BAD_REQUEST);
        };
        let user_index = UserIndex::new(config.keyspace.clone()).map_err(ise)?;
        if user_index.find_one(uid).map_err(ise)?.is_some() {
            let jrd = json!({
                "subject": subject,
                "links": [
                    {
                        "rel": "self",
                        "type": "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
                        "href": format!("{}/users/{}", config.init.activity_pub.base_url, uid)
                    }
                ]
            });
            return Ok((
                [
                    ("content-type", "application/jrd+json"),
                    ("access-control-allow-origin", "*"),
                ],
                Json(jrd),
            ));
        }
        Err(StatusCode::NOT_FOUND)
    })
    .await
    .context("task failed")
    .map_err(ise)?
}

async fn get_actor(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
) -> Result<ActivityStreamsJson<Value>, StatusCode> {
    info!(%uid, "handle get actor request");
    spawn_blocking(move || {
        let user_index = UserIndex::new(config.keyspace.clone()).map_err(ise)?;
        let crypto_repo = CryptoRepo::new(config.keyspace.clone()).map_err(ise)?;
        if let Some(object) = user_index.find_one(&uid).map_err(ise)? {
            let raw_actor = Actor::from(object);
            // TODO store public key separately?
            let key_material = crypto_repo
                .find_one(&uid)
                .map_err(ise)?
                .context("")
                .map_err(ise)?;
            let private_key = PrivateDecryptingKey::from_pkcs8(key_material.expose_secret())
                .context("")
                .map_err(ise)?;
            let pub_key = private_key
                .public_key()
                .as_der()
                .context("failed to serialize public key")
                .map_err(ise)?;
            // Public key in SubjectPublicKeyInfo format
            let pem = pem_encode("PUBLIC KEY", LineEnding::LF, pub_key.as_ref())
                .expect("must encode public key to PEM");
            let actor = raw_actor.enrich_with(&config.init.activity_pub, &pem);
            return Ok(ActivityStreamsJson(Json(actor.into())));
        }
        Err(StatusCode::NOT_FOUND)
    })
    .await
    .context("task failed")
    .map_err(ise)?
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct PostActorParams {
    gen_rsa: bool,
}

async fn post_actor(
    Path(uid): Path<String>,
    Query(params): Query<PostActorParams>,
    Json(value): Json<Value>,
) -> Result<(), StatusCode> {
    info!(%uid, "handle post actor request");
    let object = Object::from(value);
    if object.type_is("Person") {
        let key_bytes = {
            if params.gen_rsa {
                let private_key = PrivateDecryptingKey::generate(KeySize::Rsa2048)
                    .context("generate private key failed")
                    .map_err(ise)?;
                let private_key_der = private_key
                    .as_der()
                    .context("failed to serialize private key")
                    .map_err(ise)?;
                Some(KeyMaterial::from(private_key_der.as_ref().to_vec()))
            } else {
                None
            }
        };
        let client = get_raft_local_client().map_err(ise)?;
        let command = ActivityPubCommand::UpdateUser(uid, object, key_bytes);
        ractor::call!(
            client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )
        .context("RPC call failed")
        .map_err(ise)?;
        return Ok(());
    }
    Err(StatusCode::BAD_REQUEST)
}

async fn get_outbox(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<ActivityStreamsJson<Value>, StatusCode> {
    info!(%uid, "handle get outbox request");
    spawn_blocking(move || {
        let index = OutboxIndex::new(config.keyspace.clone()).map_err(ise)?;
        let ctx_index = ContextIndex::new(config.keyspace.clone()).map_err(ise)?;
        if params.has_page() {
            let query = params.to_query();
            let PageParams { before, after, .. } = params;
            let first = params
                .first
                .or_else(|| after.as_ref().map(|_| 10))
                .map(|first| first.clamp(0, 50));
            let last = params
                .last
                .or_else(|| before.as_ref().map(|_| 10))
                .map(|last| last.clamp(0, 50));
            let items: Vec<(ObjectKey, Object)> = index
                .find_all(&uid, before, after, first, last)
                .map_err(invalid)?;
            let (next, prev) = if !items.is_empty() {
                (Some(items[0].0), Some(items.last().unwrap().0))
            } else {
                (None, None)
            };
            let items = items
                .into_iter()
                // NB: outbox collection is displayed in reverse chronological order
                .rev()
                .map(|it| {
                    let (obj_key, activity) = it;
                    // FIXME abstraction
                    let object = activity.get_node_object("object").unwrap();
                    let iri = object.id().expect("stored object should have IRI");
                    let likes = ctx_index.count_likes(iri);
                    let shares = ctx_index.count_shares(iri);
                    let activity = activity.augment_node("object", "likes",
                        json!({
                            "id": format!("{}/as/objects/{obj_key}/likes", config.init.activity_pub.base_url),
                            "type": "Collection",
                            "totalItems": likes
                        }),
                    ).augment_node("object", "shares", 
                        json!({
                            "id": format!("{}/as/objects/{obj_key}/shares", config.init.activity_pub.base_url),
                            "type": "Collection",
                            "totalItems": shares
                        }),
                    );
                    activity
                })
                .collect();
            let mut outbox = OrderedCollection::new()
                .id(format!(
                    "{}/users/{uid}/outbox?{query}",
                    config.init.activity_pub.base_url,
                ))
                .part_of(format!(
                    "{}/users/{uid}/outbox",
                    config.init.activity_pub.base_url
                ))
                .last(format!(
                    "{}/users/{uid}/outbox?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .first(format!(
                    "{}/users/{uid}/outbox?before={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .with_ordered_items(items);
            if let Some(id) = next {
                outbox = outbox.next(format!(
                    "{}/users/{uid}/outbox?before={id}",
                    config.init.activity_pub.base_url
                ));
            }
            if let Some(id) = prev {
                outbox = outbox.prev(format!(
                    "{}/users/{uid}/outbox?after={id}",
                    config.init.activity_pub.base_url
                ));
            }
            Ok(ActivityStreamsJson(Json(outbox.into_page().into())))
        } else {
            let outbox = OrderedCollection::new()
                .id(format!(
                    "{}/users/{uid}/outbox",
                    config.init.activity_pub.base_url
                ))
                .last(format!(
                    "{}/users/{uid}/outbox?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .first(format!(
                    "{}/users/{uid}/outbox?before={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .total_items(index.count(&uid));
            Ok(ActivityStreamsJson(Json(outbox.into())))
        }
    })
    .await
    .context("task failed")
    .map_err(ise)?
}

async fn post_outbox(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
    Json(value): Json<Value>,
) -> Result<(), StatusCode> {
    info!(%uid, "handle post outbox request");
    let object = Object::from(value);
    if !object.is_activity() {
        // Add actor info
        let act_key = ObjectKey::new();
        let obj_key = ObjectKey::new();
        let object = object.ensure_id(format!(
            "{}/as/objects/{obj_key}",
            config.init.activity_pub.base_url
        ));
        let create = Create::try_from(object)
            .map_err(invalid)?
            .ensure_id(format!(
                "{}/as/objects/{act_key}",
                config.init.activity_pub.base_url
            ))
            .with_actor(format!("{}/users/{uid}", config.init.activity_pub.base_url));
        let client = get_raft_local_client().map_err(ise)?;
        let scoped_cmd = C2sCommand {
            uid: uid.clone(),
            act_key,
            obj_key,
            object: Value::from(create).into(),
        };
        let command = ActivityPubCommand::C2sCreate(scoped_cmd);
        ractor::call!(
            client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )
        .context("RPC call failed")
        .map_err(ise)?;
        // XXX: in case of update, the `obj_key` is not used, so this
        // queue_delivery will be unable to find the item for delivery.
        let command =
            ActivityPubCommand::QueueDelivery(uuidgen(), DeliveryQueueItem { uid, act_key });
        ractor::call!(
            client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )
        .context("RPC call failed")
        .map_err(ise)?;
        return Ok(());
    }
    Err(StatusCode::BAD_REQUEST)
}

async fn post_inbox(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
    Json(value): Json<Value>,
) -> Result<(), StatusCode> {
    info!(%uid, "handle post inbox request");
    let object = Object::from(value);
    if object.is_inbox_activity() {
        let client = get_raft_local_client().map_err(ise)?;
        let obj_type = object.get_first_type();
        let obj_type = obj_type.as_deref();
        let scoped_cmd = S2sCommand {
            uid: uid.clone(),
            obj_key: ObjectKey::new(),
            object: object.clone(),
        };
        let command = match obj_type {
            Some("Create") => ActivityPubCommand::S2sCreate(scoped_cmd),
            Some("Delete") => ActivityPubCommand::S2sDelete(scoped_cmd),
            Some("Like") => ActivityPubCommand::S2sLike(scoped_cmd),
            Some("Dislike") => ActivityPubCommand::S2sDislike(scoped_cmd),
            Some("Follow") => ActivityPubCommand::S2sFollow(scoped_cmd),
            Some("Undo") => ActivityPubCommand::S2sUndo(scoped_cmd),
            Some("Update") => ActivityPubCommand::S2sUpdate(scoped_cmd),
            Some("Announce") => ActivityPubCommand::S2sAnnounce(scoped_cmd),
            _ => return Ok(()),
        };
        ractor::call!(
            client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )
        .context("RPC call failed")
        .map_err(ise)?;
        // FIXME move to state machine effect
        if obj_type == Some("Follow") {
            let follow_id = object.id().ok_or(StatusCode::BAD_REQUEST)?;
            let req_actor = object
                .get_node_iri("actor")
                .ok_or(StatusCode::BAD_REQUEST)?;
            let act_key = ObjectKey::new();
            let accept = Object::from(json!({
                "@context": "https://www.w3.org/ns/activitystreams",
                "type": "Accept",
                "actor": format!("{}/users/{uid}", config.init.activity_pub.base_url),
                "object": follow_id,
                "to": req_actor
            }));
            let accept = accept.ensure_id(format!(
                "{}/as/objects/{act_key}",
                config.init.activity_pub.base_url
            ));
            let accept_cmd = C2sCommand {
                uid: uid.clone(),
                act_key,
                obj_key: ObjectKey::new(), // not used
                object: accept,
            };
            let command = ActivityPubCommand::C2sAccept(accept_cmd);
            ractor::call!(
                client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )
            .context("RPC call failed")
            .map_err(ise)?;
            let command =
                ActivityPubCommand::QueueDelivery(uuidgen(), DeliveryQueueItem { uid, act_key });
            ractor::call!(
                client,
                RaftClientMsg::ClientRequest,
                LogEntryValue::from(command)
            )
            .context("RPC call failed")
            .map_err(ise)?;
        }
        return Ok(());
    }
    Ok(())
}

async fn get_followers(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<ActivityStreamsJson<Value>, StatusCode> {
    info!(%uid, "handle get followers request");
    spawn_blocking(move || {
        let index = UserIndex::new(config.keyspace.clone()).map_err(ise)?;
        // TODO generic collections handling
        if params.has_page() {
            let query = params.to_query();
            let PageParams { before, after, .. } = params;
            let first = params
                .first
                .or_else(|| after.as_ref().map(|_| 10))
                .map(|first| first.clamp(0, 50));
            let last = params
                .last
                .or_else(|| before.as_ref().map(|_| 10))
                .map(|last| last.clamp(0, 50));
            let items: Vec<(ObjectKey, String)> = index
                .find_followers(&uid, before, after, first, last)
                .map_err(invalid)?;
            let (next, prev) = if !items.is_empty() {
                (Some(items[0].0), Some(items.last().unwrap().0))
            } else {
                (None, None)
            };
            let items = items.into_iter().rev().map(|it| it.1).collect();
            let mut followers = OrderedCollection::new()
                .id(format!(
                    "{}/users/{uid}/followers?{query}",
                    config.init.activity_pub.base_url,
                ))
                .part_of(format!(
                    "{}/users/{uid}/followers",
                    config.init.activity_pub.base_url
                ))
                .last(format!(
                    "{}/users/{uid}/followers?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .first(format!(
                    "{}/users/{uid}/followers?before={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .with_ordered_items(items);
            if let Some(id) = next {
                followers = followers.prev(format!(
                    "{}/users/{uid}/followers?before={id}",
                    config.init.activity_pub.base_url
                ));
            }
            if let Some(id) = prev {
                followers = followers.next(format!(
                    "{}/users/{uid}/followers?after={id}",
                    config.init.activity_pub.base_url
                ));
            }
            Ok(ActivityStreamsJson(Json(followers.into_page().into())))
        } else {
            let followers = OrderedCollection::new()
                .id(format!(
                    "{}/users/{uid}/followers",
                    config.init.activity_pub.base_url
                ))
                .last(format!(
                    "{}/users/{uid}/followers?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .first(format!(
                    "{}/users/{uid}/followers?before={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .total_items(index.count_followers(&uid));
            Ok(ActivityStreamsJson(Json(followers.into())))
        }
    })
    .await
    .context("task failed")
    .map_err(ise)?
}

#[derive(Deserialize)]
struct IngestFeed {
    uid: String,
    base_url: String,
    feed_url: String,
}

async fn post_ingest_feed(Json(ingest_feed): Json<IngestFeed>) -> Result<(), StatusCode> {
    info!(%ingest_feed.uid, %ingest_feed.feed_url, "handle ingest feed request");
    if ingest_feed.base_url.is_empty() || ingest_feed.feed_url.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let Some(feed_slurp) = ActorRef::where_is("feed_slurp".to_string()) else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    ractor::cast!(
        feed_slurp,
        FeedSlurpMsg::IngestFeed {
            uid: ingest_feed.uid,
            base_url: ingest_feed.base_url,
            feed_url: ingest_feed.feed_url
        }
    )
    .context("failed to ingest feed")
    .map_err(ise)?;
    Ok(())
}

fn ise(_error: anyhow::Error) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

fn invalid(_error: anyhow::Error) -> StatusCode {
    StatusCode::UNPROCESSABLE_ENTITY
}
