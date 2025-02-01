mod iri;

use std::str::FromStr;

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::task::block_in_place;
use tracing::info;
use uuid::Uuid;

use crate::activity_pub::machine::{ActivityPubCommand, C2sCommand, S2sCommand};
use crate::activity_pub::model::{Actor, Collection, Create, JsonLdValue, Object};
use crate::activity_pub::{ObjectKey, ObjectRepo, OutboxIndex, UserIndex};
use crate::config::RuntimeConfig;
use crate::worker::raft::{get_raft_local_client, LogEntryValue, RaftClientMsg};

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
        .route("/as/objects/{obj_key}", get(get_object))
        .route("/users/{id}", get(get_actor).post(post_actor))
        .route("/users/{id}/outbox", get(get_outbox).post(post_outbox))
        .route("/users/{id}/inbox", post(post_inbox))
        .route("/users/{id}/followers", get(get_followers))
        .with_state(config.clone());
    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.server.http.port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_actor(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    block_in_place(|| {
        let user_index = UserIndex::new(config.keyspace.clone()).map_err(ise)?;
        if let Some(object) = user_index.find_one(&uid).map_err(ise)? {
            let raw_actor = Actor::try_from(object).map_err(invalid)?;
            let actor = raw_actor.enrich_with(&config.init.activity_pub);
            return Ok(Json(actor.into()));
        }
        Err(StatusCode::NOT_FOUND)
    })
}

async fn get_object(
    State(config): State<RuntimeConfig>,
    Path(obj_key): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    block_in_place(|| {
        let obj_repo = ObjectRepo::new(config.keyspace.clone()).map_err(ise)?;
        let obj_key = ObjectKey::from_str(&obj_key)
            .context("invalid UUID")
            .map_err(invalid)?;
        if let Some(object) = obj_repo.find_one(obj_key).map_err(ise)? {
            // let iri = format!(
            //     "{}/as/objects/{}",
            //     config.init.activity_pub.base_url, obj_key
            // );
            return Ok(Json(object.into()));
        }
        Err(StatusCode::NOT_FOUND)
    })
}

async fn post_actor(Path(uid): Path<String>, Json(value): Json<Value>) -> Result<(), StatusCode> {
    if value.type_is("Person") {
        let client = get_raft_local_client().map_err(ise)?;
        let command = ActivityPubCommand::UpdateUser(uid, value.into());
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
) -> Result<Json<Value>, StatusCode> {
    block_in_place(|| {
        let index = OutboxIndex::new(config.keyspace.clone()).map_err(ise)?;
        if params.has_page() {
            let query = params.to_query();
            let PageParams { before, after, .. } = params;
            let first = params.first.and_then(|first| Some(first.clamp(0, 50)));
            let last = params.last.and_then(|last| Some(last.clamp(0, 50)));
            let items: Vec<(ObjectKey, Object)> = index
                .find_all(&uid, before, after, first, last)
                .map_err(invalid)?;
            let (prev, next) = if !items.is_empty() {
                (Some(items[0].0), Some(items.last().unwrap().0))
            } else {
                (None, None)
            };
            let items = items.into_iter().map(|it| it.1).collect();
            let mut outbox = Collection::new()
                .id(format!(
                    "{}/users/{uid}/outbox?{query}",
                    config.init.activity_pub.base_url,
                ))
                .first(format!(
                    "{}/users/{uid}/outbox?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .last(format!(
                    "{}/users/{uid}/outbox?before={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .with_ordered_items(items)
                .ordered();
            if let Some(id) = prev {
                outbox = outbox.prev(&format!(
                    "{}/users/{uid}/outbox?before={id}",
                    config.init.activity_pub.base_url
                ));
            }
            if let Some(id) = next {
                outbox = outbox.next(&format!(
                    "{}/users/{uid}/outbox?after={id}",
                    config.init.activity_pub.base_url
                ));
            }
            Ok(Json(outbox.to_page().into()))
        } else {
            let outbox = Collection::new()
                .id(format!(
                    "{}/users/{uid}/outbox",
                    config.init.activity_pub.base_url
                ))
                .first(format!(
                    "{}/users/{uid}/outbox?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .last(format!(
                    "{}/users/{uid}/outbox?before={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .total_items(index.count(&uid))
                .ordered();
            Ok(Json(outbox.into()))
        }
    })
}

async fn post_outbox(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
    Json(value): Json<Value>,
) -> Result<(), StatusCode> {
    if !value.is_object() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !value.is_activity() {
        // Add actor info
        let act_key = ObjectKey::new();
        let obj_key = ObjectKey::new();
        let object = Object::try_from(value).map_err(invalid)?.ensure_id(format!(
            "{}/as/objects/{obj_key}",
            config.init.activity_pub.base_url
        ));
        let create = Create::try_from(object)
            .map_err(invalid)?
            .with_id(format!(
                "{}/as/objects/{act_key}",
                config.init.activity_pub.base_url
            ))
            .with_actor(format!("{}/users/{uid}", config.init.activity_pub.base_url));
        let client = get_raft_local_client().map_err(ise)?;
        let scoped_cmd = C2sCommand {
            uid,
            act_key,
            obj_key,
            node: Value::from(create).into(),
        };
        let command = ActivityPubCommand::C2sCreate(scoped_cmd);
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

async fn post_inbox(Path(uid): Path<String>, Json(value): Json<Value>) -> Result<(), StatusCode> {
    if value.is_inbox_activity() {
        let client = get_raft_local_client().map_err(ise)?;
        let obj_type = value.obj_type().map(str::to_string);
        let obj_type = obj_type.as_ref().map(String::as_str);
        let scoped_cmd = S2sCommand {
            uid,
            obj_key: ObjectKey::new(),
            node: value.into(),
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
        return Ok(());
    }
    return Ok(());
}

async fn get_followers(
    State(config): State<RuntimeConfig>,
    Path(uid): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<Json<Value>, StatusCode> {
    block_in_place(|| {
        let index = UserIndex::new(config.keyspace.clone()).map_err(ise)?;
        if params.has_page() {
            let query = params.to_query();
            let PageParams { before, after, .. } = params;
            let first = params.first.and_then(|first| Some(first.clamp(0, 50)));
            let last = params.last.and_then(|last| Some(last.clamp(0, 50)));
            let items: Vec<(ObjectKey, String)> = index
                .find_followers(&uid, before, after, first, last)
                .map_err(invalid)?;
            let (prev, next) = if !items.is_empty() {
                (Some(items[0].0), Some(items.last().unwrap().0))
            } else {
                (None, None)
            };
            let items = items.into_iter().map(|it| it.1).collect();
            let mut followers = Collection::new()
                .id(format!(
                    "{}/users/{uid}/outbox?{query}",
                    config.init.activity_pub.base_url,
                ))
                .first(format!(
                    "{}/users/{uid}/followers?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .last(format!(
                    "{}/users/{uid}/followers?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .with_ordered_items(items)
                .ordered();
            if let Some(id) = prev {
                followers = followers.prev(&format!(
                    "{}/users/{uid}/followers?before={id}",
                    config.init.activity_pub.base_url
                ));
            }
            if let Some(id) = next {
                followers = followers.next(&format!(
                    "{}/users/{uid}/followers?after={id}",
                    config.init.activity_pub.base_url
                ));
            }
            Ok(Json(followers.to_page().into()))
        } else {
            let followers = Collection::new()
                .id(format!(
                    "{}/users/{uid}/followers",
                    config.init.activity_pub.base_url
                ))
                .first(format!(
                    "{}/users/{uid}/followers?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::nil().simple()
                ))
                .last(format!(
                    "{}/users/{uid}/followers?after={}",
                    config.init.activity_pub.base_url,
                    Uuid::max().simple()
                ))
                .total_items(index.count_followers(&uid))
                .ordered();
            Ok(Json(followers.into()))
        }
    })
}

fn ise(_error: anyhow::Error) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

fn invalid(_error: anyhow::Error) -> StatusCode {
    StatusCode::UNPROCESSABLE_ENTITY
}
