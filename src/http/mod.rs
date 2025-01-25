mod iri;

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;
use tokio::net::TcpListener;

use crate::activity_pub::ActorRepo;
use crate::activity_pub::machine::ActivityPubCommand;
use crate::activity_pub::model::{Actor, Create, JsonLdValue, Object};
use crate::config::RuntimeConfig;
use crate::worker::raft::{LogEntryValue, RaftClientMsg, get_raft_local_client};

use self::iri::get_actor_iri;

pub(crate) async fn serve(config: &RuntimeConfig) -> Result<()> {
    let app = Router::new()
        .route("/users/{id}", get(get_actor))
        .route("/users/{id}/outbox", post(post_outbox))
        .with_state(config.clone());
    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.server.http_port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_actor(
    State(config): State<RuntimeConfig>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let store = ActorRepo::new(config.keyspace.clone()).map_err(ise)?;
    let iri = get_actor_iri(&config.init.activity_pub, &id);
    if let Some(raw_actor) = store.find_one(&iri).map_err(ise)? {
        let actor = raw_actor.enrich_with(&config.init.activity_pub);
        return Ok(Json(actor.into()));
    }
    Err(StatusCode::NOT_FOUND)
}

async fn post_outbox(
    State(config): State<RuntimeConfig>,
    Path(id): Path<String>,
    Json(value): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let object = Object::try_from(value).map_err(invalid)?;
    if object.type_is("Create") || !object.is_activity() {
        // Send request to state machine. We must first persist the object, then
        // persist the activity, then update indexes.
        let client = get_raft_local_client().map_err(ise)?;
        let command = ActivityPubCommand::Create(object);
        ractor::call!(
            client,
            RaftClientMsg::ClientRequest,
            LogEntryValue::from(command)
        )
        .context("RPC call failed")
        .map_err(ise)?;
    }
    Err(StatusCode::NOT_IMPLEMENTED)
}

fn ise(_error: anyhow::Error) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

fn invalid(_error: anyhow::Error) -> StatusCode {
    StatusCode::UNPROCESSABLE_ENTITY
}
