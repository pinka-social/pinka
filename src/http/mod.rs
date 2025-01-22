use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::Value;
use tokio::net::TcpListener;

use crate::activity_pub::actor;
use crate::activity_pub::actor_store::ActorStore;
use crate::config::RuntimeConfig;

pub(crate) async fn serve(config: &RuntimeConfig) -> Result<()> {
    let app = Router::new()
        .route("/users/{id}", get(get_actor))
        .with_state(config.clone());
    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.server.http_port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_actor(
    State(config): State<RuntimeConfig>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let store = ActorStore::new(config.keyspace.clone()).map_err(ise)?;
    let iri = actor::get_iri(&config.init.activity_pub, &id);
    if let Some(raw_actor) = store.find_one(&iri).map_err(ise)? {
        let actor = raw_actor.enrich_with(&config.init.activity_pub);
        return Ok(Json(actor.into()));
    }
    Err(StatusCode::NOT_FOUND)
}

fn ise(_error: anyhow::Error) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}
