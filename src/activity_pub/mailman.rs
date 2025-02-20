use std::time::Duration;

use anyhow::{bail, Result};
use axum::http::HeaderValue;
use reqwest::header::HeaderMap;
use reqwest::{header, Client};
use serde_json::Value;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);
const APPLICATION_LD_JSON: HeaderValue = HeaderValue::from_static(
    "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
);

#[derive(Clone)]
pub(super) struct Mailman {
    client: Client,
}

impl Mailman {
    pub(super) fn new() -> Mailman {
        Mailman {
            client: Client::builder()
                .http1_only()
                .user_agent(APP_USER_AGENT)
                .gzip(true)
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }
    pub(super) async fn fetch(&self, iri: &str) -> Result<Value> {
        let response = self
            .client
            .get(iri)
            .header(header::ACCEPT, APPLICATION_LD_JSON)
            .send()
            .await?;
        Ok(response.json().await?)
    }
    pub(super) async fn post(&self, inbox: &str, headers: HeaderMap, body: &str) -> Result<()> {
        let response = self
            .client
            .post(inbox)
            .header(header::CONTENT_TYPE, APPLICATION_LD_JSON)
            .headers(headers)
            .body(body.to_string())
            .send()
            .await?;
        if response.error_for_status_ref().is_err() {
            let code = response.status();
            let text = response.text().await?;
            bail!("posting to {inbox} failed with error {code} {text}");
        }
        Ok(())
    }
}
