use std::time::Duration;

use anyhow::Result;
use reqwest::header::HeaderMap;
use reqwest::{header, Client};
use serde_json::Value;
use tracing::info;

// Name your user agent after your app?
static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

#[derive(Clone)]
pub(super) struct Mailman {
    client: Client,
}

impl Mailman {
    pub(super) fn new() -> Mailman {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static(
                "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
            ),
        );
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static(
                "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
            ),
        );
        Mailman {
            client: Client::builder()
                .user_agent(APP_USER_AGENT)
                .default_headers(headers)
                .gzip(true)
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }
    pub(super) async fn fetch(&self, iri: &str) -> Result<Value> {
        let response = self.client.get(iri).send().await?;
        Ok(response.json().await?)
    }
    pub(super) async fn post(&self, inbox: &str, headers: HeaderMap, body: &str) -> Result<()> {
        info!(target: "apub", ?headers, "simulate mailman posting to {inbox}");
        let _ = self
            .client
            .post(inbox)
            .headers(headers)
            .body(body.to_string())
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
