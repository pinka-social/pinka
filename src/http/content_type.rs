use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use reqwest::StatusCode;
use serde::Serialize;

pub(super) struct ActivityStreamsJson<T>(pub(super) Json<T>);

impl<T> IntoResponse for ActivityStreamsJson<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        let mut response = self.0.into_response();
        if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(
                    "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"",
                ),
            );
        }
        response
    }
}
