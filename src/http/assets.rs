use axum::{
    response::IntoResponse,
    http::{HeaderMap, header},
};

pub(super) async fn get_comments_js() -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "text/javascript".parse().unwrap());
    (headers, include_str!("comments.js"))
}
