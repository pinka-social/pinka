use std::str;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use base64ct::{Base64, Encoding};
use reqwest::StatusCode;
use secrecy::ExposeSecret;

use crate::config::AdminConfig;

pub(super) async fn admin_basic_auth(
    Extension(admin): Extension<AdminConfig>,
    req: Request,
    next: Next,
) -> Response {
    fn need_auth() -> Response {
        let authn = [("www-authenticate", "Basic realm=\"admin\"")];
        (StatusCode::UNAUTHORIZED, authn).into_response()
    }
    let Some(authz) = req.headers().get("authorization") else {
        return need_auth();
    };

    let Ok(cred) = authz.to_str() else {
        return need_auth();
    };
    let Some(cred) = cred
        .strip_prefix("Basic ")
        .map(str::trim)
        .and_then(|b64| Base64::decode_vec(b64).ok())
        .and_then(|b| String::from_utf8(b).ok())
    else {
        return need_auth();
    };

    let Some((user, password)) = cred.split_once(':') else {
        return need_auth();
    };

    if user != "pinka" || password != admin.password.expose_secret() {
        return need_auth();
    }

    next.run(req).await
}
