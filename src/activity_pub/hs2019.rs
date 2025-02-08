use anyhow::{Context, Result};
use base64ct::{Base64, Encoding};
use jiff::Timestamp;
use reqwest::header::{self, HeaderMap};
use reqwest::Url;
use sha2::{Digest, Sha256};
use tokio_rustls::rustls::sign::SigningKey;
use tokio_rustls::rustls::SignatureScheme;

const HTTP_DATE_FMT: &str = "%a, %d %b %Y %H:%M:%S GMT";

pub(super) fn post_headers(
    actor_iri: &str,
    inbox: &str,
    body: &str,
    signing_key: &dyn SigningKey,
) -> Result<HeaderMap> {
    let digest = base64_sha256_string(body.as_bytes());
    let url = Url::parse(inbox)?;
    let host = url
        .host()
        .context("inbox should have a host component")?
        .to_string();
    let path = url.path();
    let date = Timestamp::now().strftime(HTTP_DATE_FMT).to_string();
    let content_length = body.len();

    let sig_body = format!("(request-target): post {path}\nhost: {host}\ndate: {date}\ndigest: sha-256={digest}\ncontent-length: {content_length}");
    let signer = signing_key
        .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
        .context("crypt provider must support RSA_PKCS1_SHA256")?;
    let signature = base64_sha256_string(&signer.sign(sig_body.as_bytes())?);

    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, host.parse()?);
    headers.insert(header::DATE, date.parse()?);
    headers.insert("Digest", format!("sha-256={digest}").parse()?);
    headers.insert(header::CONTENT_LENGTH, content_length.to_string().parse()?);
    headers.insert("Signature", format!("keyId=\"{actor_iri}#main-key\",headers=\"(request-target) host date digest content-length\",signature=\"{signature}\"").parse()?);

    Ok(headers)
}

fn base64_sha256_string(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Base64::encode_string(hasher.finalize().as_slice())
}
