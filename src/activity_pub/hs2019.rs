use std::collections::BTreeMap;
use std::iter::Peekable;
use std::str::Chars;

use anyhow::{bail, Context, Result};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::rsa::KeyPair;
use aws_lc_rs::signature::{
    UnparsedPublicKey, VerificationAlgorithm, ECDSA_P256K1_SHA256_ASN1, ECDSA_P256K1_SHA256_FIXED,
    ECDSA_P256_SHA256_ASN1, ECDSA_P256_SHA256_FIXED, ED25519, RSA_PKCS1_2048_8192_SHA256,
    RSA_PKCS1_SHA256, RSA_PSS_2048_8192_SHA256,
};
use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::Response;
use base64ct::{Base64, Encoding};
use const_oid::db::rfc5912::{ID_EC_PUBLIC_KEY, RSA_ENCRYPTION};
use const_oid::db::rfc8410::ID_ED_25519;
use jiff::Timestamp;
use reqwest::header::{self, HeaderMap};
use reqwest::{StatusCode, Url};
use sha2::{Digest, Sha256, Sha512};
use spki::SubjectPublicKeyInfoRef;
use tracing::warn;

use super::mailman::Mailman;
use super::model::Object;

const HTTP_DATE_FMT: &str = "%a, %d %b %Y %H:%M:%S GMT";

pub(super) fn post_headers(
    actor_iri: &str,
    inbox: &str,
    body: &str,
    key_pair: &KeyPair,
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

    let sig_body = format!("(request-target): post {path}\nhost: {host}\ndate: {date}\ndigest: SHA-256={digest}\ncontent-length: {content_length}");
    let rng = SystemRandom::new();
    let mut rsa_signature = vec![0; key_pair.public_modulus_len()];
    key_pair.sign(
        &RSA_PKCS1_SHA256,
        &rng,
        sig_body.as_bytes(),
        &mut rsa_signature,
    )?;
    let signature = Base64::encode_string(&rsa_signature);

    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, host.parse()?);
    headers.insert(header::DATE, date.parse()?);
    headers.insert("Digest", format!("SHA-256={digest}").parse()?);
    headers.insert(header::CONTENT_LENGTH, content_length.to_string().parse()?);
    headers.insert("Signature", format!("keyId=\"{actor_iri}#main-key\",algorithm=\"rsa-sha256\",headers=\"(request-target) host date digest content-length\",signature=\"{signature}\"").parse()?);

    Ok(headers)
}

fn base64_sha256_string(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Base64::encode_string(hasher.finalize().as_slice())
}

fn base64_sha512_string(bytes: &[u8]) -> String {
    let mut hasher = Sha512::new();
    hasher.update(bytes);
    Base64::encode_string(hasher.finalize().as_slice())
}

/// Middleware to validate HTTP Signature HS2019
pub(crate) async fn validate_request(
    parts: Parts,
    body: Bytes,
    next: Next,
) -> Result<Response, StatusCode> {
    // TODO reuse mailman
    let mailman = Mailman::new();
    let headers = &parts.headers;
    let signature_header = headers
        .get("signature")
        .ok_or(StatusCode::BAD_REQUEST)?
        .to_str()
        .map_err(bad)?;
    let sig_params = parse_sig_params(signature_header).map_err(bad)?;
    if let Some(algorithm) = sig_params.get("algorithm") {
        if !["hs2019", "rsa-sha256"].contains(&algorithm.as_str()) {
            warn!(
                "unknown http signature algorithm {algorithm} used, verification will likely fail"
            );
        }
    }
    let signature = Base64::decode_vec(sig_params.get("signature").ok_or(StatusCode::BAD_REQUEST)?)
        .map_err(bad)?;
    let sig_headers =
        parse_headers(sig_params.get("headers").ok_or(StatusCode::BAD_REQUEST)?).map_err(bad)?;
    if sig_headers.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let key_id = sig_params.get("keyId").ok_or(StatusCode::BAD_REQUEST)?;

    // Fetch publicKeyPem
    // TODO cache actor public key
    let value = mailman.fetch(key_id).await.map_err(bad)?;
    let object = Object::from(value);
    let pubkey_pem = if object.type_is("Key") {
        object.get_str("publicKeyPem").map(str::to_string)
    } else {
        object
            .get_node_object("publicKey")
            .and_then(|obj| obj.get_str("publicKeyPem").map(str::to_string))
    }
    .ok_or(StatusCode::BAD_REQUEST)?;

    let mut sig_body = String::new();
    for header in sig_headers {
        match header.as_str() {
            "(request-target)" => {
                let path = parts.uri.path();
                sig_body.push_str(&format!("(request-target): post {path}\n"));
            }
            "(created)" => {
                let created = sig_params.get("created").ok_or(StatusCode::BAD_REQUEST)?;
                sig_body.push_str(&format!("(created): {created}\n"));
            }
            "(expires)" => {
                let expires = sig_params.get("expires").ok_or(StatusCode::BAD_REQUEST)?;
                sig_body.push_str(&format!("(expires): {expires}\n"));
            }
            "digest" => {
                let client_digest = headers
                    .get("digest")
                    .ok_or(StatusCode::BAD_REQUEST)?
                    .to_str()
                    .map_err(bad)?;
                let (alg, digest) = if client_digest.starts_with("sha-256") {
                    ("sha-256", base64_sha256_string(&body))
                } else if client_digest.starts_with("SHA-256") {
                    ("SHA-256", base64_sha256_string(&body))
                } else if client_digest.starts_with("sha-512") {
                    ("sha-512", base64_sha512_string(&body))
                } else if client_digest.starts_with("SHA-512") {
                    ("SHA-512", base64_sha512_string(&body))
                } else {
                    return Err(StatusCode::NOT_IMPLEMENTED);
                };
                sig_body.push_str(&format!("digest: {alg}={digest}\n"));
            }
            field => {
                let value = headers.get(field).ok_or(StatusCode::BAD_REQUEST)?;
                sig_body.push_str(&format!("{field}: {}\n", value.to_str().map_err(bad)?));
            }
        }
    }
    // Remove trailing newline
    let sig_body = sig_body.trim_end();

    let (label, der) = pem_rfc7468::decode_vec(pubkey_pem.as_bytes()).map_err(bad)?;
    if label != "PUBLIC KEY" {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    let spki = SubjectPublicKeyInfoRef::try_from(der.as_ref()).map_err(bad)?;
    let spk = spki
        .subject_public_key
        .as_bytes()
        .ok_or(StatusCode::BAD_REQUEST)?;

    let algorithms: &[&'static dyn VerificationAlgorithm] = match spki.algorithm.oid {
        RSA_ENCRYPTION => &[
            &RSA_PSS_2048_8192_SHA256 as &dyn VerificationAlgorithm,
            &RSA_PKCS1_2048_8192_SHA256 as &dyn VerificationAlgorithm,
        ],
        ID_ED_25519 => &[&ED25519 as &dyn VerificationAlgorithm],
        ID_EC_PUBLIC_KEY => &[
            &ECDSA_P256_SHA256_FIXED as &dyn VerificationAlgorithm,
            &ECDSA_P256K1_SHA256_FIXED as &dyn VerificationAlgorithm,
            &ECDSA_P256_SHA256_ASN1 as &dyn VerificationAlgorithm,
            &ECDSA_P256K1_SHA256_ASN1 as &dyn VerificationAlgorithm,
        ],
        _ => return Err(StatusCode::NOT_IMPLEMENTED),
    };
    if !algorithms.iter().any(|&alg| {
        UnparsedPublicKey::new(alg, spk)
            .verify(sig_body.as_bytes(), &signature)
            .is_ok()
    }) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let req = Request::from_parts(parts, Body::from(body));
    Ok(next.run(req).await)
}

fn bad<T>(_: T) -> StatusCode {
    StatusCode::BAD_REQUEST
}

fn parse_sig_params(input: &str) -> Result<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    let mut it = input.chars().peekable();
    while it.peek().is_some() {
        eat_bws(&mut it);
        let token = eat_token(&mut it)?;
        eat_bws(&mut it);
        eat_eq(&mut it)?;
        eat_bws(&mut it);
        let value = if it.peek() == Some(&'"') {
            eat_quoted_string(&mut it)?
        } else {
            eat_token(&mut it)?
        };
        params.insert(token, value);
        eat_bws(&mut it);
        if it.peek().is_none() {
            break;
        }
        eat_comma(&mut it)?;
        eat_bws(&mut it);
    }
    Ok(params)
}

fn parse_headers(input: &str) -> Result<Vec<String>> {
    let mut headers = vec![];
    let mut it = input.chars().peekable();
    while it.peek().is_some() {
        eat_bws(&mut it);
        let header = eat_string(&mut it)?;
        headers.push(header.to_ascii_lowercase());
        eat_bws(&mut it);
    }
    Ok(headers)
}

fn eat_bws(it: &mut Peekable<Chars<'_>>) {
    while it.next_if(char::is_ascii_whitespace).is_some() {}
}
fn eat_eq(it: &mut Peekable<Chars<'_>>) -> Result<()> {
    if it.next_if_eq(&'=').is_none() {
        bail!("invalid auth-param, missing expected '='");
    }
    Ok(())
}
fn eat_comma(it: &mut Peekable<Chars<'_>>) -> Result<()> {
    if it.next_if_eq(&',').is_none() {
        bail!("invalid auth-param, missing expected ','");
    }
    Ok(())
}
#[rustfmt::skip]
fn is_tchar(c: &char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(c, '!'| '#'| '$'| '%'| '&'| '\''| '*'| '+'| '-'| '.'| '^'| '_'| '`'| '|'| '~')
}
fn eat_token(it: &mut Peekable<Chars<'_>>) -> Result<String> {
    let mut token = String::new();
    while let Some(c) = it.next_if(is_tchar) {
        token.push(c);
    }
    if token.is_empty() {
        bail!("expected at least one tchar");
    }
    Ok(token)
}
fn eat_string(it: &mut Peekable<Chars<'_>>) -> Result<String> {
    let mut token = String::new();
    while let Some(c) = it.next_if(|c| !c.is_ascii_whitespace()) {
        token.push(c);
    }
    if token.is_empty() {
        bail!("expected at least one char");
    }
    Ok(token)
}
fn eat_quoted_string(it: &mut Peekable<Chars<'_>>) -> Result<String> {
    let mut string = String::new();
    if it.next_if_eq(&'"').is_none() {
        bail!("expected DQUOTE");
    }
    let mut has_right_dquote = false;
    while let Some(c) = it.next() {
        // quoted-pair
        if c == '\\' {
            if it.next_if_eq(&'"').is_some() {
                string.push('"');
            }
            continue;
        }
        if c == '"' {
            has_right_dquote = true;
            break;
        }
        string.push(c);
    }
    if !has_right_dquote {
        bail!("expected a pair of DQUOTE");
    }
    Ok(string)
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::encoding::AsDer;
    use base64ct::{Base64, Encoding};

    use super::{parse_headers, parse_sig_params};

    #[test]
    fn test_parse_sig_params() {
        let signature = r#"keyId="id=\\"123\\"",algorithm="hs2019",
            created=1402170695, expires=1402170995,
            headers="(request-target) (created) (expires)
               host date digest content-length",
            signature="6QQ1ckyr6Tge+t0sBe99S3qyMjW6AF6kLeL7bV6ByzM=""#;
        let params = parse_sig_params(signature).unwrap();

        assert_eq!(params.get("keyId"), Some(&"id=\"123\"".to_string()));
        assert_eq!(params.get("algorithm"), Some(&"hs2019".to_string()));
        assert_eq!(params.get("created"), Some(&"1402170695".to_string()));
        assert_eq!(params.get("expires"), Some(&"1402170995".to_string()));
        assert_eq!(params.get("headers"), Some(&"(request-target) (created) (expires)\n               host date digest content-length".to_string()));
        assert_eq!(
            params.get("signature"),
            Some(&"6QQ1ckyr6Tge+t0sBe99S3qyMjW6AF6kLeL7bV6ByzM=".to_string())
        );
    }

    #[test]
    fn test_parse_headers() {
        let input =
            "(request-target) (created) (expires)\n               host date digest content-length";
        let headers = parse_headers(input).unwrap();
        assert_eq!(
            headers,
            vec![
                "(request-target)".to_string(),
                "(created)".to_string(),
                "(expires)".to_string(),
                "host".to_string(),
                "date".to_string(),
                "digest".to_string(),
                "content-length".to_string()
            ]
        );
    }

    #[test]
    fn test_rsa_sign_verify() {
        use aws_lc_rs::rsa::KeyPair;
        use aws_lc_rs::rsa::KeySize;
        use aws_lc_rs::rsa::PrivateDecryptingKey;
        use aws_lc_rs::signature::UnparsedPublicKey;
        use aws_lc_rs::signature::{RSA_PKCS1_2048_8192_SHA256, RSA_PKCS1_SHA256};
        use spki::SubjectPublicKeyInfoRef;

        // PKI
        let pri_key = PrivateDecryptingKey::generate(KeySize::Rsa2048).unwrap();
        let pri_key_der = pri_key.as_der().unwrap();
        let pub_key = pri_key.public_key().as_der().unwrap();
        let pub_pem =
            pem_rfc7468::encode_string("PUBLIC KEY", pem_rfc7468::LineEnding::LF, pub_key.as_ref())
                .unwrap();

        // Sign
        let msg = "hello";
        let key_pair = KeyPair::from_pkcs8(pri_key_der.as_ref()).unwrap();
        let mut signature = vec![0; key_pair.public_modulus_len()];
        key_pair
            .sign(
                &RSA_PKCS1_SHA256,
                &aws_lc_rs::rand::SystemRandom::new(),
                msg.as_bytes(),
                &mut signature,
            )
            .unwrap();
        let signature = Base64::encode_string(&signature);

        // Verify
        let (label, der) = pem_rfc7468::decode_vec(pub_pem.as_bytes()).unwrap();
        assert_eq!(label, "PUBLIC KEY");

        let spki = SubjectPublicKeyInfoRef::try_from(der.as_ref()).unwrap();
        assert_eq!(spki.algorithm.oid, const_oid::db::rfc5912::RSA_ENCRYPTION);

        let signature = Base64::decode_vec(&signature).unwrap();
        let verified = UnparsedPublicKey::new(
            &RSA_PKCS1_2048_8192_SHA256,
            spki.subject_public_key.as_bytes().unwrap(),
        )
        .verify(msg.as_bytes(), &signature)
        .is_ok();
        assert!(verified);
    }

    #[test]
    fn test_ed25519_sign_verify() {
        use aws_lc_rs::signature::Ed25519KeyPair;
        use aws_lc_rs::signature::KeyPair as _;
        use aws_lc_rs::signature::UnparsedPublicKey;
        use aws_lc_rs::signature::ED25519;
        use spki::SubjectPublicKeyInfoRef;

        // PKI
        let pri_key = Ed25519KeyPair::generate().unwrap();
        let pri_key_der = pri_key.to_pkcs8().unwrap();
        let pub_key = pri_key.public_key().as_der().unwrap();
        let pub_pem =
            pem_rfc7468::encode_string("PUBLIC KEY", pem_rfc7468::LineEnding::LF, pub_key.as_ref())
                .unwrap();

        // Sign
        let msg = "hello";
        let key_pair = Ed25519KeyPair::from_pkcs8(pri_key_der.as_ref()).unwrap();
        let signature = key_pair.sign(msg.as_bytes());
        let signature = Base64::encode_string(signature.as_ref());

        // Verify
        let (label, der) = pem_rfc7468::decode_vec(pub_pem.as_bytes()).unwrap();
        assert_eq!(label, "PUBLIC KEY");

        let spki = SubjectPublicKeyInfoRef::try_from(der.as_ref()).unwrap();
        assert_eq!(spki.algorithm.oid, const_oid::db::rfc8410::ID_ED_25519);

        let signature = Base64::decode_vec(&signature).unwrap();
        let verified =
            UnparsedPublicKey::new(&ED25519, spki.subject_public_key.as_bytes().unwrap())
                .verify(msg.as_bytes(), &signature)
                .is_ok();
        assert!(verified);
    }

    #[test]
    fn test_ecdsa_sign_verify() {
        use aws_lc_rs::rand::SystemRandom;
        use aws_lc_rs::signature::EcdsaKeyPair;
        use aws_lc_rs::signature::KeyPair as _;
        use aws_lc_rs::signature::UnparsedPublicKey;
        use aws_lc_rs::signature::ECDSA_P256K1_SHA256_FIXED;
        use aws_lc_rs::signature::ECDSA_P256K1_SHA256_FIXED_SIGNING;
        use spki::SubjectPublicKeyInfoRef;

        // PKI
        let pri_key = EcdsaKeyPair::generate(&ECDSA_P256K1_SHA256_FIXED_SIGNING).unwrap();
        let pri_key_der = pri_key.to_pkcs8v1().unwrap();
        let pub_key = pri_key.public_key().as_der().unwrap();
        let pub_pem =
            pem_rfc7468::encode_string("PUBLIC KEY", pem_rfc7468::LineEnding::LF, pub_key.as_ref())
                .unwrap();

        // Sign
        let msg = "hello";
        let key_pair =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256K1_SHA256_FIXED_SIGNING, pri_key_der.as_ref())
                .unwrap();
        let signature = key_pair.sign(&SystemRandom::new(), msg.as_bytes()).unwrap();
        let signature = Base64::encode_string(signature.as_ref());

        // Verify
        let (label, der) = pem_rfc7468::decode_vec(pub_pem.as_bytes()).unwrap();
        assert_eq!(label, "PUBLIC KEY");

        let spki = SubjectPublicKeyInfoRef::try_from(der.as_ref()).unwrap();
        assert_eq!(spki.algorithm.oid, const_oid::db::rfc5912::ID_EC_PUBLIC_KEY);

        let signature = Base64::decode_vec(&signature).unwrap();
        let verified = UnparsedPublicKey::new(
            &ECDSA_P256K1_SHA256_FIXED,
            spki.subject_public_key.as_bytes().unwrap(),
        )
        .verify(msg.as_bytes(), &signature)
        .is_ok();
        assert!(verified);
    }
}
