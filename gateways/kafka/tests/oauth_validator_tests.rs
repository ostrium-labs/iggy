// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The OAUTHBEARER validator against a test identity provider: keys minted per test, tokens
//! signed with them, and every way a token must be refused.

use std::time::{Duration, UNIX_EPOCH};

use jsonwebtoken::Algorithm;
use serde_json::json;

use iggy_gateway_kafka::oauth::{
    DEFAULT_ALGORITHMS, JwksSource, JwksValidator, OAuthConfig, TokenError, TokenValidator,
};

#[path = "common/jwt.rs"]
mod jwt;

use jwt::{AUDIENCE, ISSUER, IdentityServer, SigningKey, claims, hmac_signed, jwks, unsigned};

fn config(jwks: JwksSource) -> OAuthConfig {
    OAuthConfig {
        issuer: ISSUER.to_string(),
        audiences: vec![AUDIENCE.to_string()],
        principal_claim: "sub".to_string(),
        algorithms: DEFAULT_ALGORITHMS.to_vec(),
        clock_skew: Duration::from_secs(5),
        jwks,
        refresh_interval: Duration::from_secs(3600),
        jwks_ca_file: None,
    }
}

/// A validator reading `document` from a file the returned guard keeps alive.
async fn validator_for(document: &str) -> (JwksValidator, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("jwks.json");
    std::fs::write(&path, document).expect("write jwks");
    let validator = JwksValidator::new(config(JwksSource::File(path))).expect("valid config");
    validator.refresh().await.expect("the JWKS loads");
    (validator, dir)
}

fn invalid(result: Result<impl std::fmt::Debug, TokenError>) -> &'static str {
    match result {
        Err(TokenError::Invalid(reason)) => reason,
        other => panic!("expected an invalid token, got {other:?}"),
    }
}

#[tokio::test]
async fn given_tokens_from_each_key_type_should_accept_them_and_name_the_principal() {
    let keys = [
        SigningKey::rsa("rsa-1"),
        SigningKey::ec("ec-1"),
        SigningKey::ed25519("ed-1"),
    ];
    let (validator, _dir) = validator_for(&jwks(&keys.iter().collect::<Vec<_>>())).await;
    for key in &keys {
        let token = key.sign(&claims("alice", 300));
        let validated = validator
            .validate(&token)
            .await
            .unwrap_or_else(|error| panic!("{:?} token refused: {error:?}", key.algorithm));
        assert_eq!(validated.principal, "alice");
        let remaining = validated
            .expires_at
            .duration_since(std::time::SystemTime::now())
            .expect("in the future");
        assert!(remaining <= Duration::from_secs(300) && remaining > Duration::from_secs(290));
    }
}

#[tokio::test]
async fn given_an_expired_token_should_refuse_it() {
    let key = SigningKey::ec("ec-1");
    let (validator, _dir) = validator_for(&jwks(&[&key])).await;
    let mut expired = claims("alice", 0);
    expired["exp"] = json!(jwt::now() - 60);
    assert_eq!(
        invalid(validator.validate(&key.sign(&expired)).await),
        "the token has expired"
    );
}

#[tokio::test]
async fn given_a_token_inside_the_clock_skew_should_accept_it() {
    let key = SigningKey::ec("ec-1");
    let (validator, _dir) = validator_for(&jwks(&[&key])).await;
    let mut just_expired = claims("alice", 0);
    just_expired["exp"] = json!(jwt::now() - 2);
    assert!(validator.validate(&key.sign(&just_expired)).await.is_ok());
}

#[tokio::test]
async fn given_a_foreign_issuer_or_audience_should_refuse_the_token() {
    let key = SigningKey::rsa("rsa-1");
    let (validator, _dir) = validator_for(&jwks(&[&key])).await;
    let mut foreign = claims("alice", 300);
    foreign["iss"] = json!("https://evil.example");
    assert_eq!(
        invalid(validator.validate(&key.sign(&foreign)).await),
        "`iss` is not the configured issuer"
    );
    let mut other_audience = claims("alice", 300);
    other_audience["aud"] = json!(["someone-else"]);
    assert_eq!(
        invalid(validator.validate(&key.sign(&other_audience)).await),
        "`aud` names no configured audience"
    );
    for missing in ["iss", "aud", "exp"] {
        let mut without = claims("alice", 300);
        without.as_object_mut().expect("object").remove(missing);
        assert!(
            validator.validate(&key.sign(&without)).await.is_err(),
            "a token without `{missing}` must be refused"
        );
    }
}

#[tokio::test]
async fn given_a_token_not_yet_valid_should_refuse_it() {
    let key = SigningKey::ec("ec-1");
    let (validator, _dir) = validator_for(&jwks(&[&key])).await;
    let mut early = claims("alice", 600);
    early["nbf"] = json!(jwt::now() + 300);
    assert_eq!(
        invalid(validator.validate(&key.sign(&early)).await),
        "the token is not valid yet (`nbf`)"
    );
}

#[tokio::test]
async fn given_an_unsigned_token_should_refuse_it() {
    let key = SigningKey::rsa("rsa-1");
    let (validator, _dir) = validator_for(&jwks(&[&key])).await;
    assert!(
        validator
            .validate(&unsigned(&claims("alice", 300)))
            .await
            .is_err()
    );
}

/// The classic algorithm-confusion attack: the token says HS256 and is signed with the RSA
/// public key's bytes as the HMAC secret. A validator that takes the algorithm from the token
/// and the key from the JWKS would accept it.
#[tokio::test]
async fn given_an_hmac_token_signed_with_the_public_key_should_refuse_it() {
    let key = SigningKey::rsa("rsa-1");
    let (validator, _dir) = validator_for(&jwks(&[&key])).await;
    let forged = hmac_signed("rsa-1", &key.public_bytes, &claims("root", 300));
    assert_eq!(
        invalid(validator.validate(&forged).await),
        "the algorithm is not allowed"
    );
}

/// The same attack against an `oct` key published in the JWKS: the key is never loaded, so an
/// operator who also allows HS256 still could not be attacked through it (and HS256 cannot be
/// allowed at all).
#[tokio::test]
async fn given_a_symmetric_key_in_the_jwks_should_never_verify_with_it() {
    let key = SigningKey::rsa("rsa-1");
    let mut document: serde_json::Value = serde_json::from_str(&jwks(&[&key])).expect("json");
    document["keys"]
        .as_array_mut()
        .expect("keys")
        .push(json!({"kty": "oct", "kid": "shared", "k": "c2VjcmV0"}));
    let (validator, _dir) = validator_for(&document.to_string()).await;
    let forged = hmac_signed("shared", b"secret", &claims("root", 300));
    assert!(validator.validate(&forged).await.is_err());
    assert!(
        JwksValidator::new(OAuthConfig {
            algorithms: vec![Algorithm::RS256, Algorithm::HS256],
            ..config(JwksSource::File("/nonexistent".into()))
        })
        .is_err(),
        "a shared-secret algorithm must be refused at configuration time"
    );
}

#[tokio::test]
async fn given_a_token_signed_by_an_unpublished_key_should_refuse_it() {
    let published = SigningKey::ec("ec-1");
    let attacker = SigningKey::ec("ec-1");
    let (validator, _dir) = validator_for(&jwks(&[&published])).await;
    assert_eq!(
        invalid(
            validator
                .validate(&attacker.sign(&claims("root", 300)))
                .await
        ),
        "the signature does not verify"
    );
}

#[tokio::test]
async fn given_a_key_of_the_wrong_type_for_the_algorithm_should_refuse_the_token() {
    // The ES256 token names the RSA key's kid: no key of the right type answers it.
    let rsa = SigningKey::rsa("shared-kid");
    let mut ec = SigningKey::ec("other");
    ec.kid = "shared-kid".to_string();
    let (validator, _dir) = validator_for(&jwks(&[&rsa])).await;
    assert!(
        validator
            .validate(&ec.sign(&claims("root", 300)))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn given_a_token_without_the_principal_claim_should_refuse_it() {
    let key = SigningKey::ec("ec-1");
    let (validator, _dir) = validator_for(&jwks(&[&key])).await;
    let mut anonymous = claims("alice", 300);
    anonymous.as_object_mut().expect("object").remove("sub");
    assert_eq!(
        invalid(validator.validate(&key.sign(&anonymous)).await),
        "the principal claim is missing or not a string"
    );
    let mut numeric = claims("alice", 300);
    numeric["sub"] = json!(42);
    assert!(validator.validate(&key.sign(&numeric)).await.is_err());
}

#[tokio::test]
async fn given_a_configured_principal_claim_should_read_the_name_from_it() {
    let key = SigningKey::ec("ec-1");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("jwks.json");
    std::fs::write(&path, jwks(&[&key])).expect("write");
    let validator = JwksValidator::new(OAuthConfig {
        principal_claim: "preferred_username".to_string(),
        ..config(JwksSource::File(path))
    })
    .expect("valid");
    validator.refresh().await.expect("loads");
    let mut with_name = claims("245678901234567890", 300);
    with_name["preferred_username"] = json!("alice");
    assert_eq!(
        validator
            .validate(&key.sign(&with_name))
            .await
            .expect("valid")
            .principal,
        "alice"
    );
}

/// A token's own `jwk` header must never be trusted: an attacker would sign with a key of their
/// own and embed it.
#[tokio::test]
async fn given_a_token_embedding_its_own_key_should_ignore_that_key() {
    let published = SigningKey::ec("ec-1");
    let attacker = SigningKey::ec("attacker");
    let (validator, _dir) = validator_for(&jwks(&[&published])).await;
    let mut header = jsonwebtoken::Header::new(Algorithm::ES256);
    header.jwk = Some(serde_json::from_value(attacker.jwk.clone()).expect("a JWK"));
    header.jku = Some("https://evil.example/jwks".to_string());
    let token =
        jsonwebtoken::encode(&header, &claims("root", 300), &attacker.encoding).expect("sign");
    assert!(validator.validate(&token).await.is_err());
}

#[tokio::test]
async fn given_no_keys_loaded_should_report_unavailable_not_invalid() {
    let validator = JwksValidator::new(config(JwksSource::File("/nonexistent/jwks.json".into())))
        .expect("valid config");
    let key = SigningKey::ec("ec-1");
    assert_eq!(
        validator
            .validate(&key.sign(&claims("alice", 300)))
            .await
            .err(),
        Some(TokenError::Unavailable),
        "a JWKS that cannot be read is an outage, which the client retries, not a bad token"
    );
}

#[tokio::test]
async fn given_a_rotated_key_from_the_jwks_endpoint_should_accept_it_after_a_refresh() {
    let old = SigningKey::rsa("2026-10");
    let new = SigningKey::rsa("2026-11");
    let server = IdentityServer::start(jwks(&[&old]), String::new()).await;
    let validator =
        JwksValidator::new(config(JwksSource::Url(server.jwks_url()))).expect("valid config");
    validator
        .refresh()
        .await
        .expect("the JWKS endpoint answers");
    assert!(
        validator
            .validate(&old.sign(&claims("alice", 300)))
            .await
            .is_ok()
    );

    // The identity provider rotates. A token from the new key names a kid the cache lacks, and
    // the on-demand refresh is rate limited, so the scheduled refresh is what picks it up here.
    server.set_jwks(jwks(&[&old, &new]));
    validator
        .refresh()
        .await
        .expect("the JWKS endpoint answers");
    let validated = validator
        .validate(&new.sign(&claims("bob", 300)))
        .await
        .expect("the rotated key verifies");
    assert_eq!(validated.principal, "bob");
    assert!(validated.expires_at > UNIX_EPOCH);
}

#[tokio::test]
async fn given_an_unknown_kid_on_a_fresh_validator_should_refresh_once_and_find_it() {
    let key = SigningKey::ec("late");
    let server = IdentityServer::start(jwks(&[]), String::new()).await;
    let validator =
        JwksValidator::new(config(JwksSource::Url(server.jwks_url()))).expect("valid config");
    // Never loaded: the first token triggers a fetch, which already finds the key.
    server.set_jwks(jwks(&[&key]));
    assert!(
        validator
            .validate(&key.sign(&claims("alice", 300)))
            .await
            .is_ok()
    );
}
