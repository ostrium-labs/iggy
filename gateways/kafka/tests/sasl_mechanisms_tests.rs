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

//! SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER over a real socket, with KIP-368
//! re-authentication.
//!
//! The verifiers are stubs (a token validator that accepts named tokens, a map of SCRAM
//! credentials, a fixed set of Iggy users), so these cover the listener's exchanges rather than
//! Iggy or a key server. `oauth_validator_tests.rs` covers the JWT checks, and the real-client
//! suite covers both against Java and librdkafka.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use kafka_protocol::messages::{
    ApiKey, MetadataRequest, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest,
};
use kafka_protocol::protocol::StrBytes;
use sha2::{Digest, Sha256, Sha512};

use iggy_gateway_kafka::GatewayConfig;
use iggy_gateway_kafka::KafkaGateway;
use iggy_gateway_kafka::auth::{
    AuthError, AuthenticatedPrincipal, PrincipalResolver, ScramCredentialSource,
};
use iggy_gateway_kafka::listener::SecurityProtocol;
use iggy_gateway_kafka::oauth::{TokenError, TokenValidator, ValidatedToken};
use iggy_gateway_kafka::protocol::acl::PrincipalPermissions;
use iggy_gateway_kafka::protocol::sasl::{EnabledMechanisms, SaslMechanism};
use iggy_gateway_kafka::scram::{ScramCredential, ScramMechanism};

#[path = "common/oracle.rs"]
mod oracle;
#[path = "common/server.rs"]
mod server;

use oracle::WireClient;

const ERROR_NONE: i16 = 0;
const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ERROR_ILLEGAL_SASL_STATE: i16 = 34;
const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;

/// Accepts tokens named in its map, as `token -> (principal, lifetime)`.
#[derive(Debug, Default)]
struct StubValidator {
    tokens: HashMap<String, (String, Duration)>,
    unavailable: bool,
}

#[async_trait]
impl TokenValidator for StubValidator {
    async fn validate(&self, token: &str) -> Result<ValidatedToken, TokenError> {
        if self.unavailable {
            return Err(TokenError::Unavailable);
        }
        self.tokens
            .get(token)
            .map(|(principal, lifetime)| ValidatedToken {
                principal: principal.clone(),
                expires_at: SystemTime::now() + *lifetime,
            })
            .ok_or(TokenError::Invalid("not a known test token"))
    }
}

#[derive(Debug, Default)]
struct StubScram {
    credentials: HashMap<(String, ScramMechanism), ScramCredential>,
}

#[async_trait]
impl ScramCredentialSource for StubScram {
    async fn scram_credential(
        &self,
        username: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>, AuthError> {
        Ok(self
            .credentials
            .get(&(username.to_string(), mechanism))
            .cloned())
    }
}

/// The Iggy users that exist.
#[derive(Debug)]
struct StubUsers(Vec<&'static str>);

#[async_trait]
impl PrincipalResolver for StubUsers {
    async fn resolve(&self, username: &str) -> Result<AuthenticatedPrincipal, AuthError> {
        if self.0.contains(&username) {
            Ok(AuthenticatedPrincipal::from_projection(
                username.to_string(),
                PrincipalPermissions::default(),
                true,
            ))
        } else {
            Err(AuthError::Rejected)
        }
    }
}

fn config(max_reauth: Option<Duration>) -> GatewayConfig {
    GatewayConfig {
        listeners: vec![server::listener(SecurityProtocol::SaslPlaintext)],
        idle_timeout: Duration::from_secs(10),
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        shutdown_drain_timeout: Duration::from_secs(5),
        connections_max_reauth: max_reauth,
        ..GatewayConfig::default()
    }
}

struct Setup {
    tokens: Vec<(&'static str, &'static str, Duration)>,
    validator_down: bool,
    max_reauth: Option<Duration>,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            tokens: vec![
                ("token-alice", "alice", Duration::from_secs(600)),
                ("token-alice-2", "alice", Duration::from_secs(600)),
                ("token-bob", "bob", Duration::from_secs(600)),
                ("token-ghost", "ghost", Duration::from_secs(600)),
                ("token-short", "alice", Duration::from_secs(2)),
            ],
            validator_down: false,
            max_reauth: None,
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn spawn(setup: Setup) -> SocketAddr {
    let validator = StubValidator {
        tokens: setup
            .tokens
            .iter()
            .map(|(token, principal, lifetime)| {
                ((*token).to_string(), ((*principal).to_string(), *lifetime))
            })
            .collect(),
        unavailable: setup.validator_down,
    };
    let mut scram = StubScram::default();
    for mechanism in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
        scram.credentials.insert(
            ("alice".to_string(), mechanism),
            ScramCredential::from_password(mechanism, "alice-secret", b"salt-alice".to_vec(), 4096),
        );
        // A credential whose Iggy user is gone.
        scram.credentials.insert(
            ("orphan".to_string(), mechanism),
            ScramCredential::from_password(
                mechanism,
                "orphan-secret",
                b"salt-orphan".to_vec(),
                4096,
            ),
        );
    }
    let (addrs, shutdown) = server::spawn_gateway(
        |config| {
            KafkaGateway::new(config)
                .with_token_validator(Arc::new(validator))
                .with_scram_credentials(Arc::new(scram))
                .with_principal_resolver(Arc::new(StubUsers(vec!["alice", "bob"])))
                .with_sasl_mechanisms(EnabledMechanisms::new([
                    SaslMechanism::ScramSha256,
                    SaslMechanism::ScramSha512,
                    SaslMechanism::OAuthBearer,
                ]))
        },
        config(setup.max_reauth),
    );
    std::mem::forget(shutdown);
    addrs[0]
}

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// Sends a handshake for `mechanism` and returns its error code and mechanism list.
async fn handshake(client: &mut WireClient, mechanism: &str) -> Result<(i16, Vec<String>), String> {
    let response = client
        .send(
            &SaslHandshakeRequest::default().with_mechanism(s(mechanism)),
            1,
        )
        .await?;
    Ok((
        response.error_code,
        response
            .mechanisms
            .iter()
            .map(ToString::to_string)
            .collect(),
    ))
}

async fn authenticate(
    client: &mut WireClient,
    token: &[u8],
) -> Result<SaslAuthenticateResponse, String> {
    client
        .send(
            &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::copy_from_slice(token)),
            2,
        )
        .await
}

fn bearer(token: &str) -> Vec<u8> {
    format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes()
}

async fn metadata(client: &mut WireClient) -> Result<(), String> {
    let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
    client
        .send(
            &MetadataRequest::default().with_topics(Some(Vec::new())),
            version,
        )
        .await
        .map(|_| ())
}

async fn oauth_login(addr: SocketAddr, token: &str) -> (WireClient, SaslAuthenticateResponse) {
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    assert_eq!(
        handshake(&mut client, "OAUTHBEARER")
            .await
            .expect("handshake")
            .0,
        0
    );
    let response = authenticate(&mut client, &bearer(token))
        .await
        .expect("answered");
    (client, response)
}

#[tokio::test]
async fn given_the_enabled_mechanisms_when_handshaking_should_list_exactly_those() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    let (code, mechanisms) = handshake(&mut client, "PLAIN").await.expect("answered");
    assert_eq!(
        code, ERROR_UNSUPPORTED_SASL_MECHANISM,
        "PLAIN is not enabled here"
    );
    assert_eq!(
        mechanisms,
        vec!["SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"],
        "the refusal names what the client may configure instead"
    );
}

#[tokio::test]
async fn given_a_valid_token_should_authenticate_and_send_its_lifetime() {
    let addr = spawn(Setup::default());
    let (mut client, response) = oauth_login(addr, "token-alice").await;
    assert_eq!(response.error_code, ERROR_NONE);
    assert!(response.auth_bytes.is_empty());
    assert!(
        (590_000..=600_000).contains(&response.session_lifetime_ms),
        "KIP-368: the session lasts as long as the token, got {}",
        response.session_lifetime_ms
    );
    metadata(&mut client)
        .await
        .expect("served once authenticated");
}

#[tokio::test]
async fn given_an_invalid_token_should_send_the_error_challenge_then_fail() {
    let addr = spawn(Setup::default());
    let (mut client, response) = oauth_login(addr, "forged").await;
    assert_eq!(
        response.error_code, ERROR_NONE,
        "RFC 7628: the refusal is a challenge first"
    );
    assert_eq!(
        response.auth_bytes.as_ref(),
        br#"{"status":"invalid_token"}"#,
        "the challenge Kafka's OAuthBearerSaslServer sends"
    );
    let failure = authenticate(&mut client, &[0x01]).await.expect("answered");
    assert_eq!(failure.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
    assert_eq!(
        failure.error_message.as_deref(),
        Some(r#"{"status":"invalid_token"}"#)
    );
    assert!(
        metadata(&mut client).await.is_err(),
        "the connection is closed after 58"
    );
}

#[tokio::test]
async fn given_a_token_for_an_unknown_iggy_user_should_fail() {
    let addr = spawn(Setup::default());
    let (mut client, response) = oauth_login(addr, "token-ghost").await;
    assert_eq!(response.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
    assert!(metadata(&mut client).await.is_err());
}

#[tokio::test]
async fn given_an_authorization_id_for_someone_else_should_fail() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "OAUTHBEARER")
        .await
        .expect("handshake");
    let response = authenticate(&mut client, b"n,a=root,\x01auth=Bearer token-alice\x01\x01")
        .await
        .expect("answered");
    assert_eq!(response.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
}

#[tokio::test]
async fn given_a_malformed_bearer_response_should_fail() {
    let addr = spawn(Setup::default());
    for malformed in [
        &b"n,,auth=Bearer token-alice"[..],
        b"y,,\x01auth=Bearer token-alice\x01\x01",
        b"n,,\x01auth=Basic token-alice\x01\x01",
        b"n,,\x01host=x\x01\x01",
    ] {
        let mut client = WireClient::connect(&addr.to_string())
            .await
            .expect("connect");
        handshake(&mut client, "OAUTHBEARER")
            .await
            .expect("handshake");
        let response = authenticate(&mut client, malformed)
            .await
            .expect("answered");
        assert_eq!(
            response.error_code,
            ERROR_SASL_AUTHENTICATION_FAILED,
            "{:?}",
            String::from_utf8_lossy(malformed)
        );
    }
}

#[tokio::test]
async fn given_no_signing_keys_should_close_rather_than_fail_the_credentials() {
    let addr = spawn(Setup {
        validator_down: true,
        ..Setup::default()
    });
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "OAUTHBEARER")
        .await
        .expect("handshake");
    assert!(
        authenticate(&mut client, &bearer("token-alice"))
            .await
            .is_err(),
        "an identity provider outage closes, which clients retry; 58 would be fatal"
    );
}

#[tokio::test]
async fn given_a_session_with_a_lifetime_should_re_authenticate_on_the_same_connection() {
    let addr = spawn(Setup::default());
    let (mut client, response) = oauth_login(addr, "token-alice").await;
    assert_eq!(response.error_code, ERROR_NONE);
    let (code, _) = handshake(&mut client, "OAUTHBEARER")
        .await
        .expect("re-handshake");
    assert_eq!(
        code, ERROR_NONE,
        "KIP-368: a new handshake starts a re-authentication"
    );
    let again = authenticate(&mut client, &bearer("token-alice-2"))
        .await
        .expect("answered");
    assert_eq!(again.error_code, ERROR_NONE);
    assert!(again.session_lifetime_ms > 0);
    metadata(&mut client)
        .await
        .expect("the same connection keeps serving");
}

#[tokio::test]
async fn given_a_re_authentication_as_someone_else_should_fail_and_close() {
    let addr = spawn(Setup::default());
    let (mut client, _) = oauth_login(addr, "token-alice").await;
    handshake(&mut client, "OAUTHBEARER")
        .await
        .expect("re-handshake");
    let switched = authenticate(&mut client, &bearer("token-bob"))
        .await
        .expect("answered");
    assert_eq!(switched.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
    assert!(metadata(&mut client).await.is_err());
}

#[tokio::test]
async fn given_a_re_authentication_under_another_mechanism_should_fail_and_close() {
    let addr = spawn(Setup::default());
    let (mut client, _) = oauth_login(addr, "token-alice").await;
    let (code, _) = handshake(&mut client, "SCRAM-SHA-256")
        .await
        .expect("re-handshake");
    assert_eq!(
        code, ERROR_NONE,
        "Kafka accepts the handshake and fails what follows"
    );
    let response = authenticate(&mut client, b"n,,n=alice,r=abc")
        .await
        .expect("answered");
    assert_eq!(response.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
}

#[tokio::test]
async fn given_an_expired_session_should_close_on_the_next_request() {
    let addr = spawn(Setup::default());
    let (mut client, response) = oauth_login(addr, "token-short").await;
    assert_eq!(response.error_code, ERROR_NONE);
    assert!(response.session_lifetime_ms <= 2000);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        metadata(&mut client).await.is_err(),
        "a session past its lifetime is not served"
    );
}

#[tokio::test]
async fn given_connections_max_reauth_should_bound_the_session_and_allow_re_authenticating() {
    let addr = spawn(Setup {
        max_reauth: Some(Duration::from_secs(30)),
        ..Setup::default()
    });
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "SCRAM-SHA-512")
        .await
        .expect("handshake");
    let response = scram_login(&mut client, ScramMechanism::Sha512, "alice", "alice-secret")
        .await
        .expect("SCRAM login");
    assert!(
        (29_000..=30_000).contains(&response.session_lifetime_ms),
        "connections.max.reauth.ms bounds a SCRAM session, got {}",
        response.session_lifetime_ms
    );
    handshake(&mut client, "SCRAM-SHA-512")
        .await
        .expect("re-handshake");
    scram_login(&mut client, ScramMechanism::Sha512, "alice", "alice-secret")
        .await
        .expect("SCRAM re-authentication");
    metadata(&mut client).await.expect("still served");
}

#[tokio::test]
async fn given_a_session_without_a_lifetime_should_refuse_a_second_handshake_and_stay_open() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "SCRAM-SHA-256")
        .await
        .expect("handshake");
    let response = scram_login(&mut client, ScramMechanism::Sha256, "alice", "alice-secret")
        .await
        .expect("SCRAM login");
    assert_eq!(response.session_lifetime_ms, 0);
    let (code, _) = handshake(&mut client, "SCRAM-SHA-256")
        .await
        .expect("answered");
    assert_eq!(code, ERROR_ILLEGAL_SASL_STATE);
    metadata(&mut client).await.expect("still served");
}

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

fn hmac(mechanism: ScramMechanism, key: &[u8], message: &[u8]) -> Vec<u8> {
    match mechanism {
        ScramMechanism::Sha256 => {
            let mut mac = HmacSha256::new_from_slice(key).expect("any key");
            mac.update(message);
            mac.finalize().into_bytes().to_vec()
        }
        ScramMechanism::Sha512 => {
            let mut mac = HmacSha512::new_from_slice(key).expect("any key");
            mac.update(message);
            mac.finalize().into_bytes().to_vec()
        }
    }
}

fn hash(mechanism: ScramMechanism, message: &[u8]) -> Vec<u8> {
    match mechanism {
        ScramMechanism::Sha256 => Sha256::digest(message).to_vec(),
        ScramMechanism::Sha512 => Sha512::digest(message).to_vec(),
    }
}

fn salted(mechanism: ScramMechanism, password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
    let len = match mechanism {
        ScramMechanism::Sha256 => 32,
        ScramMechanism::Sha512 => 64,
    };
    let mut out = vec![0u8; len];
    match mechanism {
        ScramMechanism::Sha256 => {
            pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut out);
        }
        ScramMechanism::Sha512 => {
            pbkdf2::pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, iterations, &mut out);
        }
    }
    out
}

/// The client side of a SCRAM login, after the handshake: returns the final response, or the
/// error code of the step that failed.
async fn scram_login(
    client: &mut WireClient,
    mechanism: ScramMechanism,
    user: &str,
    password: &str,
) -> Result<SaslAuthenticateResponse, i16> {
    let bare = format!("n={user},r=fyko+d2lbbFgONRv9qkxdawL");
    let first = authenticate(client, format!("n,,{bare}").as_bytes())
        .await
        .map_err(|_| -1_i16)?;
    if first.error_code != ERROR_NONE {
        return Err(first.error_code);
    }
    let server_first = String::from_utf8(first.auth_bytes.to_vec()).expect("utf-8");
    let fields: Vec<&str> = server_first.split(',').collect();
    let nonce = fields[0].strip_prefix("r=").expect("nonce");
    assert!(
        nonce.starts_with("fyko+d2lbbFgONRv9qkxdawL"),
        "the client nonce is extended"
    );
    let salt = BASE64
        .decode(fields[1].strip_prefix("s=").expect("salt"))
        .expect("base64 salt");
    let iterations: u32 = fields[2]
        .strip_prefix("i=")
        .expect("iterations")
        .parse()
        .expect("a number");
    let salted = salted(mechanism, password, &salt, iterations);
    let client_key = hmac(mechanism, &salted, b"Client Key");
    let stored_key = hash(mechanism, &client_key);
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{bare},{server_first},{without_proof}");
    let signature = hmac(mechanism, &stored_key, auth_message.as_bytes());
    let proof: Vec<u8> = client_key
        .iter()
        .zip(&signature)
        .map(|(key, sig)| key ^ sig)
        .collect();
    let last = authenticate(
        client,
        format!("{without_proof},p={}", BASE64.encode(proof)).as_bytes(),
    )
    .await
    .map_err(|_| -1_i16)?;
    if last.error_code != ERROR_NONE {
        return Err(last.error_code);
    }
    let server_key = hmac(mechanism, &salted, b"Server Key");
    let expected = format!(
        "v={}",
        BASE64.encode(hmac(mechanism, &server_key, auth_message.as_bytes()))
    );
    assert_eq!(
        last.auth_bytes.as_ref(),
        expected.as_bytes(),
        "the server proves it holds the ServerKey"
    );
    Ok(last)
}

#[tokio::test]
async fn given_the_right_password_should_authenticate_with_both_scram_mechanisms() {
    let addr = spawn(Setup::default());
    for (mechanism, name) in [
        (ScramMechanism::Sha256, "SCRAM-SHA-256"),
        (ScramMechanism::Sha512, "SCRAM-SHA-512"),
    ] {
        let mut client = WireClient::connect(&addr.to_string())
            .await
            .expect("connect");
        assert_eq!(handshake(&mut client, name).await.expect("handshake").0, 0);
        scram_login(&mut client, mechanism, "alice", "alice-secret")
            .await
            .unwrap_or_else(|code| panic!("{name} login failed with {code}"));
        metadata(&mut client).await.expect("served");
    }
}

#[tokio::test]
async fn given_a_wrong_password_should_fail_scram_at_the_proof() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "SCRAM-SHA-256")
        .await
        .expect("handshake");
    assert_eq!(
        scram_login(&mut client, ScramMechanism::Sha256, "alice", "guess")
            .await
            .err(),
        Some(ERROR_SASL_AUTHENTICATION_FAILED)
    );
    assert!(metadata(&mut client).await.is_err());
}

#[tokio::test]
async fn given_an_unknown_scram_user_should_fail_only_at_the_proof() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "SCRAM-SHA-512")
        .await
        .expect("handshake");
    // `scram_login` returns the first step's code if that step fails; reaching the proof is the
    // point: the first message is answered like a real user's.
    assert_eq!(
        scram_login(&mut client, ScramMechanism::Sha512, "nobody", "anything")
            .await
            .err(),
        Some(ERROR_SASL_AUTHENTICATION_FAILED)
    );
}

#[tokio::test]
async fn given_a_scram_credential_without_an_iggy_user_should_fail() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "SCRAM-SHA-256")
        .await
        .expect("handshake");
    assert_eq!(
        scram_login(
            &mut client,
            ScramMechanism::Sha256,
            "orphan",
            "orphan-secret"
        )
        .await
        .err(),
        Some(ERROR_SASL_AUTHENTICATION_FAILED),
        "the proof verifies, but the account it names does not exist in Iggy"
    );
}

#[tokio::test]
async fn given_a_scram_proof_for_the_other_mechanism_should_fail() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "SCRAM-SHA-512")
        .await
        .expect("handshake");
    assert!(
        scram_login(&mut client, ScramMechanism::Sha256, "alice", "alice-secret")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn given_channel_binding_should_refuse_the_scram_first_message() {
    let addr = spawn(Setup::default());
    let mut client = WireClient::connect(&addr.to_string())
        .await
        .expect("connect");
    handshake(&mut client, "SCRAM-SHA-256")
        .await
        .expect("handshake");
    let response = authenticate(&mut client, b"p=tls-unique,,n=alice,r=abc")
        .await
        .expect("answered");
    assert_eq!(response.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
}
