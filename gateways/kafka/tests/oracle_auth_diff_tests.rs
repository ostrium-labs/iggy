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

//! The gateway's SASL mechanisms against Apache Kafka configured with the same ones (KW-9).
//!
//! Apache Kafka 4.3.1 runs with a `SASL_PLAINTEXT` listener offering PLAIN, SCRAM-SHA-256,
//! SCRAM-SHA-512 and OAUTHBEARER, the last validated by Kafka's `OAuthBearerValidatorCallbackHandler`
//! against the JWKS of a test identity provider this binary serves on loopback. The gateway runs
//! the same mechanisms over a real `iggy-server`. Each scenario performs the same exchanges on
//! both and records, as facts, the error code of every step, whether the server proved it holds
//! the SCRAM `ServerKey`, the OAUTHBEARER error challenge, and whether a session lifetime came
//! back; differences must be declared in `oracle/auth_allowed_differences.toml`.
//!
//! Redpanda is not an oracle here: its OAUTHBEARER needs an OIDC discovery document, and its
//! SCRAM answers are left to a later nightly addition.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use iggy::prelude::{GlobalPermissions, Permissions, UserClient, UserCreateOptions, UserStatus};
use kafka_protocol::messages::alter_user_scram_credentials_request::ScramCredentialDeletion;
use kafka_protocol::messages::describe_user_scram_credentials_request::UserName;
use kafka_protocol::messages::{ApiKey, DescribeUserScramCredentialsRequest, MetadataRequest};
use kafka_protocol::protocol::StrBytes;
use serde_json::json;
use tokio::sync::broadcast;

use iggy_gateway_kafka::auth::IggyAuthenticator;
use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::oauth::{DEFAULT_ALGORITHMS, JwksSource, JwksValidator, OAuthConfig};
use iggy_gateway_kafka::protocol::sasl::{EnabledMechanisms, SaslMechanism};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/facts.rs"]
mod facts;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/jwt.rs"]
mod jwt;
#[path = "common/oracle.rs"]
mod oracle;
#[path = "common/sasl_wire.rs"]
mod sasl_wire;

use facts::{Difference, Facts, compare, judge, parse_allowed};
use jwt::{AUDIENCE, ISSUER, IdentityServer, SigningKey, claims, hmac_signed, unsigned};
use oracle::{Container, KAFKA_ORACLE_IMAGE, TargetKind, WireClient, free_port, image_ready};
use sasl_wire::{Scram, alter_request, oauthbearer, plain, scram, upsertion};

const ALLOWED_DIFFERENCES: &str = include_str!("oracle/auth_allowed_differences.toml");

const ADMIN: (&str, &str) = ("admin", "admin-secret");
const ALICE: (&str, &str) = ("alice", "alice-secret");
/// A user who exists on both sides but is given no SCRAM credential.
const BOB: &str = "bob";
/// A name only the unprivileged `AlterUserScramCredentials` targets.
const CAROL: &str = "carol";

fn skip(reason: &str) {
    assert!(
        std::env::var("KAFKA_E2E_REQUIRED").as_deref() != Ok("1"),
        "KAFKA_E2E_REQUIRED=1 but the auth oracle suite cannot run: {reason}"
    );
    eprintln!("skipping the auth oracle suite: {reason}");
}

fn iggy_server_built() -> bool {
    std::env::current_exe().is_ok_and(|exe| {
        exe.ancestors().any(|dir| {
            dir.join(format!("iggy-server{}", std::env::consts::EXE_SUFFIX))
                .is_file()
        })
    })
}

/// Apache Kafka with every mechanism on one `SASL_PLAINTEXT` listener.
struct SaslKafka {
    address: String,
    _container: Container,
}

impl SaslKafka {
    #[allow(clippy::too_many_lines)]
    async fn start(jwks_url: &str) -> Result<Self, String> {
        let port = free_port();
        let controller = free_port();
        let address = format!("127.0.0.1:{port}");
        let plain_jaas = format!(
            "org.apache.kafka.common.security.plain.PlainLoginModule required \
             username=\"{}\" password=\"{}\" user_{}=\"{}\" user_{}=\"{}\";",
            ADMIN.0, ADMIN.1, ADMIN.0, ADMIN.1, ALICE.0, ALICE.1
        );
        let env: Vec<(String, String)> = [
            ("KAFKA_NODE_ID", "1".to_string()),
            ("KAFKA_PROCESS_ROLES", "broker,controller".to_string()),
            (
                "KAFKA_LISTENERS",
                format!("SASL_PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}"),
            ),
            ("KAFKA_ADVERTISED_LISTENERS", format!("SASL_PLAINTEXT://{address}")),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER".to_string()),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT".to_string(),
            ),
            ("KAFKA_INTER_BROKER_LISTENER_NAME", "SASL_PLAINTEXT".to_string()),
            ("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN".to_string()),
            (
                "KAFKA_SASL_ENABLED_MECHANISMS",
                "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512,OAUTHBEARER".to_string(),
            ),
            ("KAFKA_LISTENER_NAME_SASL__PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", plain_jaas),
            (
                "KAFKA_LISTENER_NAME_SASL__PLAINTEXT_SCRAM___SHA___256_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.scram.ScramLoginModule required;".to_string(),
            ),
            (
                "KAFKA_LISTENER_NAME_SASL__PLAINTEXT_SCRAM___SHA___512_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.scram.ScramLoginModule required;".to_string(),
            ),
            (
                "KAFKA_LISTENER_NAME_SASL__PLAINTEXT_OAUTHBEARER_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
                    .to_string(),
            ),
            (
                "KAFKA_LISTENER_NAME_SASL__PLAINTEXT_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler"
                    .to_string(),
            ),
            ("KAFKA_SASL_OAUTHBEARER_JWKS_ENDPOINT_URL", jwks_url.to_string()),
            ("KAFKA_SASL_OAUTHBEARER_EXPECTED_AUDIENCE", AUDIENCE.to_string()),
            ("KAFKA_SASL_OAUTHBEARER_EXPECTED_ISSUER", ISSUER.to_string()),
            (
                "KAFKA_OPTS",
                format!("-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={jwks_url}"),
            ),
            (
                "KAFKA_CONTROLLER_QUORUM_VOTERS",
                format!("1@127.0.0.1:{controller}"),
            ),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1".to_string()),
            ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1".to_string()),
            ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1".to_string()),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1".to_string()),
            (
                "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR",
                "1".to_string(),
            ),
            ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1".to_string()),
            ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false".to_string()),
            ("KAFKA_HEAP_OPTS", "-Xmx512m -Xms256m".to_string()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        let container = Container::start(KAFKA_ORACLE_IMAGE, "kafka-sasl", &env, &[])?;
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        let mut last = String::new();
        while std::time::Instant::now() < deadline {
            match admin_client(&address).await {
                Ok(mut client) => {
                    let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
                    match client
                        .send(
                            &MetadataRequest::default().with_topics(Some(Vec::new())),
                            version,
                        )
                        .await
                    {
                        Ok(response) if !response.brokers.is_empty() => {
                            return Ok(Self {
                                address,
                                _container: container,
                            });
                        }
                        Ok(_) => last = "no broker yet".to_string(),
                        Err(error) => last = error,
                    }
                }
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(format!(
            "SASL Kafka not ready ({last}); logs:\n{}",
            container.logs_tail()
        ))
    }
}

/// A connection authenticated as the admin with PLAIN.
async fn admin_client(address: &str) -> Result<WireClient, String> {
    let mut client = WireClient::connect(address).await?;
    match plain(&mut client, ADMIN.0, ADMIN.1).await? {
        0 => Ok(client),
        code => Err(format!("admin PLAIN login answered {code}")),
    }
}

/// The gateway over a real `iggy-server`, with the same mechanisms and users as the oracle.
struct Gateway {
    address: String,
    _shutdown: broadcast::Sender<()>,
    _server: iggy_server::TestServer,
    _data_dir: tempfile::TempDir,
}

async fn start_gateway(jwks_url: &str) -> Gateway {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = iggy_server::TestServer::spawn(data_dir.path()).await;
    let raw = iggy_server::raw_client(&server).await;
    let admin = Permissions {
        global: GlobalPermissions {
            manage_servers: true,
            read_servers: true,
            manage_users: true,
            read_users: true,
            manage_streams: true,
            read_streams: true,
            manage_topics: true,
            read_topics: true,
            poll_messages: true,
            send_messages: true,
        },
        streams: None,
    };
    for (user, password, permissions) in [
        (ADMIN.0, ADMIN.1, Some(admin)),
        (ALICE.0, ALICE.1, None),
        (BOB, "bob-secret", None),
    ] {
        raw.create_user(
            user,
            password,
            UserStatus::Active,
            permissions,
            &UserCreateOptions::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("create Iggy user {user}: {error}"));
    }
    let bridge = Arc::new(
        IggyBridge::connect(server.test_config())
            .await
            .expect("bridge connects"),
    );
    let validator = JwksValidator::new(OAuthConfig {
        issuer: ISSUER.to_string(),
        audiences: vec![AUDIENCE.to_string()],
        principal_claim: "sub".to_string(),
        algorithms: DEFAULT_ALGORITHMS.to_vec(),
        clock_skew: Duration::from_secs(30),
        jwks: JwksSource::Url(jwks_url.to_string()),
        refresh_interval: Duration::from_secs(3600),
        jwks_ca_file: None,
    })
    .expect("valid OAuth config");
    validator.refresh().await.expect("the test JWKS loads");

    let socket = bind_listener("127.0.0.1:0").expect("bind");
    let address = socket.local_addr().expect("addr").to_string();
    let listener = BoundListener::new(
        ListenerConfig::new(SecurityProtocol::SaslPlaintext, address.clone()),
        socket,
    );
    let gateway = KafkaGateway::new(GatewayConfig::default())
        .with_bridge(Some(Arc::clone(&bridge)))
        .with_authenticator(Arc::new(IggyAuthenticator::new(
            server.test_config().address,
        )))
        .with_token_validator(Arc::new(validator))
        .with_scram_credentials(bridge)
        .with_sasl_mechanisms(EnabledMechanisms::new(SaslMechanism::ALL));
    let (shutdown, receiver) = broadcast::channel(1);
    tokio::spawn(async move {
        let _ = gateway.run(vec![listener], receiver).await;
    });
    Gateway {
        address,
        _shutdown: shutdown,
        _server: server,
        _data_dir: data_dir,
    }
}

/// Signing keys and tokens both targets see.
struct Tokens {
    good: String,
    good_again: String,
    bob: String,
    expired: String,
    foreign_issuer: String,
    wrong_audience: String,
    unsigned: String,
    hmac_confusion: String,
    foreign_key: String,
}

fn tokens(key: &SigningKey) -> Tokens {
    let mut expired = claims(ALICE.0, 0);
    expired["exp"] = json!(jwt::now() - 600);
    expired["iat"] = json!(jwt::now() - 1200);
    let mut foreign_issuer = claims(ALICE.0, 600);
    foreign_issuer["iss"] = json!("https://evil.example");
    let mut wrong_audience = claims(ALICE.0, 600);
    wrong_audience["aud"] = json!("someone-else");
    let impostor = SigningKey::rsa(&key.kid);
    Tokens {
        good: key.sign(&claims(ALICE.0, 600)),
        good_again: key.sign(&claims(ALICE.0, 900)),
        bob: key.sign(&claims(BOB, 600)),
        expired: key.sign(&expired),
        foreign_issuer: key.sign(&foreign_issuer),
        wrong_audience: key.sign(&wrong_audience),
        unsigned: unsigned(&claims(ALICE.0, 600)),
        hmac_confusion: hmac_signed(&key.kid, &key.public_bytes, &claims(ALICE.0, 600)),
        foreign_key: impostor.sign(&claims(ALICE.0, 600)),
    }
}

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// SCRAM credential administration: the per-user answers of `AlterUserScramCredentials` and
/// `DescribeUserScramCredentials`.
async fn scram_admin(address: &str, facts: &mut Facts) -> Result<(), String> {
    let mut client = admin_client(address).await?;
    let request = alter_request(vec![
        upsertion(ALICE.0, Scram::Sha256, ALICE.1, 4096),
        upsertion("few-iterations", Scram::Sha256, "x", 1000),
        upsertion("too-many-iterations", Scram::Sha512, "x", 20_000),
        upsertion("twice", Scram::Sha256, "x", 4096),
        upsertion("twice", Scram::Sha512, "x", 4096),
    ])
    .with_deletions(vec![
        ScramCredentialDeletion::default()
            .with_name(s("never-had-one"))
            .with_mechanism(Scram::Sha256.type_code()),
        ScramCredentialDeletion::default()
            .with_name(s("unknown-mechanism"))
            .with_mechanism(9),
    ]);
    let response = client.send(&request, 0).await?;
    for result in &response.results {
        facts.set(format!("alter.{}.error", result.user), result.error_code);
    }
    // The 512 credential goes in a second request: Kafka refuses a user altered twice in one.
    let second = alter_request(vec![upsertion(ALICE.0, Scram::Sha512, ALICE.1, 8192)]);
    let response = client.send(&second, 0).await?;
    for result in &response.results {
        facts.set(
            format!("alter_512.{}.error", result.user),
            result.error_code,
        );
    }

    let describe = |users: Option<Vec<&str>>| {
        DescribeUserScramCredentialsRequest::default().with_users(users.map(|users| {
            users
                .into_iter()
                .map(|user| UserName::default().with_name(s(user)))
                .collect()
        }))
    };
    let response = client
        .send(&describe(Some(vec![ALICE.0, BOB, ALICE.0])), 0)
        .await?;
    facts.set("describe_named.error", response.error_code);
    for result in &response.results {
        facts.set(
            format!("describe_named.{}.error", result.user),
            result.error_code,
        );
    }
    let response = client.send(&describe(Some(vec![ALICE.0, BOB])), 0).await?;
    for result in &response.results {
        let mut infos: Vec<String> = result
            .credential_infos
            .iter()
            .map(|info| format!("{}:{}", info.mechanism, info.iterations))
            .collect();
        infos.sort();
        facts.set(format!("describe.{}.error", result.user), result.error_code);
        facts.set(
            format!("describe.{}.credentials", result.user),
            infos.join(","),
        );
    }
    let response = client.send(&describe(None), 0).await?;
    let mut everyone: Vec<String> = response
        .results
        .iter()
        .map(|result| result.user.to_string())
        .collect();
    everyone.sort();
    facts.set("describe_all.users", everyone.join(","));

    // A principal without the cluster grants. Kafka here has no authorizer, so it lets alice
    // alter; the gateway checks Iggy's manage_users. Declared, not hidden. The target is a user
    // no other scenario reads: were it bob, Kafka would give him the credential the gateway
    // refused, and `scram_login`'s `no_credential` case would differ only because of this one.
    let mut alice = WireClient::connect(address).await?;
    let login = scram(&mut alice, Scram::Sha256, ALICE.0, ALICE.1).await?;
    if login.last == "0" {
        let response = alice
            .send(
                &alter_request(vec![upsertion(CAROL, Scram::Sha256, "carol-secret", 4096)]),
                0,
            )
            .await?;
        for result in &response.results {
            facts.set(
                format!("unprivileged_alter.{}.error", result.user),
                result.error_code,
            );
        }
    }
    Ok(())
}

async fn scram_login(address: &str, facts: &mut Facts) -> Result<(), String> {
    for mechanism in [Scram::Sha256, Scram::Sha512] {
        let name = mechanism.name();
        let cases = [
            ("good", ALICE.0, ALICE.1),
            ("wrong_password", ALICE.0, "guess"),
            ("unknown_user", "nobody", "anything"),
            ("no_credential", BOB, "bob-secret"),
        ];
        for (case, user, password) in cases {
            let mut client = WireClient::connect(address).await?;
            let outcome = scram(&mut client, mechanism, user, password).await?;
            facts.set(format!("{name}.{case}.first"), &outcome.first);
            facts.set(format!("{name}.{case}.last"), &outcome.last);
            facts.set(
                format!("{name}.{case}.server_verified"),
                outcome.server_verified,
            );
            if case == "good" {
                facts.set(
                    format!("{name}.good.session_lifetime_ms"),
                    outcome.session_lifetime_ms,
                );
                let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
                let served = client
                    .send(
                        &MetadataRequest::default().with_topics(Some(Vec::new())),
                        version,
                    )
                    .await
                    .is_ok();
                facts.set(format!("{name}.good.metadata_served"), served);
            }
        }
        let mut client = WireClient::connect(address).await?;
        sasl_wire::handshake(&mut client, name).await?;
        let refused = sasl_wire::authenticate(&mut client, b"p=tls-unique,,n=alice,r=abc")
            .await
            .map_or_else(|_| "closed".to_string(), |r| r.error_code.to_string());
        facts.set(format!("{name}.channel_binding.first"), refused);
    }
    Ok(())
}

async fn oauth_login(address: &str, tokens: &Tokens, facts: &mut Facts) -> Result<(), String> {
    let cases = [
        ("good", &tokens.good),
        ("expired", &tokens.expired),
        ("foreign_issuer", &tokens.foreign_issuer),
        ("wrong_audience", &tokens.wrong_audience),
        ("unsigned", &tokens.unsigned),
        ("hmac_confusion", &tokens.hmac_confusion),
        ("foreign_key", &tokens.foreign_key),
    ];
    for (case, token) in cases {
        let mut client = WireClient::connect(address).await?;
        let outcome = oauthbearer(&mut client, token).await?;
        facts.set(format!("{case}.first"), &outcome.first);
        facts.set(
            format!("{case}.challenge_status"),
            &outcome.challenge_status,
        );
        facts.set(format!("{case}.after_challenge"), &outcome.after_challenge);
        facts.set(
            format!("{case}.lifetime_positive"),
            outcome.lifetime_positive,
        );
    }
    let mut client = WireClient::connect(address).await?;
    sasl_wire::handshake(&mut client, "OAUTHBEARER").await?;
    let wrong_authzid = sasl_wire::authenticate(
        &mut client,
        format!("n,a=root,\u{1}auth=Bearer {}\u{1}\u{1}", tokens.good).as_bytes(),
    )
    .await
    .map_or_else(|_| "closed".to_string(), |r| r.error_code.to_string());
    facts.set("authzid_mismatch.first", wrong_authzid);
    Ok(())
}

/// KIP-368: re-authenticating an OAUTHBEARER session on the same connection.
async fn reauthentication(address: &str, tokens: &Tokens, facts: &mut Facts) -> Result<(), String> {
    for (case, second) in [
        ("same_principal", &tokens.good_again),
        ("other_principal", &tokens.bob),
    ] {
        let mut client = WireClient::connect(address).await?;
        let first = oauthbearer(&mut client, &tokens.good).await?;
        facts.set(format!("{case}.login"), &first.first);
        // Kafka refuses a re-authentication less than a second after the last one.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let handshake = sasl_wire::handshake(&mut client, "OAUTHBEARER")
            .await
            .map_or_else(|_| "closed".to_string(), |code| code.to_string());
        facts.set(format!("{case}.handshake"), &handshake);
        if handshake != "0" {
            continue;
        }
        let message = format!("n,,\u{1}auth=Bearer {second}\u{1}\u{1}");
        let again = sasl_wire::authenticate(&mut client, message.as_bytes()).await;
        facts.set(
            format!("{case}.reauthenticate"),
            again
                .as_ref()
                .map_or_else(|_| "closed".to_string(), |r| r.error_code.to_string()),
        );
        facts.set(
            format!("{case}.lifetime_positive"),
            again.is_ok_and(|r| r.error_code == 0 && r.session_lifetime_ms > 0),
        );
        let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
        let served = client
            .send(
                &MetadataRequest::default().with_topics(Some(Vec::new())),
                version,
            )
            .await
            .is_ok();
        facts.set(format!("{case}.served_after"), served);
    }
    // A session without a lifetime (SCRAM, no connections.max.reauth.ms) cannot re-authenticate:
    // the handshake is refused and the connection kept.
    let mut client = WireClient::connect(address).await?;
    let login = scram(&mut client, Scram::Sha256, ALICE.0, ALICE.1).await?;
    facts.set("no_lifetime.login", &login.last);
    let handshake = sasl_wire::handshake(&mut client, "SCRAM-SHA-256")
        .await
        .map_or_else(|_| "closed".to_string(), |code| code.to_string());
    facts.set("no_lifetime.handshake", handshake);
    Ok(())
}

#[derive(Clone, Copy)]
enum Scenario {
    ScramAdmin,
    ScramLogin,
    OAuthLogin,
    Reauthentication,
}

impl Scenario {
    const ALL: [Self; 4] = [
        Self::ScramAdmin,
        Self::ScramLogin,
        Self::OAuthLogin,
        Self::Reauthentication,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::ScramAdmin => "scram_admin",
            Self::ScramLogin => "scram_login",
            Self::OAuthLogin => "oauth_login",
            Self::Reauthentication => "reauthentication",
        }
    }

    async fn run(self, address: &str, tokens: &Tokens) -> Result<Facts, String> {
        let mut facts = Facts::default();
        match self {
            Self::ScramAdmin => scram_admin(address, &mut facts).await?,
            Self::ScramLogin => scram_login(address, &mut facts).await?,
            Self::OAuthLogin => oauth_login(address, tokens, &mut facts).await?,
            Self::Reauthentication => reauthentication(address, tokens, &mut facts).await?,
        }
        Ok(facts)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_the_same_sasl_exchanges_the_gateway_should_answer_as_kafka_does() {
    if let Err(reason) = image_ready(KAFKA_ORACLE_IMAGE) {
        skip(&reason);
        return;
    }
    if !iggy_server_built() {
        skip("iggy-server is not built; run `cargo build --bin iggy-server` first");
        return;
    }
    let key = SigningKey::rsa("kw9-oracle");
    let idp = IdentityServer::start(jwt::jwks(&[&key]), String::new()).await;
    let kafka = SaslKafka::start(&idp.jwks_url())
        .await
        .unwrap_or_else(|error| panic!("Apache Kafka SASL oracle: {error}"));
    let gateway = start_gateway(&idp.jwks_url()).await;
    let tokens = tokens(&key);
    let allowed = parse_allowed(ALLOWED_DIFFERENCES).expect("auth_allowed_differences.toml parses");

    let mut differences: Vec<Difference> = Vec::new();
    let mut ran = Vec::new();
    let mut dump: BTreeMap<String, (String, String)> = BTreeMap::new();
    for scenario in Scenario::ALL {
        let ours = scenario
            .run(&gateway.address, &tokens)
            .await
            .unwrap_or_else(|error| panic!("{} on the gateway: {error}", scenario.name()));
        let theirs = scenario
            .run(&kafka.address, &tokens)
            .await
            .unwrap_or_else(|error| panic!("{} on kafka: {error}", scenario.name()));
        for (fact, value) in theirs.inner() {
            dump.insert(
                format!("{}/{fact}", scenario.name()),
                (
                    value.clone(),
                    ours.get(fact).unwrap_or("<absent>").to_string(),
                ),
            );
        }
        differences.extend(compare(scenario.name(), &ours, TargetKind::Kafka, &theirs));
        ran.push((scenario.name().to_string(), TargetKind::Kafka));
    }
    if oracle::env_flag("KAFKA_ORACLE_DUMP") {
        for (fact, (kafka_value, gateway_value)) in &dump {
            eprintln!("{fact}: kafka {kafka_value} | gateway {gateway_value}");
        }
    }
    let verdict = judge(&differences, &allowed, &ran);
    let report = verdict.report();
    let mut summary = String::new();
    let _ = writeln!(summary, "SASL mechanisms vs Apache Kafka:\n{report}");
    eprintln!("{summary}");
    if let Some(path) = std::env::var_os("KAFKA_ORACLE_REPORT") {
        use std::io::Write as _;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = file.write_all(summary.as_bytes());
        }
    }
    assert!(
        verdict.passed(),
        "the gateway's SASL differs from Kafka's:\n{report}"
    );
}
