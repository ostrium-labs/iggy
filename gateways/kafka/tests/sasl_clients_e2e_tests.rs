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

//! Real clients authenticating with SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER (KW-9, #4512).
//!
//! The stack is real on every side: a spawned `iggy-server`, the gateway in-process with the
//! bridge keeping SCRAM credentials and a JWKS validator, a test identity provider on loopback
//! that publishes the JWKS and answers OAuth token requests, and the clients in containers:
//!
//! - The Java tools of `apache/kafka:3.9.0`: `kafka-configs.sh` sets and describes SCRAM
//!   credentials (`AlterUserScramCredentials`, `DescribeUserScramCredentials`), and the console
//!   producer, consumer and `kafka-topics.sh` authenticate with each mechanism, OAUTHBEARER
//!   through Kafka's own `OAuthBearerLoginCallbackHandler` against the token endpoint.
//! - kcat 1.7 (librdkafka 1.7) with both SCRAM mechanisms.
//! - confluent-kafka-python 2.16 (librdkafka 2.16) with both SCRAM mechanisms and OAUTHBEARER,
//!   including a run that outlives its session so librdkafka has to re-authenticate (KIP-368).
//!
//! Missing prerequisites skip, unless `KAFKA_E2E_REQUIRED=1`, as in the other real-client suites.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use iggy::prelude::{GlobalPermissions, Permissions, UserClient, UserCreateOptions, UserStatus};
use tokio::sync::broadcast;

use iggy_gateway_kafka::auth::IggyAuthenticator;
use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::oauth::{DEFAULT_ALGORITHMS, JwksSource, JwksValidator, OAuthConfig};
use iggy_gateway_kafka::protocol::sasl::{EnabledMechanisms, SaslMechanism};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/jwt.rs"]
mod jwt;

use jwt::{AUDIENCE, ISSUER, IdentityServer, SigningKey, claims};

const KAFKA_IMAGE: &str = "apache/kafka:3.9.0";
const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";
/// The same pin as the client matrix, so CI pulls it once.
pub const PYTHON_IMAGE: &str = "docker.io/library/python:3.13.17-slim-trixie@sha256:fe14fd57a4c0361110eaa0c284112b189750bcb43fb04bb5d1ee7b7744027ccf";
const CONFLUENT_KAFKA_PYTHON: &str = "2.16.0";

const ROOT: (&str, &str) = ("iggy", "iggy");
const ALICE: (&str, &str) = ("alice", "alice-secret");
const TOPIC: &str = "kw9-orders";

/// Wall-clock cap on one client container.
const CLIENT_TIMEOUT: &str = "90s";

fn skip(reason: &str) -> bool {
    assert!(
        std::env::var("KAFKA_E2E_REQUIRED").as_deref() != Ok("1"),
        "KAFKA_E2E_REQUIRED=1 but the SASL client suite cannot run: {reason}"
    );
    eprintln!("skipping the SASL client suite: {reason}");
    true
}

fn container_cli() -> String {
    std::env::var("KAFKA_ORACLE_CONTAINER_CLI").unwrap_or_else(|_| "docker".to_string())
}

fn image_present(image: &str) -> bool {
    Command::new(container_cli())
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn iggy_server_built() -> bool {
    std::env::current_exe().is_ok_and(|exe| {
        exe.ancestors().any(|dir| {
            dir.join(format!("iggy-server{}", std::env::consts::EXE_SUFFIX))
                .is_file()
        })
    })
}

fn prerequisites() -> bool {
    if !cfg!(target_os = "linux") {
        return !skip("`--network host` reaches the host loopback only on Linux");
    }
    if !iggy_server_built() {
        return !skip("iggy-server is not built; run `cargo build --bin iggy-server` first");
    }
    for image in [KAFKA_IMAGE, KCAT_IMAGE, PYTHON_IMAGE] {
        if !image_present(image) {
            return !skip(&format!("client image {image} is not pulled"));
        }
    }
    true
}

/// What a client container printed, and whether it exited zero.
struct Run {
    ok: bool,
    text: String,
}

impl Run {
    fn expect_ok(self, what: &str) -> String {
        assert!(self.ok, "{what} failed:\n{}", self.text);
        self.text
    }
}

/// Runs `image` with `args` on the host network, read-only `mounts` and `env`.
fn run(image: &str, args: &[&str], mounts: &[(&Path, &str)], env: &[(&str, &str)]) -> Run {
    let mut command = Command::new("timeout");
    command.args(["--kill-after=10s", CLIENT_TIMEOUT]);
    command.args([
        container_cli().as_str(),
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "host",
    ]);
    for (host, guest) in mounts {
        command.args(["-v", &format!("{}:{guest}:ro", host.display())]);
    }
    for (key, value) in env {
        command.args(["-e", &format!("{key}={value}")]);
    }
    command.arg(image).args(args);
    let output = command.output().expect("run a client container");
    Run {
        ok: output.status.success(),
        text: format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

/// Lines the gateway logged, captured once per test binary, so a test can tell a
/// re-authentication on a live connection from a reconnect.
fn gateway_log() -> &'static Arc<Mutex<Vec<u8>>> {
    static LOG: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    LOG.get_or_init(|| {
        let log = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&log);
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter("iggy_gateway_kafka=debug")
            .with_ansi(false)
            .with_writer(move || LogWriter(Arc::clone(&writer)))
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        log
    })
}

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn logged() -> String {
    String::from_utf8_lossy(&gateway_log().lock().expect("log")).into_owned()
}

struct Stack {
    gateway: SocketAddr,
    idp: IdentityServer,
    key: Arc<SigningKey>,
    _shutdown: broadcast::Sender<()>,
    _server: iggy_server::TestServer,
    _data_dir: tempfile::TempDir,
}

/// Brings up `iggy-server`, the identity provider and the gateway with every mechanism.
/// `max_reauth` is `IGGY_KAFKA_CONNECTIONS_MAX_REAUTH_MS`.
async fn stack(token_lifetime_secs: u64, max_reauth: Option<Duration>) -> Stack {
    gateway_log();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = iggy_server::TestServer::spawn(data_dir.path()).await;
    let raw = iggy_server::raw_client(&server).await;
    raw.create_user(
        ALICE.0,
        ALICE.1,
        UserStatus::Active,
        Some(Permissions {
            global: GlobalPermissions {
                read_topics: true,
                poll_messages: true,
                send_messages: true,
                read_servers: true,
                ..GlobalPermissions::default()
            },
            streams: None,
        }),
        &UserCreateOptions::default(),
    )
    .await
    .expect("create alice");
    let bridge = Arc::new(
        IggyBridge::connect(server.test_config())
            .await
            .expect("bridge connects"),
    );
    bridge
        .ensure_stream_and_topic(TOPIC, 1)
        .await
        .expect("create the topic");

    let key = Arc::new(SigningKey::rsa("kw9-e2e"));
    let idp = IdentityServer::start(jwt::jwks(&[&key]), String::new()).await;
    let minting = Arc::clone(&key);
    idp.set_minter(move || minting.sign(&claims(ALICE.0, token_lifetime_secs)));
    let validator = JwksValidator::new(OAuthConfig {
        issuer: ISSUER.to_string(),
        audiences: vec![AUDIENCE.to_string()],
        principal_claim: "sub".to_string(),
        algorithms: DEFAULT_ALGORITHMS.to_vec(),
        clock_skew: Duration::from_secs(5),
        jwks: JwksSource::Url(idp.jwks_url()),
        refresh_interval: Duration::from_secs(3600),
        jwks_ca_file: None,
    })
    .expect("valid OAuth config");
    validator.refresh().await.expect("the JWKS loads");

    let socket = bind_listener("127.0.0.1:0").expect("bind");
    let gateway = socket.local_addr().expect("addr");
    let listener = BoundListener::new(
        ListenerConfig::new(SecurityProtocol::SaslPlaintext, gateway.to_string()),
        socket,
    );
    let config = GatewayConfig {
        connections_max_reauth: max_reauth,
        ..GatewayConfig::default()
    };
    let kafka = KafkaGateway::new(config)
        .with_bridge(Some(Arc::clone(&bridge)))
        .with_authenticator(Arc::new(IggyAuthenticator::new(
            server.test_config().address,
        )))
        .with_token_validator(Arc::new(validator))
        .with_scram_credentials(bridge)
        .with_sasl_mechanisms(EnabledMechanisms::new(SaslMechanism::ALL));
    let (shutdown, receiver) = broadcast::channel(1);
    tokio::spawn(async move {
        let _ = kafka.run(vec![listener], receiver).await;
    });
    Stack {
        gateway,
        idp,
        key,
        _shutdown: shutdown,
        _server: server,
        _data_dir: data_dir,
    }
}

/// A Java client properties file for `mechanism`, in a directory the guard keeps.
fn java_properties(
    stack: &Stack,
    mechanism: &str,
    user: &str,
    password: &str,
) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let jaas = match mechanism {
        "PLAIN" => format!(
            "org.apache.kafka.common.security.plain.PlainLoginModule required \
             username=\"{user}\" password=\"{password}\";"
        ),
        "OAUTHBEARER" => "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule \
                          required clientId=\"kafka-cli\" clientSecret=\"unused\";"
            .to_string(),
        _ => format!(
            "org.apache.kafka.common.security.scram.ScramLoginModule required \
             username=\"{user}\" password=\"{password}\";"
        ),
    };
    let mut properties = format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism={mechanism}\nsasl.jaas.config={jaas}\n\
         request.timeout.ms=10000\ndefault.api.timeout.ms=20000\n"
    );
    if mechanism == "OAUTHBEARER" {
        properties.push_str(
            "sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.\
             secured.OAuthBearerLoginCallbackHandler\nsasl.oauthbearer.token.endpoint.url=",
        );
        properties.push_str(&stack.idp.token_url());
        properties.push('\n');
    }
    std::fs::write(dir.path().join("client.properties"), properties).expect("write properties");
    dir
}

/// Runs a Java tool from the Kafka image with `properties` as `--command-config` (or the
/// client config the tool takes).
fn java(stack: &Stack, properties: &tempfile::TempDir, tool: &str, args: &[&str]) -> Run {
    let bootstrap = stack.gateway.to_string();
    let mut full: Vec<&str> = vec![tool, "--bootstrap-server", &bootstrap];
    full.extend_from_slice(args);
    let allowed = format!(
        "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={}",
        stack.idp.token_url()
    );
    run(
        KAFKA_IMAGE,
        &full,
        &[(properties.path(), "/cfg")],
        &[("KAFKA_OPTS", &allowed)],
    )
}

fn root_admin(stack: &Stack) -> tempfile::TempDir {
    java_properties(stack, "PLAIN", ROOT.0, ROOT.1)
}

/// Sets alice's SCRAM credentials with `kafka-configs.sh`, as root over PLAIN.
fn provision_scram(stack: &Stack) {
    let admin = root_admin(stack);
    java(
        stack,
        &admin,
        "/opt/kafka/bin/kafka-configs.sh",
        &[
            "--command-config",
            "/cfg/client.properties",
            "--alter",
            "--add-config",
            &format!(
                "SCRAM-SHA-256=[iterations=4096,password={}],SCRAM-SHA-512=[iterations=8192,password={}]",
                ALICE.1, ALICE.1
            ),
            "--entity-type",
            "users",
            "--entity-name",
            ALICE.0,
        ],
    )
    .expect_ok("kafka-configs --alter SCRAM credentials");
}

fn kcat(stack: &Stack, mechanism: &str, password: &str, mode: &[&str]) -> Run {
    kcat_with(stack, mechanism, password, mode, &[])
}

fn kcat_with(
    stack: &Stack,
    mechanism: &str,
    password: &str,
    mode: &[&str],
    mounts: &[(&Path, &str)],
) -> Run {
    let bootstrap = stack.gateway.to_string();
    let mut args = vec![
        "-b",
        bootstrap.as_str(),
        "-X",
        "security.protocol=SASL_PLAINTEXT",
    ];
    let mechanisms = format!("sasl.mechanisms={mechanism}");
    let user = format!("sasl.username={}", ALICE.0);
    let password = format!("sasl.password={password}");
    args.extend(["-X", &mechanisms, "-X", &user, "-X", &password]);
    args.extend_from_slice(mode);
    run(KCAT_IMAGE, &args, mounts, &[])
}

/// confluent-kafka-python installed into the target directory once.
fn python_deps() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("client-matrix")
        .join(format!("python-confluent-kafka-{CONFLUENT_KAFKA_PYTHON}"));
    if dir.join("confluent_kafka").is_dir() {
        return dir;
    }
    std::fs::create_dir_all(&dir).expect("create the python cache dir");
    let mut command = Command::new("timeout");
    command.args([
        "300s",
        container_cli().as_str(),
        "run",
        "--rm",
        "--pull",
        "never",
        "-v",
        &format!("{}:/deps", dir.display()),
        "-e",
        "HOME=/tmp",
    ]);
    // Owned by the invoking user, so the cache can be deleted without root.
    if let (Some(uid), Some(gid)) = (id_of("-u"), id_of("-g")) {
        command.args(["--user", &format!("{uid}:{gid}")]);
    }
    let status = command
        .args([
            PYTHON_IMAGE,
            "pip",
            "install",
            "--quiet",
            "--no-cache-dir",
            "--disable-pip-version-check",
            "--root-user-action=ignore",
            "--only-binary=:all:",
            "--target",
            "/deps",
            &format!("confluent-kafka=={CONFLUENT_KAFKA_PYTHON}"),
        ])
        .status()
        .expect("run pip");
    assert!(
        status.success() && dir.join("confluent_kafka").is_dir(),
        "pip install confluent-kafka failed"
    );
    dir
}

fn id_of(flag: &str) -> Option<String> {
    let output = Command::new("id").arg(flag).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Runs the librdkafka 2.x probe for `seconds` with `mechanism`.
fn rdkafka(stack: &Stack, mechanism: &str, password: &str, seconds: u32) -> Run {
    let deps = python_deps();
    let probe = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/auth");
    let bootstrap = stack.gateway.to_string();
    let seconds = seconds.to_string();
    let token_url = stack.idp.token_url();
    run(
        PYTHON_IMAGE,
        &[
            "python",
            "-u",
            "/probe/rdkafka_auth_probe.py",
            &bootstrap,
            mechanism,
            TOPIC,
            &seconds,
        ],
        &[(deps.as_path(), "/deps"), (probe.as_path(), "/probe")],
        &[
            ("PYTHONPATH", "/deps"),
            ("PROBE_USER", ALICE.0),
            ("PROBE_PASSWORD", password),
            ("PROBE_TOKEN_URL", &token_url),
        ],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_kafka_configs_when_setting_scram_credentials_should_describe_and_honour_them() {
    if !prerequisites() {
        return;
    }
    let stack = stack(600, None).await;
    provision_scram(&stack);
    let admin = root_admin(&stack);
    let described = java(
        &stack,
        &admin,
        "/opt/kafka/bin/kafka-configs.sh",
        &[
            "--command-config",
            "/cfg/client.properties",
            "--describe",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE.0,
        ],
    )
    .expect_ok("kafka-configs --describe users");
    assert!(
        described.contains("SCRAM-SHA-256=iterations=4096")
            && described.contains("SCRAM-SHA-512=iterations=8192"),
        "DescribeUserScramCredentials lists both credentials: {described}"
    );

    for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let properties = java_properties(&stack, mechanism, ALICE.0, ALICE.1);
        let topics = java(
            &stack,
            &properties,
            "/opt/kafka/bin/kafka-topics.sh",
            &["--command-config", "/cfg/client.properties", "--list"],
        )
        .expect_ok(&format!("Java kafka-topics with {mechanism}"));
        assert!(topics.contains(TOPIC), "{mechanism}: {topics}");

        let refused = java_properties(&stack, mechanism, ALICE.0, "guess");
        let failed = java(
            &stack,
            &refused,
            "/opt/kafka/bin/kafka-topics.sh",
            &["--command-config", "/cfg/client.properties", "--list"],
        );
        assert!(
            !failed.ok && failed.text.contains("Authentication failed"),
            "{mechanism} with a wrong password must fail authentication: {}",
            failed.text
        );
    }

    // Deleting alice's SHA-256 credential takes effect at the next login.
    java(
        &stack,
        &admin,
        "/opt/kafka/bin/kafka-configs.sh",
        &[
            "--command-config",
            "/cfg/client.properties",
            "--alter",
            "--delete-config",
            "SCRAM-SHA-256",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE.0,
        ],
    )
    .expect_ok("kafka-configs --delete-config SCRAM-SHA-256");
    let gone = java_properties(&stack, "SCRAM-SHA-256", ALICE.0, ALICE.1);
    let failed = java(
        &stack,
        &gone,
        "/opt/kafka/bin/kafka-topics.sh",
        &["--command-config", "/cfg/client.properties", "--list"],
    );
    assert!(
        !failed.ok,
        "a deleted credential no longer logs in: {}",
        failed.text
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_scram_credentials_when_java_and_kcat_produce_and_consume_should_read_back() {
    if !prerequisites() {
        return;
    }
    let stack = stack(600, None).await;
    provision_scram(&stack);
    let properties = java_properties(&stack, "SCRAM-SHA-512", ALICE.0, ALICE.1);
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("records.txt"), "k1:java-1\nk2:java-2\n").expect("records");
    let bootstrap = stack.gateway.to_string();
    run(
        KAFKA_IMAGE,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server {bootstrap} \
                 --producer.config /cfg/client.properties --topic {TOPIC} \
                 --property parse.key=true --property key.separator=: < /records/records.txt"
            ),
        ],
        &[(properties.path(), "/cfg"), (dir.path(), "/records")],
        &[],
    )
    .expect_ok("Java console producer with SCRAM-SHA-512");

    std::fs::write(dir.path().join("kcat.txt"), "k3:kcat-3\n").expect("records");
    kcat_with(
        &stack,
        "SCRAM-SHA-256",
        ALICE.1,
        &[
            "-P",
            "-t",
            TOPIC,
            "-p",
            "0",
            "-K",
            ":",
            "-l",
            "/records/kcat.txt",
        ],
        &[(dir.path(), "/records")],
    )
    .expect_ok("kcat produce with SCRAM-SHA-256");
    let consumed = kcat(
        &stack,
        "SCRAM-SHA-512",
        ALICE.1,
        &[
            "-C",
            "-t",
            TOPIC,
            "-p",
            "0",
            "-o",
            "beginning",
            "-e",
            "-f",
            "%k=%s\n",
        ],
    )
    .expect_ok("kcat consume with SCRAM-SHA-512");
    assert!(
        consumed.contains("k1=java-1")
            && consumed.contains("k2=java-2")
            && consumed.contains("k3=kcat-3"),
        "librdkafka reads what the Java producer wrote: {consumed}"
    );

    let java_read = run(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            &bootstrap,
            "--consumer.config",
            "/cfg/client.properties",
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--offset",
            "earliest",
            "--max-messages",
            "2",
            "--timeout-ms",
            "20000",
            "--property",
            "print.key=true",
        ],
        &[(properties.path(), "/cfg")],
        &[],
    )
    .expect_ok("Java console consumer with SCRAM-SHA-512");
    assert!(java_read.contains("k1\tjava-1"), "{java_read}");

    let refused = kcat(&stack, "SCRAM-SHA-256", "guess", &["-L"]);
    assert!(
        !refused.ok || refused.text.contains("Authentication failed"),
        "kcat with a wrong password must not authenticate: {}",
        refused.text
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_an_oauth_token_endpoint_when_java_authenticates_with_oauthbearer_should_list_topics()
{
    if !prerequisites() {
        return;
    }
    let stack = stack(600, None).await;
    let properties = java_properties(&stack, "OAUTHBEARER", ALICE.0, "");
    let topics = java(
        &stack,
        &properties,
        "/opt/kafka/bin/kafka-topics.sh",
        &["--command-config", "/cfg/client.properties", "--list"],
    )
    .expect_ok("Java kafka-topics with OAUTHBEARER");
    assert!(topics.contains(TOPIC), "{topics}");

    // A token from a key the JWKS does not publish is refused.
    let impostor = Arc::new(SigningKey::rsa("kw9-e2e"));
    stack
        .idp
        .set_minter(move || impostor.sign(&claims(ALICE.0, 600)));
    let refused = java(
        &stack,
        &properties,
        "/opt/kafka/bin/kafka-topics.sh",
        &["--command-config", "/cfg/client.properties", "--list"],
    );
    assert!(
        !refused.ok && refused.text.contains("invalid_token"),
        "the Java client must report the refusal: {}",
        refused.text
    );
    drop(stack.key);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_librdkafka_2_when_authenticating_with_each_mechanism_should_round_trip() {
    if !prerequisites() {
        return;
    }
    let stack = stack(600, None).await;
    provision_scram(&stack);
    for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "PLAIN"] {
        let output = rdkafka(&stack, mechanism, ALICE.1, 0).expect_ok(mechanism);
        assert!(output.contains("DONE 1"), "{mechanism}: {output}");
    }
    let refused = rdkafka(&stack, "SCRAM-SHA-512", "guess", 0);
    assert!(
        !refused.ok && refused.text.contains("AUTHENTICATION"),
        "librdkafka with a wrong SCRAM password: {}",
        refused.text
    );
}

/// KIP-368 with a real client: the token lives 20 seconds, so librdkafka has to fetch a new one
/// and re-authenticate its live connections while the probe keeps producing and consuming.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_a_short_lived_token_when_librdkafka_runs_past_it_should_re_authenticate_in_place() {
    if !prerequisites() {
        return;
    }
    let stack = stack(20, None).await;
    let output = rdkafka(&stack, "OAUTHBEARER", "", 45).expect_ok("a 45 s OAUTHBEARER run");
    assert!(output.contains("DONE"), "{output}");
    let tokens = output.matches("TOKEN exp=").count();
    assert!(tokens >= 2, "librdkafka refreshed its token: {output}");
    let log = logged();
    assert!(
        log.contains("SASL re-authentication started"),
        "the gateway saw a re-authentication on an open connection"
    );
    assert!(
        !log.contains("SASL session expired without re-authenticating"),
        "no connection outlived its session: {log}"
    );
}

/// The same with SCRAM and `IGGY_KAFKA_CONNECTIONS_MAX_REAUTH_MS`: the Java client
/// re-authenticates with its stored credentials.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_connections_max_reauth_when_a_java_consumer_runs_past_it_should_re_authenticate() {
    if !prerequisites() {
        return;
    }
    let stack = stack(600, Some(Duration::from_secs(8))).await;
    provision_scram(&stack);
    let properties = java_properties(&stack, "SCRAM-SHA-256", ALICE.0, ALICE.1);
    let bootstrap = stack.gateway.to_string();
    // Produces a record every 2 s for 24 s on one long-lived producer, three sessions' worth.
    run(
        KAFKA_IMAGE,
        &[
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 12); do echo k$i:v$i; sleep 2; done | \
                 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server {bootstrap} \
                 --producer.config /cfg/client.properties --topic {TOPIC} \
                 --property parse.key=true --property key.separator=:"
            ),
        ],
        &[(properties.path(), "/cfg")],
        &[],
    )
    .expect_ok("a 24 s Java producer");
    let log = logged();
    assert!(
        log.contains("SASL re-authentication started"),
        "the Java producer re-authenticated its connection"
    );
}
