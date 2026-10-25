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

//! Real Kafka brokers as oracles for the gateway, and a wire client that talks to all of them.
//!
//! Three oracles, each a black box behind the Kafka wire:
//!
//! - Apache Kafka in its official container, `KRaft` single node ([`KafkaOracle`]). The default,
//!   and the one fork CI runs on every pull request.
//! - Redpanda in its official container ([`RedpandaOracle`]). Opt-in with
//!   `KAFKA_ORACLE_REDPANDA=1`, which the nightly job sets. Redpanda's source is BSL: only its
//!   container is used, never its code.
//! - Confluent Cloud, a managed multi-broker Kafka ([`ConfluentOracle`]), over `SASL_SSL` with
//!   a scoped API key. Local only: it runs when `~/confluent-oracle.env` (or the file
//!   `KAFKA_ORACLE_CONFLUENT_ENV` names) exists, never in CI. See `docs/loams/confluent-oracle.md`.
//!
//! [`WireClient`] encodes requests with `kafka-protocol`'s client half and negotiates each API's
//! version down to what the target advertises, so one scenario runs unchanged on every target.
//! On a multi-broker target it also sends each partition and coordinator request to the broker
//! that owns it, as a real client does.
#![allow(dead_code)]

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, ApiKey, ApiVersionsRequest,
    DeleteTopicsRequest, EndTxnRequest, FetchRequest, FindCoordinatorRequest, HeartbeatRequest,
    InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest, ListOffsetsRequest,
    MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest, RequestHeader,
    ResponseHeader, SaslAuthenticateRequest, SaslHandshakeRequest, SyncGroupRequest, TopicName,
    TxnOffsetCommitRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// The Apache Kafka oracle image. Pinned; CI pulls every `*_IMAGE` constant under `tests/`.
pub const KAFKA_ORACLE_IMAGE: &str = "apache/kafka:4.3.1";

/// The Redpanda oracle image. Deliberately not named `*_IMAGE`: pull requests do not pull it,
/// only the nightly job does.
pub const REDPANDA_ORACLE_TAG: &str = "redpandadata/redpanda:v26.1.18";

/// Client id every oracle request carries, so broker logs can tell harness traffic apart.
pub const CLIENT_ID: &str = "loams-oracle";

/// Prefix of every container, topic, group and transactional id the harness creates.
pub const RESOURCE_PREFIX: &str = "loams-oracle-";

/// Budget for a container broker to answer Metadata with a broker in it.
const ORACLE_READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Budget for one request's response.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest response frame the harness accepts.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

static NEXT_CONTAINER: AtomicUsize = AtomicUsize::new(0);

/// Which broker a target is. Only the gateway is the system under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TargetKind {
    Gateway,
    Kafka,
    Redpanda,
    Confluent,
}

impl TargetKind {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Kafka => "kafka",
            Self::Redpanda => "redpanda",
            Self::Confluent => "confluent",
        }
    }
}

impl fmt::Display for TargetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One broker the scenarios run against: a plaintext loopback `host:port`, or a managed
/// cluster's bootstrap `host:port` reached over `SASL_SSL`.
#[derive(Debug, Clone)]
pub struct Target {
    pub kind: TargetKind,
    pub address: String,
    /// TLS and SASL/PLAIN for a managed cluster; `None` for a plaintext loopback broker.
    pub secure: Option<Arc<Secure>>,
}

impl Target {
    /// A plaintext broker at `address`.
    #[must_use]
    pub const fn plain(kind: TargetKind, address: String) -> Self {
        Self {
            kind,
            address,
            secure: None,
        }
    }
}

/// TLS settings and SASL/PLAIN credentials for a managed cluster. `Debug` never shows them.
pub struct Secure {
    tls: Arc<rustls::ClientConfig>,
    username: String,
    password: String,
}

impl fmt::Debug for Secure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secure { SASL_SSL PLAIN, credentials redacted }")
    }
}

/// Why a target is not available, and whether that is fatal.
#[derive(Debug)]
pub enum Unavailable {
    /// Not asked for, or its prerequisite is absent. A skip, never a failure.
    Skipped(String),
    /// Asked for and broken.
    Failed(String),
}

/// Whether `name` is set to `1`.
#[must_use]
pub fn env_flag(name: &str) -> bool {
    std::env::var(name).as_deref() == Ok("1")
}

/// The container CLI, `docker` unless `KAFKA_ORACLE_CONTAINER_CLI` names another (`podman`).
fn container_cli() -> String {
    std::env::var("KAFKA_ORACLE_CONTAINER_CLI").unwrap_or_else(|_| "docker".to_string())
}

/// Whether the container CLI answers and `image` is in its local store. Containers run with
/// `--pull never`, so a missing image is a skip that names it rather than a slow pull mid-test.
pub fn image_ready(image: &str) -> Result<(), String> {
    let cli = container_cli();
    let present = Command::new(&cli)
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("`{cli}` is unavailable: {error}"))?;
    if present.success() {
        Ok(())
    } else {
        Err(format!(
            "image {image} is not pulled; run `{cli} pull {image}`"
        ))
    }
}

/// A free loopback port. The window between release and the broker's bind is acceptable here:
/// the oracle suites run in a serialized nextest group.
#[must_use]
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind an ephemeral port")
        .port()
}

/// A running container, removed on drop.
pub struct Container {
    name: String,
}

impl Container {
    /// Starts `image` detached on the host network with `env` and `args`.
    pub fn start(
        image: &str,
        label: &str,
        env: &[(String, String)],
        args: &[String],
    ) -> Result<Self, String> {
        let name = format!(
            "{RESOURCE_PREFIX}{label}-{}-{}",
            std::process::id(),
            NEXT_CONTAINER.fetch_add(1, Ordering::Relaxed)
        );
        let mut command = Command::new(container_cli());
        command.args([
            "run",
            "-d",
            "--rm",
            "--network",
            "host",
            "--pull",
            "never",
            "--name",
            &name,
        ]);
        for (key, value) in env {
            command.arg("-e").arg(format!("{key}={value}"));
        }
        command.arg(image).args(args);
        let output = command
            .output()
            .map_err(|error| format!("start {label}: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "start {label}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(Self { name })
    }

    /// Restarts the container: its broker stops, as on a shutdown, and starts on the same data
    /// and ports.
    pub fn restart(&self) -> Result<(), String> {
        let output = Command::new(container_cli())
            .args(["restart", "--time", "30", &self.name])
            .output()
            .map_err(|error| format!("restart {}: {error}", self.name))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "restart {}: {}",
                self.name,
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }

    /// Stops the container the way an operator does, with SIGTERM and a grace period, so a broker
    /// runs its controlled shutdown.
    pub fn stop(&self) {
        let _ = Command::new(container_cli())
            .args(["stop", "-t", "30", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    /// The container's recent output, for a failure message.
    #[must_use]
    pub fn logs_tail(&self) -> String {
        Command::new(container_cli())
            .args(["logs", "--tail", "40", &self.name])
            .output()
            .map(|output| {
                let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&output.stderr));
                text
            })
            .unwrap_or_default()
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new(container_cli())
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Polls `address` until Metadata lists a broker, or the budget runs out.
async fn wait_for_broker(address: &str, container: &Container) -> Result<(), String> {
    let deadline = Instant::now() + ORACLE_READY_TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        match WireClient::connect(address).await {
            Ok(mut client) => {
                let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
                match client
                    .send(
                        &MetadataRequest::default().with_topics(Some(Vec::new())),
                        version,
                    )
                    .await
                {
                    Ok(response) if !response.brokers.is_empty() => return Ok(()),
                    Ok(_) => last = "metadata lists no broker yet".to_string(),
                    Err(error) => last = error,
                }
            }
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "broker at {address} not ready within {ORACLE_READY_TIMEOUT:?} ({last}); logs:\n{}",
        container.logs_tail()
    ))
}

/// Apache Kafka, `KRaft` combined mode, one node, on loopback.
pub struct KafkaOracle {
    pub target: Target,
    container: Container,
}

impl KafkaOracle {
    /// Restarts the broker on its data, and waits until it serves again.
    pub async fn restart(&self) -> Result<(), String> {
        self.container.restart()?;
        wait_for_broker(&self.target.address, &self.container).await
    }

    pub async fn start() -> Result<Self, Unavailable> {
        image_ready(KAFKA_ORACLE_IMAGE).map_err(Unavailable::Skipped)?;
        let port = free_port();
        let controller = free_port();
        let address = format!("127.0.0.1:{port}");
        let env: Vec<(String, String)> = [
            ("KAFKA_NODE_ID", "1".to_string()),
            ("KAFKA_PROCESS_ROLES", "broker,controller".to_string()),
            (
                "KAFKA_LISTENERS",
                format!("PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}"),
            ),
            (
                "KAFKA_ADVERTISED_LISTENERS",
                format!("PLAINTEXT://{address}"),
            ),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER".to_string()),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT".to_string(),
            ),
            (
                "KAFKA_CONTROLLER_QUORUM_VOTERS",
                format!("1@127.0.0.1:{controller}"),
            ),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1".to_string()),
            ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1".to_string()),
            (
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                "1".to_string(),
            ),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1".to_string()),
            (
                "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR",
                "1".to_string(),
            ),
            (
                "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR",
                "1".to_string(),
            ),
            ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0".to_string()),
            ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false".to_string()),
            ("KAFKA_HEAP_OPTS", "-Xmx512m -Xms256m".to_string()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        let container = Container::start(KAFKA_ORACLE_IMAGE, "kafka", &env, &[])
            .map_err(Unavailable::Failed)?;
        wait_for_broker(&address, &container)
            .await
            .map_err(Unavailable::Failed)?;
        Ok(Self {
            target: Target::plain(TargetKind::Kafka, address),
            container,
        })
    }
}

/// Redpanda in dev-container mode, one shard, on loopback.
pub struct RedpandaOracle {
    pub target: Target,
    container: Container,
}

impl RedpandaOracle {
    /// Restarts the broker on its data, and waits until it serves again.
    pub async fn restart(&self) -> Result<(), String> {
        self.container.restart()?;
        wait_for_broker(&self.target.address, &self.container).await
    }

    pub async fn start() -> Result<Self, Unavailable> {
        if !env_flag("KAFKA_ORACLE_REDPANDA") {
            return Err(Unavailable::Skipped(
                "Redpanda runs nightly; set KAFKA_ORACLE_REDPANDA=1".to_string(),
            ));
        }
        image_ready(REDPANDA_ORACLE_TAG).map_err(Unavailable::Failed)?;
        let port = free_port();
        let rpc = free_port();
        let proxy = free_port();
        let registry = free_port();
        let admin = free_port();
        let address = format!("127.0.0.1:{port}");
        let args: Vec<String> = [
            "redpanda".to_string(),
            "start".to_string(),
            "--mode".to_string(),
            "dev-container".to_string(),
            "--smp".to_string(),
            "1".to_string(),
            "--memory".to_string(),
            "1G".to_string(),
            "--overprovisioned".to_string(),
            "--node-id".to_string(),
            "0".to_string(),
            "--kafka-addr".to_string(),
            format!("PLAINTEXT://{address}"),
            "--advertise-kafka-addr".to_string(),
            format!("PLAINTEXT://{address}"),
            "--rpc-addr".to_string(),
            format!("127.0.0.1:{rpc}"),
            "--advertise-rpc-addr".to_string(),
            format!("127.0.0.1:{rpc}"),
            "--pandaproxy-addr".to_string(),
            format!("127.0.0.1:{proxy}"),
            "--schema-registry-addr".to_string(),
            format!("127.0.0.1:{registry}"),
            // No spaces: the image's entrypoint splits its arguments again.
            "--set".to_string(),
            format!(r#"redpanda.admin=[{{"address":"127.0.0.1","port":{admin}}}]"#),
        ]
        .into();
        let container = Container::start(REDPANDA_ORACLE_TAG, "redpanda", &[], &args)
            .map_err(Unavailable::Failed)?;
        wait_for_broker(&address, &container)
            .await
            .map_err(Unavailable::Failed)?;
        Ok(Self {
            target: Target::plain(TargetKind::Redpanda, address),
            container,
        })
    }
}

/// Confluent Cloud: a managed, multi-broker Apache Kafka, reached over `SASL_SSL` with SASL/PLAIN.
///
/// Opt-in by presence: it runs when the env file exists (`KAFKA_ORACLE_CONFLUENT_ENV`, else
/// `~/confluent-oracle.env`) and holds `CONFLUENT_ORACLE_BOOTSTRAP`, `CONFLUENT_ORACLE_API_KEY`
/// and `CONFLUENT_ORACLE_API_SECRET`. It never runs in CI (`CI` or `GITHUB_ACTIONS` set), and
/// `KAFKA_ORACLE_CONFLUENT=0` turns it off. The cluster is shared and lives between runs, so
/// every resource a run creates carries the run's prefix and [`Self::delete_topics`] removes it.
pub struct ConfluentOracle {
    pub target: Target,
}

/// Topics older than this, left by a run that could not clean up, are swept at the next start.
const CONFLUENT_STALE_AFTER: Duration = Duration::from_hours(6);

impl ConfluentOracle {
    pub async fn start() -> Result<Self, Unavailable> {
        if std::env::var_os("CI").is_some() || std::env::var_os("GITHUB_ACTIONS").is_some() {
            return Err(Unavailable::Skipped(
                "the Confluent Cloud oracle never runs in CI".to_string(),
            ));
        }
        if std::env::var("KAFKA_ORACLE_CONFLUENT").as_deref() == Ok("0") {
            return Err(Unavailable::Skipped(
                "KAFKA_ORACLE_CONFLUENT=0 turns it off".to_string(),
            ));
        }
        let path = std::env::var_os("KAFKA_ORACLE_CONFLUENT_ENV")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(|home| PathBuf::from(home).join("confluent-oracle.env"))
            })
            .ok_or_else(|| {
                Unavailable::Skipped("no HOME to find confluent-oracle.env".to_string())
            })?;
        let Ok(text) = std::fs::read_to_string(&path) else {
            return Err(Unavailable::Skipped(format!(
                "{} is absent (see docs/loams/confluent-oracle.md)",
                path.display()
            )));
        };
        let env = parse_env_file(&text);
        let get = |key: &str| {
            env.get(key)
                .filter(|value| !value.is_empty())
                .cloned()
                .ok_or_else(|| Unavailable::Failed(format!("{} has no {key}", path.display())))
        };
        let address = get("CONFLUENT_ORACLE_BOOTSTRAP")?;
        let address = address
            .strip_prefix("SASL_SSL://")
            .map_or_else(|| address.clone(), ToString::to_string);
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| Unavailable::Failed(format!("TLS versions: {error}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let target = Target {
            kind: TargetKind::Confluent,
            address,
            secure: Some(Arc::new(Secure {
                tls: Arc::new(tls),
                username: get("CONFLUENT_ORACLE_API_KEY")?,
                password: get("CONFLUENT_ORACLE_API_SECRET")?,
            })),
        };
        let mut client = WireClient::connect_target(&target)
            .await
            .map_err(|error| Unavailable::Failed(format!("Confluent Cloud: {error}")))?;
        let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
        let metadata = client
            .send(
                &MetadataRequest::default().with_topics(Some(Vec::new())),
                version,
            )
            .await
            .map_err(|error| Unavailable::Failed(format!("Confluent Cloud: {error}")))?;
        if metadata.brokers.is_empty() {
            return Err(Unavailable::Failed(
                "Confluent Cloud: Metadata lists no broker".to_string(),
            ));
        }
        Ok(Self { target })
    }

    /// Deletes every topic whose name starts with `prefix`; answers the names it deleted.
    pub async fn delete_topics(&self, prefix: &str) -> Result<Vec<String>, String> {
        self.delete_where(|name| name.starts_with(prefix)).await
    }

    /// Deletes topics a run left behind more than [`CONFLUENT_STALE_AFTER`] ago: names are
    /// `{RESOURCE_PREFIX}{unix seconds}-...` (see [`run_id`]).
    pub async fn sweep_stale(&self) -> Result<Vec<String>, String> {
        let cutoff = unix_now().saturating_sub(CONFLUENT_STALE_AFTER.as_secs());
        self.delete_where(|name| {
            name.strip_prefix(RESOURCE_PREFIX)
                .and_then(|rest| rest.split('-').next())
                .and_then(|started| started.parse::<u64>().ok())
                .is_some_and(|started| started < cutoff)
        })
        .await
    }

    /// Deletes matching topics until a listing shows none. A topic created moments ago can be
    /// missing from one broker's Metadata, so one pass is not enough.
    async fn delete_where<F: Fn(&str) -> bool + Sync>(
        &self,
        wanted: F,
    ) -> Result<Vec<String>, String> {
        let mut client = WireClient::connect_target(&self.target).await?;
        let mut deleted = Vec::new();
        for pass in 0..6 {
            let names = Self::listed(&mut client, &wanted).await?;
            if names.is_empty() && pass > 0 {
                return Ok(deleted);
            }
            if !names.is_empty() {
                Self::delete_names(&mut client, &names).await?;
                for name in names {
                    if !deleted.contains(&name) {
                        deleted.push(name);
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let left = Self::listed(&mut client, &wanted).await?;
        if left.is_empty() {
            Ok(deleted)
        } else {
            Err(format!(
                "topics still listed after deleting: {}",
                left.join(", ")
            ))
        }
    }

    /// The topics Metadata lists that carry [`RESOURCE_PREFIX`] and match `wanted`.
    async fn listed<F: Fn(&str) -> bool + Sync>(
        client: &mut WireClient,
        wanted: &F,
    ) -> Result<Vec<String>, String> {
        let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
        // `None` lists every topic; the scoped key only sees its own prefix.
        let metadata = client
            .send(&MetadataRequest::default().with_topics(None), version)
            .await?;
        Ok(metadata
            .topics
            .iter()
            .filter_map(|topic| topic.name.as_ref().map(|name| name.0.to_string()))
            .filter(|name| name.starts_with(RESOURCE_PREFIX) && wanted(name))
            .collect())
    }

    async fn delete_names(client: &mut WireClient, names: &[String]) -> Result<(), String> {
        let version = client
            .version(ApiKey::DeleteTopics, 5)
            .ok_or("Confluent Cloud does not serve DeleteTopics")?;
        let response = client
            .send(
                &DeleteTopicsRequest::default()
                    .with_topic_names(
                        names
                            .iter()
                            .map(|name| TopicName(StrBytes::from_string(name.clone())))
                            .collect(),
                    )
                    .with_timeout_ms(30_000),
                version,
            )
            .await?;
        let failed: Vec<String> = response
            .responses
            .iter()
            .filter(|result| result.error_code != 0 && result.error_code != 3)
            .map(|result| {
                format!(
                    "{}: error {}",
                    result.name.as_ref().map_or("?", |name| name.0.as_str()),
                    result.error_code
                )
            })
            .collect();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(format!("DeleteTopics failed for {}", failed.join(", ")))
        }
    }
}

/// `KEY=VALUE` lines, `export ` and surrounding quotes allowed, `#` comments skipped.
pub fn parse_env_file(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let line = line.strip_prefix("export ").unwrap_or(line);
            let (key, value) = line.split_once('=')?;
            let value = value.trim();
            let value = value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    value
                        .strip_prefix('\'')
                        .and_then(|value| value.strip_suffix('\''))
                })
                .unwrap_or(value);
            Some((key.trim().to_string(), value.to_string()))
        })
        .collect()
}

#[must_use]
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// A run id, unique per process start, for resource names on shared oracles.
#[must_use]
pub fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    format!("{}-{:x}", unix_now(), nanos ^ std::process::id())
}

/// A byte stream to a broker: plaintext TCP, or TLS over TCP.
trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

/// One Kafka connection that speaks to any target.
pub struct WireClient {
    io: Box<dyn Stream>,
    next_correlation: i32,
    /// `(min, max)` per API key, from the target's `ApiVersions`.
    versions: BTreeMap<i16, (i16, i16)>,
    /// On a multi-broker target, where partition and coordinator requests go.
    router: Option<Router>,
}

/// The brokers of a multi-broker target, and one connection to each that a request needed.
struct Router {
    secure: Arc<Secure>,
    /// `host:port` per node id, from Metadata and `FindCoordinator`.
    brokers: BTreeMap<i32, String>,
    connections: BTreeMap<i32, WireClient>,
}

/// Where a request must go on a multi-broker target.
enum Route {
    Leader(String, i32),
    /// `(key type, key)`: 0 a group, 1 a transactional id.
    Coordinator(i8, String),
}

impl WireClient {
    /// Connects to the plaintext broker at `address` and reads its versions.
    pub async fn connect(address: &str) -> Result<Self, String> {
        Self::open(address, None).await
    }

    /// Connects to `target`; over `SASL_SSL` and routed per broker when it is a managed cluster.
    pub async fn connect_target(target: &Target) -> Result<Self, String> {
        let Some(secure) = &target.secure else {
            return Self::connect(&target.address).await;
        };
        let mut client = Self::open(&target.address, Some(secure)).await?;
        client.router = Some(Router {
            secure: Arc::clone(secure),
            brokers: BTreeMap::new(),
            connections: BTreeMap::new(),
        });
        Ok(client)
    }

    /// One connection to `address`, authenticated when `secure` is given; never routed.
    async fn open(address: &str, secure: Option<&Arc<Secure>>) -> Result<Self, String> {
        let tcp = tokio::time::timeout(RESPONSE_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| format!("connect {address}: timed out"))?
            .map_err(|error| format!("connect {address}: {error}"))?;
        let io: Box<dyn Stream> = match secure {
            None => Box::new(tcp),
            Some(secure) => {
                let host = address.rsplit_once(':').map_or(address, |(host, _)| host);
                let server_name = ServerName::try_from(host.to_string())
                    .map_err(|error| format!("server name {host}: {error}"))?;
                let tls = tokio::time::timeout(
                    RESPONSE_TIMEOUT,
                    TlsConnector::from(Arc::clone(&secure.tls)).connect(server_name, tcp),
                )
                .await
                .map_err(|_| format!("TLS to {address}: timed out"))?
                .map_err(|error| format!("TLS to {address}: {error}"))?;
                Box::new(tls)
            }
        };
        let mut client = Self {
            io,
            next_correlation: 1,
            versions: BTreeMap::new(),
            router: None,
        };
        let response = client
            .send_direct(
                &ApiVersionsRequest::default()
                    .with_client_software_name(StrBytes::from_static_str(CLIENT_ID))
                    .with_client_software_version(StrBytes::from_static_str("1")),
                3,
            )
            .await?;
        if response.error_code != 0 {
            return Err(format!("ApiVersions answered {}", response.error_code));
        }
        client.versions = response
            .api_keys
            .iter()
            .map(|key| (key.api_key, (key.min_version, key.max_version)))
            .collect();
        if let Some(secure) = secure {
            client.sasl_plain(secure).await?;
        }
        Ok(client)
    }

    /// SASL/PLAIN over `SaslHandshake` v1 and `SaslAuthenticate`. Errors never carry the secret.
    async fn sasl_plain(&mut self, secure: &Secure) -> Result<(), String> {
        let handshake = self
            .send_direct(
                &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
                1,
            )
            .await?;
        if handshake.error_code != 0 {
            return Err(format!("SaslHandshake answered {}", handshake.error_code));
        }
        let mut token = BytesMut::new();
        token.put_u8(0);
        token.put_slice(secure.username.as_bytes());
        token.put_u8(0);
        token.put_slice(secure.password.as_bytes());
        let version = self
            .version(ApiKey::SaslAuthenticate, 2)
            .ok_or("the target does not serve SaslAuthenticate")?;
        let answer = self
            .send_direct(
                &SaslAuthenticateRequest::default().with_auth_bytes(token.freeze()),
                version,
            )
            .await?;
        if answer.error_code != 0 {
            return Err(format!(
                "SASL/PLAIN refused with {}: {}",
                answer.error_code,
                answer.error_message.as_deref().unwrap_or("")
            ));
        }
        Ok(())
    }

    /// The advertised `(min, max)` of every key.
    #[must_use]
    pub const fn advertised(&self) -> &BTreeMap<i16, (i16, i16)> {
        &self.versions
    }

    /// `preferred`, lowered to what the target advertises; `None` when it does not serve `key`.
    #[must_use]
    pub fn version(&self, key: ApiKey, preferred: i16) -> Option<i16> {
        let (min, max) = *self.versions.get(&(key as i16))?;
        let version = preferred.min(max);
        (version >= min).then_some(version)
    }

    /// Sends `request` at `version` and decodes its response. On a multi-broker target a
    /// partition request goes to the partition's leader and a group or transactional request to
    /// its coordinator; everything else, and anything whose owner is unknown, to the bootstrap.
    pub async fn send<R: Request + Sync + 'static>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<R::Response, String> {
        if self.router.is_some()
            && let Some(route) = route_of(request)
            && let Some(node) = self.owner(route).await?
        {
            let connection = self.connection(node).await?;
            return connection.send_direct(request, version).await;
        }
        self.send_direct(request, version).await
    }

    /// On a multi-broker target, waits until each partition's leader serves `topic`: a leader
    /// can answer 3 (`UNKNOWN_TOPIC_OR_PARTITION`) for a while after Metadata lists the topic.
    /// Does nothing on a single broker, where Metadata and the leader are the same process.
    pub async fn await_leaders(&mut self, topic: &str, partitions: i32) -> Result<(), String> {
        if self.router.is_none() {
            return Ok(());
        }
        let version = self
            .version(ApiKey::ListOffsets, 7)
            .ok_or("the target does not serve ListOffsets")?;
        for partition in 0..partitions {
            let request = ListOffsetsRequest::default()
                .with_replica_id(kafka_protocol::messages::BrokerId(-1))
                .with_topics(vec![
                    ListOffsetsTopic::default()
                        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                        .with_partitions(vec![
                            ListOffsetsPartition::default()
                                .with_partition_index(partition)
                                .with_current_leader_epoch(-1)
                                .with_timestamp(-1),
                        ]),
                ]);
            let mut last = -1;
            for _ in 0..120 {
                let response = self.send(&request, version).await?;
                last = response
                    .topics
                    .first()
                    .and_then(|found| found.partitions.first())
                    .map_or(-1, |found| found.error_code);
                if last == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if last != 0 {
                return Err(format!(
                    "the leader of {topic}/{partition} still answers {last}"
                ));
            }
        }
        Ok(())
    }

    /// The node that owns `route`, asked of the bootstrap; `None` when none does yet.
    async fn owner(&mut self, route: Route) -> Result<Option<i32>, String> {
        match route {
            Route::Leader(topic, partition) => {
                let version = self.version(ApiKey::Metadata, 9).unwrap_or(1);
                let request = MetadataRequest::default()
                    .with_topics(Some(vec![
                        MetadataRequestTopic::default()
                            .with_name(Some(TopicName(StrBytes::from_string(topic)))),
                    ]))
                    .with_allow_auto_topic_creation(false);
                let response = self.send_direct(&request, version).await?;
                let router = self.router.as_mut().ok_or("not routed")?;
                for broker in &response.brokers {
                    router
                        .brokers
                        .insert(broker.node_id.0, format!("{}:{}", broker.host, broker.port));
                }
                Ok(response
                    .topics
                    .first()
                    .and_then(|found| {
                        found
                            .partitions
                            .iter()
                            .find(|candidate| candidate.partition_index == partition)
                    })
                    .map(|found| found.leader_id.0)
                    .filter(|leader| *leader >= 0))
            }
            Route::Coordinator(key_type, key) => {
                let Some(version) = self.version(ApiKey::FindCoordinator, 4) else {
                    return Ok(None);
                };
                let request = if version >= 4 {
                    FindCoordinatorRequest::default()
                        .with_key_type(key_type)
                        .with_coordinator_keys(vec![StrBytes::from_string(key)])
                } else {
                    FindCoordinatorRequest::default()
                        .with_key_type(key_type)
                        .with_key(StrBytes::from_string(key))
                };
                for _ in 0..40 {
                    let response = self.send_direct(&request, version).await?;
                    let (error, node, host, port) = response.coordinators.first().map_or_else(
                        || {
                            (
                                response.error_code,
                                response.node_id.0,
                                response.host.to_string(),
                                response.port,
                            )
                        },
                        |found| {
                            (
                                found.error_code,
                                found.node_id.0,
                                found.host.to_string(),
                                found.port,
                            )
                        },
                    );
                    if error == 0 && node >= 0 {
                        let router = self.router.as_mut().ok_or("not routed")?;
                        router.brokers.insert(node, format!("{host}:{port}"));
                        return Ok(Some(node));
                    }
                    if error != 14 && error != 15 {
                        return Ok(None);
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Ok(None)
            }
        }
    }

    /// The connection to `node`, opened on first use.
    async fn connection(&mut self, node: i32) -> Result<&mut Self, String> {
        let router = self.router.as_mut().ok_or("not routed")?;
        if !router.connections.contains_key(&node) {
            let address = router
                .brokers
                .get(&node)
                .cloned()
                .ok_or_else(|| format!("node {node} has no known address"))?;
            let opened = Self::open(&address, Some(&router.secure)).await?;
            router.connections.insert(node, opened);
        }
        router
            .connections
            .get_mut(&node)
            .ok_or_else(|| format!("node {node} has no connection"))
    }

    /// Sends `request` on this connection, whatever owns it.
    async fn send_direct<R: Request + Sync>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<R::Response, String> {
        let correlation_id = self.next_correlation;
        self.next_correlation += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));
        let mut body = BytesMut::new();
        header
            .encode(&mut body, R::header_version(version))
            .map_err(|error| format!("encode header for key {}: {error}", R::KEY))?;
        request
            .encode(&mut body, version)
            .map_err(|error| format!("encode key {} v{version}: {error}", R::KEY))?;
        let length = i32::try_from(body.len()).map_err(|_| "request too large".to_string())?;
        let mut frame = BytesMut::with_capacity(body.len() + 4);
        frame.put_i32(length);
        frame.extend_from_slice(&body);
        let mut payload = tokio::time::timeout(RESPONSE_TIMEOUT, async {
            self.io
                .write_all(&frame)
                .await
                .map_err(|error| format!("write key {}: {error}", R::KEY))?;
            self.io
                .flush()
                .await
                .map_err(|error| format!("flush key {}: {error}", R::KEY))?;
            read_frame(&mut self.io).await
        })
        .await
        .map_err(|_| {
            format!(
                "key {} v{version}: no response within {RESPONSE_TIMEOUT:?}",
                R::KEY
            )
        })??;
        let response_header =
            ResponseHeader::decode(&mut payload, R::Response::header_version(version))
                .map_err(|error| format!("decode response header for key {}: {error}", R::KEY))?;
        if response_header.correlation_id != correlation_id {
            return Err(format!(
                "key {}: correlation id {} answered {correlation_id}",
                R::KEY,
                response_header.correlation_id
            ));
        }
        R::Response::decode(&mut payload, version)
            .map_err(|error| format!("decode key {} v{version}: {error}", R::KEY))
    }
}

/// Which broker owns `request`: its first partition's leader, or its group's or transactional
/// id's coordinator. `None` for a request any broker answers.
fn route_of<R: 'static>(request: &R) -> Option<Route> {
    let any = request as &dyn Any;
    let group = |id: &str| Some(Route::Coordinator(0, id.to_string()));
    if let Some(produce) = any.downcast_ref::<ProduceRequest>() {
        let topic = produce.topic_data.first()?;
        let partition = topic.partition_data.first()?;
        return Some(Route::Leader(topic.name.0.to_string(), partition.index));
    }
    if let Some(fetch) = any.downcast_ref::<FetchRequest>() {
        let topic = fetch.topics.first()?;
        let partition = topic.partitions.first()?;
        return Some(Route::Leader(
            topic.topic.0.to_string(),
            partition.partition,
        ));
    }
    if let Some(list) = any.downcast_ref::<ListOffsetsRequest>() {
        let topic = list.topics.first()?;
        let partition = topic.partitions.first()?;
        return Some(Route::Leader(
            topic.name.0.to_string(),
            partition.partition_index,
        ));
    }
    if let Some(join) = any.downcast_ref::<JoinGroupRequest>() {
        return group(&join.group_id.0);
    }
    if let Some(sync) = any.downcast_ref::<SyncGroupRequest>() {
        return group(&sync.group_id.0);
    }
    if let Some(heartbeat) = any.downcast_ref::<HeartbeatRequest>() {
        return group(&heartbeat.group_id.0);
    }
    if let Some(leave) = any.downcast_ref::<LeaveGroupRequest>() {
        return group(&leave.group_id.0);
    }
    if let Some(commit) = any.downcast_ref::<OffsetCommitRequest>() {
        return group(&commit.group_id.0);
    }
    if let Some(fetch) = any.downcast_ref::<OffsetFetchRequest>() {
        if fetch.group_id.0.is_empty() {
            return group(&fetch.groups.first()?.group_id.0);
        }
        return group(&fetch.group_id.0);
    }
    if let Some(init) = any.downcast_ref::<InitProducerIdRequest>() {
        let id = init.transactional_id.as_ref()?;
        return Some(Route::Coordinator(1, id.0.to_string()));
    }
    if let Some(add) = any.downcast_ref::<AddPartitionsToTxnRequest>() {
        return Some(Route::Coordinator(
            1,
            add.v3_and_below_transactional_id.0.to_string(),
        ));
    }
    if let Some(add) = any.downcast_ref::<AddOffsetsToTxnRequest>() {
        return Some(Route::Coordinator(1, add.transactional_id.0.to_string()));
    }
    if let Some(end) = any.downcast_ref::<EndTxnRequest>() {
        return Some(Route::Coordinator(1, end.transactional_id.0.to_string()));
    }
    if let Some(commit) = any.downcast_ref::<TxnOffsetCommitRequest>() {
        return group(&commit.group_id.0);
    }
    None
}

async fn read_frame(io: &mut Box<dyn Stream>) -> Result<Bytes, String> {
    let mut length = [0u8; 4];
    io.read_exact(&mut length)
        .await
        .map_err(|error| format!("read response length: {error}"))?;
    let length = usize::try_from(i32::from_be_bytes(length))
        .map_err(|_| "negative response length".to_string())?;
    if length > MAX_RESPONSE_BYTES {
        return Err(format!(
            "response of {length} bytes is over the harness cap"
        ));
    }
    let mut payload = vec![0u8; length];
    io.read_exact(&mut payload)
        .await
        .map_err(|error| format!("read response body: {error}"))?;
    Ok(Bytes::from(payload))
}
