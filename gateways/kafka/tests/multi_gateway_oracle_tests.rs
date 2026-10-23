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

//! The failover scenario of #4515, against three gateways and against a three-broker Apache Kafka.
//!
//! One scenario runs on both: a Java consumer group member subscribes through a bootstrap list of
//! all three brokers, a round of records is written, the broker that coordinates the group is
//! stopped the way an operator stops it, a second round is written, and the member reads on until
//! it has every record. Each target's observations are reduced to facts (the first assignment,
//! and the values read per partition, deduplicated, since a member may read again from its last
//! commit, as Kafka allows) and the gateway's facts must equal Kafka's.
//!
//! Needs a container CLI with `apache/kafka:4.3.1` (the oracle and the Java client) and
//! `edenhill/kcat:1.7.1` (the producer) pulled, and a built `iggy-server`. Missing any skips,
//! unless `KAFKA_E2E_REQUIRED=1`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::{
    ApiKey, CreateTopicsRequest, FindCoordinatorRequest, MetadataRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use serde_json::Value;
use serial_test::serial;

#[path = "common/gateways.rs"]
mod gateways;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/oracle.rs"]
mod oracle;

use gateways::Gateway;
use iggy_server::TestServer;
use oracle::{Container, KAFKA_ORACLE_IMAGE, WireClient, free_port, image_ready};

const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";
const TOPIC: &str = "kw5-failover";
const GROUP: &str = "kw5-failover-group";
const PARTITIONS: i32 = 3;
const PER_ROUND: usize = 3;
const NODES: [i32; 3] = [1, 2, 3];
/// How long the scenario waits for one step a client takes.
const STEP_TIMEOUT: Duration = Duration::from_secs(90);

fn skip(reason: &str) -> bool {
    assert!(
        std::env::var("KAFKA_E2E_REQUIRED").as_deref() != Ok("1"),
        "KAFKA_E2E_REQUIRED=1 but the failover scenario cannot run: {reason}"
    );
    eprintln!("skipping the failover scenario: {reason}");
    true
}

fn container_cli() -> String {
    std::env::var("KAFKA_ORACLE_CONTAINER_CLI").unwrap_or_else(|_| "docker".to_string())
}

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// What one target observed. Equal across targets, or the gateway differs from Kafka.
#[derive(Debug, PartialEq, Eq)]
struct Facts {
    first_assignment: Vec<i64>,
    /// Distinct values read, per partition, in offset order.
    read: BTreeMap<i64, Vec<String>>,
    /// Whether the member read nothing it was not sent.
    nothing_foreign: bool,
}

/// Three brokers of one kind, by node id.
enum Brokers {
    Gateways(Vec<Gateway>),
    Kafka(Vec<(i32, String, Container)>),
}

impl Brokers {
    fn addresses(&self) -> Vec<(i32, String)> {
        match self {
            Self::Gateways(gateways) => gateways
                .iter()
                .map(|gateway| (gateway.node_id, gateway.addr.to_string()))
                .collect(),
            Self::Kafka(nodes) => nodes
                .iter()
                .map(|(node_id, address, _)| (*node_id, address.clone()))
                .collect(),
        }
    }

    fn bootstrap(&self) -> String {
        self.addresses()
            .into_iter()
            .map(|(_, address)| address)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stops `node_id` as an operator does, and waits until it has stopped.
    async fn stop(&mut self, node_id: i32) {
        match self {
            Self::Gateways(gateways) => {
                let at = gateways
                    .iter()
                    .position(|gateway| gateway.node_id == node_id)
                    .expect("a running gateway");
                gateways.remove(at).stop().await;
            }
            Self::Kafka(nodes) => {
                let at = nodes
                    .iter()
                    .position(|(id, _, _)| *id == node_id)
                    .expect("a running broker");
                let (_, _, container) = nodes.remove(at);
                tokio::task::spawn_blocking(move || container.stop())
                    .await
                    .expect("stop the broker");
            }
        }
    }
}

async fn start_kafka() -> Result<Brokers, String> {
    image_ready(KAFKA_ORACLE_IMAGE)?;
    let ports: Vec<(u16, u16)> = NODES.iter().map(|_| (free_port(), free_port())).collect();
    let voters = NODES
        .iter()
        .zip(&ports)
        .map(|(node_id, (_, controller))| format!("{node_id}@127.0.0.1:{controller}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut nodes = Vec::new();
    for (node_id, (port, controller)) in NODES.iter().zip(&ports) {
        let address = format!("127.0.0.1:{port}");
        let env: Vec<(String, String)> = [
            ("KAFKA_NODE_ID", node_id.to_string()),
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
            ("KAFKA_CONTROLLER_QUORUM_VOTERS", voters.clone()),
            // One cluster id for the three, or each formats its own.
            ("CLUSTER_ID", "a3c2bWFzLWZhaWxvdmVyMQ".to_string()),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3".to_string()),
            ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "3".to_string()),
            (
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                "3".to_string(),
            ),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1".to_string()),
            ("KAFKA_MIN_INSYNC_REPLICAS", "1".to_string()),
            ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0".to_string()),
            ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false".to_string()),
            ("KAFKA_HEAP_OPTS", "-Xmx384m -Xms192m".to_string()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        let container =
            Container::start(KAFKA_ORACLE_IMAGE, &format!("kafka{node_id}"), &env, &[])?;
        nodes.push((*node_id, address, container));
    }
    let brokers = Brokers::Kafka(nodes);
    wait_for_brokers(&brokers).await?;
    Ok(brokers)
}

/// Waits until a broker's Metadata lists all three.
async fn wait_for_brokers(brokers: &Brokers) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let (_, address) = brokers.addresses()[0].clone();
    let mut last = String::new();
    while Instant::now() < deadline {
        match WireClient::connect(&address).await {
            Ok(mut client) => {
                let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
                match client
                    .send(
                        &MetadataRequest::default().with_topics(Some(Vec::new())),
                        version,
                    )
                    .await
                {
                    Ok(response) if response.brokers.len() == NODES.len() => return Ok(()),
                    Ok(response) => last = format!("{} brokers listed", response.brokers.len()),
                    Err(error) => last = error,
                }
            }
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("the three brokers never listed each other: {last}"))
}

async fn create_topic(brokers: &Brokers, replication_factor: i16) {
    let (_, address) = brokers.addresses()[0].clone();
    let mut client = WireClient::connect(&address).await.expect("connect");
    let version = client
        .version(ApiKey::CreateTopics, 5)
        .expect("CreateTopics");
    let request = CreateTopicsRequest::default()
        .with_timeout_ms(30_000)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(TopicName(s(TOPIC)))
                .with_num_partitions(PARTITIONS)
                .with_replication_factor(replication_factor),
        ]);
    let response = client.send(&request, version).await.expect("CreateTopics");
    assert_eq!(response.topics[0].error_code, 0, "{response:?}");
    // Every broker must know the topic with a leader per partition before a client asks.
    let deadline = Instant::now() + Duration::from_secs(60);
    for (_, address) in brokers.addresses() {
        loop {
            let mut client = WireClient::connect(&address).await.expect("connect");
            let version = client.version(ApiKey::Metadata, 9).expect("Metadata");
            let request = MetadataRequest::default().with_topics(Some(vec![
                kafka_protocol::messages::metadata_request::MetadataRequestTopic::default()
                    .with_name(Some(TopicName(s(TOPIC)))),
            ]));
            let response = client.send(&request, version).await.expect("Metadata");
            let ready = response.topics.first().is_some_and(|topic| {
                topic.error_code == 0
                    && topic.partitions.len() == usize::try_from(PARTITIONS).unwrap_or(0)
                    && topic
                        .partitions
                        .iter()
                        .all(|partition| partition.leader_id.0 >= 0)
            });
            if ready {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{TOPIC} never ready at {address}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// The node that coordinates the group, as the first live broker answers.
async fn coordinator(brokers: &Brokers) -> i32 {
    let (_, address) = brokers.addresses()[0].clone();
    let mut client = WireClient::connect(&address).await.expect("connect");
    let version = client
        .version(ApiKey::FindCoordinator, 4)
        .expect("FindCoordinator");
    let request = FindCoordinatorRequest::default()
        .with_key_type(0)
        .with_coordinator_keys(vec![s(GROUP)]);
    for _ in 0..60 {
        let response = client
            .send(&request, version)
            .await
            .expect("FindCoordinator");
        let found = &response.coordinators[0];
        if found.error_code == 0 {
            return found.node_id.0;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("no coordinator for {GROUP}");
}

/// Writes one round of records to every partition with kcat, which routes each to its leader.
fn produce_round(bootstrap: &str, round: usize) {
    for partition in 0..PARTITIONS {
        let lines: Vec<String> = (0..PER_ROUND)
            .map(|at| format!("r{round}-p{partition}-{at}"))
            .collect();
        let lines = lines.join("\n") + "\n";
        let mut child = Command::new(container_cli())
            .args([
                "run",
                "--rm",
                "-i",
                "--network",
                "host",
                "--pull",
                "never",
                KCAT_IMAGE,
                "-b",
                bootstrap,
                "-P",
                "-t",
                TOPIC,
                "-p",
                &partition.to_string(),
                "-X",
                "message.timeout.ms=60000",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start kcat");
        std::io::Write::write_all(child.stdin.as_mut().expect("stdin"), lines.as_bytes())
            .expect("write records");
        drop(child.stdin.take());
        let output = child.wait_with_output().expect("kcat runs");
        assert!(
            output.status.success(),
            "kcat failed to write round {round} to partition {partition}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// A Java verifiable consumer, printing one JSON event per line.
struct Member {
    name: String,
    child: Child,
    lines: tokio::sync::mpsc::UnboundedReceiver<String>,
    seen: Vec<Value>,
}

impl Member {
    fn start(bootstrap: &str, label: &str) -> Self {
        let name = format!("kw5-member-{label}-{}", std::process::id());
        let mut child = Command::new(container_cli())
            .args([
                "run",
                "--rm",
                "--network",
                "host",
                "--pull",
                "never",
                "--name",
                &name,
                KAFKA_ORACLE_IMAGE,
                "/opt/kafka/bin/kafka-verifiable-consumer.sh",
                "--bootstrap-server",
                bootstrap,
                "--topic",
                TOPIC,
                "--group-id",
                GROUP,
                "--session-timeout",
                "10000",
                "--reset-policy",
                "earliest",
                "--verbose",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start the verifiable consumer");
        let (sender, lines) = tokio::sync::mpsc::unbounded_channel();
        let stdout = child.stdout.take().expect("stdout");
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            name,
            child,
            lines,
            seen: Vec::new(),
        }
    }

    /// Reads events until `done` holds over everything seen so far.
    async fn wait_until(&mut self, what: &str, done: impl Fn(&[Value]) -> bool) {
        let deadline = Instant::now() + STEP_TIMEOUT;
        while !done(&self.seen) {
            let left = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(left, self.lines.recv()).await {
                Ok(Some(line)) => {
                    if let Ok(event) = serde_json::from_str::<Value>(&line) {
                        self.seen.push(event);
                    }
                }
                _ => panic!("the member never reached: {what}; saw {:#?}", self.seen),
            }
        }
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        let _ = Command::new(container_cli())
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.child.wait();
    }
}

fn assignments(events: &[Value]) -> Vec<Vec<i64>> {
    events
        .iter()
        .filter(|event| event["name"] == "partitions_assigned")
        .map(|event| {
            let mut partitions: Vec<i64> = event["partitions"]
                .as_array()
                .map(|partitions| {
                    partitions
                        .iter()
                        .filter_map(|partition| partition["partition"].as_i64())
                        .collect()
                })
                .unwrap_or_default();
            partitions.sort_unstable();
            partitions
        })
        .collect()
}

/// `(partition, offset) -> value` of every record read.
fn records(events: &[Value]) -> BTreeMap<(i64, i64), String> {
    events
        .iter()
        .filter(|event| event["name"] == "record_data")
        .filter_map(|event| {
            Some((
                (event["partition"].as_i64()?, event["offset"].as_i64()?),
                event["value"].as_str()?.to_string(),
            ))
        })
        .collect()
}

fn committed_past(events: &[Value], offset: i64) -> bool {
    let mut committed = BTreeMap::new();
    for event in events.iter().filter(|event| {
        event["name"] == "offsets_committed" && event["success"].as_bool() == Some(true)
    }) {
        for partition in event["offsets"].as_array().into_iter().flatten() {
            if let (Some(index), Some(at)) = (
                partition["partition"].as_i64(),
                partition["offset"].as_i64(),
            ) {
                committed.insert(index, at);
            }
        }
    }
    (0..i64::from(PARTITIONS)).all(|partition| committed.get(&partition) >= Some(&offset))
}

fn expected_values(rounds: usize) -> BTreeSet<String> {
    (1..=rounds)
        .flat_map(|round| {
            (0..PARTITIONS).flat_map(move |partition| {
                (0..PER_ROUND).map(move |at| format!("r{round}-p{partition}-{at}"))
            })
        })
        .collect()
}

/// The scenario, on `brokers`. Returns what it observed.
async fn failover(mut brokers: Brokers, label: &str) -> Facts {
    let bootstrap = brokers.bootstrap();
    let mut member = Member::start(&bootstrap, label);
    member
        .wait_until("an assignment of every partition", |events| {
            assignments(events)
                .last()
                .is_some_and(|last| last.len() == 3)
        })
        .await;

    let first = bootstrap.clone();
    tokio::task::spawn_blocking(move || produce_round(&first, 1))
        .await
        .expect("round 1");
    let per_partition = i64::try_from(PER_ROUND).expect("a small round");
    member
        .wait_until("round 1 read and committed", |events| {
            records(events).len() >= 9 && committed_past(events, per_partition)
        })
        .await;

    let coordinator = coordinator(&brokers).await;
    brokers.stop(coordinator).await;

    let bootstrap = brokers.bootstrap();
    tokio::task::spawn_blocking(move || produce_round(&bootstrap, 2))
        .await
        .expect("round 2");
    let expected = expected_values(2);
    member
        .wait_until("every record of both rounds", |events| {
            let read: BTreeSet<String> = records(events).into_values().collect();
            expected.is_subset(&read)
        })
        .await;

    let records = records(&member.seen);
    let mut read: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for ((partition, _), value) in &records {
        let values = read.entry(*partition).or_default();
        if !values.contains(value) {
            values.push(value.clone());
        }
    }
    Facts {
        first_assignment: assignments(&member.seen)
            .into_iter()
            .find(|assignment| !assignment.is_empty())
            .unwrap_or_default(),
        nothing_foreign: records.values().all(|value| expected.contains(value)),
        read,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_the_coordinator_stops_should_consume_every_record_once_per_group_as_kafka_does() {
    if let Err(reason) = image_ready(KAFKA_ORACLE_IMAGE).and_then(|()| image_ready(KCAT_IMAGE))
        && skip(&reason)
    {
        return;
    }
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let on_gateways = async {
        let mut gateways = Vec::new();
        for node_id in NODES {
            gateways.push(Gateway::start(&server, node_id).await);
        }
        let brokers = Brokers::Gateways(gateways);
        wait_for_brokers(&brokers).await.expect("three gateways");
        create_topic(&brokers, -1).await;
        failover(brokers, "gateway").await
    };
    // The two clusters are independent, so the scenario runs on both at once.
    let on_kafka = async {
        let kafka = start_kafka().await.expect("a three-broker Kafka");
        create_topic(&kafka, 3).await;
        failover(kafka, "kafka").await
    };
    let (gateway, kafka) = tokio::join!(on_gateways, on_kafka);

    assert_eq!(gateway, kafka, "the gateways differ from Apache Kafka");
    assert_eq!(gateway.first_assignment, vec![0, 1, 2]);
    assert!(gateway.nothing_foreign);
}
