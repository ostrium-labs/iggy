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

//! The gateway against real Kafka brokers, on the wire.
//!
//! Each scenario sends the same requests to the gateway (bridged to a spawned `iggy-server`) and to
//! every oracle (`tests/common/oracle.rs`), records what each answered as facts
//! (`tests/common/facts.rs`), and fails on any difference that
//! `tests/oracle/allowed_differences.toml` does not declare, or on a declared one that no longer
//! occurs. The report lists every difference with the issue that owns it.
//!
//! Apache Kafka runs on every pull request. Redpanda runs when `KAFKA_ORACLE_REDPANDA=1` (the
//! nightly job). Missing Docker, images or the `iggy-server` binary skip the suite, unless
//! `KAFKA_E2E_REQUIRED=1` turns the skip into a failure, as for the real-client suite.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalAlterConfigsResource,
    AlterableConfig as IncrementalAlterableConfig,
};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, BrokerId, CreateTopicsRequest, DescribeConfigsRequest, FetchRequest,
    FindCoordinatorRequest, GroupId, HeartbeatRequest, IncrementalAlterConfigsRequest,
    InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest, ListOffsetsRequest,
    MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest, ProducerId,
    SyncGroupRequest, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::sync::broadcast;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/facts.rs"]
mod facts;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/oracle.rs"]
mod oracle;

use facts::{Allowed, Difference, Facts, compare, judge, parse_allowed};
use oracle::{
    KafkaOracle, RESOURCE_PREFIX, RedpandaOracle, Target, TargetKind, Unavailable, WireClient,
    env_flag, run_id,
};

const ALLOWED_DIFFERENCES: &str = include_str!("oracle/allowed_differences.toml");

/// Partitions of every topic the scenarios create.
const PARTITIONS: i32 = 3;

/// Request timeout the scenarios send to brokers.
const TIMEOUT_MS: i32 = 30_000;

/// What `produce_fetch` writes: key, value, headers in send order.
type Sent = (
    Option<&'static str>,
    Option<&'static str>,
    &'static [(&'static str, Option<&'static str>)],
);

const RECORDS: [Sent; 5] = [
    (Some("k0"), Some("alpha"), &[]),
    (
        Some("k1"),
        Some("beta"),
        &[("trace", Some("t1")), ("app", Some("demo"))],
    ),
    (None, Some("no-key"), &[]),
    (Some("k1"), None, &[]),
    (Some("k4"), Some(""), &[("empty", Some(""))]),
];

/// The scenarios, in the order they run.
#[derive(Debug, Clone, Copy)]
enum Scenario {
    ApiVersions,
    TopicAdmin,
    ProduceFetch,
    ListOffsets,
    ClassicGroup,
    StandaloneCommit,
    InitProducerId,
    IdempotentProducer,
    DescribeConfigs,
    TopicConfigs,
}

impl Scenario {
    const ALL: [Self; 10] = [
        Self::ApiVersions,
        Self::TopicAdmin,
        Self::ProduceFetch,
        Self::ListOffsets,
        Self::ClassicGroup,
        Self::StandaloneCommit,
        Self::InitProducerId,
        Self::IdempotentProducer,
        Self::DescribeConfigs,
        Self::TopicConfigs,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::ApiVersions => "api_versions",
            Self::TopicAdmin => "topic_admin",
            Self::ProduceFetch => "produce_fetch",
            Self::ListOffsets => "list_offsets",
            Self::ClassicGroup => "classic_group",
            Self::StandaloneCommit => "standalone_commit",
            Self::InitProducerId => "init_producer_id",
            Self::IdempotentProducer => "idempotent_producer",
            Self::DescribeConfigs => "describe_configs",
            Self::TopicConfigs => "topic_configs",
        }
    }

    async fn run(self, cx: &Context) -> Result<Facts, String> {
        let mut client = WireClient::connect(&cx.target.address).await?;
        let mut facts = Facts::default();
        match self {
            Self::ApiVersions => api_versions(cx, &client, &mut facts),
            Self::TopicAdmin => topic_admin(cx, &mut client, &mut facts).await?,
            Self::ProduceFetch => produce_fetch(cx, &mut client, &mut facts).await?,
            Self::ListOffsets => list_offsets(cx, &mut client, &mut facts).await?,
            Self::ClassicGroup => classic_group(cx, &mut client, &mut facts).await?,
            Self::StandaloneCommit => standalone_commit(cx, &mut client, &mut facts).await?,
            Self::InitProducerId => init_producer_id(cx, &mut client, &mut facts).await?,
            Self::IdempotentProducer => idempotent_producer(cx, &mut client, &mut facts).await?,
            Self::DescribeConfigs => describe_configs(cx, &mut client, &mut facts).await?,
            Self::TopicConfigs => topic_configs(cx, &mut client, &mut facts).await?,
        }
        Ok(facts)
    }
}

/// What a scenario knows about its run.
struct Context {
    target: Target,
    run: String,
    /// The gateway's advertised `(min, max)` per key, for the `ApiVersions` comparison.
    gateway_versions: BTreeMap<i16, (i16, i16)>,
}

impl Context {
    /// A resource name unique to this run and scenario.
    fn name(&self, scenario: &str, suffix: &str) -> String {
        format!("{RESOURCE_PREFIX}{}-{scenario}-{suffix}", self.run)
    }
}

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

fn topic(value: &str) -> TopicName {
    TopicName(s(value))
}

fn group(value: &str) -> GroupId {
    GroupId(s(value))
}

fn version(client: &WireClient, key: ApiKey, preferred: i16) -> Result<i16, String> {
    client
        .version(key, preferred)
        .ok_or_else(|| format!("the target does not serve {key:?}"))
}

fn text(bytes: Option<&Bytes>) -> String {
    bytes.map_or_else(
        || "null".to_string(),
        |bytes| format!("'{}'", String::from_utf8_lossy(bytes)),
    )
}

/// The gateway never advertises a key the oracle does not serve, nor a version above the
/// oracle's. On the gateway itself every fact is `true`.
fn api_versions(cx: &Context, client: &WireClient, facts: &mut Facts) {
    let theirs = client.advertised();
    for (key, (_min, max)) in &cx.gateway_versions {
        let name =
            ApiKey::try_from(*key).map_or_else(|()| key.to_string(), |key| format!("{key:?}"));
        let served = theirs
            .get(key)
            .is_some_and(|(_, their_max)| their_max >= max);
        facts.set(format!("{name}.served_up_to_gateway_max"), served);
    }
}

async fn create_topic(
    client: &mut WireClient,
    name: &str,
    configs: &[(&str, &str)],
) -> Result<i16, String> {
    let request = CreateTopicsRequest::default()
        .with_timeout_ms(TIMEOUT_MS)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(topic(name))
                .with_num_partitions(PARTITIONS)
                .with_replication_factor(-1)
                .with_configs(
                    configs
                        .iter()
                        .map(|(key, value)| {
                            CreatableTopicConfig::default()
                                .with_name(s(key))
                                .with_value(Some(s(value)))
                        })
                        .collect(),
                ),
        ]);
    let version = version(client, ApiKey::CreateTopics, 5)?;
    let response = client.send(&request, version).await?;
    response
        .topics
        .first()
        .map(|result| result.error_code)
        .ok_or_else(|| "CreateTopics answered no topic".to_string())
}

/// Waits until Metadata reports every partition of `name` with a leader, as a client would.
async fn await_topic(client: &mut WireClient, name: &str) -> Result<(), String> {
    for _ in 0..60 {
        let (error, partitions) = metadata(client, name).await?;
        if error == 0
            && partitions.len() == usize::try_from(PARTITIONS).unwrap_or(0)
            && partitions
                .iter()
                .all(|(error, leader)| *error == 0 && *leader >= 0)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("topic {name} never got leaders"))
}

/// The topic error and each partition's `(error, leader)`.
async fn metadata(client: &mut WireClient, name: &str) -> Result<(i16, Vec<(i16, i32)>), String> {
    let request = MetadataRequest::default()
        .with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(topic(name))),
        ]))
        .with_allow_auto_topic_creation(false);
    let version = version(client, ApiKey::Metadata, 9)?;
    let response = client.send(&request, version).await?;
    let found = response
        .topics
        .first()
        .ok_or_else(|| "Metadata answered no topic".to_string())?;
    let mut partitions: Vec<(i32, i16, i32)> = found
        .partitions
        .iter()
        .map(|partition| {
            (
                partition.partition_index,
                partition.error_code,
                partition.leader_id.0,
            )
        })
        .collect();
    partitions.sort_unstable();
    Ok((
        found.error_code,
        partitions
            .into_iter()
            .map(|(_, error, leader)| (error, leader))
            .collect(),
    ))
}

async fn topic_admin(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("topic_admin", "t");
    facts.set("create.error", create_topic(client, &name, &[]).await?);
    await_topic(client, &name).await?;
    let (error, partitions) = metadata(client, &name).await?;
    facts.set("metadata.error", error);
    facts.set("metadata.partitions", partitions.len());
    facts.set(
        "create_again.error",
        create_topic(client, &name, &[]).await?,
    );
    let (missing, _) = metadata(client, &cx.name("topic_admin", "missing")).await?;
    facts.set("metadata_missing.error", missing);
    let configured = cx.name("topic_admin", "configured");
    facts.set(
        "create_with_retention_ms.error",
        create_topic(client, &configured, &[("retention.ms", "3600000")]).await?,
    );
    Ok(())
}

fn now_ms() -> i64 {
    i64::try_from(oracle::unix_now()).unwrap_or(0) * 1000
}

fn batch(records: &[Sent], first_timestamp: i64) -> Result<Bytes, String> {
    sequenced(records, first_timestamp, None)
}

/// As [`batch`], from producer `(id, epoch)` with its first record at `sequence`.
fn sequenced(
    records: &[Sent],
    first_timestamp: i64,
    producer: Option<(i64, i16, i32)>,
) -> Result<Bytes, String> {
    let records: Vec<Record> = records
        .iter()
        .zip(0_i64..)
        .map(|((key, value, headers), index)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: producer.map_or(NO_PRODUCER_ID, |(id, _, _)| id),
            producer_epoch: producer.map_or(NO_PRODUCER_EPOCH, |(_, epoch, _)| epoch),
            timestamp_type: TimestampType::Creation,
            offset: index,
            // A fixed `offset - sequence`, or the encoder splits the records into one batch
            // each, and a Produce partition carries exactly one batch.
            sequence: producer.map_or(NO_SEQUENCE, |(_, _, sequence)| sequence)
                + i32::try_from(index).unwrap_or(0),
            timestamp: first_timestamp + index * 1000,
            key: key.map(|key| Bytes::from(key.as_bytes().to_vec())),
            value: value.map(|value| Bytes::from(value.as_bytes().to_vec())),
            headers: headers
                .iter()
                .map(|(name, value)| {
                    (
                        s(name),
                        value.map(|value| Bytes::from(value.as_bytes().to_vec())),
                    )
                })
                .collect(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|error| format!("encode batch: {error}"))?;
    Ok(buf.freeze())
}

/// `(error, base_offset)` of one Produce to `partition`.
async fn produce(
    client: &mut WireClient,
    name: &str,
    partition: i32,
    records: Bytes,
) -> Result<(i16, i64), String> {
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(TIMEOUT_MS)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic(name))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(records)),
                ]),
        ]);
    let version = version(client, ApiKey::Produce, 9)?;
    // A leader elected moments ago can still answer 5 or 6, which write nothing; clients retry
    // both, so the scenario does too.
    for attempt in 0.. {
        let response = client.send(&request, version).await?;
        let answer = response
            .responses
            .first()
            .and_then(|topic| topic.partition_responses.first())
            .ok_or_else(|| "Produce answered no partition".to_string())?;
        if !matches!(answer.error_code, 5 | 6) || attempt == 40 {
            return Ok((answer.error_code, answer.base_offset));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    unreachable!("the loop returns")
}

/// The partition's fetch answer, and its decoded records.
async fn fetch(
    client: &mut WireClient,
    name: &str,
    offset: i64,
) -> Result<
    (
        kafka_protocol::messages::fetch_response::PartitionData,
        Vec<Record>,
    ),
    String,
> {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_isolation_level(0)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic(name))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_current_leader_epoch(-1)
                        .with_fetch_offset(offset)
                        .with_last_fetched_epoch(-1)
                        .with_log_start_offset(-1)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ]);
    let version = version(client, ApiKey::Fetch, 12)?;
    let response = client.send(&request, version).await?;
    let partition = response
        .responses
        .into_iter()
        .next()
        .and_then(|topic| topic.partitions.into_iter().next())
        .ok_or_else(|| {
            format!(
                "Fetch answered no partition (error {})",
                response.error_code
            )
        })?;
    let mut records = Vec::new();
    if let Some(mut bytes) = partition.records.clone() {
        for set in RecordBatchDecoder::decode_all(&mut bytes)
            .map_err(|error| format!("decode records: {error}"))?
        {
            records.extend(set.records.into_iter().filter(|record| !record.control));
        }
    }
    Ok((partition, records))
}

async fn produce_fetch(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("produce_fetch", "t");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    let start = now_ms() - 60_000;
    let (error, base) = produce(client, &name, 0, batch(&RECORDS[..2], start)?).await?;
    facts.set("produce_first.error", error);
    facts.set("produce_first.base_offset", base);
    let (error, base) = produce(client, &name, 0, batch(&RECORDS[2..], start + 2000)?).await?;
    facts.set("produce_second.error", error);
    facts.set("produce_second.base_offset", base);
    let (error, _) = produce(client, &name, PARTITIONS + 6, batch(&RECORDS[..1], start)?).await?;
    facts.set("produce_missing_partition.error", error);

    let (partition, records) = fetch(client, &name, 0).await?;
    facts.set("fetch.error", partition.error_code);
    facts.set("fetch.high_watermark", partition.high_watermark);
    facts.set("fetch.last_stable_offset", partition.last_stable_offset);
    facts.set("fetch.log_start_offset", partition.log_start_offset);
    facts.set("fetch.records", records.len());
    for record in &records {
        let prefix = format!("fetch.offset_{}", record.offset);
        facts.set(format!("{prefix}.key"), text(record.key.as_ref()));
        facts.set(format!("{prefix}.value"), text(record.value.as_ref()));
        facts.set(
            format!("{prefix}.timestamp_delta"),
            record.timestamp - start,
        );
        let headers: Vec<String> = record
            .headers
            .iter()
            .map(|(name, value)| format!("{}={}", name.as_str(), text(value.as_ref())))
            .collect();
        facts.set(format!("{prefix}.headers"), headers.join(","));
    }
    let (end, end_records) = fetch(client, &name, 5).await?;
    facts.set("fetch_at_end.error", end.error_code);
    facts.set("fetch_at_end.records", end_records.len());
    let (beyond, _) = fetch(client, &name, 100).await?;
    facts.set("fetch_beyond_end.error", beyond.error_code);
    Ok(())
}

/// `(error, offset, timestamp)` for one `ListOffsets` lookup on partition 0.
async fn list_offset(
    client: &mut WireClient,
    name: &str,
    timestamp: i64,
) -> Result<(i16, i64, i64), String> {
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_isolation_level(0)
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(topic(name))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_current_leader_epoch(-1)
                        .with_timestamp(timestamp),
                ]),
        ]);
    let version = version(client, ApiKey::ListOffsets, 7)?;
    // A partition elected moments ago can answer 5 or 6 once more; clients retry both.
    for attempt in 0.. {
        let response = client.send(&request, version).await?;
        let answer = response
            .topics
            .first()
            .and_then(|topic| topic.partitions.first())
            .ok_or_else(|| "ListOffsets answered no partition".to_string())?;
        if !matches!(answer.error_code, 5 | 6) || attempt == 40 {
            return Ok((answer.error_code, answer.offset, answer.timestamp));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    unreachable!("the loop returns")
}

async fn list_offsets(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("list_offsets", "t");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    let (error, empty_latest, _) = list_offset(client, &name, -1).await?;
    facts.set("empty_latest.error", error);
    facts.set("empty_latest.offset", empty_latest);
    let start = now_ms() - 60_000;
    produce(client, &name, 0, batch(&RECORDS, start)?).await?;
    for (label, timestamp) in [
        ("earliest", -2),
        ("latest", -1),
        ("max_timestamp", -3),
        ("at_third_record", start + 2000),
        ("between_records", start + 2500),
        ("before_all", start - 10_000),
        ("future", start + 3_600_000),
    ] {
        let (error, offset, found) = list_offset(client, &name, timestamp).await?;
        facts.set(format!("{label}.error"), error);
        facts.set(format!("{label}.offset"), offset);
        // A timestamp lookup answers the record's timestamp; EARLIEST and LATEST answer -1.
        let delta = if found < 0 { found } else { found - start };
        facts.set(format!("{label}.timestamp_delta"), delta);
    }
    Ok(())
}

async fn find_group_coordinator(client: &mut WireClient, group_id: &str) -> Result<i16, String> {
    let version = version(client, ApiKey::FindCoordinator, 4)?;
    let request = if version >= 4 {
        FindCoordinatorRequest::default()
            .with_key_type(0)
            .with_coordinator_keys(vec![s(group_id)])
    } else {
        FindCoordinatorRequest::default()
            .with_key_type(0)
            .with_key(s(group_id))
    };
    // A fresh broker may still be loading its offsets topic; clients retry 14 and 15.
    for _ in 0..40 {
        let response = client.send(&request, version).await?;
        let error = response
            .coordinators
            .first()
            .map_or(response.error_code, |coordinator| coordinator.error_code);
        if error != 14 && error != 15 {
            return Ok(error);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err("the group coordinator never became available".to_string())
}

/// A `ConsumerProtocolSubscription` v0 for `topic`, as a Java consumer sends it.
fn subscription(name: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    buf.put_i32(1);
    put_string(&mut buf, name);
    buf.put_i32(-1);
    buf.freeze()
}

/// A `ConsumerProtocolAssignment` v0 giving every partition of `topic`.
fn assignment(name: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    buf.put_i32(1);
    put_string(&mut buf, name);
    buf.put_i32(PARTITIONS);
    for partition in 0..PARTITIONS {
        buf.put_i32(partition);
    }
    buf.put_i32(-1);
    buf.freeze()
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_i16(i16::try_from(value.len()).expect("a short name"));
    buf.put_slice(value.as_bytes());
}

async fn join(
    client: &mut WireClient,
    group_id: &str,
    member_id: &str,
    name: &str,
) -> Result<kafka_protocol::messages::JoinGroupResponse, String> {
    let request = JoinGroupRequest::default()
        .with_group_id(group(group_id))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(10_000)
        .with_member_id(s(member_id))
        .with_protocol_type(s("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(s("range"))
                .with_metadata(subscription(name)),
        ]);
    let version = version(client, ApiKey::JoinGroup, 9)?;
    // A coordinator that is still loading answers 14, 15 or 16; clients find it again and retry.
    for attempt in 0.. {
        let response = client.send(&request, version).await?;
        if !coordinator_loading(response.error_code) || attempt == 40 {
            return Ok(response);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    unreachable!("the loop returns")
}

async fn commit(
    client: &mut WireClient,
    group_id: &str,
    generation: i32,
    member_id: &str,
    name: &str,
    offset: i64,
) -> Result<i16, String> {
    let request = OffsetCommitRequest::default()
        .with_group_id(group(group_id))
        .with_generation_id_or_member_epoch(generation)
        .with_member_id(s(member_id))
        .with_retention_time_ms(-1)
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(topic(name))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(offset)
                        .with_committed_leader_epoch(-1)
                        .with_committed_metadata(Some(s("m"))),
                ]),
        ]);
    let version = version(client, ApiKey::OffsetCommit, 8)?;
    let response = client.send(&request, version).await?;
    response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
        .map(|partition| partition.error_code)
        .ok_or_else(|| "OffsetCommit answered no partition".to_string())
}

/// `(error, committed offset, metadata)` of partitions 0 and 1.
async fn fetch_offsets(
    client: &mut WireClient,
    group_id: &str,
    name: &str,
) -> Result<Vec<(i16, i64, String)>, String> {
    let request = OffsetFetchRequest::default()
        .with_group_id(group(group_id))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(topic(name))
                .with_partition_indexes(vec![0, 1]),
        ]));
    let version = version(client, ApiKey::OffsetFetch, 7)?;
    let response = client.send(&request, version).await?;
    if response.error_code != 0 {
        return Ok(vec![(response.error_code, -1, String::new())]);
    }
    let mut partitions: Vec<_> = response
        .topics
        .first()
        .map(|topic| {
            topic
                .partitions
                .iter()
                .map(|partition| {
                    (
                        partition.partition_index,
                        partition.error_code,
                        partition.committed_offset,
                        partition
                            .metadata
                            .as_ref()
                            .map_or_else(|| "null".to_string(), ToString::to_string),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    partitions.sort();
    Ok(partitions
        .into_iter()
        .map(|(_, error, offset, metadata)| (error, offset, metadata))
        .collect())
}

fn set_offsets(facts: &mut Facts, label: &str, offsets: &[(i16, i64, String)]) {
    for (index, (error, offset, metadata)) in offsets.iter().enumerate() {
        facts.set(format!("{label}.p{index}.error"), error);
        facts.set(format!("{label}.p{index}.offset"), offset);
        // A partition with nothing committed has no meaningful metadata on either side.
        if *offset >= 0 {
            facts.set(format!("{label}.p{index}.metadata"), metadata);
        }
    }
}

async fn heartbeat(
    client: &mut WireClient,
    group_id: &str,
    generation: i32,
    member_id: &str,
) -> Result<i16, String> {
    let request = HeartbeatRequest::default()
        .with_group_id(group(group_id))
        .with_generation_id(generation)
        .with_member_id(s(member_id));
    let version = version(client, ApiKey::Heartbeat, 4)?;
    Ok(client.send(&request, version).await?.error_code)
}

/// Syncs as the group's leader, assigning itself every partition.
async fn sync(
    client: &mut WireClient,
    group_id: &str,
    generation: i32,
    member_id: &str,
    name: &str,
    facts: &mut Facts,
) -> Result<(), String> {
    let assignment = assignment(name);
    let request = SyncGroupRequest::default()
        .with_group_id(group(group_id))
        .with_generation_id(generation)
        .with_member_id(s(member_id))
        .with_protocol_type(Some(s("consumer")))
        .with_protocol_name(Some(s("range")))
        .with_assignments(vec![
            SyncGroupRequestAssignment::default()
                .with_member_id(s(member_id))
                .with_assignment(assignment.clone()),
        ]);
    let version = version(client, ApiKey::SyncGroup, 5)?;
    let synced = client.send(&request, version).await?;
    facts.set("sync.error", synced.error_code);
    facts.set("sync.assignment_echoed", synced.assignment == assignment);
    Ok(())
}

async fn classic_group(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("classic_group", "t");
    let group_id = cx.name("classic_group", "g");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    facts.set(
        "find_coordinator.error",
        find_group_coordinator(client, &group_id).await?,
    );

    let first = join(client, &group_id, "", &name)
        .await
        .map_err(|error| format!("first join: {error}"))?;
    facts.set("join_without_member_id.error", first.error_code);
    facts.set(
        "join_without_member_id.assigned_member_id",
        !first.member_id.is_empty(),
    );
    let joined = join(client, &group_id, first.member_id.as_str(), &name)
        .await
        .map_err(|error| format!("join with member id: {error}"))?;
    let member_id = joined.member_id.to_string();
    facts.set("join.error", joined.error_code);
    facts.set("join.generation", joined.generation_id);
    facts.set("join.leader_is_self", joined.leader == joined.member_id);
    facts.set(
        "join.protocol",
        joined
            .protocol_name
            .as_ref()
            .map_or("null", |name| name.as_str()),
    );
    facts.set("join.members", joined.members.len());

    sync(
        client,
        &group_id,
        joined.generation_id,
        &member_id,
        &name,
        facts,
    )
    .await?;
    facts.set(
        "heartbeat.error",
        heartbeat(client, &group_id, joined.generation_id, &member_id).await?,
    );
    facts.set(
        "commit.error",
        commit(
            client,
            &group_id,
            joined.generation_id,
            &member_id,
            &name,
            3,
        )
        .await?,
    );
    facts.set(
        "commit_stale_generation.error",
        commit(
            client,
            &group_id,
            joined.generation_id + 5,
            &member_id,
            &name,
            4,
        )
        .await?,
    );
    set_offsets(
        facts,
        "fetch_offsets",
        &fetch_offsets(client, &group_id, &name).await?,
    );

    let leave = LeaveGroupRequest::default()
        .with_group_id(group(&group_id))
        .with_members(vec![
            MemberIdentity::default().with_member_id(s(&member_id)),
        ]);
    let leave_version = version(client, ApiKey::LeaveGroup, 5)?;
    let left = client.send(&leave, leave_version).await?;
    facts.set("leave.error", left.error_code);
    facts.set(
        "leave.member_error",
        left.members.first().map_or(-1, |member| member.error_code),
    );
    facts.set(
        "heartbeat_after_leave.error",
        heartbeat(client, &group_id, joined.generation_id, &member_id).await?,
    );
    set_offsets(
        facts,
        "fetch_offsets_after_leave",
        &fetch_offsets(client, &group_id, &name).await?,
    );
    Ok(())
}

/// A commit from a consumer that never joins its group, as Flink's source commits.
async fn standalone_commit(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("standalone_commit", "t");
    let group_id = cx.name("standalone_commit", "g");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    find_group_coordinator(client, &group_id).await?;
    set_offsets(
        facts,
        "before",
        &fetch_offsets(client, &group_id, &name).await?,
    );
    facts.set(
        "commit.error",
        commit(client, &group_id, -1, "", &name, 7).await?,
    );
    set_offsets(
        facts,
        "after",
        &fetch_offsets(client, &group_id, &name).await?,
    );
    facts.set(
        "commit_reset.error",
        commit(client, &group_id, -1, "", &name, 2).await?,
    );
    set_offsets(
        facts,
        "after_reset",
        &fetch_offsets(client, &group_id, &name).await?,
    );
    Ok(())
}

/// Retriable while a broker loads its coordinator state: clients retry these codes.
const fn coordinator_loading(error: i16) -> bool {
    matches!(error, 14 | 15 | 16 | 51)
}

async fn init_producer_id(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let init_version = version(client, ApiKey::InitProducerId, 4)?;
    let request = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(-1)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    let mut idempotent = client.send(&request, init_version).await?;
    for _ in 0..60 {
        if !coordinator_loading(idempotent.error_code) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        idempotent = client.send(&request, init_version).await?;
    }
    facts.set("idempotent.error", idempotent.error_code);
    facts.set("idempotent.epoch", idempotent.producer_epoch);
    facts.set("idempotent.has_producer_id", idempotent.producer_id.0 >= 0);

    // A transactional producer finds its coordinator first, which is also what makes a broker
    // create its transaction log.
    let transactional_id = cx.name("init_producer_id", "tx");
    let find_version = version(client, ApiKey::FindCoordinator, 4)?;
    let find = if find_version >= 4 {
        FindCoordinatorRequest::default()
            .with_key_type(1)
            .with_coordinator_keys(vec![s(&transactional_id)])
    } else {
        FindCoordinatorRequest::default()
            .with_key_type(1)
            .with_key(s(&transactional_id))
    };
    let mut find_error = 0;
    for _ in 0..60 {
        let response = client.send(&find, find_version).await?;
        find_error = response
            .coordinators
            .first()
            .map_or(response.error_code, |coordinator| coordinator.error_code);
        if !coordinator_loading(find_error) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    facts.set("find_transaction_coordinator.error", find_error);

    let transactional = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(s(&transactional_id))))
        .with_transaction_timeout_ms(60_000)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    // A broker loads its transaction log on first use; clients retry these codes.
    let mut error = 0;
    for _ in 0..60 {
        error = client.send(&transactional, init_version).await?.error_code;
        if !coordinator_loading(error) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    facts.set("transactional.error", error);
    Ok(())
}

/// Kafka's idempotence rules on one partition: a retry answers its first offset and is not
/// written again, a gap is 45, the missing batch then lands in order.
async fn idempotent_producer(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("idempotent_producer", "t");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    let init_version = version(client, ApiKey::InitProducerId, 4)?;
    let request = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(-1)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    let mut init = client.send(&request, init_version).await?;
    for _ in 0..60 {
        if !coordinator_loading(init.error_code) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        init = client.send(&request, init_version).await?;
    }
    let (id, epoch) = (init.producer_id.0, init.producer_epoch);
    let start = now_ms() - 60_000;
    let first = sequenced(&RECORDS[..2], start, Some((id, epoch, 0)))?;
    let (error, base) = produce(client, &name, 0, first.clone()).await?;
    facts.set("first.error", error);
    facts.set("first.base_offset", base);
    let (error, base) = produce(client, &name, 0, first).await?;
    facts.set("retry.error", error);
    facts.set("retry.base_offset", base);
    let gap = sequenced(&RECORDS[3..4], start, Some((id, epoch, 3)))?;
    let (error, _) = produce(client, &name, 0, gap).await?;
    facts.set("gap.error", error);
    let next = sequenced(&RECORDS[2..3], start, Some((id, epoch, 2)))?;
    let (error, base) = produce(client, &name, 0, next).await?;
    facts.set("next.error", error);
    facts.set("next.base_offset", base);
    let (partition, records) = fetch(client, &name, 0).await?;
    facts.set("fetch.high_watermark", partition.high_watermark);
    facts.set("fetch.records", records.len());
    Ok(())
}

async fn describe_configs(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("describe_configs", "t");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    let request = DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(s(&name))
            .with_configuration_keys(Some(vec![s("retention.ms"), s("cleanup.policy")])),
    ]);
    let version = version(client, ApiKey::DescribeConfigs, 4)?;
    let response = client.send(&request, version).await?;
    let result = response
        .results
        .first()
        .ok_or_else(|| "DescribeConfigs answered no resource".to_string())?;
    facts.set("error", result.error_code);
    for config in &result.configs {
        let key = config.name.as_str();
        facts.set(
            format!("{key}.value"),
            config.value.as_ref().map_or("null", |value| value.as_str()),
        );
        facts.set(format!("{key}.source"), config.config_source);
        facts.set(format!("{key}.read_only"), config.read_only);
    }
    Ok(())
}

/// The configurations Kafka frameworks create their own topics with (apache/iggy#4508).
const FRAMEWORK_CONFIGS: [(&str, &[(&str, &str)]); 5] = [
    // Kafka Connect's config, offset and status topics.
    ("connect", &[("cleanup.policy", "compact")]),
    // Kafka Streams changelog and repartition topics.
    (
        "streams_changelog",
        &[
            ("cleanup.policy", "compact"),
            ("message.timestamp.type", "CreateTime"),
        ],
    ),
    (
        "streams_repartition",
        &[
            ("cleanup.policy", "delete"),
            ("message.timestamp.type", "CreateTime"),
            ("retention.ms", "-1"),
            ("segment.bytes", "52428800"),
        ],
    ),
    // Debezium's schema history topic.
    (
        "debezium_history",
        &[
            ("cleanup.policy", "delete"),
            ("retention.bytes", "-1"),
            ("retention.ms", "9223372036854775807"),
        ],
    ),
    // Values Kafka normalises before it stores them.
    (
        "normalised",
        &[
            ("cleanup.policy", "compact, delete"),
            ("min.cleanable.dirty.ratio", "1"),
            ("min.insync.replicas", "+2"),
            ("unclean.leader.election.enable", "TRUE"),
        ],
    ),
];

/// The broker configurations the gateway answers for its node.
const BROKER_KEYS: [&str; 11] = [
    "auto.create.topics.enable",
    "compression.type",
    "default.replication.factor",
    "delete.topic.enable",
    "log.cleanup.policy",
    "log.retention.bytes",
    "log.retention.ms",
    "log.segment.bytes",
    "min.insync.replicas",
    "num.partitions",
    "unclean.leader.election.enable",
];

/// Describes `keys` (all of them when `None`) of topic `name`, as facts under `label`.
async fn describe_topic(
    client: &mut WireClient,
    name: &str,
    keys: Option<&[&str]>,
    label: &str,
    facts: &mut Facts,
) -> Result<(), String> {
    let request = DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(s(name))
            .with_configuration_keys(keys.map(|keys| keys.iter().map(|key| s(key)).collect())),
    ]);
    let version = version(client, ApiKey::DescribeConfigs, 4)?;
    let response = client.send(&request, version).await?;
    let result = response
        .results
        .first()
        .ok_or_else(|| "DescribeConfigs answered no resource".to_string())?;
    facts.set(format!("{label}.error"), result.error_code);
    for config in &result.configs {
        let key = config.name.as_str();
        facts.set(
            format!("{label}.{key}.value"),
            config.value.as_ref().map_or("null", |value| value.as_str()),
        );
        facts.set(format!("{label}.{key}.source"), config.config_source);
    }
    Ok(())
}

/// One `IncrementalAlterConfigs` resource on topic `name`: its error code.
async fn incremental(
    client: &mut WireClient,
    name: &str,
    configs: &[(&str, i8, Option<&str>)],
) -> Result<i16, String> {
    let request = IncrementalAlterConfigsRequest::default().with_resources(vec![
        IncrementalAlterConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(s(name))
            .with_configs(
                configs
                    .iter()
                    .map(|(key, operation, value)| {
                        IncrementalAlterableConfig::default()
                            .with_name(s(key))
                            .with_config_operation(*operation)
                            .with_value(value.map(s))
                    })
                    .collect(),
            ),
    ]);
    let version = version(client, ApiKey::IncrementalAlterConfigs, 1)?;
    let response = client.send(&request, version).await?;
    response
        .responses
        .first()
        .map(|result| result.error_code)
        .ok_or_else(|| "IncrementalAlterConfigs answered no resource".to_string())
}

/// Every Kafka topic configuration through create, describe and both alters.
async fn topic_configs(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    // Every key, its type and its default, on a topic that sets nothing.
    let plain = cx.name("topic_configs", "plain");
    create_topic(client, &plain, &[]).await?;
    await_topic(client, &plain).await?;
    let request = DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(s(&plain))
            .with_configuration_keys(None),
    ]);
    let version = version(client, ApiKey::DescribeConfigs, 4)?;
    let response = client.send(&request, version).await?;
    let result = response
        .results
        .first()
        .ok_or_else(|| "DescribeConfigs answered no resource".to_string())?;
    let mut names: Vec<&str> = result
        .configs
        .iter()
        .map(|config| config.name.as_str())
        .collect();
    names.sort_unstable();
    facts.set("all.names", names.join(","));
    for config in &result.configs {
        let key = config.name.as_str();
        facts.set(format!("all.{key}.type"), config.config_type);
        facts.set(format!("all.{key}.read_only"), config.read_only);
        facts.set(
            format!("all.{key}.value"),
            config.value.as_ref().map_or("null", |value| value.as_str()),
        );
    }

    // The framework sets: created, then described back.
    for (label, configs) in FRAMEWORK_CONFIGS {
        let name = cx.name("topic_configs", &label.replace('_', "-"));
        facts.set(
            format!("{label}.create.error"),
            create_topic(client, &name, configs).await?,
        );
        await_topic(client, &name).await?;
        let keys: Vec<&str> = configs.iter().map(|(key, _)| *key).collect();
        describe_topic(client, &name, Some(&keys), label, facts).await?;
    }

    // Refused creates.
    for (label, config) in [
        ("create_unknown", ("foo.bar", "1")),
        ("create_bad_value", ("min.insync.replicas", "0")),
        ("create_bad_policy", ("cleanup.policy", "Compact")),
    ] {
        let name = cx.name("topic_configs", &label.replace('_', "-"));
        facts.set(
            format!("{label}.error"),
            create_topic(client, &name, &[config]).await?,
        );
    }

    // Incremental alters on the plain topic.
    let keys = ["cleanup.policy", "min.insync.replicas", "retention.ms"];
    facts.set(
        "set.error",
        incremental(
            client,
            &plain,
            &[
                ("min.insync.replicas", 0, Some("2")),
                ("cleanup.policy", 0, Some("compact")),
                ("retention.ms", 0, Some("3600000")),
            ],
        )
        .await?,
    );
    describe_topic(client, &plain, Some(&keys), "after_set", facts).await?;
    facts.set(
        "append.error",
        incremental(client, &plain, &[("cleanup.policy", 2, Some("delete"))]).await?,
    );
    facts.set(
        "delete.error",
        incremental(
            client,
            &plain,
            &[("min.insync.replicas", 1, None), ("retention.ms", 1, None)],
        )
        .await?,
    );
    describe_topic(client, &plain, Some(&keys), "after_delete", facts).await?;
    for (label, config) in [
        ("set_unknown", ("foo.bar", 0, Some("1"))),
        ("set_bad_value", ("segment.ms", 0, Some("0"))),
        ("append_not_list", ("retention.ms", 2, Some("1"))),
        ("delete_unknown", ("foo.bar", 1, None)),
    ] {
        facts.set(
            format!("{label}.error"),
            incremental(client, &plain, &[config]).await?,
        );
    }

    // The broker.
    let request = DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(4)
            .with_resource_name(s("1"))
            .with_configuration_keys(Some(BROKER_KEYS.iter().map(|key| s(key)).collect())),
    ]);
    let response = client.send(&request, version).await?;
    let result = response
        .results
        .first()
        .ok_or_else(|| "DescribeConfigs answered no broker".to_string())?;
    facts.set("broker.error", result.error_code);
    for key in BROKER_KEYS {
        let entry = result
            .configs
            .iter()
            .find(|config| config.name.as_str() == key);
        facts.set(format!("broker.{key}.present"), entry.is_some());
        if let Some(entry) = entry {
            facts.set(format!("broker.{key}.type"), entry.config_type);
        }
    }
    Ok(())
}

/// Skips, or fails under `KAFKA_E2E_REQUIRED=1`.
fn skip(reason: &str) {
    assert!(
        !env_flag("KAFKA_E2E_REQUIRED"),
        "KAFKA_E2E_REQUIRED=1 but the oracle suite cannot run: {reason}"
    );
    eprintln!("skipping the oracle wire diff: {reason}");
}

fn iggy_server_built() -> bool {
    std::env::current_exe().is_ok_and(|exe| {
        exe.parent()
            .and_then(std::path::Path::parent)
            .is_some_and(|dir| {
                dir.join(format!("iggy-server{}", std::env::consts::EXE_SUFFIX))
                    .is_file()
            })
    })
}

/// The gateway, bridged to a spawned `iggy-server`, until dropped.
struct Gateway {
    address: String,
    _shutdown: broadcast::Sender<()>,
    _server: iggy_server::TestServer,
    _data_dir: tempfile::TempDir,
}

async fn start_gateway() -> Gateway {
    let data_dir = tempfile::tempdir().expect("create the server data dir");
    let server = iggy_server::TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("the bridge connects to a ready server");
    let listener = bind_listener("127.0.0.1:0").expect("bind an ephemeral port");
    let address = listener.local_addr().expect("local address").to_string();
    let listener = BoundListener::new(
        ListenerConfig::new(SecurityProtocol::Plaintext, address.clone()),
        listener,
    );
    let mut config = GatewayConfig::default();
    // As the oracles run: Kafka's group.initial.rebalance.delay.ms is 0 there.
    config.group.initial_rebalance_delay = Duration::ZERO;
    let gateway = KafkaGateway::new(config).with_bridge(Some(Arc::new(bridge)));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_the_same_requests_the_gateway_should_answer_as_the_oracles_do() {
    if !iggy_server_built() {
        skip("iggy-server is not built; run `cargo build --bin iggy-server` first");
        return;
    }
    let mut oracles = Vec::new();
    let mut held = (None, None);
    match KafkaOracle::start().await {
        Ok(kafka) => {
            oracles.push(kafka.target.clone());
            held.0 = Some(kafka);
        }
        Err(Unavailable::Skipped(reason)) => {
            skip(&reason);
            return;
        }
        Err(Unavailable::Failed(reason)) => panic!("Apache Kafka oracle: {reason}"),
    }
    match RedpandaOracle::start().await {
        Ok(redpanda) => {
            oracles.push(redpanda.target.clone());
            held.1 = Some(redpanda);
        }
        Err(Unavailable::Skipped(reason)) => eprintln!("Redpanda oracle skipped: {reason}"),
        Err(Unavailable::Failed(reason)) => panic!("Redpanda oracle: {reason}"),
    }

    let gateway = start_gateway().await;
    let gateway_target = Target {
        kind: TargetKind::Gateway,
        address: gateway.address.clone(),
    };
    let gateway_versions = WireClient::connect(&gateway.address)
        .await
        .expect("the gateway answers ApiVersions")
        .advertised()
        .clone();
    let allowed = parse_allowed(ALLOWED_DIFFERENCES).expect("allowed_differences.toml parses");
    let run = run_id();

    let mut differences: Vec<Difference> = Vec::new();
    let mut disagreements: Vec<Difference> = Vec::new();
    let mut ran = Vec::new();
    for scenario in Scenario::ALL {
        let context = |target: &Target| Context {
            target: target.clone(),
            run: run.clone(),
            gateway_versions: gateway_versions.clone(),
        };
        let ours = scenario
            .run(&context(&gateway_target))
            .await
            .unwrap_or_else(|error| panic!("{} on the gateway: {error}", scenario.name()));
        let mut kafka_facts: Option<Facts> = None;
        for oracle in &oracles {
            let theirs = scenario
                .run(&context(oracle))
                .await
                .unwrap_or_else(|error| panic!("{} on {}: {error}", scenario.name(), oracle.kind));
            if env_flag("KAFKA_ORACLE_DUMP") {
                for (fact, value) in theirs.inner() {
                    let gateway_value = ours.get(fact).unwrap_or("<absent>");
                    eprintln!(
                        "{}/{fact}: {} {value} | gateway {gateway_value}",
                        scenario.name(),
                        oracle.kind
                    );
                }
            }
            for difference in compare(scenario.name(), &ours, oracle.kind, &theirs) {
                // Apache Kafka is the reference: where the gateway agrees with it and another
                // oracle does not, the other oracle is the odd one out, not the gateway.
                let kafka_agrees = kafka_facts.as_ref().is_some_and(|kafka| {
                    oracle.kind != TargetKind::Kafka
                        && kafka.get(&difference.fact) == difference.gateway.as_deref()
                });
                if kafka_agrees {
                    disagreements.push(difference);
                } else {
                    differences.push(difference);
                }
            }
            ran.push((scenario.name().to_string(), oracle.kind));
            if oracle.kind == TargetKind::Kafka {
                kafka_facts = Some(theirs);
            }
        }
    }

    let verdict = judge(&differences, &allowed, &ran);
    let mut report = verdict.report();
    for difference in &disagreements {
        let _ = writeln!(
            report,
            "oracles disagree (gateway matches kafka) {difference}"
        );
    }
    eprintln!("oracle wire diff ({} oracles):\n{report}", oracles.len());
    if let Some(path) = std::env::var_os("KAFKA_ORACLE_REPORT") {
        let _ = std::fs::write(path, &report);
    }
    assert!(
        verdict.passed(),
        "the gateway differs from the oracles:\n{report}"
    );
    drop(held);
}

fn planted(scenario: &str, fact: &str, oracle: TargetKind) -> Difference {
    Difference {
        scenario: scenario.to_string(),
        fact: fact.to_string(),
        oracle,
        gateway: Some("1".to_string()),
        expected: Some("0".to_string()),
    }
}

#[test]
fn given_an_undeclared_difference_the_judge_should_fail_the_run() {
    let mut gateway = Facts::default();
    gateway.set("produce.error", 6);
    let mut kafka = Facts::default();
    kafka.set("produce.error", 0);
    let differences = compare("produce_fetch", &gateway, TargetKind::Kafka, &kafka);
    assert_eq!(differences.len(), 1);
    let verdict = judge(
        &differences,
        &[],
        &[("produce_fetch".to_string(), TargetKind::Kafka)],
    );
    assert!(!verdict.passed());
    assert!(
        verdict
            .report()
            .contains("UNEXPECTED produce_fetch/produce.error")
    );
}

#[test]
fn given_a_declared_difference_the_judge_should_pass_and_name_its_issue() {
    let allowed = parse_allowed(
        r#"
        [[difference]]
        scenario = "produce_fetch"
        fact = "fetch.log_start_offset"
        issue = "apache/iggy#4525"
        reason = "no log start offset yet"
        "#,
    )
    .expect("parses");
    let differences = [planted(
        "produce_fetch",
        "fetch.log_start_offset",
        TargetKind::Kafka,
    )];
    let verdict = judge(
        &differences,
        &allowed,
        &[("produce_fetch".to_string(), TargetKind::Kafka)],
    );
    assert!(verdict.passed(), "{}", verdict.report());
    assert!(verdict.report().contains("[apache/iggy#4525]"));
}

#[test]
fn given_a_declared_difference_that_no_longer_occurs_the_judge_should_report_it_stale() {
    let allowed = parse_allowed(
        r#"
        [[difference]]
        scenario = "produce_fetch"
        fact = "fetch.log_start_offset"
        issue = "apache/iggy#4525"
        reason = "no log start offset yet"
        "#,
    )
    .expect("parses");
    let verdict = judge(
        &[],
        &allowed,
        &[("produce_fetch".to_string(), TargetKind::Kafka)],
    );
    assert!(!verdict.passed());
    assert!(verdict.report().contains("STALE"));
    // Not stale when its scenario did not run against that oracle.
    let verdict = judge(&[], &allowed, &[]);
    assert!(verdict.passed());
}

#[test]
fn given_an_oracle_restriction_the_judge_should_apply_it_to_that_oracle_only() {
    let allowed = parse_allowed(
        r#"
        [[difference]]
        scenario = "classic_group"
        fact = "join.protocol"
        oracles = ["redpanda"]
        issue = "apache/iggy#4531"
        reason = "example"
        "#,
    )
    .expect("parses");
    let differences = [planted("classic_group", "join.protocol", TargetKind::Kafka)];
    let verdict = judge(
        &differences,
        &allowed,
        &[("classic_group".to_string(), TargetKind::Kafka)],
    );
    assert_eq!(verdict.unexpected.len(), 1);
}

#[test]
fn every_allowed_difference_should_name_a_scenario_and_an_issue() {
    let allowed: Vec<Allowed> =
        parse_allowed(ALLOWED_DIFFERENCES).expect("allowed_differences.toml parses");
    let scenarios: Vec<&str> = Scenario::ALL
        .iter()
        .map(|scenario| scenario.name())
        .collect();
    for entry in &allowed {
        assert!(
            scenarios.contains(&entry.scenario.as_str()),
            "unknown scenario in {entry:?}"
        );
        assert!(
            entry.issue.starts_with("apache/iggy#"),
            "issue must be apache/iggy#N: {entry:?}"
        );
        for oracle in &entry.oracles {
            assert!(
                matches!(oracle.as_str(), "kafka" | "redpanda"),
                "unknown oracle in {entry:?}"
            );
        }
    }
}
