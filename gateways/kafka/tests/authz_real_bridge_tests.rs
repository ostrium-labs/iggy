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

//! Per-principal authorization (KW-10) against a real `iggy-server`.
//!
//! Users hold global, per-stream and per-topic Iggy grants; each logs in to the gateway with
//! SASL/PLAIN and is checked topic by topic, with the bridge's Iggy user behind every allowed
//! request. Then two principals use a group of the same name and each sees only its own.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{
    GlobalPermissions, Identifier, Permissions, StreamClient, StreamPermissions, TopicClient,
    TopicPermissions, UserClient, UserCreateOptions, UserStatus,
};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, BrokerId, DeleteGroupsRequest, FetchRequest, GroupId, InitProducerIdRequest,
    JoinGroupRequest, ListGroupsRequest, ListOffsetsRequest, MetadataRequest, OffsetCommitRequest,
    OffsetFetchRequest, ProduceRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serial_test::serial;
use tokio::sync::broadcast;

use iggy_gateway_kafka::auth::IggyAuthenticator;
use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/oracle.rs"]
mod oracle;
#[path = "common/sasl_wire.rs"]
mod sasl_wire;

use iggy_server::TestServer;
use oracle::WireClient;

const STREAM: &str = "kafka";
const ORDERS: &str = "orders";
const PAYMENTS: &str = "payments";
const PASSWORD: &str = "secret-123";
const NONE: i16 = 0;
const TOPIC_DENIED: i16 = 29;
const GROUP_DENIED: i16 = 30;
const CLUSTER_DENIED: i16 = 31;

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

struct Stack {
    address: String,
    _shutdown: broadcast::Sender<()>,
    _server: TestServer,
    _data_dir: tempfile::TempDir,
}

/// One Iggy server with `orders` and `payments` in the default stream, users holding the grants
/// the test needs, and a gateway with a `SASL_PLAINTEXT` listener in front.
async fn stack() -> Stack {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = Arc::new(
        IggyBridge::connect(server.test_config())
            .await
            .expect("bridge connects"),
    );
    for topic in [ORDERS, PAYMENTS] {
        bridge
            .ensure_stream_and_topic(topic, 1)
            .await
            .expect("create the topic");
    }
    let raw = iggy_server::raw_client(&server).await;
    let stream_id = Identifier::named(STREAM).expect("stream id");
    let stream = raw
        .get_stream(&stream_id)
        .await
        .expect("read the stream")
        .expect("the stream exists");
    let orders = raw
        .get_topic(&stream_id, &Identifier::named(ORDERS).expect("topic id"))
        .await
        .expect("read the topic")
        .expect("the topic exists");
    let stream_key = stream.id as usize;

    let per_stream = |configure: &dyn Fn(&mut StreamPermissions)| {
        let mut permissions = StreamPermissions::default();
        configure(&mut permissions);
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(BTreeMap::from([(stream_key, permissions)])),
        })
    };
    let orders_poller = |stream: &mut StreamPermissions| {
        stream.topics = Some(BTreeMap::from([(
            orders.id as usize,
            TopicPermissions {
                poll_messages: true,
                ..TopicPermissions::default()
            },
        )]));
    };
    let users = [
        ("reader", per_stream(&orders_poller)),
        ("other-reader", per_stream(&orders_poller)),
        (
            "writer",
            per_stream(&|stream: &mut StreamPermissions| stream.send_messages = true),
        ),
        ("nobody", None),
    ];
    for (user, permissions) in users {
        raw.create_user(
            user,
            PASSWORD,
            UserStatus::Active,
            permissions,
            &UserCreateOptions::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("create {user}: {error}"));
    }

    let socket = bind_listener("127.0.0.1:0").expect("bind");
    let address = socket.local_addr().expect("addr").to_string();
    let listener = BoundListener::new(
        ListenerConfig::new(SecurityProtocol::SaslPlaintext, address.clone()),
        socket,
    );
    let gateway = KafkaGateway::new(GatewayConfig::default())
        .with_bridge(Some(bridge))
        .with_authenticator(Arc::new(IggyAuthenticator::new(
            server.test_config().address,
        )));
    let (shutdown, receiver) = broadcast::channel(1);
    tokio::spawn(async move {
        let _ = gateway.run(vec![listener], receiver).await;
    });
    Stack {
        address,
        _shutdown: shutdown,
        _server: server,
        _data_dir: data_dir,
    }
}

async fn login(stack: &Stack, user: &str) -> WireClient {
    let mut client = WireClient::connect(&stack.address)
        .await
        .expect("connect to the gateway");
    let code = sasl_wire::plain(&mut client, user, PASSWORD)
        .await
        .expect("PLAIN exchange");
    assert_eq!(code, NONE, "{user} logs in");
    client
}

fn one_record() -> Bytes {
    let record = Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: NO_SEQUENCE,
        timestamp: 1_700_000_000_000,
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::from_static(b"v")),
        headers: IndexMap::new(),
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        [record].iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode a batch");
    buf.freeze()
}

async fn produce(client: &mut WireClient, topic: &str) -> i16 {
    let version = client.version(ApiKey::Produce, 9).expect("Produce");
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(s(topic)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(one_record())),
                ]),
        ]);
    let response = client.send(&request, version).await.expect("Produce");
    response.responses[0].partition_responses[0].error_code
}

async fn fetch(client: &mut WireClient, topic: &str) -> i16 {
    let version = client.version(ApiKey::Fetch, 12).expect("Fetch");
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(s(topic)))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ]);
    let response = client.send(&request, version).await.expect("Fetch");
    response.responses[0].partitions[0].error_code
}

async fn list_offsets(client: &mut WireClient, topic: &str) -> i16 {
    let version = client.version(ApiKey::ListOffsets, 7).expect("ListOffsets");
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(TopicName(s(topic)))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_timestamp(-1),
                ]),
        ]);
    let response = client.send(&request, version).await.expect("ListOffsets");
    response.topics[0].partitions[0].error_code
}

/// The topics an all-topics Metadata lists.
async fn listed_topics(client: &mut WireClient) -> Vec<String> {
    let version = client.version(ApiKey::Metadata, 9).expect("Metadata");
    let response = client
        .send(&MetadataRequest::default().with_topics(None), version)
        .await
        .expect("Metadata");
    let mut names: Vec<String> = response
        .topics
        .iter()
        .filter_map(|topic| topic.name.as_ref().map(|name| name.as_str().to_string()))
        .collect();
    names.sort();
    names
}

async fn named_metadata(client: &mut WireClient, topic: &str) -> i16 {
    let version = client.version(ApiKey::Metadata, 9).expect("Metadata");
    let request = MetadataRequest::default()
        .with_allow_auto_topic_creation(false)
        .with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(TopicName(s(topic)))),
        ]));
    let response = client.send(&request, version).await.expect("Metadata");
    response.topics[0].error_code
}

async fn commit(client: &mut WireClient, group: &str, offset: i64) -> i16 {
    let version = client
        .version(ApiKey::OffsetCommit, 8)
        .expect("OffsetCommit");
    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_generation_id_or_member_epoch(-1)
        .with_member_id(StrBytes::new())
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(s(ORDERS)))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(offset),
                ]),
        ]);
    let response = client.send(&request, version).await.expect("OffsetCommit");
    response.topics[0].partitions[0].error_code
}

async fn committed(client: &mut WireClient, group: &str) -> i64 {
    let version = client.version(ApiKey::OffsetFetch, 7).expect("OffsetFetch");
    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(TopicName(s(ORDERS)))
                .with_partition_indexes(vec![0]),
        ]));
    let response = client.send(&request, version).await.expect("OffsetFetch");
    assert_eq!(response.error_code, NONE, "OffsetFetch of {group}");
    response.topics[0].partitions[0].committed_offset
}

async fn listed_groups(client: &mut WireClient) -> Vec<String> {
    let version = client.version(ApiKey::ListGroups, 4).expect("ListGroups");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let response = client
            .send(&ListGroupsRequest::default(), version)
            .await
            .expect("ListGroups");
        // The first scan of Iggy's groups answers COORDINATOR_LOAD_IN_PROGRESS while it runs.
        if response.error_code == NONE {
            let mut groups: Vec<String> = response
                .groups
                .iter()
                .map(|group| group.group_id.as_str().to_string())
                .collect();
            groups.sort();
            return groups;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "ListGroups kept answering {}",
            response.error_code
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_per_topic_and_per_stream_grants_each_topic_should_be_authorized_on_its_own() {
    let stack = stack().await;

    // A poll grant on `orders` alone: it reads and looks up `orders`, and nothing else.
    let mut reader = login(&stack, "reader").await;
    assert_eq!(listed_topics(&mut reader).await, vec![ORDERS.to_string()]);
    assert_eq!(fetch(&mut reader, ORDERS).await, NONE);
    assert_eq!(list_offsets(&mut reader, ORDERS).await, NONE);
    assert_eq!(produce(&mut reader, ORDERS).await, TOPIC_DENIED);
    assert_eq!(fetch(&mut reader, PAYMENTS).await, TOPIC_DENIED);
    assert_eq!(list_offsets(&mut reader, PAYMENTS).await, TOPIC_DENIED);
    assert_eq!(named_metadata(&mut reader, PAYMENTS).await, TOPIC_DENIED);
    // A topic that does not exist answers the same, so nothing leaks about which ones do.
    assert_eq!(named_metadata(&mut reader, "absent").await, TOPIC_DENIED);

    // A send grant on the whole stream: it writes every topic of the stream and reads none.
    let mut writer = login(&stack, "writer").await;
    assert_eq!(produce(&mut writer, ORDERS).await, NONE);
    assert_eq!(produce(&mut writer, PAYMENTS).await, NONE);
    assert_eq!(fetch(&mut writer, ORDERS).await, TOPIC_DENIED);
    assert_eq!(named_metadata(&mut writer, PAYMENTS).await, NONE);

    // No grant at all: refused the topics, the groups and a producer id.
    let mut nobody = login(&stack, "nobody").await;
    assert_eq!(listed_topics(&mut nobody).await, Vec::<String>::new());
    assert_eq!(named_metadata(&mut nobody, ORDERS).await, TOPIC_DENIED);
    let version = nobody.version(ApiKey::JoinGroup, 5).expect("JoinGroup");
    let join = JoinGroupRequest::default()
        .with_group_id(GroupId(s("app")))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(10_000)
        .with_member_id(StrBytes::new())
        .with_protocol_type(s("consumer"));
    let joined = nobody.send(&join, version).await.expect("JoinGroup");
    assert_eq!(joined.error_code, GROUP_DENIED);
    let version = nobody
        .version(ApiKey::InitProducerId, 4)
        .expect("InitProducerId");
    let init = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(-1);
    let initialized = nobody.send(&init, version).await.expect("InitProducerId");
    assert_eq!(initialized.error_code, CLUSTER_DENIED);
    // The writer may write, so it may have a producer id.
    let version = writer
        .version(ApiKey::InitProducerId, 4)
        .expect("InitProducerId");
    let initialized = writer.send(&init, version).await.expect("InitProducerId");
    assert_eq!(initialized.error_code, NONE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_two_principals_a_group_of_the_same_name_should_be_two_groups() {
    let stack = stack().await;
    let mut reader = login(&stack, "reader").await;
    let mut other = login(&stack, "other-reader").await;

    assert_eq!(commit(&mut reader, "app", 5).await, NONE);
    assert_eq!(commit(&mut other, "app", 9).await, NONE);
    assert_eq!(committed(&mut reader, "app").await, 5);
    assert_eq!(committed(&mut other, "app").await, 9);

    // Each lists its own group under the name it gave, and nothing of the other's.
    assert_eq!(listed_groups(&mut reader).await, vec!["app".to_string()]);
    assert_eq!(listed_groups(&mut other).await, vec!["app".to_string()]);

    // Deleting `app` deletes the caller's own.
    let version = other
        .version(ApiKey::DeleteGroups, 2)
        .expect("DeleteGroups");
    let deleted = other
        .send(
            &DeleteGroupsRequest::default().with_groups_names(vec![GroupId(s("app"))]),
            version,
        )
        .await
        .expect("DeleteGroups");
    assert_eq!(deleted.results[0].group_id.as_str(), "app");
    assert_eq!(deleted.results[0].error_code, NONE);
    assert_eq!(committed(&mut other, "app").await, -1);
    assert_eq!(committed(&mut reader, "app").await, 5);
}
