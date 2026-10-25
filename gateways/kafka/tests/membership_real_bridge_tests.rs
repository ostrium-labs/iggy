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

//! Several gateways behind one bootstrap list (#4515), against a real `iggy-server`.
//!
//! Three gateways run in this process, each with its own bridge and node id, and find each other
//! through the gateways topic. The tests speak the Kafka wire to each of them and check that they
//! agree: the same brokers, the same coordinator for every group, the same leader for every
//! partition, and a refusal from every gateway that is not the one named. Then a gateway leaves,
//! once with a goodbye and once without, and its groups and partitions move to the others.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, DescribeClusterRequest, FindCoordinatorRequest, GroupId, JoinGroupRequest,
    MetadataRequest, ProduceRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serial_test::serial;
use uuid::Uuid;

use iggy_gateway_kafka::bridge::IggyBridge;

#[path = "common/gateways.rs"]
mod gateways;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/oracle.rs"]
mod oracle;

use gateways::{Gateway, HEARTBEAT, JOIN_DELAY, LEASE};
use iggy_server::TestServer;
use oracle::WireClient;

const TOPIC: &str = "kw5-orders";
const PARTITIONS: i32 = 6;
const GROUPS: usize = 24;
const ERROR_NONE: i16 = 0;
const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
const ERROR_NOT_COORDINATOR: i16 = 16;
const ERROR_MEMBER_ID_REQUIRED: i16 = 79;

/// How long a test waits for the gateways to agree.
const CONVERGE: Duration = Duration::from_secs(20);

/// How long an agreement must hold before a test relies on it. Every gateway can list a node as
/// a broker while it is still in its join delay and leads nothing; it takes the partitions it
/// leads once the delay is over, and the others hear that within a heartbeat. An agreement seen
/// earlier can be the one before that move.
const SETTLE: Duration = JOIN_DELAY.saturating_add(HEARTBEAT.saturating_mul(2));

async fn client(gateway: &Gateway) -> WireClient {
    WireClient::connect(&gateway.addr.to_string())
        .await
        .expect("connect to the gateway")
}

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// Broker ids and ports, controller, and each partition's leader, as one gateway reports them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reported {
    brokers: BTreeMap<i32, u16>,
    controller: i32,
    leaders: Vec<i32>,
}

async fn metadata(gateway: &Gateway) -> Reported {
    let mut client = client(gateway).await;
    let version = client.version(ApiKey::Metadata, 9).expect("Metadata");
    let request = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default().with_name(Some(TopicName(s(TOPIC)))),
    ]));
    let response = client.send(&request, version).await.expect("Metadata");
    let brokers = response
        .brokers
        .iter()
        .map(|broker| {
            (
                broker.node_id.0,
                u16::try_from(broker.port).expect("a port"),
            )
        })
        .collect();
    let topic = response.topics.first().expect("the topic");
    assert_eq!(topic.error_code, ERROR_NONE, "Metadata of {TOPIC}");
    let mut partitions: Vec<_> = topic
        .partitions
        .iter()
        .map(|partition| (partition.partition_index, partition.leader_id.0))
        .collect();
    partitions.sort_unstable();
    Reported {
        brokers,
        controller: response.controller_id.0,
        leaders: partitions.into_iter().map(|(_, leader)| leader).collect(),
    }
}

/// The topic's id (KIP-516), from Metadata v12.
async fn topic_id(gateway: &Gateway) -> Uuid {
    let mut client = client(gateway).await;
    let version = client.version(ApiKey::Metadata, 12).expect("Metadata");
    assert_eq!(version, 12, "the gateway serves Metadata v12");
    let request = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default().with_name(Some(TopicName(s(TOPIC)))),
    ]));
    let response = client.send(&request, version).await.expect("Metadata");
    let topic = response.topics.first().expect("the topic");
    assert!(!topic.topic_id.is_nil(), "{TOPIC} has a topic id");
    topic.topic_id
}

/// The coordinator each of `groups` is given, by node id, from one gateway.
async fn coordinators(gateway: &Gateway, groups: &[String]) -> Vec<(i32, u16)> {
    let mut client = client(gateway).await;
    let version = client
        .version(ApiKey::FindCoordinator, 4)
        .expect("FindCoordinator");
    assert_eq!(version, 4, "the gateway serves FindCoordinator v4");
    let request = FindCoordinatorRequest::default()
        .with_key_type(0)
        .with_coordinator_keys(groups.iter().map(|group| s(group)).collect());
    let response = client
        .send(&request, version)
        .await
        .expect("FindCoordinator");
    response
        .coordinators
        .iter()
        .map(|coordinator| {
            assert_eq!(coordinator.error_code, ERROR_NONE, "{coordinator:?}");
            (
                coordinator.node_id.0,
                u16::try_from(coordinator.port).expect("a port"),
            )
        })
        .collect()
}

/// Waits until every gateway of `gateways` reports exactly `expected` as its brokers, and all
/// agree on the controller and the leaders, and keep agreeing for [`SETTLE`]. Returns what they
/// agreed on, and when they first did.
async fn converge(gateways: &[&Gateway], expected: &[i32]) -> (Reported, tokio::time::Instant) {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    let mut agreed: Option<(Reported, tokio::time::Instant)> = None;
    loop {
        let mut reports = Vec::new();
        for gateway in gateways {
            reports.push(metadata(gateway).await);
        }
        let ids_match = reports.iter().all(|report| {
            report.brokers.keys().copied().collect::<Vec<_>>() == expected
                && report
                    .leaders
                    .iter()
                    .all(|leader| expected.contains(leader))
        });
        if ids_match && reports.windows(2).all(|pair| pair[0] == pair[1]) {
            let report = reports.swap_remove(0);
            match agreed.take() {
                Some((previous, since)) if previous == report => {
                    if since.elapsed() >= SETTLE {
                        return (report, since);
                    }
                    agreed = Some((previous, since));
                }
                _ => agreed = Some((report, tokio::time::Instant::now())),
            }
        } else {
            agreed = None;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the gateways never agreed on brokers {expected:?}: {reports:#?}"
        );
        tokio::time::sleep(HEARTBEAT).await;
    }
}

/// Waits until every gateway names the same coordinators for `groups`, all in `expected`.
async fn converge_coordinators(
    gateways: &[&Gateway],
    groups: &[String],
    expected: &[i32],
) -> Vec<i32> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let mut answers = Vec::new();
        for gateway in gateways {
            answers.push(coordinators(gateway, groups).await);
        }
        let agreed = answers.windows(2).all(|pair| pair[0] == pair[1])
            && answers[0].iter().all(|(node, _)| expected.contains(node));
        if agreed {
            return answers[0].iter().map(|(node, _)| *node).collect();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the gateways never agreed on the coordinators: {answers:?}"
        );
        tokio::time::sleep(HEARTBEAT).await;
    }
}

/// The `JoinGroup` error a gateway answers for a new member of `group`.
async fn join_error(gateway: &Gateway, group: &str) -> i16 {
    let mut client = client(gateway).await;
    let version = client.version(ApiKey::JoinGroup, 9).expect("JoinGroup");
    let request = JoinGroupRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(10_000)
        .with_member_id(StrBytes::new())
        .with_protocol_type(s("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(s("range"))
                .with_metadata(Bytes::new()),
        ]);
    client
        .send(&request, version)
        .await
        .expect("JoinGroup")
        .error_code
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

/// The Produce error a gateway answers for one record to `partition`.
async fn produce_error(gateway: &Gateway, partition: i32) -> i16 {
    produce_error_at(gateway, partition, 9, None).await
}

/// The Produce error at `version`, naming the topic by `topic_id` (v13 and later) or by name.
async fn produce_error_at(
    gateway: &Gateway,
    partition: i32,
    version: i16,
    topic_id: Option<Uuid>,
) -> i16 {
    let mut client = client(gateway).await;
    let version = client.version(ApiKey::Produce, version).expect("Produce");
    let topic = match topic_id {
        Some(id) => TopicProduceData::default().with_topic_id(id),
        None => TopicProduceData::default().with_name(TopicName(s(TOPIC))),
    };
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![topic.with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(one_record())),
                ])]);
    let response = client.send(&request, version).await.expect("Produce");
    response.responses[0].partition_responses[0].error_code
}

async fn start_three(server: &TestServer) -> [Gateway; 3] {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge");
    bridge
        .ensure_stream_and_topic(TOPIC, u32::try_from(PARTITIONS).expect("partitions"))
        .await
        .expect("create the topic");
    bridge.close().await.expect("close the setup bridge");
    let one = Gateway::start(server, 1).await;
    let two = Gateway::start(server, 2).await;
    let three = Gateway::start(server, 3).await;
    [one, two, three]
}

fn groups() -> Vec<String> {
    (0..GROUPS)
        .map(|group| format!("kw5-group-{group}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_three_gateways_should_agree_on_brokers_coordinators_and_leaders() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let gateways = start_three(&server).await;
    let all: Vec<&Gateway> = gateways.iter().collect();

    let (reported, _) = converge(&all, &[1, 2, 3]).await;
    for gateway in &gateways {
        assert_eq!(
            reported.brokers.get(&gateway.node_id),
            Some(&gateway.addr.port()),
            "node {} is advertised at its own listener",
            gateway.node_id
        );
    }
    assert_eq!(
        reported.controller, 1,
        "the lowest node id is the controller"
    );
    let leaders: BTreeSet<i32> = reported.leaders.iter().copied().collect();
    assert!(
        leaders.len() >= 2,
        "{PARTITIONS} partitions spread over the gateways: {:?}",
        reported.leaders
    );

    let groups = groups();
    let owners = converge_coordinators(&all, &groups, &[1, 2, 3]).await;
    let owner_set: BTreeSet<i32> = owners.iter().copied().collect();
    assert_eq!(
        owner_set,
        BTreeSet::from([1, 2, 3]),
        "{GROUPS} groups spread over every gateway: {owners:?}"
    );

    // DescribeCluster lists the same brokers.
    let mut client = client(&gateways[1]).await;
    let version = client
        .version(ApiKey::DescribeCluster, 1)
        .expect("DescribeCluster");
    let described = client
        .send(&DescribeClusterRequest::default(), version)
        .await
        .expect("DescribeCluster");
    let described: Vec<i32> = described.brokers.iter().map(|b| b.broker_id.0).collect();
    assert_eq!(described, vec![1, 2, 3]);

    // Only the coordinator takes a member; the others send it to the coordinator.
    for (group, owner) in groups.iter().zip(&owners).take(6) {
        for gateway in &gateways {
            let error = join_error(gateway, group).await;
            if gateway.node_id == *owner {
                assert_eq!(
                    error, ERROR_MEMBER_ID_REQUIRED,
                    "{group} at its coordinator"
                );
            } else {
                assert_eq!(
                    error, ERROR_NOT_COORDINATOR,
                    "{group} at node {}",
                    gateway.node_id
                );
            }
        }
    }

    // Only the leader writes a partition, whether the topic is named or, from v13, given by id.
    let topic_id = topic_id(&gateways[0]).await;
    for (partition, leader) in (0..PARTITIONS).zip(&reported.leaders) {
        for gateway in &gateways {
            for (version, id) in [(9, None), (13, Some(topic_id))] {
                let error = produce_error_at(gateway, partition, version, id).await;
                if gateway.node_id == *leader {
                    assert_eq!(
                        error, ERROR_NONE,
                        "partition {partition} at its leader, Produce v{version}"
                    );
                } else {
                    assert_eq!(
                        error, ERROR_NOT_LEADER_OR_FOLLOWER,
                        "partition {partition} at node {}, Produce v{version}",
                        gateway.node_id
                    );
                }
            }
        }
    }

    for gateway in gateways {
        gateway.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_gateway_that_leaves_should_move_only_its_groups_and_partitions() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let [one, two, three] = start_three(&server).await;
    let groups = groups();

    let (before, _) = converge(&[&one, &two, &three], &[1, 2, 3]).await;
    let owners_before = converge_coordinators(&[&one, &two, &three], &groups, &[1, 2, 3]).await;

    // A goodbye moves its share at once, well inside the lease.
    three.stop().await;
    let started = tokio::time::Instant::now();
    let (after, moved) = converge(&[&one, &two], &[1, 2]).await;
    let owners_after = converge_coordinators(&[&one, &two], &groups, &[1, 2]).await;
    assert!(
        moved - started < LEASE + HEARTBEAT * 4,
        "a goodbye is heard before the lease runs out: {:?}",
        moved - started
    );
    for (before, after) in owners_before.iter().zip(&owners_after) {
        if *before != 3 {
            assert_eq!(
                before, after,
                "a group of a gateway that stays does not move"
            );
        }
    }
    for (before, after) in before.leaders.iter().zip(&after.leaders) {
        if *before != 3 {
            assert_eq!(
                before, after,
                "a partition of a gateway that stays does not move"
            );
        }
    }

    // A crash moves its share once its lease runs out.
    two.crash();
    let (after_crash, _) = converge(&[&one], &[1]).await;
    assert!(after_crash.leaders.iter().all(|leader| *leader == 1));
    let owners = converge_coordinators(&[&one], &groups, &[1]).await;
    assert!(owners.iter().all(|owner| *owner == 1));
    assert_eq!(join_error(&one, &groups[0]).await, ERROR_MEMBER_ID_REQUIRED);

    // A gateway that comes back takes its share again once it has been heard for a join delay.
    let back = Gateway::start(&server, 2).await;
    converge(&[&one, &back], &[1, 2]).await;
    let owners_back = converge_coordinators(&[&one, &back], &groups, &[1, 2]).await;
    assert!(
        owners_back.contains(&2),
        "node 2 owns groups again: {owners_back:?}"
    );

    back.stop().await;
    one.stop().await;
}
