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

//! KW-12: the metadata string of an offset commit, kept in the gateway's own Iggy topic and
//! replayed from it, against a real `iggy-server` process. A second store over the same server
//! stands for another gateway, or this one after a restart.

#[path = "common/iggy_server.rs"]
mod iggy_server;

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::{
    GroupId, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::{IggyBridge, IggyMetadataLog};
use iggy_gateway_kafka::commit_metadata::{CommitMetadata, MetadataLog};
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::limits::RequestLimits;
use iggy_gateway_kafka::protocol::api::{
    ConnectionState, ERROR_NONE, ERROR_OFFSET_METADATA_TOO_LARGE, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::{offset_commit, offset_fetch};

use iggy_server::TestServer;

const COMMIT_VERSION: i16 = 8;
const FETCH_VERSION: i16 = 7;
const TOPIC: &str = "orders";
const GROUP: &str = "streams-app";

/// A gateway's state over `server`, with a commit metadata store of its own, replayed as at
/// startup.
async fn gateway(server: &TestServer) -> GatewayState {
    let bridge = Arc::new(
        IggyBridge::connect(server.test_config())
            .await
            .expect("bridge should connect to a ready server"),
    );
    let log = Arc::new(IggyMetadataLog::new(Arc::clone(&bridge)));
    let store = Arc::new(CommitMetadata::new(log as Arc<dyn MetadataLog>));
    store.replay().await.expect("replay the metadata log");
    GatewayState::new(
        Some(bridge),
        8 * 1024 * 1024,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
    .with_commit_metadata(store)
}

async fn seed_topic(server: &TestServer) {
    IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect")
        .ensure_stream_and_topic(TOPIC, 2)
        .await
        .expect("seed the topic");
}

fn encode(message: &impl Encodable, version: i16) -> Bytes {
    let mut buf = BytesMut::new();
    message.encode(&mut buf, version).expect("encode");
    buf.freeze()
}

/// Commits `(partition, offset, metadata)` outside the group protocol; one code per partition.
async fn commit(state: &GatewayState, partitions: &[(i32, i64, Option<&str>)]) -> Vec<i16> {
    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_static_str(GROUP)))
        .with_generation_id_or_member_epoch(-1)
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(
                    partitions
                        .iter()
                        .map(|&(index, offset, metadata)| {
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(index)
                                .with_committed_offset(offset)
                                .with_committed_metadata(
                                    metadata.map(|text| StrBytes::from_string(text.to_string())),
                                )
                        })
                        .collect(),
                ),
        ]);
    let mut response = offset_commit::handle(
        state,
        &ConnectionState::default(),
        None,
        COMMIT_VERSION,
        encode(&request, COMMIT_VERSION),
    )
    .await
    .expect_response("OffsetCommit answers");
    let response = OffsetCommitResponse::decode(&mut response, COMMIT_VERSION).expect("decode");
    response.topics[0]
        .partitions
        .iter()
        .map(|partition| partition.error_code)
        .collect()
}

/// `(offset, metadata)` of partitions 0 and 1.
async fn fetch(state: &GatewayState) -> Vec<(i64, String)> {
    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId(StrBytes::from_static_str(GROUP)))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_indexes(vec![0, 1]),
        ]));
    let mut response = offset_fetch::handle(
        state,
        &ConnectionState::default(),
        None,
        FETCH_VERSION,
        encode(&request, FETCH_VERSION),
    )
    .await
    .expect_response("OffsetFetch answers");
    let response = OffsetFetchResponse::decode(&mut response, FETCH_VERSION).expect("decode");
    assert_eq!(response.error_code, ERROR_NONE);
    response.topics[0]
        .partitions
        .iter()
        .map(|partition| {
            assert_eq!(partition.error_code, ERROR_NONE);
            (
                partition.committed_offset,
                partition
                    .metadata
                    .as_ref()
                    .map_or_else(String::new, ToString::to_string),
            )
        })
        .collect()
}

#[tokio::test]
#[serial]
async fn given_a_commit_with_metadata_should_read_it_back_also_after_a_restart() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server).await;
    let first = gateway(&server).await;

    let codes = commit(&first, &[(0, 5, Some("task 0_1")), (1, 6, None)]).await;
    assert_eq!(codes, vec![ERROR_NONE, ERROR_NONE]);
    assert_eq!(
        fetch(&first).await,
        vec![(5, "task 0_1".to_string()), (6, String::new())]
    );

    // Another gateway, or this one restarted: it knows only what the log holds.
    drop(first);
    let restarted = gateway(&server).await;
    assert_eq!(
        fetch(&restarted).await,
        vec![(5, "task 0_1".to_string()), (6, String::new())]
    );

    // A commit without a string clears the old one; a string for the other partition comes
    // through the log to the first store too.
    let codes = commit(&restarted, &[(0, 9, Some("")), (1, 10, Some("task 0_2"))]).await;
    assert_eq!(codes, vec![ERROR_NONE, ERROR_NONE]);
    assert_eq!(
        fetch(&restarted).await,
        vec![(9, String::new()), (10, "task 0_2".to_string())]
    );
}

/// Two gateways over one Iggy cluster: a commit through one reads back through the other.
#[tokio::test]
#[serial]
async fn given_two_gateways_should_each_see_the_others_commit_metadata() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server).await;
    let one = gateway(&server).await;
    let other = gateway(&server).await;

    assert_eq!(
        commit(&one, &[(0, 3, Some("from one"))]).await,
        vec![ERROR_NONE]
    );
    assert_eq!(fetch(&other).await[0], (3, "from one".to_string()));
    assert_eq!(
        commit(&other, &[(0, 4, Some("from other"))]).await,
        vec![ERROR_NONE]
    );
    assert_eq!(fetch(&one).await[0], (4, "from other".to_string()));
}

/// `IGGY_KAFKA_OFFSET_METADATA_MAX_BYTES`: a longer string is refused on its partition and
/// stores nothing there; the other partition commits.
#[tokio::test]
#[serial]
async fn given_a_string_past_the_cap_should_refuse_only_its_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server).await;
    let state = gateway(&server).await.with_limits(RequestLimits {
        offset_metadata_max_bytes: 8,
        ..RequestLimits::default()
    });

    let codes = commit(
        &state,
        &[(0, 5, Some("123456789")), (1, 6, Some("12345678"))],
    )
    .await;
    assert_eq!(codes, vec![ERROR_OFFSET_METADATA_TOO_LARGE, ERROR_NONE]);
    assert_eq!(
        fetch(&state).await,
        vec![(-1, String::new()), (6, "12345678".to_string())]
    );
}
