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

//! The log start offset against a real `iggy-server` (apache/iggy#4525): `DeleteRecords`, and
//! what `ListOffsets` EARLIEST and Fetch report after it and after retention removed a segment.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{
    Identifier, IggyByteSize, IggyMessage, MessageClient, Partitioning, SegmentClient, TopicClient,
    TopicCreateOptions,
};
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{
    DeleteRecordsRequest, DeleteRecordsResponse, FetchRequest, FetchResponse, ListOffsetsRequest,
    ListOffsetsResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use serial_test::serial;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_DELETE_RECORDS, API_KEY_FETCH, API_KEY_LIST_OFFSETS, GatewayState,
    handle_request_bounded,
};

#[path = "common/iggy_server.rs"]
mod iggy_server;

use iggy_server::{TestServer, raw_client};

const STREAM: &str = "kafka";
const TOPIC: &str = "orders";
const MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
const DELETE_RECORDS_VERSION: i16 = 2;
const LIST_OFFSETS_VERSION: i16 = 7;
const FETCH_VERSION: i16 = 12;
const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

/// Spelled out rather than imported, so a changed constant in the handler fails here.
const ERROR_NONE: i16 = 0;
const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

async fn gateway(server: &TestServer) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    GatewayState::new(
        Some(Arc::new(bridge)),
        MAX_FRAME_SIZE,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
}

async fn gateway_with_topic(server: &TestServer) -> GatewayState {
    let state = gateway(server).await;
    state
        .bridge
        .as_ref()
        .expect("a bridge")
        .ensure_stream_and_topic(TOPIC, 1)
        .await
        .expect("the topic must exist");
    state
}

async fn send(server: &TestServer, messages: &mut [IggyMessage]) {
    raw_client(server)
        .await
        .send_messages(
            &Identifier::named(STREAM).expect("valid stream name"),
            &Identifier::named(TOPIC).expect("valid topic name"),
            &Partitioning::partition_id(0),
            messages,
        )
        .await
        .expect("send");
}

async fn send_values(server: &TestServer, count: usize) {
    let mut messages: Vec<IggyMessage> = (0..count)
        .map(|i| IggyMessage::from(format!("message-{i}")))
        .collect();
    send(server, &mut messages).await;
}

async fn call<Req: Encodable, Resp: Decodable>(
    state: &GatewayState,
    api_key: i16,
    version: i16,
    request: &Req,
) -> Resp {
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .expect("the request encodes at this version");
    let handled = handle_request_bounded(state, api_key, version, body.freeze());
    let mut response: Bytes = tokio::time::timeout(Duration::from_secs(20), handled)
        .await
        .expect("answers in time")
        .expect_response("always answers");
    let decoded = Resp::decode(&mut response, version).expect("a Kafka client decodes it");
    assert!(response.is_empty(), "no bytes after the response");
    decoded
}

/// `(error, low_watermark)` of one `DeleteRecords` on `partition` of the topic.
async fn delete_records(state: &GatewayState, partition: i32, offset: i64) -> (i16, i64) {
    let request = DeleteRecordsRequest::default()
        .with_timeout_ms(10_000)
        .with_topics(vec![
            DeleteRecordsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![
                    DeleteRecordsPartition::default()
                        .with_partition_index(partition)
                        .with_offset(offset),
                ]),
        ]);
    let response: DeleteRecordsResponse = call(
        state,
        API_KEY_DELETE_RECORDS,
        DELETE_RECORDS_VERSION,
        &request,
    )
    .await;
    let answer = &response.topics[0].partitions[0];
    assert_eq!(answer.partition_index, partition);
    (answer.error_code, answer.low_watermark)
}

/// `(error, offset)` of one `ListOffsets` lookup on partition 0.
async fn list_offset(state: &GatewayState, timestamp: i64) -> (i16, i64) {
    let request = ListOffsetsRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId(-1))
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_current_leader_epoch(-1)
                        .with_timestamp(timestamp),
                ]),
        ]);
    let response: ListOffsetsResponse =
        call(state, API_KEY_LIST_OFFSETS, LIST_OFFSETS_VERSION, &request).await;
    let answer = &response.topics[0].partitions[0];
    (answer.error_code, answer.offset)
}

/// Partition 0's Fetch answer from `offset`.
async fn fetch(state: &GatewayState, offset: i64) -> PartitionData {
    let request = FetchRequest::default()
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(52_428_800)
        .with_session_epoch(0)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(offset)
                        .with_partition_max_bytes(8 * 1024 * 1024),
                ]),
        ]);
    let response: FetchResponse = call(state, API_KEY_FETCH, FETCH_VERSION, &request).await;
    response.responses[0].partitions[0].clone()
}

fn first_offset(partition: &PartitionData) -> Option<i64> {
    let mut bytes = partition.records.clone().unwrap_or_default();
    RecordBatchDecoder::decode_all(&mut bytes)
        .expect("only complete batches")
        .into_iter()
        .flat_map(|set| set.records)
        .map(|record| record.offset)
        .next()
}

#[tokio::test]
#[serial]
async fn given_delete_records_the_start_offset_should_move_and_reads_below_it_should_be_out_of_range()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server).await;
    send_values(&server, 5).await;

    assert_eq!(list_offset(&state, EARLIEST).await, (ERROR_NONE, 0));
    let before = fetch(&state, 0).await;
    assert_eq!(before.error_code, ERROR_NONE);
    assert_eq!(before.log_start_offset, 0);

    assert_eq!(delete_records(&state, 0, 2).await, (ERROR_NONE, 2));
    assert_eq!(list_offset(&state, EARLIEST).await, (ERROR_NONE, 2));
    assert_eq!(list_offset(&state, LATEST).await, (ERROR_NONE, 5));
    // A time before every record answers the first one a client may read.
    assert_eq!(list_offset(&state, 0).await, (ERROR_NONE, 2));

    let below = fetch(&state, 0).await;
    assert_eq!(below.error_code, ERROR_OFFSET_OUT_OF_RANGE);
    let at_start = fetch(&state, 2).await;
    assert_eq!(at_start.error_code, ERROR_NONE);
    assert_eq!(first_offset(&at_start), Some(2));
    assert_eq!(at_start.log_start_offset, 2);
}

#[tokio::test]
#[serial]
async fn given_delete_records_it_should_answer_as_kafka_does_for_each_offset() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server).await;
    send_values(&server, 5).await;

    assert_eq!(delete_records(&state, 0, 3).await, (ERROR_NONE, 3));
    // Below the start offset: nothing moves.
    assert_eq!(delete_records(&state, 0, 1).await, (ERROR_NONE, 3));
    assert_eq!(
        delete_records(&state, 0, 6).await,
        (ERROR_OFFSET_OUT_OF_RANGE, -1)
    );
    assert_eq!(
        delete_records(&state, 0, -7).await,
        (ERROR_OFFSET_OUT_OF_RANGE, -1)
    );
    assert_eq!(
        delete_records(&state, 9, 0).await,
        (ERROR_UNKNOWN_TOPIC_OR_PARTITION, -1)
    );
    // -1 is the high watermark.
    assert_eq!(delete_records(&state, 0, -1).await, (ERROR_NONE, 5));
    assert_eq!(list_offset(&state, EARLIEST).await, (ERROR_NONE, 5));
    assert_eq!(list_offset(&state, 0).await, (ERROR_NONE, -1));

    send_values(&server, 2).await;
    let after = fetch(&state, 5).await;
    assert_eq!(after.error_code, ERROR_NONE);
    assert_eq!(first_offset(&after), Some(5));
}

#[tokio::test]
#[serial]
async fn given_delete_records_on_one_gateway_another_should_see_the_start_offset() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let first = gateway_with_topic(&server).await;
    send_values(&server, 5).await;
    assert_eq!(delete_records(&first, 0, 4).await, (ERROR_NONE, 4));

    // A gateway that starts later, or another one, reads the watermark from Iggy.
    let restarted = gateway(&server).await;
    assert_eq!(list_offset(&restarted, EARLIEST).await, (ERROR_NONE, 4));
    assert_eq!(
        fetch(&restarted, 3).await.error_code,
        ERROR_OFFSET_OUT_OF_RANGE
    );
}

#[tokio::test]
#[serial]
async fn given_a_topic_created_again_its_old_start_offset_should_not_count() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server).await;
    send_values(&server, 5).await;
    assert_eq!(delete_records(&state, 0, 4).await, (ERROR_NONE, 4));

    let bridge = state.bridge.as_ref().expect("a bridge");
    bridge
        .delete_kafka_topic(TOPIC)
        .await
        .expect("delete the topic");
    bridge
        .ensure_stream_and_topic(TOPIC, 1)
        .await
        .expect("create it again");
    send_values(&server, 2).await;

    assert_eq!(list_offset(&state, EARLIEST).await, (ERROR_NONE, 0));
    let fetched = fetch(&state, 0).await;
    assert_eq!(fetched.error_code, ERROR_NONE);
    assert_eq!(first_offset(&fetched), Some(0));
}

/// About 300 KB a message, so four batches seal segments of the 1 MiB minimum.
fn large(index: usize) -> IggyMessage {
    IggyMessage::from(format!("{index:06}-{}", "x".repeat(300 * 1024)))
}

#[tokio::test]
#[serial]
async fn given_retention_removed_a_segment_earliest_and_fetch_should_report_the_first_kept_offset()
{
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway(&server).await;
    let bridge = state.bridge.as_ref().expect("a bridge");
    // Creates the stream. The topic under test then gets small segments of its own.
    bridge
        .ensure_stream_and_topic("other", 1)
        .await
        .expect("create the stream");
    let raw = raw_client(&server).await;
    raw.create_topic(
        &Identifier::named(STREAM).expect("valid stream name"),
        TOPIC,
        &TopicCreateOptions {
            partitions_count: Some(1),
            segment_size: Some(IggyByteSize::from(1024 * 1024)),
            ..TopicCreateOptions::default()
        },
    )
    .await
    .expect("create the topic with 1 MiB segments");
    for batch in 0..4 {
        let mut messages: Vec<IggyMessage> = (0..4).map(|i| large(batch * 4 + i)).collect();
        send(&server, &mut messages).await;
    }
    assert_eq!(list_offset(&state, EARLIEST).await, (ERROR_NONE, 0));

    // What retention does to the oldest sealed segment.
    raw.delete_segments(
        &Identifier::named(STREAM).expect("valid stream name"),
        &Identifier::named(TOPIC).expect("valid topic name"),
        0,
        1,
    )
    .await
    .expect("delete the oldest segment");
    // Each replica removes the segment when it applies the commit.
    let give_up = Instant::now() + Duration::from_secs(20);
    let start = loop {
        let (error, start) = list_offset(&state, EARLIEST).await;
        assert_eq!(error, ERROR_NONE);
        if start > 0 || Instant::now() >= give_up {
            break start;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(start > 0, "EARLIEST moves past the removed segment");
    assert!(start < 16, "later segments stay: {start}");

    let below = fetch(&state, 0).await;
    assert_eq!(below.error_code, ERROR_OFFSET_OUT_OF_RANGE);
    let at_start = fetch(&state, start).await;
    assert_eq!(at_start.error_code, ERROR_NONE);
    assert_eq!(first_offset(&at_start), Some(start));
    assert_eq!(at_start.log_start_offset, start);
}
