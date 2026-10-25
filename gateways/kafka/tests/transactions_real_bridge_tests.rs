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

//! Kafka transactions against a real `iggy-server`, through the gateway binary (KW-T).
//!
//! The coordinator's log, its markers and its recovery are Iggy writes and reads, so these run
//! the gateway process with `IGGY_KAFKA_TRANSACTIONS_ENABLED=true` and kill it where a crash
//! matters.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{Consumer, Identifier, IggyMessage, MessageClient, PollingStrategy};
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, ApiKey, BrokerId, EndTxnRequest, FetchRequest,
    InitProducerIdRequest, ProduceRequest, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, Record, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};
use serial_test::serial;

use iggy_gateway_kafka::bridge::{IggyBridge, IggyTxnLog};
use iggy_gateway_kafka::records::{ControlType, control_of};
use iggy_gateway_kafka::transactions::log::{LOG_PARTITIONS, partition_for};
use iggy_gateway_kafka::transactions::metadata::PartitionStart;
use iggy_gateway_kafka::transactions::{LogRecord, TxnLog, TxnMetadata, TxnState};

#[path = "common/gateway_process.rs"]
mod gateway_process;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/oracle.rs"]
mod oracle;

use gateway_process::TestGateway;
use iggy_server::{TestServer, raw_client};
use oracle::WireClient;

const STREAM: &str = "kafka";
const TOPIC: &str = "orders";

const TXN_ON: &[(&str, &str)] = &[("IGGY_KAFKA_TRANSACTIONS_ENABLED", "true")];

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// A transactional batch of `values`, its first record at `sequence`.
fn batch(producer_id: i64, epoch: i16, sequence: i32, values: &[&str]) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .zip(0_i32..)
        .map(|(value, index)| Record {
            transactional: true,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id,
            producer_epoch: epoch,
            timestamp_type: TimestampType::Creation,
            offset: i64::from(index),
            sequence: sequence + index,
            timestamp: 1_700_000_000_000 + i64::from(index),
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: kafka_protocol::indexmap::IndexMap::default(),
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
    .expect("encode the batch");
    buf.freeze()
}

/// `InitProducerId` for `transactional_id`, retried while the coordinator loads or aborts.
async fn init(client: &mut WireClient, transactional_id: &str, timeout_ms: i32) -> (i64, i16) {
    let version = client.version(ApiKey::InitProducerId, 4).expect("served");
    let request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(s(transactional_id))))
        .with_transaction_timeout_ms(timeout_ms)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    for _ in 0..200 {
        let response = client
            .send(&request, version)
            .await
            .expect("InitProducerId");
        match response.error_code {
            0 => return (response.producer_id.0, response.producer_epoch),
            14 | 15 | 51 => tokio::time::sleep(Duration::from_millis(100)).await,
            other => panic!("InitProducerId answered {other}"),
        }
    }
    panic!("InitProducerId kept answering retriable codes")
}

async fn add(client: &mut WireClient, transactional_id: &str, id: i64, epoch: i16) -> i16 {
    let version = client
        .version(ApiKey::AddPartitionsToTxn, 3)
        .expect("served")
        .min(3);
    let request = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(TransactionalId(s(transactional_id)))
        .with_v3_and_below_producer_id(ProducerId(id))
        .with_v3_and_below_producer_epoch(epoch)
        .with_v3_and_below_topics(vec![
            AddPartitionsToTxnTopic::default()
                .with_name(TopicName(s(TOPIC)))
                .with_partitions(vec![0]),
        ]);
    for _ in 0..200 {
        let response = client
            .send(&request, version)
            .await
            .expect("AddPartitionsToTxn");
        let code =
            response.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code;
        if code != 51 {
            return code;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    51
}

async fn end(
    client: &mut WireClient,
    transactional_id: &str,
    id: i64,
    epoch: i16,
    commit: bool,
) -> i16 {
    let version = client.version(ApiKey::EndTxn, 3).expect("served").min(4);
    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(s(transactional_id)))
        .with_producer_id(ProducerId(id))
        .with_producer_epoch(epoch)
        .with_committed(commit);
    client
        .send(&request, version)
        .await
        .expect("EndTxn")
        .error_code
}

async fn produce(client: &mut WireClient, transactional_id: &str, records: Bytes) -> (i16, i64) {
    let request = ProduceRequest::default()
        .with_transactional_id(Some(TransactionalId(s(transactional_id))))
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(s(TOPIC)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(records)),
                ]),
        ]);
    let version = client.version(ApiKey::Produce, 9).expect("served");
    let response = client.send(&request, version).await.expect("Produce");
    let answer = &response.responses[0].partition_responses[0];
    (answer.error_code, answer.base_offset)
}

/// Every message of partition 0, straight from Iggy.
async fn stored(server: &TestServer) -> Vec<IggyMessage> {
    raw_client(server)
        .await
        .poll_messages(
            &Identifier::named(STREAM).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            Some(0),
            &Consumer::new(Identifier::named("reader").expect("consumer name")),
            &PollingStrategy::offset(0),
            100,
            false,
        )
        .await
        .expect("poll the partition back")
        .messages
}

/// The control records stored on partition 0, in order, waiting until `count` are there.
async fn controls(server: &TestServer, count: usize) -> Vec<(ControlType, i64, i16)> {
    let mut found = Vec::new();
    for _ in 0..100 {
        found = stored(server)
            .await
            .iter()
            .filter_map(control_of)
            .map(|(control, mark)| (control, mark.producer_id, mark.epoch))
            .collect();
        if found.len() >= count {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    found
}

/// What a `read_uncommitted` Fetch from 0 serves: per record, `(control, transactional,
/// producer id, value)`.
async fn fetched(client: &mut WireClient) -> Vec<(bool, bool, i64, Option<Bytes>)> {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_isolation_level(0)
        .with_session_epoch(-1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(s(TOPIC)))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_current_leader_epoch(-1)
                        .with_last_fetched_epoch(-1)
                        .with_log_start_offset(-1)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ]);
    let version = client.version(ApiKey::Fetch, 12).expect("served");
    let response = client.send(&request, version).await.expect("Fetch");
    let mut bytes = response.responses[0].partitions[0]
        .records
        .clone()
        .unwrap_or_default();
    RecordBatchDecoder::decode_all(&mut bytes)
        .expect("the batches decode")
        .into_iter()
        .flat_map(|set| set.records)
        .map(|record| {
            (
                record.control,
                record.transactional,
                record.producer_id,
                record.value,
            )
        })
        .collect()
}

async fn seeded_server() -> (tempfile::TempDir, TestServer) {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge")
        .ensure_stream_and_topic(TOPIC, 1)
        .await
        .expect("seed the topic");
    (data_dir, server)
}

#[tokio::test]
#[serial]
async fn given_a_commit_and_an_abort_should_store_markers_serve_control_batches_and_survive_a_crash()
 {
    let (_dir, server) = seeded_server().await;
    let gateway = TestGateway::spawn_with(&server, TXN_ON).await;
    let mut client = WireClient::connect(&gateway.address)
        .await
        .expect("connect");

    let (id, epoch) = init(&mut client, "tx-a", 60_000).await;
    assert_eq!(epoch, 0);
    assert_eq!(add(&mut client, "tx-a", id, epoch).await, 0);
    assert_eq!(
        produce(&mut client, "tx-a", batch(id, epoch, 0, &["a", "b"])).await,
        (0, 0)
    );
    assert_eq!(end(&mut client, "tx-a", id, epoch, true).await, 0);
    assert_eq!(
        controls(&server, 1).await,
        [(ControlType::Commit, id, epoch)]
    );

    assert_eq!(add(&mut client, "tx-a", id, epoch).await, 0);
    assert_eq!(
        produce(&mut client, "tx-a", batch(id, epoch, 2, &["c"])).await,
        (0, 3)
    );
    assert_eq!(end(&mut client, "tx-a", id, epoch, false).await, 0);
    assert_eq!(
        controls(&server, 2).await,
        [
            (ControlType::Commit, id, epoch),
            (ControlType::Abort, id, epoch)
        ]
    );

    let served = fetched(&mut client).await;
    let shape: Vec<(bool, bool, i64)> = served
        .iter()
        .map(|(control, transactional, producer, _)| (*control, *transactional, *producer))
        .collect();
    assert_eq!(
        shape,
        [
            (false, true, id),
            (false, true, id),
            (true, true, id),
            (false, true, id),
            (true, true, id)
        ],
        "data as transactional batches of the producer, markers as control batches"
    );
    let values: Vec<Option<Bytes>> = served
        .into_iter()
        .filter(|(control, ..)| !control)
        .map(|(.., value)| value)
        .collect();
    assert_eq!(
        values,
        ["a", "b", "c"]
            .iter()
            .map(|value| Some(Bytes::from_static(value.as_bytes())))
            .collect::<Vec<_>>()
    );

    gateway.crash();
    let gateway = TestGateway::spawn_with(&server, TXN_ON).await;
    let mut client = WireClient::connect(&gateway.address)
        .await
        .expect("reconnect");
    let (same, bumped) = init(&mut client, "tx-a", 60_000).await;
    assert_eq!(
        (same, bumped),
        (id, 1),
        "the new gateway replayed the log: same producer id, next epoch"
    );
    assert_eq!(
        end(&mut client, "tx-a", id, epoch, true).await,
        90,
        "the old epoch is fenced"
    );
    drop(gateway);
}

#[tokio::test]
#[serial]
async fn given_a_decision_whose_markers_were_never_written_should_write_them_on_start() {
    let (_dir, server) = seeded_server().await;
    // A coordinator that died after its PrepareCommit record and before any marker.
    let bridge = Arc::new(
        IggyBridge::connect(server.test_config())
            .await
            .expect("bridge"),
    );
    let log = IggyTxnLog::new(Arc::clone(&bridge));
    let mut prepared = TxnMetadata::new("tx-r", 77, 60_000, 1_000);
    prepared.producer_epoch = 3;
    prepared.state = TxnState::PrepareCommit;
    prepared.start_ms = 1_000;
    prepared.partitions =
        BTreeMap::from([((TOPIC.to_string(), 0), PartitionStart { start: Some(0) })]);
    log.append(
        partition_for("tx-r", LOG_PARTITIONS),
        &LogRecord::of(&prepared, Vec::new()),
    )
    .await
    .expect("write the decision");

    let gateway = TestGateway::spawn_with(&server, TXN_ON).await;
    assert_eq!(
        controls(&server, 1).await,
        [(ControlType::Commit, 77, 3)],
        "the recovered decision's marker"
    );
    let mut state = TxnState::PrepareCommit;
    for _ in 0..100 {
        let records = log.read_all().await.expect("read the log");
        state = records
            .iter()
            .rev()
            .find(|record| record.transactional_id() == "tx-r")
            .map(|record| record.metadata().state)
            .expect("tx-r is in the log");
        if state == TxnState::CompleteCommit {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(state, TxnState::CompleteCommit);
    let mut client = WireClient::connect(&gateway.address)
        .await
        .expect("connect");
    assert_eq!(init(&mut client, "tx-r", 60_000).await, (77, 4));
    log.close().await;
    drop(gateway);
}

#[tokio::test]
#[serial]
async fn given_a_transaction_past_its_timeout_should_abort_it_and_fence_its_producer() {
    let (_dir, server) = seeded_server().await;
    let gateway = TestGateway::spawn_with(
        &server,
        &[
            ("IGGY_KAFKA_TRANSACTIONS_ENABLED", "true"),
            ("IGGY_KAFKA_TRANSACTION_ABORT_INTERVAL_MS", "200"),
        ],
    )
    .await;
    let mut client = WireClient::connect(&gateway.address)
        .await
        .expect("connect");
    let (id, epoch) = init(&mut client, "tx-slow", 1_000).await;
    assert_eq!(add(&mut client, "tx-slow", id, epoch).await, 0);
    assert_eq!(
        produce(&mut client, "tx-slow", batch(id, epoch, 0, &["late"]))
            .await
            .0,
        0
    );
    assert_eq!(
        controls(&server, 1).await,
        [(ControlType::Abort, id, epoch + 1)],
        "aborted at the bumped epoch"
    );
    assert_eq!(end(&mut client, "tx-slow", id, epoch, true).await, 90);
    assert_eq!(
        produce(&mut client, "tx-slow", batch(id, epoch, 1, &["zombie"]))
            .await
            .0,
        47,
        "a zombie's write is refused"
    );
    assert_eq!(
        stored(&server).await.len(),
        2,
        "the record and its abort marker only"
    );
    drop(gateway);
}
