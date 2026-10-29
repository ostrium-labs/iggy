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

//! Transactional ids scoped per principal (KW-TXP, under the KW-10 ruling) against a real
//! `iggy-server`.
//!
//! Users log in to a gateway with transactions on over SASL/PLAIN. Two writers that use the same
//! transactional id run two transactions: neither fences, ends, describes or lists the other's.
//! A principal that may write no topic is refused every transaction API with
//! `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53), as Apache Kafka 4.3.1 refuses a principal
//! without `WRITE` or `DESCRIBE` on the `TRANSACTIONAL_ID`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{
    GlobalPermissions, Identifier, Permissions, StreamClient, StreamPermissions, UserClient,
    UserCreateOptions, UserStatus,
};
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, ApiKey, DescribeTransactionsRequest,
    EndTxnRequest, FindCoordinatorRequest, GroupId, InitProducerIdRequest, ListTransactionsRequest,
    ProduceRequest, ProducerId, TopicName, TransactionalId, TxnOffsetCommitRequest,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, Record, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType,
};
use serial_test::serial;
use tokio::sync::broadcast;

use iggy_gateway_kafka::auth::IggyAuthenticator;
use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::transactions::TransactionConfig;
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
const PASSWORD: &str = "secret-123";
const NONE: i16 = 0;
const TXN_ID_DENIED: i16 = 53;
const TXN_ID_NOT_FOUND: i16 = 105;
/// `COORDINATOR_LOAD_IN_PROGRESS`, `NOT_COORDINATOR` and `CONCURRENT_TRANSACTIONS`: retried.
const RETRIABLE: [i16; 3] = [14, 16, 51];

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

struct Stack {
    address: String,
    _shutdown: broadcast::Sender<()>,
    _server: TestServer,
    _data_dir: tempfile::TempDir,
}

/// One Iggy server with `orders` in the default stream; `alice` and `bob` may write it,
/// `reader` may only poll it, `nobody` holds nothing. A gateway with transactions on and a
/// `SASL_PLAINTEXT` listener in front.
async fn stack() -> Stack {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let bridge = Arc::new(
        IggyBridge::connect(server.test_config())
            .await
            .expect("bridge connects"),
    );
    bridge
        .ensure_stream_and_topic(ORDERS, 1)
        .await
        .expect("create the topic");
    let raw = iggy_server::raw_client(&server).await;
    let stream = raw
        .get_stream(&Identifier::named(STREAM).expect("stream id"))
        .await
        .expect("read the stream")
        .expect("the stream exists");
    let stream_key = stream.id as usize;
    let per_stream = |configure: &dyn Fn(&mut StreamPermissions)| {
        let mut permissions = StreamPermissions::default();
        configure(&mut permissions);
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(BTreeMap::from([(stream_key, permissions)])),
        })
    };
    let writer = |stream: &mut StreamPermissions| stream.send_messages = true;
    let users = [
        ("alice", per_stream(&writer)),
        ("bob", per_stream(&writer)),
        (
            "reader",
            per_stream(&|stream: &mut StreamPermissions| stream.poll_messages = true),
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
    let config = GatewayConfig {
        transactions: Some(TransactionConfig::default()),
        ..GatewayConfig::default()
    };
    let gateway = KafkaGateway::new(config)
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

/// `InitProducerId` for `transactional_id`: the code, producer id and epoch, retried while the
/// coordinator loads.
async fn init(client: &mut WireClient, transactional_id: &str) -> (i16, i64, i16) {
    let version = client.version(ApiKey::InitProducerId, 4).expect("served");
    let request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(s(transactional_id))))
        .with_transaction_timeout_ms(60_000)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    for _ in 0..200 {
        let response = client
            .send(&request, version)
            .await
            .expect("InitProducerId");
        if !RETRIABLE.contains(&response.error_code) {
            return (
                response.error_code,
                response.producer_id.0,
                response.producer_epoch,
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("InitProducerId kept answering retriable codes")
}

/// `AddPartitionsToTxn` of `orders` partition 0, retried while retriable.
async fn add(client: &mut WireClient, transactional_id: &str, (id, epoch): (i64, i16)) -> i16 {
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
                .with_name(TopicName(s(ORDERS)))
                .with_partitions(vec![0]),
        ]);
    for _ in 0..200 {
        let response = client
            .send(&request, version)
            .await
            .expect("AddPartitionsToTxn");
        let code =
            response.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code;
        if !RETRIABLE.contains(&code) {
            return code;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("AddPartitionsToTxn kept answering retriable codes")
}

async fn end(
    client: &mut WireClient,
    transactional_id: &str,
    (id, epoch): (i64, i16),
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

/// A transactional batch of one record.
fn batch(producer_id: i64, epoch: i16, sequence: i32) -> Bytes {
    let record = Record {
        transactional: true,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id,
        producer_epoch: epoch,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence,
        timestamp: 1_700_000_000_000,
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::from_static(b"v")),
        headers: kafka_protocol::indexmap::IndexMap::default(),
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
    .expect("encode the batch");
    buf.freeze()
}

async fn produce(client: &mut WireClient, transactional_id: &str, records: Bytes) -> i16 {
    let version = client.version(ApiKey::Produce, 9).expect("served");
    let request = ProduceRequest::default()
        .with_transactional_id(Some(TransactionalId(s(transactional_id))))
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(s(ORDERS)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(records)),
                ]),
        ]);
    let response = client.send(&request, version).await.expect("Produce");
    response.responses[0].partition_responses[0].error_code
}

/// `DescribeTransactions` of `transactional_id`: the code and the producer id.
async fn describe(client: &mut WireClient, transactional_id: &str) -> (i16, i64) {
    let version = client
        .version(ApiKey::DescribeTransactions, 0)
        .expect("served");
    let request = DescribeTransactionsRequest::default()
        .with_transactional_ids(vec![TransactionalId(s(transactional_id))]);
    let response = client
        .send(&request, version)
        .await
        .expect("DescribeTransactions");
    let state = &response.transaction_states[0];
    assert_eq!(
        state.transactional_id.as_str(),
        transactional_id,
        "the id is answered as the caller named it"
    );
    (state.error_code, state.producer_id.0)
}

/// `ListTransactions`: the code and every `(transactional id, producer id)` listed.
async fn list(client: &mut WireClient) -> (i16, Vec<(String, i64)>) {
    let version = client.version(ApiKey::ListTransactions, 1).expect("served");
    let request = ListTransactionsRequest::default().with_duration_filter(-1);
    let response = client
        .send(&request, version)
        .await
        .expect("ListTransactions");
    let mut listed: Vec<(String, i64)> = response
        .transaction_states
        .iter()
        .map(|state| (state.transactional_id.to_string(), state.producer_id.0))
        .collect();
    listed.sort();
    (response.error_code, listed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_two_principals_a_transactional_id_of_the_same_name_should_be_two_transactions() {
    let stack = stack().await;
    let mut alice = login(&stack, "alice").await;
    let mut bob = login(&stack, "bob").await;

    let (code, alice_id, alice_epoch) = init(&mut alice, "orders-tx").await;
    assert_eq!(code, NONE);
    assert_eq!(
        add(&mut alice, "orders-tx", (alice_id, alice_epoch)).await,
        NONE
    );
    assert_eq!(
        produce(&mut alice, "orders-tx", batch(alice_id, alice_epoch, 0)).await,
        NONE
    );

    // Bob's `orders-tx` is his own: a fresh producer id, not a bump of Alice's epoch, so Alice is
    // not fenced. In Kafka the same id would have fenced her.
    let (code, bob_id, bob_epoch) = init(&mut bob, "orders-tx").await;
    assert_eq!(code, NONE);
    assert_ne!(bob_id, alice_id, "Bob does not take over Alice's producer");
    assert_eq!(bob_epoch, 0);

    // Bob cannot end Alice's transaction, under her name or by spelling her scoped key.
    assert_ne!(
        end(&mut bob, "orders-tx", (alice_id, alice_epoch), false).await,
        NONE
    );
    assert_ne!(
        end(
            &mut bob,
            "5:alice/orders-tx",
            (alice_id, alice_epoch),
            false
        )
        .await,
        NONE
    );

    // Each describes only its own `orders-tx`.
    assert_eq!(describe(&mut alice, "orders-tx").await, (NONE, alice_id));
    assert_eq!(describe(&mut bob, "orders-tx").await, (NONE, bob_id));
    assert_eq!(
        describe(&mut bob, "5:alice/orders-tx").await.0,
        TXN_ID_NOT_FOUND,
        "another principal's key is just an unknown id of Bob's own"
    );

    // Each lists only its own transactions, under the names it gave them.
    assert_eq!(
        list(&mut alice).await,
        (NONE, vec![("orders-tx".to_string(), alice_id)])
    );
    assert_eq!(
        list(&mut bob).await,
        (NONE, vec![("orders-tx".to_string(), bob_id)])
    );

    // Alice's transaction was untouched by all of Bob's calls: it still commits.
    assert_eq!(
        end(&mut alice, "orders-tx", (alice_id, alice_epoch), true).await,
        NONE
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_principal_that_may_write_no_topic_every_transaction_api_should_answer_53() {
    let stack = stack().await;
    let mut alice = login(&stack, "alice").await;
    let (code, alice_id, alice_epoch) = init(&mut alice, "orders-tx").await;
    assert_eq!(code, NONE);

    for user in ["reader", "nobody"] {
        let mut client = login(&stack, user).await;

        let version = client
            .version(ApiKey::FindCoordinator, 3)
            .expect("served")
            .min(3);
        let found = client
            .send(
                &FindCoordinatorRequest::default()
                    .with_key(s("orders-tx"))
                    .with_key_type(1),
                version,
            )
            .await
            .expect("FindCoordinator");
        assert_eq!(found.error_code, TXN_ID_DENIED, "{user}: FindCoordinator");

        assert_eq!(
            init(&mut client, "orders-tx").await.0,
            TXN_ID_DENIED,
            "{user}: InitProducerId"
        );
        assert_eq!(
            add(&mut client, "orders-tx", (alice_id, alice_epoch)).await,
            TXN_ID_DENIED,
            "{user}: AddPartitionsToTxn"
        );
        assert_eq!(
            produce(&mut client, "orders-tx", batch(alice_id, alice_epoch, 0)).await,
            TXN_ID_DENIED,
            "{user}: Produce"
        );

        let version = client.version(ApiKey::AddOffsetsToTxn, 3).expect("served");
        let added = client
            .send(
                &AddOffsetsToTxnRequest::default()
                    .with_transactional_id(TransactionalId(s("orders-tx")))
                    .with_producer_id(ProducerId(alice_id))
                    .with_producer_epoch(alice_epoch)
                    .with_group_id(GroupId(s("app"))),
                version,
            )
            .await
            .expect("AddOffsetsToTxn");
        assert_eq!(added.error_code, TXN_ID_DENIED, "{user}: AddOffsetsToTxn");

        let version = client.version(ApiKey::TxnOffsetCommit, 3).expect("served");
        let committed = client
            .send(
                &TxnOffsetCommitRequest::default()
                    .with_transactional_id(TransactionalId(s("orders-tx")))
                    .with_group_id(GroupId(s("app")))
                    .with_producer_id(ProducerId(alice_id))
                    .with_producer_epoch(alice_epoch)
                    .with_generation_id(-1)
                    .with_topics(vec![
                        TxnOffsetCommitRequestTopic::default()
                            .with_name(TopicName(s(ORDERS)))
                            .with_partitions(vec![
                                TxnOffsetCommitRequestPartition::default()
                                    .with_partition_index(0)
                                    .with_committed_offset(1)
                                    .with_committed_leader_epoch(-1),
                            ]),
                    ]),
                version,
            )
            .await
            .expect("TxnOffsetCommit");
        assert_eq!(
            committed.topics[0].partitions[0].error_code, TXN_ID_DENIED,
            "{user}: TxnOffsetCommit"
        );

        assert_eq!(
            end(&mut client, "orders-tx", (alice_id, alice_epoch), false).await,
            TXN_ID_DENIED,
            "{user}: EndTxn"
        );
        assert_eq!(
            describe(&mut client, "orders-tx").await.0,
            TXN_ID_DENIED,
            "{user}: DescribeTransactions"
        );
        assert_eq!(
            list(&mut client).await,
            (NONE, Vec::new()),
            "{user}: ListTransactions"
        );
    }

    // None of it touched Alice's transaction.
    assert_eq!(describe(&mut alice, "orders-tx").await, (NONE, alice_id));
    assert_eq!(
        add(&mut alice, "orders-tx", (alice_id, alice_epoch)).await,
        NONE
    );
    assert_eq!(
        end(&mut alice, "orders-tx", (alice_id, alice_epoch), false).await,
        NONE
    );
}
