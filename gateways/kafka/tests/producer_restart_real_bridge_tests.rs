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

//! An idempotent producer's retries across a gateway crash (KW-3 part 3, #4513).
//!
//! The gateway keeps producer state in memory, so a killed gateway loses it. The one that starts
//! next rebuilds it from the `kafka.p` marks in Iggy, and a retry of a batch that landed before
//! the crash is answered with its offset, not written again.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{Consumer, Identifier, MessageClient, PollingStrategy};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, InitProducerIdRequest, ProduceRequest, ProducerId, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, Record, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType,
};
use serial_test::serial;

use iggy_gateway_kafka::bridge::IggyBridge;

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

/// A batch of `values` from `(producer_id, epoch)`, its first record at `sequence`.
fn batch(producer_id: i64, epoch: i16, sequence: i32, values: &[&str]) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .zip(0_i32..)
        .map(|(value, index)| Record {
            transactional: false,
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

/// `(error, base_offset)` of one Produce to partition 0, retrying the retriable 6 a gateway that
/// just started can answer.
async fn produce(client: &mut WireClient, records: Bytes) -> (i16, i64) {
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(records)),
                ]),
        ]);
    let version = client
        .version(ApiKey::Produce, 9)
        .expect("Produce is served");
    for attempt in 0.. {
        let response = client.send(&request, version).await.expect("Produce");
        let answer = &response.responses[0].partition_responses[0];
        if answer.error_code != 6 || attempt == 40 {
            return (answer.error_code, answer.base_offset);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    unreachable!("the loop returns")
}

async fn producer_id(client: &mut WireClient) -> (i64, i16) {
    let version = client
        .version(ApiKey::InitProducerId, 4)
        .expect("InitProducerId is served");
    let request = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(-1)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    let response = client
        .send(&request, version)
        .await
        .expect("InitProducerId");
    assert_eq!(response.error_code, 0);
    (response.producer_id.0, response.producer_epoch)
}

async fn stored_values(server: &TestServer) -> Vec<Bytes> {
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
        .into_iter()
        .map(|message| message.payload)
        .collect()
}

#[tokio::test]
#[serial]
async fn given_a_gateway_killed_mid_retry_when_the_producer_retries_on_the_next_should_store_once()
{
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge")
        .ensure_stream_and_topic(TOPIC, 1)
        .await
        .expect("seed the topic");

    let gateway = TestGateway::spawn(&server).await;
    let mut client = WireClient::connect(&gateway.address)
        .await
        .expect("connect to the gateway");
    let (id, epoch) = producer_id(&mut client).await;
    let first = batch(id, epoch, 0, &["a", "b"]);
    let second = batch(id, epoch, 2, &["c"]);
    assert_eq!(produce(&mut client, first.clone()).await, (0, 0));
    assert_eq!(
        produce(&mut client, second.clone()).await,
        (0, 2),
        "lands, and its answer is lost: the producer will retry it"
    );

    gateway.crash();
    let gateway = TestGateway::spawn(&server).await;
    let mut client = WireClient::connect(&gateway.address)
        .await
        .expect("connect to the new gateway");

    assert_eq!(
        produce(&mut client, second).await,
        (0, 2),
        "the new gateway rebuilt the producer from the log: the retry gets its first offset"
    );
    assert_eq!(
        produce(&mut client, first).await,
        (0, 0),
        "an older batch of the last five is a duplicate too"
    );
    assert_eq!(
        produce(&mut client, batch(id, epoch, 5, &["f"])).await.0,
        45,
        "a gap after the restart is refused, as before it"
    );
    assert_eq!(
        produce(&mut client, batch(id, epoch, 3, &["d"])).await,
        (0, 3)
    );
    assert_eq!(
        stored_values(&server).await,
        ["a", "b", "c", "d"]
            .iter()
            .map(|value| Bytes::from_static(value.as_bytes()))
            .collect::<Vec<_>>(),
        "every record once, in order"
    );
    drop(gateway);
}
