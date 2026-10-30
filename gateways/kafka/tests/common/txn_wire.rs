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

//! A transactional producer and a `read_committed` reader on the wire, for the KW-T suites that
//! drive the gateway request by request.

#![allow(dead_code)]

use std::time::Duration;

use bytes::{Bytes, BytesMut};
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

use crate::oracle::WireClient;

pub fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// A transactional batch of `values`, its first record at `sequence`.
pub fn batch(producer_id: i64, epoch: i16, sequence: i32, values: &[&str]) -> Bytes {
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

/// `InitProducerId` for `transactional_id`, retried while the coordinator loads or aborts and
/// while the connection fails. Reconnects to `address` when it does.
pub async fn init(
    client: &mut WireClient,
    address: &str,
    transactional_id: &str,
    timeout_ms: i32,
) -> (i64, i16) {
    let request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(s(transactional_id))))
        .with_transaction_timeout_ms(timeout_ms)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    for _ in 0..600 {
        let version = client.version(ApiKey::InitProducerId, 4).expect("served");
        match client.send(&request, version).await {
            Ok(response) => match response.error_code {
                0 => return (response.producer_id.0, response.producer_epoch),
                14 | 15 | 51 => tokio::time::sleep(Duration::from_millis(100)).await,
                other => panic!("InitProducerId answered {other}"),
            },
            Err(_) => reconnect(client, address).await,
        }
    }
    panic!("InitProducerId kept failing")
}

/// A new connection to `address`, retried until the gateway listens again.
pub async fn reconnect(client: &mut WireClient, address: &str) {
    for _ in 0..600 {
        if let Ok(fresh) = WireClient::connect(address).await {
            *client = fresh;
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the gateway at {address} never came back")
}

/// `AddPartitionsToTxn` of `partitions` of `topic`, retried while `CONCURRENT_TRANSACTIONS`.
/// The first partition's code.
pub async fn add(
    client: &mut WireClient,
    transactional_id: &str,
    (id, epoch): (i64, i16),
    topic: &str,
    partitions: &[i32],
) -> i16 {
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
                .with_name(TopicName(s(topic)))
                .with_partitions(partitions.to_vec()),
        ]);
    for _ in 0..200 {
        let response = client
            .send(&request, version)
            .await
            .expect("AddPartitionsToTxn");
        let code =
            response.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code;
        if !matches!(code, 14 | 15 | 51) {
            return code;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    51
}

/// `EndTxn`'s first answer.
pub async fn end(
    client: &mut WireClient,
    transactional_id: &str,
    (id, epoch): (i64, i16),
    commit: bool,
) -> Result<i16, String> {
    let version = client.version(ApiKey::EndTxn, 3).expect("served").min(4);
    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(s(transactional_id)))
        .with_producer_id(ProducerId(id))
        .with_producer_epoch(epoch)
        .with_committed(commit);
    Ok(client.send(&request, version).await?.error_code)
}

/// A transactional Produce of `records` to `partition` of `topic`. `(error, base_offset)`.
pub async fn produce(
    client: &mut WireClient,
    transactional_id: &str,
    topic: &str,
    partition: i32,
    records: Bytes,
) -> (i16, i64) {
    let request = ProduceRequest::default()
        .with_transactional_id(Some(TransactionalId(s(transactional_id))))
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(s(topic)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(records)),
                ]),
        ]);
    let version = client.version(ApiKey::Produce, 9).expect("served");
    let response = client.send(&request, version).await.expect("Produce");
    let answer = &response.responses[0].partition_responses[0];
    (answer.error_code, answer.base_offset)
}

/// What a `read_committed` Java consumer keeps of `partition` of `topic` from offset 0, by the
/// client's own rules, with the last stable offset and the high watermark.
pub async fn read_committed(
    client: &mut WireClient,
    topic: &str,
    partition: i32,
) -> Result<(Vec<String>, i64, i64), String> {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_isolation_level(1)
        .with_session_epoch(-1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(s(topic)))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(partition)
                        .with_fetch_offset(0)
                        .with_current_leader_epoch(-1)
                        .with_last_fetched_epoch(-1)
                        .with_log_start_offset(-1)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ]);
    let version = client.version(ApiKey::Fetch, 12).expect("served");
    let response = client.send(&request, version).await?;
    let answer = response.responses[0].partitions[0].clone();
    let mut aborted: Vec<(i64, i64)> = answer
        .aborted_transactions
        .iter()
        .flatten()
        .map(|txn| (txn.first_offset, txn.producer_id.0))
        .collect();
    aborted.sort_unstable();
    let mut aborted = aborted.into_iter().peekable();
    let mut aborted_producers = std::collections::HashSet::new();
    let mut values = Vec::new();
    let mut bytes = answer.records.clone().unwrap_or_default();
    for set in RecordBatchDecoder::decode_all(&mut bytes).map_err(|error| error.to_string())? {
        let Some(first) = set.records.first() else {
            continue;
        };
        let last = set
            .records
            .last()
            .map_or(first.offset, |record| record.offset);
        if first.producer_id >= 0 {
            while let Some((_, producer)) = aborted.next_if(|(offset, _)| *offset <= last) {
                aborted_producers.insert(producer);
            }
            if first.control {
                if first.key.as_ref().and_then(|key| key.get(2..4)) == Some(&[0, 0][..]) {
                    aborted_producers.remove(&first.producer_id);
                }
                continue;
            }
            if first.transactional && aborted_producers.contains(&first.producer_id) {
                continue;
            }
        }
        values.extend(
            set.records
                .iter()
                .filter(|record| !record.control)
                .map(|record| {
                    record.value.as_ref().map_or_else(String::new, |value| {
                        String::from_utf8_lossy(value).into_owned()
                    })
                }),
        );
    }
    Ok((values, answer.last_stable_offset, answer.high_watermark))
}

/// Polls `read_committed` until `ready` holds or 30 s pass; the last read.
pub async fn read_until(
    client: &mut WireClient,
    address: &str,
    topic: &str,
    partition: i32,
    ready: impl Fn(&[String], i64, i64) -> bool,
) -> (Vec<String>, i64, i64) {
    let mut last = (Vec::new(), -1, -1);
    for _ in 0..300 {
        match read_committed(client, topic, partition).await {
            Ok(read) => {
                if ready(&read.0, read.1, read.2) {
                    return read;
                }
                last = read;
            }
            Err(_) => reconnect(client, address).await,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    last
}
