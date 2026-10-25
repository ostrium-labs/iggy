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

//! KW-T scenarios: Kafka's transactional handshake on the wire, against the gateway and every
//! oracle. The plan's test item 1 (handshake and error codes) and the wire part of item 2
//! (fencing).

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, ApiKey, EndTxnRequest, FindCoordinatorRequest,
    InitProducerIdRequest, ProduceRequest, ProducerId, TransactionalId,
};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, Record, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};

use super::{
    Context, Facts, TIMEOUT_MS, WireClient, await_topic, coordinator_loading, create_topic,
    fetch_raw, find_group_coordinator, now_ms, s, topic, version,
};

/// Codes a client retries on, while a coordinator loads or markers are being written.
const fn retriable(error: i16) -> bool {
    matches!(error, 14 | 15 | 16 | 51)
}

/// One transactional producer as the scenario drives it.
#[allow(clippy::struct_field_names)] // Kafka's own names
struct Producer {
    id: String,
    producer_id: i64,
    epoch: i16,
    sequence: i32,
}

/// A batch of `values` from `producer`, transactional, at its next sequence.
fn txn_batch(producer: &mut Producer, values: &[&str]) -> Result<Bytes, String> {
    let start = now_ms() - 60_000;
    let records: Vec<Record> = values
        .iter()
        .zip(0_i64..)
        .map(|(value, index)| Record {
            transactional: true,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: producer.producer_id,
            producer_epoch: producer.epoch,
            timestamp_type: TimestampType::Creation,
            offset: index,
            sequence: producer.sequence + i32::try_from(index).unwrap_or(0),
            timestamp: start + index,
            key: Some(Bytes::from(format!("k{index}"))),
            value: Some(Bytes::from(value.to_string())),
            headers: kafka_protocol::indexmap::IndexMap::default(),
        })
        .collect();
    producer.sequence += i32::try_from(values.len()).unwrap_or(0);
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

async fn find_coordinator(client: &mut WireClient, transactional_id: &str) -> Result<i16, String> {
    let find_version = version(client, ApiKey::FindCoordinator, 4)?;
    let find = if find_version >= 4 {
        FindCoordinatorRequest::default()
            .with_key_type(1)
            .with_coordinator_keys(vec![s(transactional_id)])
    } else {
        FindCoordinatorRequest::default()
            .with_key_type(1)
            .with_key(s(transactional_id))
    };
    let mut error = 0;
    for _ in 0..60 {
        let response = client
            .send(&find, find_version)
            .await
            .map_err(|error| format!("FindCoordinator: {error}"))?;
        error = response
            .coordinators
            .first()
            .map_or(response.error_code, |coordinator| coordinator.error_code);
        if !coordinator_loading(error) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok(error)
}

/// `InitProducerId` for `transactional_id`, retried while retriable. `(error, id, epoch)`.
async fn init(
    client: &mut WireClient,
    transactional_id: &str,
    timeout_ms: i32,
) -> Result<(i16, i64, i16), String> {
    let init_version = version(client, ApiKey::InitProducerId, 4)?;
    let request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(s(transactional_id))))
        .with_transaction_timeout_ms(timeout_ms)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    let mut answer = (0, -1, -1);
    for _ in 0..120 {
        let response = client
            .send(&request, init_version)
            .await
            .map_err(|error| format!("InitProducerId: {error}"))?;
        answer = (
            response.error_code,
            response.producer_id.0,
            response.producer_epoch,
        );
        if !retriable(response.error_code) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(answer)
}

/// `AddPartitionsToTxn` at v3, retried while every partition answers retriable. The codes, by
/// partition, in request order.
async fn add_partitions(
    client: &mut WireClient,
    producer: &Producer,
    name: &str,
    partitions: &[i32],
) -> Result<Vec<i16>, String> {
    let add_version = version(client, ApiKey::AddPartitionsToTxn, 3)?.min(3);
    let request = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(TransactionalId(s(&producer.id)))
        .with_v3_and_below_producer_id(ProducerId(producer.producer_id))
        .with_v3_and_below_producer_epoch(producer.epoch)
        .with_v3_and_below_topics(vec![
            AddPartitionsToTxnTopic::default()
                .with_name(topic(name))
                .with_partitions(partitions.to_vec()),
        ]);
    let mut codes = Vec::new();
    for _ in 0..120 {
        let response = client
            .send(&request, add_version)
            .await
            .map_err(|error| format!("AddPartitionsToTxn v{add_version}: {error}"))?;
        codes = response
            .results_by_topic_v3_and_below
            .iter()
            .flat_map(|topic| topic.results_by_partition.iter())
            .map(|partition| partition.partition_error_code)
            .collect();
        if !codes.iter().all(|code| retriable(*code)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(codes)
}

/// `EndTxn`, not retried: its first answer.
async fn end(client: &mut WireClient, producer: &Producer, commit: bool) -> Result<i16, String> {
    let end_version = version(client, ApiKey::EndTxn, 3)?.min(4);
    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(s(&producer.id)))
        .with_producer_id(ProducerId(producer.producer_id))
        .with_producer_epoch(producer.epoch)
        .with_committed(commit);
    Ok(client
        .send(&request, end_version)
        .await
        .map_err(|error| format!("EndTxn v{end_version}: {error}"))?
        .error_code)
}

/// `EndTxn` retried while it answers retriable, as a client retries it.
async fn end_settled(
    client: &mut WireClient,
    producer: &Producer,
    commit: bool,
) -> Result<i16, String> {
    let mut error = 0;
    for _ in 0..120 {
        error = end(client, producer, commit).await?;
        if !retriable(error) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(error)
}

/// A transactional Produce of one batch to `partition`. `(error, base_offset)`.
async fn produce_txn(
    client: &mut WireClient,
    producer: &Producer,
    name: &str,
    partition: i32,
    records: Bytes,
) -> Result<(i16, i64), String> {
    let request = ProduceRequest::default()
        .with_transactional_id(Some(TransactionalId(s(&producer.id))))
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
    let produce_version = version(client, ApiKey::Produce, 9)?;
    for attempt in 0.. {
        let response = client
            .send(&request, produce_version)
            .await
            .map_err(|error| format!("transactional Produce: {error}"))?;
        let answer = response
            .responses
            .first()
            .and_then(|topic| topic.partition_responses.first())
            .ok_or_else(|| "Produce answered no partition".to_string())?;
        // 5 and 6 while a new leader settles, 19 while the coordinator verifies.
        if !matches!(answer.error_code, 5 | 6 | 19) || attempt == 40 {
            return Ok((answer.error_code, answer.base_offset));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    unreachable!("the loop returns")
}

/// What a `read_uncommitted` Fetch of `partition` from 0 shows: data records, control records,
/// and whether every data batch is transactional with `producer_id`.
async fn read(
    client: &mut WireClient,
    name: &str,
    partition: i32,
    producer_id: i64,
    facts: &mut Facts,
    (label, markers): (&str, usize),
) -> Result<(), String> {
    // Markers land after EndTxn answers; wait until `markers` show up, or 15 s.
    let mut data = 0;
    let mut controls = Vec::new();
    let mut all_transactional = true;
    let mut high_watermark = -1;
    for attempt in 0..60 {
        let (answer, bytes) = fetch_raw(client, name, partition, 0, 0)
            .await
            .map_err(|error| format!("Fetch {label} p{partition} attempt {attempt}: {error}"))?;
        high_watermark = answer.high_watermark;
        data = 0;
        controls.clear();
        all_transactional = true;
        let mut bytes = bytes;
        for set in RecordBatchDecoder::decode_all(&mut bytes)
            .map_err(|error| format!("decode records: {error}"))?
        {
            for record in &set.records {
                if record.control {
                    let kind = record
                        .key
                        .as_ref()
                        .and_then(|key| key.get(2..4))
                        .map_or(-1, |kind| i16::from_be_bytes([kind[0], kind[1]]));
                    controls.push(kind);
                } else {
                    data += 1;
                    all_transactional &= record.transactional && record.producer_id == producer_id;
                }
            }
        }
        if controls.len() >= markers
            && i64::try_from(data + controls.len()).unwrap_or(0) >= high_watermark
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    facts.set(format!("{label}.high_watermark"), high_watermark);
    facts.set(format!("{label}.data_records"), data);
    facts.set(
        format!("{label}.controls"),
        controls
            .iter()
            .map(|kind| match kind {
                0 => "abort",
                1 => "commit",
                _ => "other",
            })
            .collect::<Vec<_>>()
            .join(","),
    );
    facts.set(format!("{label}.data_transactional"), all_transactional);
    Ok(())
}

/// The handshake, a commit and an abort on one partition, the errors at each refusal, and a second
/// producer fencing the first.
#[allow(clippy::too_many_lines)] // one scenario, read top to bottom
pub async fn transactions(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("transactions", "t");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    let transactional_id = cx.name("transactions", "tx");

    facts.set(
        "find_coordinator.error",
        find_coordinator(client, &transactional_id).await?,
    );
    let (error, producer_id, epoch) = init(client, &transactional_id, 60_000).await?;
    facts.set("init.error", error);
    facts.set("init.epoch", epoch);
    facts.set("init.has_producer_id", producer_id >= 0);
    let mut producer = Producer {
        id: transactional_id.clone(),
        producer_id,
        epoch,
        sequence: 0,
    };
    facts.set(
        "init.too_long_timeout.error",
        init(client, &cx.name("transactions", "tx-long"), 900_001)
            .await?
            .0,
    );
    facts.set(
        "end_without_partitions.error",
        end(client, &producer, true).await?,
    );

    // A transaction that commits: two records on partition 0.
    let codes = add_partitions(client, &producer, &name, &[0]).await?;
    facts.set("add.codes", format!("{codes:?}"));
    let batch = txn_batch(&mut producer, &["c1", "c2"])?;
    let (error, base) = produce_txn(client, &producer, &name, 0, batch).await?;
    facts.set("produce.error", error);
    facts.set("produce.base_offset", base);
    let stray = txn_batch(
        &mut Producer {
            sequence: 0,
            ..clone(&producer)
        },
        &["x"],
    )?;
    let (error, _) = produce_txn(client, &producer, &name, 2, stray).await?;
    facts.set("produce_unadded_partition.error", error);
    facts.set("commit.error", end(client, &producer, true).await?);
    facts.set(
        "commit_settled.error",
        end_settled(client, &producer, true).await?,
    );
    read(client, &name, 0, producer_id, facts, ("after_commit", 1)).await?;

    // A transaction that aborts: one record.
    let codes = add_partitions(client, &producer, &name, &[0]).await?;
    facts.set("add_again.codes", format!("{codes:?}"));
    let batch = txn_batch(&mut producer, &["a1"])?;
    let (error, base) = produce_txn(client, &producer, &name, 0, batch).await?;
    facts.set("produce_aborted.error", error);
    facts.set("produce_aborted.base_offset", base);
    facts.set("abort.error", end_settled(client, &producer, false).await?);
    facts.set(
        "commit_after_abort.error",
        end_settled(client, &producer, true).await?,
    );
    read(client, &name, 0, producer_id, facts, ("after_abort", 2)).await?;

    // A second producer with the same id fences the first.
    let (error, second_id, second_epoch) = init(client, &transactional_id, 60_000).await?;
    facts.set("second_init.error", error);
    facts.set("second_init.same_producer_id", second_id == producer_id);
    facts.set("second_init.epoch_bump", second_epoch - epoch);
    let codes = add_partitions(client, &producer, &name, &[1]).await?;
    facts.set("fenced_add.codes", format!("{codes:?}"));
    facts.set("fenced_end.error", end(client, &producer, true).await?);
    let mut second = Producer {
        id: transactional_id.clone(),
        producer_id: second_id,
        epoch: second_epoch,
        sequence: 0,
    };
    let codes = add_partitions(client, &second, &name, &[1]).await?;
    facts.set("second_add.codes", format!("{codes:?}"));
    let batch = txn_batch(&mut second, &["s1"])?;
    facts.set(
        "second_produce.error",
        produce_txn(client, &second, &name, 1, batch).await?.0,
    );
    let zombie = txn_batch(
        &mut Producer {
            sequence: 1,
            ..clone(&producer)
        },
        &["z1"],
    )?;
    facts.set(
        "zombie_produce.error",
        produce_txn(client, &producer, &name, 1, zombie).await?.0,
    );

    // A third producer fences the second while its transaction is open: that one is aborted, and
    // the epoch is bumped twice.
    let (error, _, third_epoch) = init(client, &transactional_id, 60_000).await?;
    facts.set("third_init.error", error);
    facts.set("third_init.epoch_bump", third_epoch - second_epoch);
    // A fresh connection: a managed cluster may have closed the partition leader's while the
    // fence was retried on the coordinator's.
    *client = WireClient::connect_target(&cx.target).await?;
    read(client, &name, 1, second_id, facts, ("after_fence", 1)).await?;

    let unknown = Producer {
        id: cx.name("transactions", "nobody"),
        producer_id: 42,
        epoch: 0,
        sequence: 0,
    };
    let codes = add_partitions(client, &unknown, &name, &[0]).await?;
    facts.set("unknown_id_add.codes", format!("{codes:?}"));
    Ok(())
}

fn clone(producer: &Producer) -> Producer {
    Producer {
        id: producer.id.clone(),
        producer_id: producer.producer_id,
        epoch: producer.epoch,
        sequence: producer.sequence,
    }
}

/// What a Java `read_committed` consumer keeps of one Fetch answer: the data records of batches
/// not aborted, by `CompletedFetch`'s rules (an aborted transaction joins the aborted set once a
/// batch reaches its first offset, and its abort marker takes it out).
fn committed_view(
    answer: &kafka_protocol::messages::fetch_response::PartitionData,
    bytes: Bytes,
) -> Result<Vec<String>, String> {
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
    let mut bytes = bytes;
    for set in RecordBatchDecoder::decode_all(&mut bytes)
        .map_err(|error| format!("decode records: {error}"))?
    {
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
                let abort = first.key.as_ref().and_then(|key| key.get(2..4)) == Some(&[0, 0][..]);
                if abort {
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
    Ok(values)
}

/// A Fetch of partition 0 from `offset` at `isolation`, recorded under `label`: the values a Java
/// consumer keeps, the last stable offset, the high watermark, and the aborted transactions
/// listed.
async fn read_at(
    client: &mut WireClient,
    name: &str,
    offset: i64,
    isolation: i8,
    facts: &mut Facts,
    label: &str,
) -> Result<(), String> {
    let (answer, bytes) = fetch_raw(client, name, 0, offset, isolation)
        .await
        .map_err(|error| format!("Fetch {label}: {error}"))?;
    facts.set(format!("{label}.error"), answer.error_code);
    facts.set(format!("{label}.high_watermark"), answer.high_watermark);
    facts.set(
        format!("{label}.last_stable_offset"),
        answer.last_stable_offset,
    );
    facts.set(
        format!("{label}.values"),
        committed_view(&answer, bytes)?.join(","),
    );
    facts.set(
        format!("{label}.aborted"),
        answer.aborted_transactions.as_ref().map_or_else(
            || "null".to_string(),
            |list| {
                list.iter()
                    .map(|txn| txn.first_offset.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            },
        ),
    );
    Ok(())
}

/// `ListOffsets` `LATEST` of partition 0 at `isolation`.
async fn latest(client: &mut WireClient, name: &str, isolation: i8) -> Result<(i16, i64), String> {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    let request = kafka_protocol::messages::ListOffsetsRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId(-1))
        .with_isolation_level(isolation)
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(topic(name))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_current_leader_epoch(-1)
                        .with_timestamp(-1),
                ]),
        ]);
    let list_version = version(client, ApiKey::ListOffsets, 7)?;
    let response = client
        .send(&request, list_version)
        .await
        .map_err(|error| format!("ListOffsets: {error}"))?;
    let answer = response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
        .ok_or_else(|| "ListOffsets answered no partition".to_string())?;
    Ok((answer.error_code, answer.offset))
}

/// `read_committed` on one partition: a commit, an abort, and an open transaction with a plain
/// record after it. The open one holds the last stable offset, the aborted one is listed, and a
/// consumer keeps only committed records; once the open one commits, everything up to the high
/// watermark is stable.
pub async fn read_committed(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("read_committed", "t");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    let transactional_id = cx.name("read_committed", "tx");
    find_coordinator(client, &transactional_id).await?;
    let (error, producer_id, epoch) = init(client, &transactional_id, 60_000).await?;
    if error != 0 {
        return Err(format!("InitProducerId answered {error}"));
    }
    let mut producer = Producer {
        id: transactional_id,
        producer_id,
        epoch,
        sequence: 0,
    };
    for (values, commit) in [(&["c1", "c2"][..], true), (&["a1"][..], false)] {
        add_partitions(client, &producer, &name, &[0]).await?;
        let batch = txn_batch(&mut producer, values)?;
        produce_txn(client, &producer, &name, 0, batch).await?;
        end_settled(client, &producer, commit).await?;
    }
    // Both markers in place before the next transaction starts writing.
    let mut high_watermark = 0;
    for _ in 0..60 {
        high_watermark = fetch_raw(client, &name, 0, 0, 0).await?.0.high_watermark;
        if high_watermark >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    facts.set("before_open.high_watermark", high_watermark);
    add_partitions(client, &producer, &name, &[0]).await?;
    let batch = txn_batch(&mut producer, &["o1"])?;
    facts.set(
        "open.produce.error",
        produce_txn(client, &producer, &name, 0, batch).await?.0,
    );
    let plain = super::batch(&[(Some("k"), Some("plain"), &[])], now_ms())?;
    facts.set(
        "plain.produce.error",
        super::produce(client, &name, 0, plain).await?.0,
    );

    read_at(client, &name, 0, 1, facts, "open.committed").await?;
    read_at(client, &name, 0, 0, facts, "open.uncommitted").await?;
    read_at(client, &name, 4, 1, facts, "open.committed_from_marker").await?;
    let (error, offset) = latest(client, &name, 1).await?;
    facts.set("open.latest_committed", format!("{error}:{offset}"));
    let (error, offset) = latest(client, &name, 0).await?;
    facts.set("open.latest_uncommitted", format!("{error}:{offset}"));

    end_settled(client, &producer, true).await?;
    let mut stable = 0;
    for _ in 0..60 {
        let (answer, _) = fetch_raw(client, &name, 0, 0, 1).await?;
        stable = answer.last_stable_offset;
        if stable >= 8 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    facts.set("after.stable_reached", stable);
    read_at(client, &name, 0, 1, facts, "after.committed").await?;
    let (error, offset) = latest(client, &name, 1).await?;
    facts.set("after.latest_committed", format!("{error}:{offset}"));
    Ok(())
}

/// `AddOffsetsToTxn` for `group`, retried while retriable.
async fn add_offsets(
    client: &mut WireClient,
    producer: &Producer,
    group: &str,
) -> Result<i16, String> {
    let add_version = version(client, ApiKey::AddOffsetsToTxn, 3)?.min(3);
    let request = kafka_protocol::messages::AddOffsetsToTxnRequest::default()
        .with_transactional_id(TransactionalId(s(&producer.id)))
        .with_producer_id(ProducerId(producer.producer_id))
        .with_producer_epoch(producer.epoch)
        .with_group_id(kafka_protocol::messages::GroupId(s(group)));
    let mut error = 0;
    for _ in 0..120 {
        error = client
            .send(&request, add_version)
            .await
            .map_err(|error| format!("AddOffsetsToTxn: {error}"))?
            .error_code;
        if !retriable(error) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(error)
}

/// `TxnOffsetCommit` of `offset` for partition 0 of `name`, standalone (no member), retried while
/// retriable. Its code.
async fn txn_commit(
    client: &mut WireClient,
    producer: &Producer,
    group: &str,
    name: &str,
    offset: i64,
) -> Result<i16, String> {
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    let commit_version = version(client, ApiKey::TxnOffsetCommit, 3)?.min(3);
    let request = kafka_protocol::messages::TxnOffsetCommitRequest::default()
        .with_transactional_id(TransactionalId(s(&producer.id)))
        .with_group_id(kafka_protocol::messages::GroupId(s(group)))
        .with_producer_id(ProducerId(producer.producer_id))
        .with_producer_epoch(producer.epoch)
        .with_generation_id(-1)
        .with_member_id(s(""))
        .with_topics(vec![
            TxnOffsetCommitRequestTopic::default()
                .with_name(topic(name))
                .with_partitions(vec![
                    TxnOffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(offset)
                        .with_committed_leader_epoch(-1),
                ]),
        ]);
    let mut error = 0;
    for _ in 0..120 {
        let response = client
            .send(&request, commit_version)
            .await
            .map_err(|error| format!("TxnOffsetCommit: {error}"))?;
        error = response
            .topics
            .first()
            .and_then(|topic| topic.partitions.first())
            .map_or(-2, |partition| partition.error_code);
        if !retriable(error) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(error)
}

/// `OffsetFetch` v7 of partition 0 of `name` for `group`: `"error:offset"` of the partition.
async fn committed_offset(
    client: &mut WireClient,
    group: &str,
    name: &str,
    require_stable: bool,
) -> Result<String, String> {
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    let fetch_version = version(client, ApiKey::OffsetFetch, 7)?.min(7);
    let request = kafka_protocol::messages::OffsetFetchRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId(s(group)))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(topic(name))
                .with_partition_indexes(vec![0]),
        ]))
        .with_require_stable(require_stable);
    // A group coordinator loads its partition on first use: 14 and 16 until then.
    for _ in 0..60 {
        let response = client
            .send(&request, fetch_version)
            .await
            .map_err(|error| format!("OffsetFetch: {error}"))?;
        if let Some(partition) = response
            .topics
            .first()
            .and_then(|topic| topic.partitions.first())
        {
            return Ok(format!(
                "{}:{}",
                partition.error_code, partition.committed_offset
            ));
        }
        if !retriable(response.error_code) {
            return Err(format!(
                "OffsetFetch answered no partition (error {})",
                response.error_code
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err("OffsetFetch kept answering retriable codes".to_string())
}

/// Consume-transform-produce on the wire (KIP-98, KIP-447): a transaction writes a record and
/// commits the input group's offset; the offset is unstable while the transaction is open, and
/// committed only when it commits. An aborted one leaves the committed offset as it was.
pub async fn exactly_once(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let input = cx.name("exactly_once", "in");
    let output = cx.name("exactly_once", "out");
    for name in [&input, &output] {
        create_topic(client, name, &[]).await?;
        await_topic(client, name).await?;
    }
    let group = cx.name("exactly_once", "g");
    find_group_coordinator(client, &group).await?;
    let transactional_id = cx.name("exactly_once", "tx");
    find_coordinator(client, &transactional_id).await?;
    let (error, producer_id, epoch) = init(client, &transactional_id, 60_000).await?;
    if error != 0 {
        return Err(format!("InitProducerId answered {error}"));
    }
    let mut producer = Producer {
        id: transactional_id,
        producer_id,
        epoch,
        sequence: 0,
    };

    facts.set(
        "commit_without_add.error",
        txn_commit(client, &producer, &group, &input, 1).await?,
    );
    add_partitions(client, &producer, &output, &[0]).await?;
    let batch = txn_batch(&mut producer, &["out1"])?;
    produce_txn(client, &producer, &output, 0, batch).await?;
    facts.set(
        "add_offsets.error",
        add_offsets(client, &producer, &group).await?,
    );
    facts.set(
        "txn_commit.error",
        txn_commit(client, &producer, &group, &input, 2).await?,
    );
    facts.set(
        "open.stable",
        committed_offset(client, &group, &input, true).await?,
    );
    facts.set(
        "open.unstable_read",
        committed_offset(client, &group, &input, false).await?,
    );
    facts.set("commit.error", end_settled(client, &producer, true).await?);
    let mut after = String::new();
    for _ in 0..60 {
        after = committed_offset(client, &group, &input, true).await?;
        if !after.starts_with("88:") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    facts.set("after_commit.stable", after);

    add_partitions(client, &producer, &output, &[0]).await?;
    let batch = txn_batch(&mut producer, &["out2"])?;
    produce_txn(client, &producer, &output, 0, batch).await?;
    add_offsets(client, &producer, &group).await?;
    txn_commit(client, &producer, &group, &input, 5).await?;
    facts.set("abort.error", end_settled(client, &producer, false).await?);
    let mut after = String::new();
    for _ in 0..60 {
        after = committed_offset(client, &group, &input, true).await?;
        if !after.starts_with("88:") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    facts.set("after_abort.stable", after);

    let (_, _, bumped) = init(client, &producer.id, 60_000).await?;
    facts.set("reinit.epoch_bump", bumped - producer.epoch);
    let stale = Producer {
        epoch: producer.epoch,
        ..clone(&producer)
    };
    facts.set(
        "fenced_add_offsets.error",
        add_offsets(client, &stale, &group).await?,
    );
    facts.set(
        "fenced_txn_commit.error",
        txn_commit(client, &stale, &group, &input, 9).await?,
    );
    Ok(())
}
