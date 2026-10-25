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

//! `DeleteRecords` (API key 21).
//!
//! Raises each partition's `DeleteRecords` watermark (`bridge/iggy_bridge/log_start.rs`): Fetch
//! refuses offsets below it with `OFFSET_OUT_OF_RANGE`, and `ListOffsets` EARLIEST answers it.
//! As on a Kafka broker, `-1` means the high watermark, an offset past the high watermark answers
//! `OFFSET_OUT_OF_RANGE`, and an offset below the start offset leaves it. The answer is the low
//! watermark the partition then has.
//!
//! Iggy keeps the records below the watermark on disk until retention removes their segment.
//! Kafka frees whole segments below it too, but on its own schedule; see `docs/SCOPE.md`.

use std::collections::HashMap;

use bytes::Bytes;
use kafka_protocol::messages::delete_records_request::DeleteRecordsTopic;
use kafka_protocol::messages::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
};
use kafka_protocol::messages::{DeleteRecordsRequest, DeleteRecordsResponse};
use tokio::time::Instant;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::iggy_bridge::{OffsetCalls, PartitionProbe};
use crate::bridge::{BridgeError, IggyBridge};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_DELETE_RECORDS, ApiVersionRange, ERROR_NOT_LEADER_OR_FOLLOWER,
    ERROR_OFFSET_OUT_OF_RANGE, ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_AUTHORIZATION_FAILED,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    UNKNOWN_OFFSET,
};
use crate::protocol::bounds_guard::validate_delete_records_shape;
use crate::protocol::handlers::{
    clamp_request_timeout, decode_guarded, decode_request, encode_message, respond_or_close,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DELETE_RECORDS,
    min_version: 0,
    max_version: 2,
};

/// Kafka's sentinel for "up to the high watermark".
const HIGH_WATERMARK_OFFSET: i64 = -1;

/// Cap on distinct topics one request resolves through the bridge, as in `ListOffsets`. The rest
/// answer [`ERROR_REQUEST_TIMED_OUT`], which the admin client retries.
const MAX_BRIDGE_BACKED_TOPICS: usize = 100;

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let decoded = decode_request(
        API_KEY_DELETE_RECORDS,
        api_version,
        body,
        |v, b| {
            decode_guarded::<DeleteRecordsRequest>(v, b, |v, b| {
                validate_delete_records_shape(v, b, state.max_frame_size)
            })
        },
        encode_error_response,
        "DeleteRecords",
    );
    let request = match decoded {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let topics = if let Some(code) = refusal(principal) {
        answer_all(&request.topics, code)
    } else if let Some(bridge) = &state.bridge {
        let deadline = Instant::now() + clamp_request_timeout(request.timeout_ms);
        delete_all(bridge, &request.topics, deadline).await
    } else {
        // Without a bridge there is no partition to lead, as in Produce and Fetch.
        answer_all(&request.topics, ERROR_NOT_LEADER_OR_FOLLOWER)
    };
    let response = DeleteRecordsResponse::default().with_topics(topics);
    respond_or_close(encode_message(&response, api_version, 256), "DeleteRecords")
}

/// The code that refuses the whole request to `principal`, if any.
///
/// Kafka asks for `DELETE` on the topic. The gateway's mapping of Iggy permissions gives that to
/// `manage_topics` (`docs/ACL_MAPPING.md`), as for `DeleteTopics`. `None` means no listener of
/// this gateway names principals. A principal whose permissions were never read is refused: a
/// deletion must not rest on a value nothing read.
fn refusal(principal: Option<&AuthenticatedPrincipal>) -> Option<i16> {
    let principal = principal?;
    if principal.permissions_known && principal.permissions.manage_topics {
        return None;
    }
    if !principal.permissions_known {
        tracing::warn!(
            principal = %principal.username,
            "DeleteRecords asked on a connection whose permissions were never read; denying"
        );
    }
    Some(ERROR_TOPIC_AUTHORIZATION_FAILED)
}

/// Each topic and partition of `topics`, all answered `code`.
fn answer_all(topics: &[DeleteRecordsTopic], code: i16) -> Vec<DeleteRecordsTopicResult> {
    topics
        .iter()
        .map(|topic| {
            let partitions = topic
                .partitions
                .iter()
                .map(|partition| refused(partition.partition_index, code))
                .collect();
            DeleteRecordsTopicResult::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions)
        })
        .collect()
}

/// Deletes up to the requested offset of every partition, one topic at a time, one partition at a
/// time, so the watermark writes of a request land in order. A topic past the cap or the
/// deadline answers [`ERROR_REQUEST_TIMED_OUT`] without a bridge call.
async fn delete_all(
    bridge: &IggyBridge,
    topics: &[DeleteRecordsTopic],
    deadline: Instant,
) -> Vec<DeleteRecordsTopicResult> {
    let calls = OffsetCalls::new(deadline);
    let mut distinct: HashMap<&str, usize> = HashMap::new();
    let mut results = Vec::with_capacity(topics.len());
    for topic in topics {
        let seen = distinct.len();
        let order = *distinct.entry(topic.name.as_str()).or_insert(seen);
        let partitions = if order >= MAX_BRIDGE_BACKED_TOPICS || Instant::now() >= deadline {
            topic
                .partitions
                .iter()
                .map(|partition| refused(partition.partition_index, ERROR_REQUEST_TIMED_OUT))
                .collect()
        } else {
            delete_in_topic(bridge, topic, &calls).await
        };
        results.push(
            DeleteRecordsTopicResult::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }
    results
}

async fn delete_in_topic(
    bridge: &IggyBridge,
    topic: &DeleteRecordsTopic,
    calls: &OffsetCalls,
) -> Vec<DeleteRecordsPartitionResult> {
    let name = topic.name.as_str();
    let mut wanted: Vec<u32> = topic
        .partitions
        .iter()
        .filter_map(|partition| u32::try_from(partition.partition_index).ok())
        .collect();
    wanted.sort_unstable();
    wanted.dedup();
    let probes =
        match tokio::time::timeout_at(calls.deadline(), bridge.partition_probes(name, &wanted))
            .await
        {
            Ok(Ok(probes)) => probes,
            Ok(Err(error)) => {
                let code = error.to_list_offsets_error_code();
                if code != ERROR_UNKNOWN_TOPIC_OR_PARTITION {
                    tracing::warn!(topic = name, %error, "DeleteRecords could not probe the topic");
                }
                return answer_partitions(topic, retriable(code, &error));
            }
            Err(_elapsed) => return answer_partitions(topic, ERROR_REQUEST_TIMED_OUT),
        };
    let mut results = Vec::with_capacity(topic.partitions.len());
    for requested in &topic.partitions {
        let index = requested.partition_index;
        let probe = u32::try_from(index).ok().and_then(|partition| {
            probes
                .binary_search_by_key(&partition, |(id, _)| *id)
                .ok()
                .map(|found| (partition, &probes[found].1))
        });
        let result = match probe {
            None => refused(index, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            Some((_, Err(error))) => {
                let code = error.to_list_offsets_error_code();
                refused(index, retriable(code, error))
            }
            Some((partition, Ok(probe))) => match target(requested.offset, probe) {
                Err(code) => refused(index, code),
                Ok(offset) => {
                    let deleted = bridge
                        .delete_records_below(name, partition, offset, calls)
                        .await;
                    answer(index, probe, deleted)
                }
            },
        };
        results.push(result);
    }
    results
}

/// The offset to delete below: the high watermark for `-1`. Kafka answers
/// [`ERROR_OFFSET_OUT_OF_RANGE`] to any other negative offset and to one past the high watermark.
fn target(requested: i64, probe: &PartitionProbe) -> core::result::Result<u64, i16> {
    if probe.is_loading() {
        // The high watermark reads too low until the load ends.
        return Err(ERROR_NOT_LEADER_OR_FOLLOWER);
    }
    if requested == HIGH_WATERMARK_OFFSET {
        return Ok(probe.high_watermark);
    }
    match u64::try_from(requested) {
        Ok(offset) if offset <= probe.high_watermark => Ok(offset),
        _ => Err(ERROR_OFFSET_OUT_OF_RANGE),
    }
}

/// The answer after a watermark write: the low watermark the partition then has, the first offset
/// retention kept when that is higher.
fn answer(
    partition: i32,
    probe: &PartitionProbe,
    deleted: core::result::Result<u64, BridgeError>,
) -> DeleteRecordsPartitionResult {
    match deleted {
        Ok(watermark) => {
            let start = probe
                .log_start()
                .filter(|&start| start <= probe.high_watermark)
                .map_or(watermark, |start| start.max(watermark));
            DeleteRecordsPartitionResult::default()
                .with_partition_index(partition)
                .with_low_watermark(i64::try_from(start).unwrap_or(i64::MAX))
        }
        Err(BridgeError::Timeout) => refused(partition, ERROR_REQUEST_TIMED_OUT),
        Err(error) => {
            let code = error.to_kafka_error_code();
            tracing::warn!(partition, %error, "DeleteRecords could not store the watermark");
            refused(partition, retriable(code, &error))
        }
    }
}

/// `code`, but 6 where it would be -1, which the admin client fails at once. A rejected bridge
/// login keeps -1: retrying cannot fix it.
const fn retriable(code: i16, error: &BridgeError) -> i16 {
    if code == ERROR_UNKNOWN_SERVER_ERROR && !error.is_bridge_login_rejected() {
        ERROR_NOT_LEADER_OR_FOLLOWER
    } else {
        code
    }
}

fn answer_partitions(topic: &DeleteRecordsTopic, code: i16) -> Vec<DeleteRecordsPartitionResult> {
    topic
        .partitions
        .iter()
        .map(|partition| refused(partition.partition_index, code))
        .collect()
}

fn refused(partition: i32, code: i16) -> DeleteRecordsPartitionResult {
    DeleteRecordsPartitionResult::default()
        .with_partition_index(partition)
        .with_low_watermark(UNKNOWN_OFFSET)
        .with_error_code(code)
}

/// A response with one placeholder partition answered `error_code`.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let response = DeleteRecordsResponse::default().with_topics(vec![
        DeleteRecordsTopicResult::default().with_partitions(vec![refused(0, error_code)]),
    ]);
    encode_message(&response, version, 64)
}

#[cfg(test)]
mod tests {
    use iggy::prelude::IggyError;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::messages::delete_records_request::DeleteRecordsPartition;
    use kafka_protocol::protocol::StrBytes;

    use super::*;
    use crate::protocol::api::ERROR_NONE;

    fn probe(high_watermark: u64, messages_count: u64, deleted_below: u64) -> PartitionProbe {
        PartitionProbe {
            high_watermark,
            average_size: 1,
            messages_count,
            deleted_below,
        }
    }

    #[test]
    fn given_minus_one_the_target_should_be_the_high_watermark() {
        assert_eq!(target(-1, &probe(42, 42, 0)), Ok(42));
    }

    #[test]
    fn given_an_offset_up_to_the_high_watermark_the_target_should_be_that_offset() {
        assert_eq!(target(0, &probe(42, 42, 0)), Ok(0));
        assert_eq!(target(17, &probe(42, 42, 0)), Ok(17));
        assert_eq!(target(42, &probe(42, 42, 0)), Ok(42));
    }

    #[test]
    fn given_an_offset_past_the_end_or_another_negative_it_should_be_out_of_range() {
        for requested in [43, i64::MAX, -2, i64::MIN] {
            assert_eq!(
                target(requested, &probe(42, 42, 0)),
                Err(ERROR_OFFSET_OUT_OF_RANGE),
                "{requested}"
            );
        }
    }

    #[test]
    fn given_a_partition_that_loads_the_target_should_wait() {
        assert_eq!(
            target(1, &probe(1, 50, 0)),
            Err(ERROR_NOT_LEADER_OR_FOLLOWER)
        );
    }

    #[test]
    fn given_a_stored_watermark_the_answer_should_be_the_start_offset_after_it() {
        let stored = answer(0, &probe(42, 42, 0), Ok(17));
        assert_eq!((stored.error_code, stored.low_watermark), (ERROR_NONE, 17));
        // Retention already removed offsets 0 to 29.
        let retained = answer(0, &probe(42, 12, 0), Ok(17));
        assert_eq!(retained.low_watermark, 30);
    }

    #[test]
    fn given_a_failed_write_the_answer_should_be_retriable_unless_the_login_failed() {
        let timed_out = answer(0, &probe(42, 42, 0), Err(BridgeError::Timeout));
        assert_eq!(timed_out.error_code, ERROR_REQUEST_TIMED_OUT);
        let refused = answer(
            0,
            &probe(42, 42, 0),
            Err(BridgeError::Iggy(IggyError::InvalidCommand)),
        );
        assert_ne!(refused.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        assert_eq!(refused.low_watermark, UNKNOWN_OFFSET);
        let rejected = answer(
            0,
            &probe(42, 42, 0),
            Err(BridgeError::Iggy(IggyError::InvalidCredentials)),
        );
        assert_eq!(rejected.error_code, ERROR_UNKNOWN_SERVER_ERROR);
    }

    fn principal(manage_topics: bool, known: bool) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal::with_global(
            "app",
            iggy::prelude::GlobalPermissions {
                manage_topics,
                ..iggy::prelude::GlobalPermissions::default()
            },
            known,
        )
    }

    #[test]
    fn given_a_principal_without_manage_topics_the_request_should_be_refused() {
        assert_eq!(refusal(None), None);
        assert_eq!(refusal(Some(&principal(true, true))), None);
        for denied in [principal(false, true), principal(true, false)] {
            assert_eq!(
                refusal(Some(&denied)),
                Some(ERROR_TOPIC_AUTHORIZATION_FAILED)
            );
        }
    }

    #[test]
    fn given_a_refusal_every_partition_should_carry_it() {
        let topics = vec![
            DeleteRecordsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![
                    DeleteRecordsPartition::default().with_partition_index(0),
                    DeleteRecordsPartition::default().with_partition_index(3),
                ]),
        ];
        let answered = answer_all(&topics, ERROR_TOPIC_AUTHORIZATION_FAILED);
        assert_eq!(answered.len(), 1);
        let partitions: Vec<_> = answered[0]
            .partitions
            .iter()
            .map(|partition| {
                (
                    partition.partition_index,
                    partition.error_code,
                    partition.low_watermark,
                )
            })
            .collect();
        assert_eq!(
            partitions,
            [
                (0, ERROR_TOPIC_AUTHORIZATION_FAILED, UNKNOWN_OFFSET),
                (3, ERROR_TOPIC_AUTHORIZATION_FAILED, UNKNOWN_OFFSET),
            ]
        );
    }

    #[test]
    fn every_advertised_version_should_encode_an_error_response() {
        for version in RANGE.min_version..=RANGE.max_version {
            assert!(
                encode_error_response(version, ERROR_TOPIC_AUTHORIZATION_FAILED).is_ok(),
                "v{version}"
            );
        }
    }
}
