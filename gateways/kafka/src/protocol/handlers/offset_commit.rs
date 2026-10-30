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

//! `OffsetCommit` (API key 8).
//!
//! Checks the committer against its group, then stores each offset in Iggy
//! (`docs/OFFSET_STORAGE.md`). The response has no top-level error, so a group answer goes on
//! every partition.

use std::collections::HashSet;

use bytes::Bytes;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::{OffsetCommitRequest, OffsetCommitResponse};

use crate::auth::AuthenticatedPrincipal;
use crate::authz::TopicOp;
use crate::bridge::{IggyBridge, OffsetCalls, TopicTarget};
use crate::commit_metadata::{CommitMetadata, Committed, MetadataRecord, kafka_length};
use crate::error::Result;
use crate::group::CommitRequest;
use crate::protocol::api::{
    API_KEY_OFFSET_COMMIT, ApiVersionRange, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_NONE, ERROR_OFFSET_METADATA_TOO_LARGE, ERROR_TOPIC_AUTHORIZATION_FAILED,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    is_supported_version,
};
use crate::protocol::bounds_guard::validate_offset_commit_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, may_topic, offset_deadline, offset_refusal_cause,
    respond_or_close,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_OFFSET_COMMIT,
    min_version: 2,
    max_version: 9,
};

/// Partition index, error code and tagged fields of one echoed partition.
const PER_PARTITION_BYTES: usize = 7;

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    // A client reads a partition left out of the answer as committed, so a request that cannot be
    // answered partition by partition closes the connection instead.
    if !is_supported_version(API_KEY_OFFSET_COMMIT, api_version) {
        return HandleOutcome::Close;
    }
    let mut request =
        match decode_guarded::<OffsetCommitRequest>(api_version, body, |version, body| {
            validate_offset_commit_shape(version, body, state.max_frame_size)
        }) {
            Ok(request) => request,
            Err(error) => {
                // debug!, not warn!: attacker-controlled, not operator-actionable.
                tracing::debug!(%error, api_version, "Failed to decode OffsetCommit request");
                return HandleOutcome::Close;
            }
        };

    // Kafka's READ on each topic: Iggy's `store_consumer_offset`, which is `poll_messages`. A
    // denied topic is answered, the others are still committed.
    let (allowed, denied): (Vec<_>, Vec<_>) = std::mem::take(&mut request.topics)
        .into_iter()
        .partition(|topic| may_topic(state, principal, topic.name.as_str(), TopicOp::Read));
    request.topics = allowed;
    let denied = same_code(
        &OffsetCommitRequest::default().with_topics(denied),
        ERROR_TOPIC_AUTHORIZATION_FAILED,
    );

    let validated = state
        .groups
        .validate_commit(&CommitRequest::from(&request))
        .await;
    let mut topics = match (&state.bridge, validated) {
        (Some(bridge), Ok(commit)) => {
            // The commit stays counted until none of its Iggy calls can land, also past the
            // deadline, so a deletion of the group's offsets waits for them.
            let calls = OffsetCalls::new(offset_deadline(state, connection).await).keeping(commit);
            let metadata = MetadataPolicy {
                store: state.commit_metadata.as_deref(),
                max_len: state.limits.offset_metadata_max_bytes,
            };
            commit_all(bridge, &request, &calls, metadata).await
        }
        (None, Ok(_)) => same_code(&request, ERROR_COORDINATOR_LOAD_IN_PROGRESS),
        (_, Err(error)) => same_code(&request, error),
    };
    topics.extend(denied);
    respond_or_close(encode_response(api_version, topics), "OffsetCommit")
}

/// Where commit metadata goes, and how long it may be.
#[derive(Clone, Copy)]
struct MetadataPolicy<'a> {
    store: Option<&'a CommitMetadata>,
    /// Kafka's `offset.metadata.max.bytes`, in Java string length.
    max_len: usize,
}

/// Stores every requested offset. The slots run at once, each one partition at a time, and a
/// partition that the deadline leaves out answers 14 and makes no call.
///
/// A metadata string longer than the cap answers `OFFSET_METADATA_TOO_LARGE` (12) and stores
/// nothing for its partition, as in Kafka. The strings of the rest go to the metadata log first
/// (KW-12): a partition whose string cannot be kept is not committed either, and answers the
/// log's error, so a client never reads back an offset without the string it committed.
async fn commit_all(
    bridge: &IggyBridge,
    request: &OffsetCommitRequest,
    calls: &OffsetCalls,
    metadata: MetadataPolicy<'_>,
) -> Vec<OffsetCommitResponseTopic> {
    let group = request.group_id.0.as_str();
    let targets: Vec<_> = request
        .topics
        .iter()
        .map(|topic| bridge.topic_target(topic.name.0.as_str()))
        .collect();
    let mut topics = same_code(request, ERROR_NONE);
    let mut commits = Vec::new();
    // Where each commit's code goes: (topic, partition) in `topics`.
    let mut positions = Vec::new();
    for (topic_at, (topic, target)) in request.topics.iter().zip(&targets).enumerate() {
        for (partition_at, partition) in topic.partitions.iter().enumerate() {
            let too_large = partition
                .committed_metadata
                .as_ref()
                .is_some_and(|text| kafka_length(text) > metadata.max_len);
            let code = match (target, u32::try_from(partition.partition_index)) {
                (Err(error), _) => error.to_offset_error_code(),
                (Ok(_), Err(_)) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                (Ok(_), Ok(_)) if too_large => ERROR_OFFSET_METADATA_TOO_LARGE,
                (Ok(target), Ok(index)) => {
                    commits.push((target, index, partition.committed_offset));
                    positions.push((topic_at, partition_at));
                    continue;
                }
            };
            topics[topic_at].partitions[partition_at].error_code = code;
        }
    }

    if let Some(store) = metadata.store {
        (commits, positions) = keep_metadata(store, request, commits, positions, &mut topics).await;
    }

    let results = bridge.commit_group_offsets(group, &commits, calls).await;
    let mut refused = 0_usize;
    let mut first_refusal = None;
    for ((topic_at, partition_at), result) in positions.into_iter().zip(results) {
        let Err(error) = result else {
            continue;
        };
        let code = error.to_offset_error_code();
        topics[topic_at].partitions[partition_at].error_code = code;
        if code == ERROR_UNKNOWN_SERVER_ERROR {
            refused += 1;
            if first_refusal.is_none() {
                first_refusal = Some((topic_at, partition_at, error));
            }
        }
    }
    // One line per request: a full partition or an old server refuses each commit, every few
    // seconds.
    if let Some((topic_at, partition_at, error)) = first_refusal {
        let topic = &topics[topic_at];
        tracing::error!(
            group,
            kafka_topic = topic.name.0.as_str(),
            partition = topic.partitions[partition_at].partition_index,
            refused,
            %error,
            cause = offset_refusal_cause(&error),
            "Iggy refused Kafka offset commits"
        );
    }
    topics
}

/// Appends the metadata records the commits at `positions` need, and returns the commits and
/// positions that may go ahead. When the append fails, the partitions that needed a record answer
/// its error and are left out; the rest commit as usual.
async fn keep_metadata<'a>(
    store: &CommitMetadata,
    request: &OffsetCommitRequest,
    commits: Vec<(&'a TopicTarget, u32, i64)>,
    positions: Vec<(usize, usize)>,
    topics: &mut [OffsetCommitResponseTopic],
) -> (Vec<(&'a TopicTarget, u32, i64)>, Vec<(usize, usize)>) {
    let group = request.group_id.0.as_str();
    let committed: Vec<Committed<'_>> = positions
        .iter()
        .map(|&(topic_at, partition_at)| {
            let topic = &request.topics[topic_at];
            let partition = &topic.partitions[partition_at];
            Committed {
                topic: topic.name.0.as_str(),
                partition: partition.partition_index,
                offset: partition.committed_offset,
                metadata: partition.committed_metadata.as_deref(),
            }
        })
        .collect();
    let records = store.records_for(group, &committed);
    let Err(error) = store.append(&records).await else {
        return (commits, positions);
    };
    let code = error.to_offset_error_code();
    tracing::warn!(
        group,
        %error,
        "could not keep Kafka commit metadata; refusing those commits"
    );
    let failed: HashSet<(&str, i32)> = records
        .iter()
        .filter_map(|record| match record {
            MetadataRecord::Partition {
                topic, partition, ..
            } => Some((topic.as_str(), *partition)),
            MetadataRecord::GroupDeleted { .. } => None,
        })
        .collect();
    let mut kept_commits = Vec::with_capacity(commits.len());
    let mut kept_positions = Vec::with_capacity(positions.len());
    for (commit, (topic_at, partition_at)) in commits.into_iter().zip(positions) {
        let topic = &request.topics[topic_at];
        let index = topic.partitions[partition_at].partition_index;
        if failed.contains(&(topic.name.0.as_str(), index)) {
            topics[topic_at].partitions[partition_at].error_code = code;
        } else {
            kept_commits.push(commit);
            kept_positions.push((topic_at, partition_at));
        }
    }
    (kept_commits, kept_positions)
}

/// Every requested topic and partition, in request order, with `code`.
fn same_code(request: &OffsetCommitRequest, code: i16) -> Vec<OffsetCommitResponseTopic> {
    request
        .topics
        .iter()
        .map(|topic| {
            OffsetCommitResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            OffsetCommitResponsePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_error_code(code)
                        })
                        .collect(),
                )
        })
        .collect()
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, topics: Vec<OffsetCommitResponseTopic>) -> Result<Bytes> {
    let echoed: usize = topics
        .iter()
        .map(|topic| topic.name.0.len() + topic.partitions.len() * PER_PARTITION_BYTES)
        .sum();
    encode_message(
        &OffsetCommitResponse::default().with_topics(topics),
        version,
        16 + echoed,
    )
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use iggy::prelude::IggyError;
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::{GroupId, TopicName};
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

    use super::*;
    use crate::bridge::BridgeError;
    use crate::protocol::api::ERROR_UNKNOWN_MEMBER_ID;

    fn request(generation_id: i32, member_id: &'static str) -> OffsetCommitRequest {
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("g")))
            .with_generation_id_or_member_epoch(generation_id)
            .with_member_id(StrBytes::from_static_str(member_id))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(5),
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(1)
                            .with_committed_offset(7),
                    ]),
            ])
    }

    fn body(version: i16, request: &OffsetCommitRequest) -> Bytes {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    async fn codes(version: i16, request: &OffsetCommitRequest) -> Vec<(String, i32, i16)> {
        let state = GatewayState::stub(8 * 1024 * 1024);
        let connection = ConnectionState::default();
        let mut response = handle(&state, &connection, None, version, body(version, request))
            .await
            .expect_response("OffsetCommit answers");
        let response = OffsetCommitResponse::decode(&mut response, version).unwrap();
        response
            .topics
            .iter()
            .flat_map(|topic| {
                topic.partitions.iter().map(|partition| {
                    (
                        topic.name.0.to_string(),
                        partition.partition_index,
                        partition.error_code,
                    )
                })
            })
            .collect()
    }

    /// After a gateway restart every group is gone. A member's commit must make it rejoin.
    #[tokio::test]
    async fn given_an_unknown_group_when_a_member_commits_should_answer_unknown_member_id() {
        for version in [2, 8, 9] {
            assert_eq!(
                codes(version, &request(3, "m-1")).await,
                vec![
                    ("orders".to_string(), 0, ERROR_UNKNOWN_MEMBER_ID),
                    ("orders".to_string(), 1, ERROR_UNKNOWN_MEMBER_ID),
                ],
                "v{version}"
            );
        }
    }

    #[tokio::test]
    async fn given_no_bridge_when_committing_should_answer_retriable_on_every_partition() {
        assert_eq!(
            codes(9, &request(-1, "")).await,
            vec![
                ("orders".to_string(), 0, ERROR_COORDINATOR_LOAD_IN_PROGRESS),
                ("orders".to_string(), 1, ERROR_COORDINATOR_LOAD_IN_PROGRESS),
            ]
        );
    }

    #[test]
    fn given_bridge_errors_when_mapped_for_offsets_should_keep_retriable_failures_retriable() {
        for retriable in [
            BridgeError::Timeout,
            BridgeError::Iggy(IggyError::Disconnected),
            BridgeError::Iggy(IggyError::TransientNotCommitted),
            BridgeError::Iggy(IggyError::RequestTooOld),
            BridgeError::Iggy(IggyError::LaneBlocked(7)),
        ] {
            assert_eq!(
                retriable.to_offset_error_code(),
                ERROR_COORDINATOR_LOAD_IN_PROGRESS
            );
        }
        assert_eq!(
            BridgeError::Iggy(IggyError::TooManyConsumerOffsets).to_offset_error_code(),
            ERROR_UNKNOWN_SERVER_ERROR
        );
        assert_eq!(
            BridgeError::Iggy(IggyError::TopicNameNotFound(
                "orders".to_string(),
                "kafka".to_string(),
            ))
            .to_offset_error_code(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    /// A partition left out of the answer reads as committed, so nothing short of a full answer
    /// may go back.
    #[tokio::test]
    async fn given_an_unreadable_request_should_close_rather_than_answer() {
        let state = GatewayState::stub(8 * 1024 * 1024);
        let connection = ConnectionState::default();

        let truncated = body(9, &request(-1, "")).slice(..5);
        assert!(
            handle(&state, &connection, None, 9, truncated)
                .await
                .is_close()
        );
        assert!(
            handle(&state, &connection, None, 10, body(9, &request(-1, "")))
                .await
                .is_close()
        );
    }
}
