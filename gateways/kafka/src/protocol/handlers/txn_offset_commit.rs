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

//! `TxnOffsetCommit` (API key 28): a consumer group's offsets, stored in the producer's
//! transaction and committed with it (KW-T step 3, KIP-98 and KIP-447).
//!
//! From v3 the request names the consumer's member and generation, which are checked as Kafka's
//! group coordinator checks them (`doTxnCommitOffsets`): an unknown member, a stale generation or
//! a fenced static member is refused. Before v3, and from a producer that names no member, the
//! commit is taken as a standalone one.

use std::collections::BTreeMap;

use bytes::Bytes;
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::{TxnOffsetCommitRequest, TxnOffsetCommitResponse};

use crate::error::Result;
use crate::group::CommitRequest;
use crate::protocol::api::{ApiVersionRange, GatewayState, HandleOutcome};
use crate::protocol::bounds_guard::validate_txn_offset_commit_shape;
use crate::protocol::handlers::txn::decode_txn_request;
use crate::protocol::handlers::{encode_message, respond_or_close};
use crate::transactions::{TopicPartition, codes};

pub const API_KEY: i16 = 28;

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY,
    min_version: 0,
    max_version: 4,
};

/// The first version that names the consumer's member and generation (KIP-447).
const MEMBER_VERSION: i16 = 3;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let Some(coordinator) = state.transactions.as_ref() else {
        return HandleOutcome::Close;
    };
    let request = match decode_txn_request::<TxnOffsetCommitRequest>(
        RANGE,
        api_version,
        body,
        |v, b| validate_txn_offset_commit_shape(v, b, state.max_frame_size),
        |v, _| encode_message(&TxnOffsetCommitResponse::default(), v, 16),
        "TxnOffsetCommit",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let code = commit(state, coordinator, api_version, &request).await;
    respond_or_close(
        encode_response(api_version, &request, code),
        "TxnOffsetCommit",
    )
}

async fn commit(
    state: &GatewayState,
    coordinator: &std::sync::Arc<crate::transactions::TransactionCoordinator>,
    api_version: i16,
    request: &TxnOffsetCommitRequest,
) -> i16 {
    let transactional_id = request.transactional_id.to_string();
    if transactional_id.is_empty() || request.group_id.is_empty() {
        return codes::INVALID_REQUEST;
    }
    // The member is checked as the group coordinator checks it; a producer that names none
    // commits standalone, as Kafka allows.
    let names_member = api_version >= MEMBER_VERSION
        && (!request.member_id.is_empty() || request.generation_id >= 0);
    let _running = if names_member {
        match state
            .groups
            .validate_commit(&CommitRequest {
                group_id: request.group_id.0.clone(),
                generation_id: request.generation_id,
                member_id: request.member_id.clone(),
                group_instance_id: request.group_instance_id.clone(),
            })
            .await
        {
            Ok(running) => Some(running),
            Err(code) => return code,
        }
    } else {
        None
    };
    let offsets: BTreeMap<TopicPartition, i64> = request
        .topics
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|partition| {
                (
                    (topic.name.to_string(), partition.partition_index),
                    partition.committed_offset,
                )
            })
        })
        .collect();
    coordinator
        .txn_offset_commit(
            &transactional_id,
            request.producer_id.0,
            request.producer_epoch,
            &request.group_id,
            &offsets,
        )
        .await
        .err()
        .unwrap_or(codes::NONE)
}

/// Every requested partition with `code`: a transaction's offsets are stored together or not at
/// all.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, request: &TxnOffsetCommitRequest, code: i16) -> Result<Bytes> {
    let topics = request
        .topics
        .iter()
        .map(|topic| {
            TxnOffsetCommitResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            TxnOffsetCommitResponsePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_error_code(code)
                        })
                        .collect(),
                )
        })
        .collect();
    let capacity = 16 + request.topics.len() * 64;
    encode_message(
        &TxnOffsetCommitResponse::default().with_topics(topics),
        version,
        capacity,
    )
}
