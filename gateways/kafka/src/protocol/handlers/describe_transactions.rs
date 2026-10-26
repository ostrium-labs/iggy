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

//! `DescribeTransactions` (API key 65) and `ListTransactions` (API key 66): what
//! `kafka-transactions.sh describe` and `list` show (KIP-664), from the coordinator (KW-T
//! step 4).

use std::collections::BTreeMap;

use bytes::Bytes;
use kafka_protocol::messages::describe_transactions_response::{TopicData, TransactionState};
use kafka_protocol::messages::list_transactions_response::TransactionState as ListedState;
use kafka_protocol::messages::{
    DescribeTransactionsRequest, DescribeTransactionsResponse, ListTransactionsRequest,
    ListTransactionsResponse, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
use crate::protocol::api::{ApiVersionRange, GatewayState, HandleOutcome};
use crate::protocol::bounds_guard::{
    validate_describe_transactions_shape, validate_list_transactions_shape,
};
use crate::protocol::handlers::txn::decode_txn_request;
use crate::protocol::handlers::{encode_message, respond_or_close};
use crate::transactions::coordinator::wall_clock_ms;
use crate::transactions::{TxnMetadata, TxnState, codes};

pub const DESCRIBE_API_KEY: i16 = 65;
pub const LIST_API_KEY: i16 = 66;

pub const DESCRIBE_RANGE: ApiVersionRange = ApiVersionRange {
    api_key: DESCRIBE_API_KEY,
    min_version: 0,
    max_version: 0,
};

/// v2 adds a transactional id pattern, a Java regular expression; not served.
pub const LIST_RANGE: ApiVersionRange = ApiVersionRange {
    api_key: LIST_API_KEY,
    min_version: 0,
    max_version: 1,
};

/// Every state name `ListTransactions` filters on.
const STATES: [TxnState; 7] = [
    TxnState::Empty,
    TxnState::Ongoing,
    TxnState::PrepareCommit,
    TxnState::PrepareAbort,
    TxnState::CompleteCommit,
    TxnState::CompleteAbort,
    TxnState::Dead,
];

#[expect(clippy::unused_async, reason = "the shared handler signature")]
pub async fn describe(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let Some(coordinator) = state.transactions.as_ref() else {
        return HandleOutcome::Close;
    };
    let request = match decode_txn_request::<DescribeTransactionsRequest>(
        DESCRIBE_RANGE,
        api_version,
        body,
        |v, b| validate_describe_transactions_shape(v, b, state.max_frame_size),
        |v, _| encode_message(&DescribeTransactionsResponse::default(), v, 16),
        "DescribeTransactions",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let loaded = coordinator.is_loaded();
    let states = request
        .transactional_ids
        .iter()
        .map(|id| {
            let described = TransactionState::default().with_transactional_id(id.clone());
            if !loaded {
                return described.with_error_code(codes::COORDINATOR_LOAD_IN_PROGRESS);
            }
            if id.is_empty() {
                return described.with_error_code(codes::INVALID_REQUEST);
            }
            match coordinator.describe(id) {
                Some(metadata) if metadata.state != TxnState::Dead => {
                    described_state(described, &metadata)
                }
                _ => described.with_error_code(codes::TRANSACTIONAL_ID_NOT_FOUND),
            }
        })
        .collect();
    respond_or_close(
        encode_message(
            &DescribeTransactionsResponse::default().with_transaction_states(states),
            api_version,
            256,
        ),
        "DescribeTransactions",
    )
}

fn described_state(described: TransactionState, metadata: &TxnMetadata) -> TransactionState {
    let mut topics: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in metadata.partitions.keys() {
        topics.entry(topic.as_str()).or_default().push(*partition);
    }
    described
        .with_error_code(codes::NONE)
        .with_transaction_state(StrBytes::from_static_str(metadata.state.name()))
        .with_transaction_timeout_ms(metadata.timeout_ms)
        .with_transaction_start_time_ms(metadata.start_ms)
        .with_producer_id(ProducerId(metadata.producer_id))
        .with_producer_epoch(metadata.producer_epoch)
        .with_topics(
            topics
                .into_iter()
                .map(|(topic, partitions)| {
                    TopicData::default()
                        .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                        .with_partitions(partitions)
                })
                .collect(),
        )
}

#[expect(clippy::unused_async, reason = "the shared handler signature")]
pub async fn list(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let Some(coordinator) = state.transactions.as_ref() else {
        return HandleOutcome::Close;
    };
    let request = match decode_txn_request::<ListTransactionsRequest>(
        LIST_RANGE,
        api_version,
        body,
        |v, b| validate_list_transactions_shape(v, b, state.max_frame_size),
        encode_list_error,
        "ListTransactions",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    if !coordinator.is_loaded() {
        return respond_or_close(
            encode_list_error(api_version, codes::COORDINATOR_LOAD_IN_PROGRESS),
            "ListTransactions",
        );
    }
    let unknown: Vec<StrBytes> = request
        .state_filters
        .iter()
        .filter(|name| !STATES.iter().any(|known| known.name() == name.as_str()))
        .cloned()
        .collect();
    let now = wall_clock_ms();
    let listed = coordinator
        .list()
        .into_iter()
        .filter(|metadata| metadata.state != TxnState::Dead)
        .filter(|metadata| {
            request.state_filters.is_empty()
                || request
                    .state_filters
                    .iter()
                    .any(|name| name.as_str() == metadata.state.name())
        })
        .filter(|metadata| {
            request.producer_id_filters.is_empty()
                || request
                    .producer_id_filters
                    .iter()
                    .any(|id| id.0 == metadata.producer_id)
        })
        // v1 (KIP-994): only transactions open longer than the filter.
        .filter(|metadata| {
            request.duration_filter < 0
                || now.saturating_sub(metadata.start_ms) >= request.duration_filter
        })
        .map(|metadata| {
            ListedState::default()
                .with_transactional_id(TransactionalId(StrBytes::from_string(
                    metadata.transactional_id.clone(),
                )))
                .with_producer_id(ProducerId(metadata.producer_id))
                .with_transaction_state(StrBytes::from_static_str(metadata.state.name()))
        })
        .collect();
    let response = ListTransactionsResponse::default()
        .with_unknown_state_filters(unknown)
        .with_transaction_states(listed);
    respond_or_close(
        encode_message(&response, api_version, 256),
        "ListTransactions",
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_list_error(version: i16, error_code: i16) -> Result<Bytes> {
    encode_message(
        &ListTransactionsResponse::default().with_error_code(error_code),
        version,
        16,
    )
}
