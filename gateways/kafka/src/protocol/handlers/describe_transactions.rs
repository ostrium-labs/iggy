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

use crate::auth::AuthenticatedPrincipal;
use crate::authz::TopicOp;
use crate::error::Result;
use crate::protocol::api::{
    ApiVersionRange, ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED, GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::{
    validate_describe_transactions_shape, validate_list_transactions_shape,
};
use crate::protocol::handlers::txn::{
    coordinator_key, decode_txn_request, may_use_transactional_ids, visible_id,
};
use crate::protocol::handlers::{encode_message, may_topic, respond_or_close};
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

/// `DescribeTransactions`. KW-TXP: each id is looked up in the caller's own namespace, so a
/// principal can only ever describe its own transactions, and the topics shown are those it may
/// describe, as Kafka's `KafkaApis` filters them. A principal that may not use transactional ids
/// is answered `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) for each id.
#[expect(clippy::unused_async, reason = "the shared handler signature")]
pub async fn describe(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
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
    let authorized = may_use_transactional_ids(principal);
    let states = request
        .transactional_ids
        .iter()
        .map(|id| {
            let described = TransactionState::default().with_transactional_id(id.clone());
            // Kafka checks DESCRIBE on each id before it asks the coordinator.
            if !authorized {
                return described.with_error_code(ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
            }
            if !loaded {
                return described.with_error_code(codes::COORDINATOR_LOAD_IN_PROGRESS);
            }
            if id.is_empty() {
                return described.with_error_code(codes::INVALID_REQUEST);
            }
            match coordinator.describe(&coordinator_key(principal, id.as_str())) {
                Some(metadata) if metadata.state != TxnState::Dead => {
                    described_state(described, &metadata, |topic| {
                        may_topic(state, principal, topic, TopicOp::Describe)
                    })
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

/// `described` filled from `metadata`, showing only the topics `may_describe` lets through.
fn described_state(
    described: TransactionState,
    metadata: &TxnMetadata,
    may_describe: impl Fn(&str) -> bool,
) -> TransactionState {
    let mut topics: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in metadata.partitions.keys() {
        topics.entry(topic.as_str()).or_default().push(*partition);
    }
    topics.retain(|topic, _| may_describe(topic));
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

/// `ListTransactions`. KW-TXP: only the caller's own transactions are listed, by the ids it
/// named them by; Kafka lists only the ids the caller may describe, and a principal that may not
/// use transactional ids gets an empty list.
#[expect(clippy::unused_async, reason = "the shared handler signature")]
pub async fn list(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
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
    let authorized = may_use_transactional_ids(principal);
    let listed = coordinator
        .list()
        .into_iter()
        .filter(|_| authorized)
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
        .filter_map(|metadata| {
            // Another principal's transaction stays out of the answer altogether.
            let id = visible_id(principal, &metadata.transactional_id)?;
            Some(
                ListedState::default()
                    .with_transactional_id(TransactionalId(StrBytes::from_string(id.to_string())))
                    .with_producer_id(ProducerId(metadata.producer_id))
                    .with_transaction_state(StrBytes::from_static_str(metadata.state.name())),
            )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactions::metadata::PartitionStart;

    #[test]
    fn a_described_transaction_shows_only_the_topics_the_caller_may_describe() {
        let mut metadata = TxnMetadata::new("5:alice/orders-tx", 7, 60_000, 0);
        metadata.state = TxnState::Ongoing;
        for (topic, partition) in [("orders", 0), ("orders", 1), ("secret", 0)] {
            metadata.partitions.insert(
                (topic.to_string(), partition),
                PartitionStart { start: None },
            );
        }
        let described = described_state(
            TransactionState::default()
                .with_transactional_id(TransactionalId(StrBytes::from_static_str("orders-tx"))),
            &metadata,
            |topic| topic != "secret",
        );
        assert_eq!(described.error_code, codes::NONE);
        assert_eq!(described.transactional_id.as_str(), "orders-tx");
        assert_eq!(described.producer_id.0, 7);
        let topics: Vec<(&str, &[i32])> = described
            .topics
            .iter()
            .map(|topic| (topic.topic.as_str(), topic.partitions.as_slice()))
            .collect();
        assert_eq!(topics, [("orders", &[0, 1][..])]);
    }
}
