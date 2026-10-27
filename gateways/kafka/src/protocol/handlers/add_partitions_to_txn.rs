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

//! `AddPartitionsToTxn` (API key 24), the client form (v0 to v3) only: v4 and later batch the
//! partition leaders' verification calls, which this gateway makes in process (KW-T).

use bytes::Bytes;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse};

use crate::auth::AuthenticatedPrincipal;
use crate::authz::TopicOp;
use crate::error::Result;
use crate::protocol::api::{
    ApiVersionRange, ERROR_TOPIC_AUTHORIZATION_FAILED, ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
    GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_add_partitions_to_txn_shape;
use crate::protocol::handlers::txn::{
    coordinator_key, decode_txn_request, may_use_transactional_ids,
};
use crate::protocol::handlers::{encode_message, may_topic, respond_or_close};
use crate::transactions::{AddError, TopicPartition, codes};

pub const API_KEY: i16 = 24;

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY,
    min_version: 0,
    max_version: 3,
};

/// The first version whose clients know `PRODUCER_FENCED` (KIP-588).
const PRODUCER_FENCED_VERSION: i16 = 2;

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let Some(coordinator) = state.transactions.as_ref() else {
        return HandleOutcome::Close;
    };
    let request = match decode_txn_request::<AddPartitionsToTxnRequest>(
        RANGE,
        api_version,
        body,
        |v, b| validate_add_partitions_to_txn_shape(v, b, state.max_frame_size),
        encode_error_response,
        "AddPartitionsToTxn",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let partitions: Vec<TopicPartition> = request
        .v3_and_below_topics
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(|partition| (topic.name.to_string(), *partition))
        })
        .collect();
    // KW-TXP: the id is the principal's own, in its namespace.
    let transactional_id =
        coordinator_key(principal, request.v3_and_below_transactional_id.as_str());
    // KW-10: a transaction may only take in partitions its principal may write, since committing
    // it writes a marker to each. As in Kafka, the refused ones answer 29 and the rest 55.
    let refused: std::collections::BTreeMap<TopicPartition, i16> = partitions
        .iter()
        .filter(|(topic, _)| !may_topic(state, principal, topic, TopicOp::Write))
        .map(|partition| (partition.clone(), ERROR_TOPIC_AUTHORIZATION_FAILED))
        .collect();
    // As Kafka's `KafkaApis`: WRITE on the transactional id first, for every partition (53).
    let outcome = if !may_use_transactional_ids(principal) {
        Err(AddError::All(ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
    } else if transactional_id.is_empty() {
        Err(AddError::All(codes::INVALID_REQUEST))
    } else if !refused.is_empty() {
        Err(AddError::Partitions(refused))
    } else if let Some(negative) = partitions.iter().find(|(_, partition)| *partition < 0) {
        Err(AddError::Partitions(
            [(negative.clone(), codes::UNKNOWN_TOPIC_OR_PARTITION)].into(),
        ))
    } else {
        coordinator
            .add_partitions(
                &transactional_id,
                request.v3_and_below_producer_id.0,
                request.v3_and_below_producer_epoch,
                &partitions,
                principal.map(|principal| principal.username.as_str()),
            )
            .await
    };
    respond_or_close(
        encode_response(api_version, &request, &outcome),
        "AddPartitionsToTxn",
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(
    version: i16,
    request: &AddPartitionsToTxnRequest,
    outcome: &std::result::Result<(), AddError>,
) -> Result<Bytes> {
    let code_of = |topic: &str, partition: i32| match outcome {
        Ok(()) => codes::NONE,
        Err(AddError::All(code)) => codes::fenced_for(*code, version >= PRODUCER_FENCED_VERSION),
        Err(AddError::Partitions(refused)) => refused
            .get(&(topic.to_string(), partition))
            .copied()
            .unwrap_or(codes::OPERATION_NOT_ATTEMPTED),
    };
    let results = request
        .v3_and_below_topics
        .iter()
        .map(|topic| {
            AddPartitionsToTxnTopicResult::default()
                .with_name(topic.name.clone())
                .with_results_by_partition(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            AddPartitionsToTxnPartitionResult::default()
                                .with_partition_index(*partition)
                                .with_partition_error_code(code_of(&topic.name, *partition))
                        })
                        .collect(),
                )
        })
        .collect();
    let response =
        AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(results);
    let capacity = 64 + request.v3_and_below_topics.len() * 64;
    encode_message(&response, version, capacity)
}

/// An `AddPartitionsToTxn` answer with no partition, for a request that did not decode.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, _error_code: i16) -> Result<Bytes> {
    // v0 to v3 carry their codes per partition only, and no partition decoded.
    encode_message(&AddPartitionsToTxnResponse::default(), version, 16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::{ProducerId, TopicName, TransactionalId};
    use kafka_protocol::protocol::{Decodable, StrBytes};

    fn request() -> AddPartitionsToTxnRequest {
        AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str("tx")))
            .with_v3_and_below_producer_id(ProducerId(7))
            .with_v3_and_below_producer_epoch(0)
            .with_v3_and_below_topics(vec![
                AddPartitionsToTxnTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("a")))
                    .with_partitions(vec![0, 1]),
            ])
    }

    fn codes_of(version: i16, outcome: &std::result::Result<(), AddError>) -> Vec<i16> {
        let mut bytes = encode_response(version, &request(), outcome).unwrap();
        let response = AddPartitionsToTxnResponse::decode(&mut bytes, version).unwrap();
        response.results_by_topic_v3_and_below[0]
            .results_by_partition
            .iter()
            .map(|result| result.partition_error_code)
            .collect()
    }

    #[test]
    fn given_an_outcome_should_answer_every_partition() {
        for version in 0..=3 {
            assert_eq!(codes_of(version, &Ok(())), [0, 0]);
        }
        let refused = Err(AddError::Partitions([(("a".to_string(), 1), 3)].into()));
        assert_eq!(codes_of(3, &refused), [codes::OPERATION_NOT_ATTEMPTED, 3]);
    }

    #[test]
    fn given_an_unauthorized_transactional_id_should_answer_53_for_every_partition() {
        let refused = Err(AddError::All(ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        for version in 0..=3 {
            assert_eq!(codes_of(version, &refused), [53, 53]);
        }
    }

    #[test]
    fn given_a_fenced_producer_should_answer_47_before_v2_and_90_after() {
        let fenced = Err(AddError::All(codes::PRODUCER_FENCED));
        assert_eq!(codes_of(1, &fenced), [47, 47]);
        assert_eq!(codes_of(2, &fenced), [90, 90]);
    }
}
