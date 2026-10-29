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

//! `DescribeProducers` (API key 61): the producers a partition knows (KIP-664, KW-T step 4).
//!
//! With the start of each one's open transaction, as `kafka-transactions.sh describe-producers`
//! and `find-hanging` read them.
//!
//! From this gateway's producer state (`producer_state.rs`) and the coordinator's transaction
//! index. A producer this gateway has not seen since it started, and whose partition it has not
//! rebuilt from the log yet, is not listed; its last timestamp is not kept (-1).

use bytes::Bytes;
use kafka_protocol::messages::describe_producers_response::{
    PartitionResponse, ProducerState, TopicResponse,
};
use kafka_protocol::messages::{DescribeProducersRequest, DescribeProducersResponse, ProducerId};

use crate::auth::AuthenticatedPrincipal;
use crate::authz::TopicOp;
use crate::protocol::api::{
    ApiVersionRange, ERROR_TOPIC_AUTHORIZATION_FAILED, GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_describe_producers_shape;
use crate::protocol::handlers::txn::decode_txn_request;
use crate::protocol::handlers::{encode_message, may_topic, respond_or_close};
use crate::transactions::{DataPlane, codes};

pub const API_KEY: i16 = 61;

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY,
    min_version: 0,
    max_version: 0,
};

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let (Some(coordinator), Some(bridge)) = (state.transactions.as_ref(), state.bridge.as_ref())
    else {
        return HandleOutcome::Close;
    };
    let request = match decode_txn_request::<DescribeProducersRequest>(
        RANGE,
        api_version,
        body,
        |v, b| validate_describe_producers_shape(v, b, state.max_frame_size),
        |v, _| encode_message(&DescribeProducersResponse::default(), v, 16),
        "DescribeProducers",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        // KW-10: Kafka asks for READ on the topic and answers 29 for each partition of a topic the
        // principal may not read, before it looks the partition up.
        let readable = may_topic(state, principal, topic.name.as_str(), TopicOp::Read);
        let mut partitions = Vec::with_capacity(topic.partition_indexes.len());
        for &index in &topic.partition_indexes {
            let answer = PartitionResponse::default().with_partition_index(index);
            if !readable {
                partitions.push(answer.with_error_code(ERROR_TOPIC_AUTHORIZATION_FAILED));
                continue;
            }
            // Kafka answers for a partition it leads; this gateway reads every partition.
            if let Err(error) = DataPlane::high_watermark(bridge.as_ref(), &topic.name, index).await
            {
                let code = match error.to_kafka_error_code() {
                    codes::UNKNOWN_TOPIC_OR_PARTITION => codes::UNKNOWN_TOPIC_OR_PARTITION,
                    _ => codes::NOT_ENOUGH_REPLICAS,
                };
                partitions.push(answer.with_error_code(code));
                continue;
            }
            let key = (topic.name.to_string(), index);
            let producers = state
                .producer_states
                .producers_on(&topic.name, index)
                .into_iter()
                .map(|(producer_id, epoch, last_sequence)| {
                    let start = coordinator
                        .index()
                        .first_of(&key, producer_id)
                        .and_then(|offset| i64::try_from(offset).ok())
                        .unwrap_or(-1);
                    ProducerState::default()
                        .with_producer_id(ProducerId(producer_id))
                        .with_producer_epoch(i32::from(epoch))
                        .with_last_sequence(last_sequence)
                        .with_last_timestamp(-1)
                        .with_coordinator_epoch(coordinator.config().coordinator_epoch)
                        .with_current_txn_start_offset(start)
                })
                .collect();
            partitions.push(answer.with_active_producers(producers));
        }
        topics.push(
            TopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }
    respond_or_close(
        encode_message(
            &DescribeProducersResponse::default().with_topics(topics),
            api_version,
            256,
        ),
        "DescribeProducers",
    )
}
