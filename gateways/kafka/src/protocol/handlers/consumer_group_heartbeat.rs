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

//! `ConsumerGroupHeartbeat` (API key 68): the KIP-848 consumer group protocol.
//!
//! The handler resolves the topics the group subscribes to (by name, or every topic for a regex
//! subscription), then hands the heartbeat to the coordinator, which runs the state machine in
//! `group::consumer_protocol`. Assignments name topics by id, so every topic resolved here has
//! its id registered for the Fetch, Metadata and `OffsetCommit` requests that follow.

use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::consumer_group_heartbeat_response::{Assignment, TopicPartitions};
use kafka_protocol::messages::{ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse};
use kafka_protocol::protocol::StrBytes;

use crate::bridge::IggyBridge;
use crate::error::Result;
use crate::group::consumer_protocol::{self, Heartbeat, HeartbeatAnswer, TopicMeta, TopicsImage};
use crate::protocol::api::{
    API_KEY_CONSUMER_GROUP_HEARTBEAT, ApiVersionRange, ERROR_COORDINATOR_NOT_AVAILABLE,
    GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_consumer_group_heartbeat_shape;
use crate::protocol::handlers::{decode_guarded, decode_request, encode_message, respond_or_close};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_CONSUMER_GROUP_HEARTBEAT,
    min_version: 0,
    max_version: 1,
};

/// How long a heartbeat waits for Iggy to resolve the subscribed topics. A heartbeat that cannot
/// get them goes on with what the group saw last, well inside the member's session.
const RESOLVE_WAIT: Duration = Duration::from_secs(3);

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let decoded = decode_request(
        API_KEY_CONSUMER_GROUP_HEARTBEAT,
        api_version,
        body,
        |version, body| {
            decode_guarded::<ConsumerGroupHeartbeatRequest>(version, body, |version, body| {
                validate_consumer_group_heartbeat_shape(version, body)
            })
        },
        encode_error_response,
        "ConsumerGroupHeartbeat",
    );
    let request = match decoded {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let Some(bridge) = &state.bridge else {
        // No Iggy, no topics to assign. Retriable: the client keeps heartbeating.
        return respond_or_close(
            encode_error_response(api_version, ERROR_COORDINATOR_NOT_AVAILABLE),
            "ConsumerGroupHeartbeat",
        );
    };
    let heartbeat = heartbeat_of(api_version, &request);
    let interest = state.groups.consumer_interest(&heartbeat).await;
    let image = resolve(state, bridge, &interest).await;
    let answer = state.groups.consumer_heartbeat(&heartbeat, image).await;
    respond_or_close(encode_answer(api_version, &answer), "ConsumerGroupHeartbeat")
}

fn heartbeat_of(version: i16, request: &ConsumerGroupHeartbeatRequest) -> Heartbeat {
    Heartbeat {
        version,
        group_id: request.group_id.0.to_string(),
        member_id: request.member_id.to_string(),
        member_epoch: request.member_epoch,
        instance_id: request.instance_id.as_ref().map(ToString::to_string),
        rack_id: request.rack_id.as_ref().map(ToString::to_string),
        rebalance_timeout_ms: request.rebalance_timeout_ms,
        subscribed_topic_names: request
            .subscribed_topic_names
            .as_ref()
            .map(|names| names.iter().map(|name| name.0.to_string()).collect()),
        subscribed_topic_regex: request
            .subscribed_topic_regex
            .as_ref()
            .map(ToString::to_string),
        server_assignor: request.server_assignor.as_ref().map(ToString::to_string),
        owned: request.topic_partitions.as_ref().map(|owned| {
            owned
                .iter()
                .map(|topic| (topic.topic_id, topic.partitions.iter().copied().collect()))
                .collect()
        }),
        // KW-12 carries the request header's client id and the peer address to handlers.
        client_id: String::new(),
        client_host: String::new(),
    }
}

/// The subscribed topics, by name, with their ids registered. `None` when Iggy did not answer.
async fn resolve(
    state: &GatewayState,
    bridge: &IggyBridge,
    interest: &consumer_protocol::Interest,
) -> Option<TopicsImage> {
    let topics = if interest.needs_all_topics {
        tokio::time::timeout(RESOLVE_WAIT, bridge.list_kafka_topics())
            .await
            .ok()?
            .ok()?
    } else {
        if interest.names.is_empty() {
            return Some(TopicsImage::new());
        }
        let names: Vec<StrBytes> = interest
            .names
            .iter()
            .map(|name| StrBytes::from_string(name.clone()))
            .collect();
        tokio::time::timeout(RESOLVE_WAIT, bridge.get_kafka_topics(&names))
            .await
            .ok()?
            .ok()?
            .into_iter()
            .filter_map(|(_, metadata)| metadata)
            .collect()
    };
    let mut image = TopicsImage::new();
    for topic in topics {
        state.topic_ids.register(&topic.kafka_topic, topic.topic_id);
        image.insert(
            topic.kafka_topic.clone(),
            TopicMeta {
                name: topic.kafka_topic,
                id: topic.topic_id,
                partitions: topic.partitions_count,
            },
        );
    }
    Some(image)
}

fn encode_answer(version: i16, answer: &HeartbeatAnswer) -> Result<Bytes> {
    let response = ConsumerGroupHeartbeatResponse::default()
        .with_error_code(answer.error_code)
        .with_error_message(answer.error_message.clone().map(StrBytes::from_string))
        .with_member_id(answer.member_id.clone().map(StrBytes::from_string))
        .with_member_epoch(answer.member_epoch)
        .with_heartbeat_interval_ms(answer.heartbeat_interval_ms)
        .with_assignment(answer.assignment.as_ref().map(|assignment| {
            Assignment::default().with_topic_partitions(
                assignment
                    .iter()
                    .map(|(topic_id, partitions)| {
                        TopicPartitions::default()
                            .with_topic_id(*topic_id)
                            .with_partitions(partitions.iter().copied().collect())
                    })
                    .collect(),
            )
        }));
    encode_message(&response, version, 256)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let response = ConsumerGroupHeartbeatResponse::default()
        .with_error_code(error_code)
        .with_member_epoch(0);
    encode_message(&response, version, 64)
}
