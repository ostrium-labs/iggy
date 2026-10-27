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

//! `ConsumerGroupDescribe` (API key 69): what `kafka-consumer-groups.sh --describe` and the Java
//! admin client's `describeConsumerGroups` ask a Kafka 4 broker first.

use bytes::Bytes;
use kafka_protocol::messages::consumer_group_describe_response::{
    Assignment, DescribedGroup, Member, TopicPartitions,
};
use kafka_protocol::messages::{
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, GroupId, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
use crate::group::consumer_protocol::{self, ConsumerGroup};
use crate::group::is_valid_group_id;
use crate::protocol::api::{
    API_KEY_CONSUMER_GROUP_DESCRIBE, ApiVersionRange, ERROR_INVALID_GROUP_ID, GatewayState,
    HandleOutcome,
};
use crate::protocol::bounds_guard::validate_consumer_group_describe_shape;
use crate::protocol::handlers::{decode_guarded, decode_request, encode_message, respond_or_close};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_CONSUMER_GROUP_DESCRIBE,
    min_version: 0,
    max_version: 1,
};

/// KIP-1099 `MemberType`: the member speaks the consumer protocol.
const MEMBER_TYPE_CONSUMER: i8 = 1;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let decoded = decode_request(
        API_KEY_CONSUMER_GROUP_DESCRIBE,
        api_version,
        body,
        |version, body| {
            decode_guarded::<ConsumerGroupDescribeRequest>(version, body, |version, body| {
                validate_consumer_group_describe_shape(version, body, state.max_frame_size)
            })
        },
        encode_error_response,
        "ConsumerGroupDescribe",
    );
    let request = match decoded {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let ids: Vec<StrBytes> = request.group_ids.iter().map(|id| id.0.clone()).collect();
    let found = state.groups.describe_consumer_groups(&ids).await;
    let groups = ids
        .iter()
        .zip(found)
        .map(|(group_id, found)| {
            if !is_valid_group_id(group_id) {
                return DescribedGroup::default()
                    .with_group_id(GroupId(group_id.clone()))
                    .with_error_code(ERROR_INVALID_GROUP_ID);
            }
            match found {
                Ok(snapshot) => described(state, group_id, snapshot.state, &snapshot.group),
                Err((error_code, message)) => DescribedGroup::default()
                    .with_group_id(GroupId(group_id.clone()))
                    .with_error_code(error_code)
                    .with_error_message(Some(StrBytes::from_string(message))),
            }
        })
        .collect();
    let response = ConsumerGroupDescribeResponse::default().with_groups(groups);
    respond_or_close(
        encode_message(&response, api_version, 256),
        "ConsumerGroupDescribe",
    )
}

fn described(
    state: &GatewayState,
    group_id: &StrBytes,
    group_state: consumer_protocol::GroupState,
    group: &ConsumerGroup,
) -> DescribedGroup {
    let assignment = |assignment: &consumer_protocol::Assignment| {
        Assignment::default().with_topic_partitions(
            assignment
                .iter()
                .map(|(topic_id, partitions)| {
                    let name = group
                        .topic_name(topic_id)
                        .map(ToString::to_string)
                        .or_else(|| state.topic_ids.name(topic_id))
                        .unwrap_or_default();
                    TopicPartitions::default()
                        .with_topic_id(*topic_id)
                        .with_topic_name(TopicName(StrBytes::from_string(name)))
                        .with_partitions(partitions.iter().copied().collect())
                })
                .collect(),
        )
    };
    let members = group
        .members
        .values()
        .map(|member| {
            Member::default()
                .with_member_id(StrBytes::from_string(member.member_id.clone()))
                .with_instance_id(member.instance_id.clone().map(StrBytes::from_string))
                .with_rack_id(member.rack_id.clone().map(StrBytes::from_string))
                .with_member_epoch(member.member_epoch)
                .with_client_id(StrBytes::from_string(member.client_id.clone()))
                .with_client_host(StrBytes::from_string(member.client_host.clone()))
                .with_subscribed_topic_names(
                    member
                        .subscribed_names
                        .iter()
                        .map(|name| TopicName(StrBytes::from_string(name.clone())))
                        .collect(),
                )
                .with_subscribed_topic_regex(
                    member.subscribed_regex.clone().map(StrBytes::from_string),
                )
                .with_assignment(assignment(&member.assigned))
                .with_target_assignment(assignment(
                    &group
                        .target
                        .get(&member.member_id)
                        .cloned()
                        .unwrap_or_default(),
                ))
                .with_member_type(MEMBER_TYPE_CONSUMER)
        })
        .collect();
    DescribedGroup::default()
        .with_group_id(GroupId(group_id.clone()))
        .with_group_state(StrBytes::from_static_str(group_state.name()))
        .with_group_epoch(group.group_epoch)
        .with_assignment_epoch(group.assignment_epoch)
        .with_assignor_name(StrBytes::from_static_str(group.assignor.name()))
        .with_members(members)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let response = ConsumerGroupDescribeResponse::default()
        .with_groups(vec![DescribedGroup::default().with_error_code(error_code)]);
    encode_message(&response, version, 64)
}
