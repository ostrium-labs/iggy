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

//! Consumer groups scoped per principal (KW-10, owner ruling of 2026-10-25).
//!
//! On a gateway whose listeners name principals, every group API runs in the principal's own
//! namespace ([`GroupScope`]): the group ids a request names are rewritten to their scoped form
//! before the handler sees them, and the ids an answer names are rewritten back, with any group
//! of another namespace left out. The handlers, the coordinator, the membership hash and the Iggy
//! offset groups only ever see scoped ids, so two tenants that both name `orders-app` use two
//! groups, and neither can list, describe, join, commit to or delete the other's.
//!
//! Kafka groups have no Iggy resource, so group access follows topic read access: a principal
//! that may read no topic at all is refused every group API with `GROUP_AUTHORIZATION_FAILED`
//! (30), and an empty list for `ListGroups`. Which topics a member may commit or fetch offsets
//! for is checked per topic by the handlers (`crate::authz`).
//!
//! A request this layer cannot decode is passed on unchanged: the handler decodes it with the
//! same guards, fails the same way and answers as it always does, without acting on any group.

use bytes::Bytes;
use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::describe_groups_response::DescribedGroup;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::OffsetFetchResponseGroup;
use kafka_protocol::messages::{
    BrokerId, DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetDeleteRequest, OffsetDeleteResponse, OffsetFetchRequest,
    OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

use crate::auth::AuthenticatedPrincipal;
use crate::authz::GroupScope;
use crate::protocol::api::{
    API_KEY_DELETE_GROUPS, API_KEY_DESCRIBE_GROUPS, API_KEY_FIND_COORDINATOR, API_KEY_HEARTBEAT,
    API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS, API_KEY_OFFSET_COMMIT,
    API_KEY_OFFSET_DELETE, API_KEY_OFFSET_FETCH, API_KEY_SYNC_GROUP, ConnectionState,
    ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED, GatewayState,
    HandleOutcome,
};
use crate::protocol::bounds_guard::{
    validate_delete_groups_shape, validate_describe_groups_shape, validate_find_coordinator_shape,
    validate_heartbeat_shape, validate_join_group_shape, validate_leave_group_shape,
    validate_list_groups_shape, validate_offset_commit_shape, validate_offset_delete_shape,
    validate_offset_fetch_shape, validate_sync_group_shape,
};
use crate::protocol::handlers::txn::may_use_transactional_ids;
use crate::protocol::handlers::{
    decode_exhaustive, decode_guarded, dispatch_unscoped, encode_message,
};

/// `FindCoordinator`'s key type for a consumer group.
const GROUP_KEY_TYPE: i8 = 0;

/// `FindCoordinator`'s key type for a transactional id, scoped per principal as well (KW-TXP).
const TRANSACTION_KEY_TYPE: i8 = 1;

/// Whether `api_key` names consumer groups, and so runs in the principal's namespace.
pub const fn is_group_api(api_key: i16) -> bool {
    matches!(
        api_key,
        API_KEY_FIND_COORDINATOR
            | API_KEY_JOIN_GROUP
            | API_KEY_HEARTBEAT
            | API_KEY_LEAVE_GROUP
            | API_KEY_SYNC_GROUP
            | API_KEY_DESCRIBE_GROUPS
            | API_KEY_LIST_GROUPS
            | API_KEY_OFFSET_COMMIT
            | API_KEY_OFFSET_FETCH
            | API_KEY_DELETE_GROUPS
            | API_KEY_OFFSET_DELETE
    )
}

fn scoped(scope: &GroupScope, group: &GroupId) -> GroupId {
    GroupId(StrBytes::from_string(scope.scope(group.as_str())))
}

/// The id a client named, for a scoped one of this namespace. An id of another namespace cannot
/// reach an answer, so it is kept as it is only to stay well-formed.
fn unscoped(scope: &GroupScope, group: &GroupId) -> GroupId {
    scope.unscope(group.as_str()).map_or_else(
        || group.clone(),
        |name| GroupId(StrBytes::from_string(name.to_string())),
    )
}

fn respond<T: Encodable>(message: &T, version: i16) -> HandleOutcome {
    encode_message(message, version, 128).map_or(HandleOutcome::Close, HandleOutcome::Respond)
}

/// Rewrites the answer of the scoped request with `rewrite`. An answer that does not decode is
/// passed on as it is: there is nothing scoped in it to hide.
fn rewrite<T: Decodable + Encodable>(
    outcome: HandleOutcome,
    version: i16,
    rewrite: impl FnOnce(&mut T),
) -> HandleOutcome {
    let (body, close) = match outcome {
        HandleOutcome::Respond(body) => (body, false),
        HandleOutcome::RespondThenClose(body) => (body, true),
        other => return other,
    };
    let Ok(mut answer) = decode_exhaustive::<T>(version, body.clone()) else {
        return if close {
            HandleOutcome::RespondThenClose(body)
        } else {
            HandleOutcome::Respond(body)
        };
    };
    rewrite(&mut answer);
    match encode_message(&answer, version, body.len() + 64) {
        Ok(body) if close => HandleOutcome::RespondThenClose(body),
        Ok(body) => HandleOutcome::Respond(body),
        Err(_) => HandleOutcome::Close,
    }
}

/// Runs the group API `api_key` in `principal`'s namespace.
#[allow(clippy::too_many_lines)]
pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: &AuthenticatedPrincipal,
    api_key: i16,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let scope = GroupScope::of(&principal.username);
    let allowed = principal.permissions_known && principal.grants.reads_any_topic();
    let max = state.max_frame_size;
    let version = api_version;
    let pass =
        |body: Bytes| dispatch_unscoped(state, connection, Some(principal), api_key, version, body);

    match api_key {
        API_KEY_FIND_COORDINATOR => {
            let Ok(mut request) =
                decode_guarded::<FindCoordinatorRequest>(version, body.clone(), |v, b| {
                    validate_find_coordinator_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            // KW-TXP: a transactional id is looked up in the principal's namespace too, and Kafka
            // asks for DESCRIBE on it (53). With transactions off the handler refuses every
            // transactional key itself, so the request passes unchanged.
            let transactional =
                request.key_type == TRANSACTION_KEY_TYPE && state.transactions.is_some();
            if request.key_type != GROUP_KEY_TYPE && !transactional {
                return pass(body).await;
            }
            let (allowed, refusal) = if transactional {
                (
                    may_use_transactional_ids(Some(principal)),
                    ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                )
            } else {
                (allowed, ERROR_GROUP_AUTHORIZATION_FAILED)
            };
            if !allowed {
                let refused = |key: StrBytes| {
                    Coordinator::default()
                        .with_key(key)
                        .with_node_id(BrokerId(-1))
                        .with_port(-1)
                        .with_error_code(refusal)
                };
                let answer = FindCoordinatorResponse::default()
                    .with_error_code(refusal)
                    .with_node_id(BrokerId(-1))
                    .with_port(-1)
                    .with_coordinators(request.coordinator_keys.into_iter().map(refused).collect());
                return respond(&answer, version);
            }
            request.key = StrBytes::from_string(scope.scope(request.key.as_str()));
            for key in &mut request.coordinator_keys {
                *key = StrBytes::from_string(scope.scope(key.as_str()));
            }
            let Some(body) = encode_request(&request, version) else {
                return HandleOutcome::Close;
            };
            rewrite::<FindCoordinatorResponse>(pass(body).await, version, |answer| {
                for coordinator in &mut answer.coordinators {
                    if let Some(key) = scope.unscope(coordinator.key.as_str()) {
                        coordinator.key = StrBytes::from_string(key.to_string());
                    }
                }
            })
        }
        API_KEY_JOIN_GROUP => {
            let Ok(mut request) =
                decode_guarded::<JoinGroupRequest>(version, body.clone(), |v, b| {
                    validate_join_group_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let answer = JoinGroupResponse::default()
                    .with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED)
                    .with_generation_id(-1)
                    .with_member_id(request.member_id);
                return respond(&answer, version);
            }
            request.group_id = scoped(&scope, &request.group_id);
            match encode_request(&request, version) {
                Some(body) => pass(body).await,
                None => HandleOutcome::Close,
            }
        }
        API_KEY_HEARTBEAT => {
            let Ok(mut request) =
                decode_guarded::<HeartbeatRequest>(version, body.clone(), validate_heartbeat_shape)
            else {
                return pass(body).await;
            };
            if !allowed {
                let answer =
                    HeartbeatResponse::default().with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED);
                return respond(&answer, version);
            }
            request.group_id = scoped(&scope, &request.group_id);
            match encode_request(&request, version) {
                Some(body) => pass(body).await,
                None => HandleOutcome::Close,
            }
        }
        API_KEY_LEAVE_GROUP => {
            let Ok(mut request) =
                decode_guarded::<LeaveGroupRequest>(version, body.clone(), |v, b| {
                    validate_leave_group_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let answer =
                    LeaveGroupResponse::default().with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED);
                return respond(&answer, version);
            }
            request.group_id = scoped(&scope, &request.group_id);
            match encode_request(&request, version) {
                Some(body) => pass(body).await,
                None => HandleOutcome::Close,
            }
        }
        API_KEY_SYNC_GROUP => {
            let Ok(mut request) =
                decode_guarded::<SyncGroupRequest>(version, body.clone(), |v, b| {
                    validate_sync_group_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let answer =
                    SyncGroupResponse::default().with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED);
                return respond(&answer, version);
            }
            request.group_id = scoped(&scope, &request.group_id);
            match encode_request(&request, version) {
                Some(body) => pass(body).await,
                None => HandleOutcome::Close,
            }
        }
        API_KEY_DESCRIBE_GROUPS => {
            let Ok(mut request) =
                decode_guarded::<DescribeGroupsRequest>(version, body.clone(), |v, b| {
                    validate_describe_groups_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let groups = request
                    .groups
                    .into_iter()
                    .map(|group| {
                        DescribedGroup::default()
                            .with_group_id(group)
                            .with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED)
                    })
                    .collect();
                return respond(
                    &DescribeGroupsResponse::default().with_groups(groups),
                    version,
                );
            }
            for group in &mut request.groups {
                *group = scoped(&scope, group);
            }
            let Some(body) = encode_request(&request, version) else {
                return HandleOutcome::Close;
            };
            rewrite::<DescribeGroupsResponse>(pass(body).await, version, |answer| {
                for group in &mut answer.groups {
                    group.group_id = unscoped(&scope, &group.group_id);
                }
            })
        }
        API_KEY_LIST_GROUPS => {
            // Decoded only to refuse what the handler would refuse, before anything is listed.
            let Ok(_request) =
                decode_guarded::<ListGroupsRequest>(version, body.clone(), |v, b| {
                    validate_list_groups_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                return respond(&ListGroupsResponse::default(), version);
            }
            rewrite::<ListGroupsResponse>(pass(body).await, version, |answer| {
                answer.groups.retain_mut(|group| {
                    let Some(name) = scope.unscope(group.group_id.as_str()) else {
                        return false;
                    };
                    group.group_id = GroupId(StrBytes::from_string(name.to_string()));
                    true
                });
            })
        }
        API_KEY_OFFSET_COMMIT => {
            let Ok(mut request) =
                decode_guarded::<OffsetCommitRequest>(version, body.clone(), |v, b| {
                    validate_offset_commit_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let topics = request
                    .topics
                    .into_iter()
                    .map(|topic| {
                        OffsetCommitResponseTopic::default()
                            .with_name(topic.name)
                            .with_partitions(
                                topic
                                    .partitions
                                    .into_iter()
                                    .map(|partition| {
                                        OffsetCommitResponsePartition::default()
                                            .with_partition_index(partition.partition_index)
                                            .with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED)
                                    })
                                    .collect(),
                            )
                    })
                    .collect();
                return respond(
                    &OffsetCommitResponse::default().with_topics(topics),
                    version,
                );
            }
            request.group_id = scoped(&scope, &request.group_id);
            match encode_request(&request, version) {
                Some(body) => pass(body).await,
                None => HandleOutcome::Close,
            }
        }
        API_KEY_OFFSET_FETCH => {
            let Ok(mut request) =
                decode_guarded::<OffsetFetchRequest>(version, body.clone(), |v, b| {
                    validate_offset_fetch_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let groups = request
                    .groups
                    .into_iter()
                    .map(|group| {
                        OffsetFetchResponseGroup::default()
                            .with_group_id(group.group_id)
                            .with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED)
                    })
                    .collect();
                let answer = OffsetFetchResponse::default()
                    .with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED)
                    .with_groups(groups);
                return respond(&answer, version);
            }
            request.group_id = scoped(&scope, &request.group_id);
            for group in &mut request.groups {
                group.group_id = scoped(&scope, &group.group_id);
            }
            let Some(body) = encode_request(&request, version) else {
                return HandleOutcome::Close;
            };
            rewrite::<OffsetFetchResponse>(pass(body).await, version, |answer| {
                for group in &mut answer.groups {
                    group.group_id = unscoped(&scope, &group.group_id);
                }
            })
        }
        API_KEY_DELETE_GROUPS => {
            let Ok(mut request) =
                decode_guarded::<DeleteGroupsRequest>(version, body.clone(), |v, b| {
                    validate_delete_groups_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let results = request
                    .groups_names
                    .into_iter()
                    .map(|group| {
                        DeletableGroupResult::default()
                            .with_group_id(group)
                            .with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED)
                    })
                    .collect();
                return respond(
                    &DeleteGroupsResponse::default().with_results(results),
                    version,
                );
            }
            for group in &mut request.groups_names {
                *group = scoped(&scope, group);
            }
            let Some(body) = encode_request(&request, version) else {
                return HandleOutcome::Close;
            };
            rewrite::<DeleteGroupsResponse>(pass(body).await, version, |answer| {
                for result in &mut answer.results {
                    result.group_id = unscoped(&scope, &result.group_id);
                }
            })
        }
        API_KEY_OFFSET_DELETE => {
            let Ok(mut request) =
                decode_guarded::<OffsetDeleteRequest>(version, body.clone(), |v, b| {
                    validate_offset_delete_shape(v, b, max)
                })
            else {
                return pass(body).await;
            };
            if !allowed {
                let answer = OffsetDeleteResponse::default()
                    .with_error_code(ERROR_GROUP_AUTHORIZATION_FAILED);
                return respond(&answer, version);
            }
            request.group_id = scoped(&scope, &request.group_id);
            match encode_request(&request, version) {
                Some(body) => pass(body).await,
                None => HandleOutcome::Close,
            }
        }
        _ => pass(body).await,
    }
}

/// A rewritten request, encoded at the version it came at.
fn encode_request<T: Encodable>(request: &T, version: i16) -> Option<Bytes> {
    encode_message(request, version, 256).ok()
}
