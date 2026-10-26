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

//! `DeleteTopics` (API key 20).
//!
//! Deletes the Iggy topic a Kafka topic name resolves to. Never deletes the backing Iggy
//! stream - see [`IggyBridge::delete_kafka_topic`]'s own doc comment for why: the shared
//! `default_stream` can hold other Kafka topics' data, and this bridge has no way to tell
//! whether the stream it just emptied is actually abandoned or just temporarily topic-less.
//!
//! `DeletableTopicResult.error_message` is only encoded on the wire at v5+ (`kafka_protocol`'s
//! own gate) - a client negotiating v1-v4 gets `error_code` only, with any text this module sets
//! silently dropped. Unlike `CreateTopics`, whose equivalent field is encoded at every version it
//! supports.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use iggy::prelude::IggyError;
use kafka_protocol::messages::delete_topics_request::DeleteTopicState;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::{DeleteTopicsRequest, DeleteTopicsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use uuid::Uuid;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::{BridgeError, IggyBridge};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_DELETE_TOPICS, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION, ERROR_REQUEST_TIMED_OUT,
    ERROR_TOPIC_AUTHORIZATION_FAILED, GatewayState, HandleOutcome, REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_delete_topics_shape;
use crate::protocol::handlers::{
    clamp_request_timeout, decode_guarded, encode_message, handle_versioned_request,
    is_supported_version, learn_topic_ids, respond_or_close, unsupported_version_response,
};

/// v1 to v6. v1 is the crate's own floor (`kafka_protocol` does not implement v0 for this message).
///
/// v6 names each topic by name or by id (KIP-516, `topics: Vec<DeleteTopicState>`); ids resolve
/// through `GatewayState::topic_ids`.
pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DELETE_TOPICS,
    min_version: 1,
    max_version: 6,
};

/// First version that may name a topic by id.
const TOPIC_ID_MIN_VERSION: i16 = 6;

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let Some(bridge) = &state.bridge else {
        return handle_versioned_request(
            API_KEY_DELETE_TOPICS,
            api_version,
            body,
            |v, b| {
                decode_guarded::<DeleteTopicsRequest>(v, b, |v, b| {
                    validate_delete_topics_shape(v, b, state.max_frame_size)
                })
            },
            encode_response,
            encode_error_response,
            "DeleteTopics",
        );
    };

    if !is_supported_version(API_KEY_DELETE_TOPICS, api_version) {
        return unsupported_version_response(API_KEY_DELETE_TOPICS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<DeleteTopicsRequest>(api_version, body, |v, b| {
        validate_delete_topics_shape(v, b, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, "Failed to decode DeleteTopics request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "DeleteTopics",
            );
        }
    };

    // From v6 each topic is a name or an id. Those that resolve to a name go through the same
    // path as a v1-v5 name list; the rest are answered here.
    let (topic_names, mut answered) = if api_version >= TOPIC_ID_MIN_VERSION {
        name_topics(state, bridge, &req.topics).await
    } else {
        (req.topic_names.clone(), Vec::new())
    };

    if let Some(mut results) = authorize(principal, &topic_names) {
        results.append(&mut answered);
        let resp = DeleteTopicsResponse::default().with_responses(results);
        return respond_or_close(encode_message(&resp, api_version, 256), "DeleteTopics");
    }

    // The cap `CreateTopics` applies (`IGGY_KAFKA_MAX_ADMIN_TOPICS_PER_REQUEST`), but counted
    // differently: every name here costs its own bridge round trip against the single lockstep
    // `IggyClient` every Kafka connection on this gateway shares, duplicates included, so this
    // charges the raw count rather than the distinct one `CreateTopics` charges.
    let max_topics = state.limits.admin_topics_per_request;
    if topic_names.len() > max_topics {
        tracing::warn!(
            requested_topics = topic_names.len(),
            max = max_topics,
            "DeleteTopics request addresses too many topics; rejecting"
        );
        let message = StrBytes::from(format!(
            "this gateway addresses at most {max_topics} topic names per DeleteTopics request"
        ));
        let results = topic_names
            .iter()
            .map(|name| {
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(ERROR_POLICY_VIOLATION)
                    .with_error_message(Some(message.clone()))
            })
            .collect();
        let resp = DeleteTopicsResponse::default().with_responses(results);
        return respond_or_close(encode_message(&resp, api_version, 256), "DeleteTopics");
    }

    // Real Kafka (`ControllerApis.deleteTopics`) refuses every occurrence of a duplicate name
    // with `INVALID_REQUEST` (42) and deletes nothing for it, the same choice this gateway's own
    // `CreateTopics` already makes for a repeated topic name.
    let duplicate_names = find_duplicate_names(&topic_names);

    // The answer names each deleted topic's id from v6, which has to be read before the delete.
    let ids = if api_version >= TOPIC_ID_MIN_VERSION {
        topic_ids(bridge, &topic_names).await
    } else {
        HashMap::new()
    };
    let deadline = Instant::now() + clamp_request_timeout(req.timeout_ms);
    let mut results = delete_all_topics(
        bridge,
        &state.partition_growth,
        &topic_names,
        &duplicate_names,
        deadline,
    )
    .await;
    for result in &mut results {
        let Some(name) = &result.name else {
            continue;
        };
        if result.error_code == ERROR_NONE {
            state.topic_ids.forget(name.as_str());
        }
        if let Some(id) = ids.get(name.as_str()) {
            result.topic_id = *id;
        }
    }
    results.append(&mut answered);
    let resp = DeleteTopicsResponse::default().with_responses(results);
    respond_or_close(encode_message(&resp, api_version, 256), "DeleteTopics")
}

/// The names of a v6 request's topics, and the answers of those that name none.
///
/// As in Kafka's `ControllerApis.deleteTopics`, an entry with both a name and an id is refused with
/// `INVALID_REQUEST`, and an id no topic has with `UNKNOWN_TOPIC_ID` (see [`learn_topic_ids`]).
async fn name_topics(
    state: &GatewayState,
    bridge: &IggyBridge,
    topics: &[DeleteTopicState],
) -> (Vec<TopicName>, Vec<DeletableTopicResult>) {
    let by_id = topics
        .iter()
        .filter(|topic| topic.name.is_none())
        .map(|topic| topic.topic_id);
    let refusal = learn_topic_ids(state, bridge, by_id, REQUEST_DEADLINE).await;
    let mut names = Vec::with_capacity(topics.len());
    let mut answered = Vec::new();
    for topic in topics {
        match (&topic.name, topic.topic_id.is_nil()) {
            (Some(_), false) => answered.push(
                DeletableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_topic_id(topic.topic_id)
                    .with_error_code(ERROR_INVALID_REQUEST)
                    .with_error_message(Some(StrBytes::from_static_str(
                        "You may not specify both topic name and topic id.",
                    ))),
            ),
            (Some(name), true) => names.push(name.clone()),
            (None, _) => match state.topic_ids.name(&topic.topic_id) {
                Some(name) if !topic.topic_id.is_nil() => {
                    names.push(TopicName(StrBytes::from_string(name)));
                }
                _ => answered.push(
                    DeletableTopicResult::default()
                        .with_name(None)
                        .with_topic_id(topic.topic_id)
                        .with_error_code(refusal)
                        .with_error_message(None),
                ),
            },
        }
    }
    (names, answered)
}

/// The current id of each of `names` that exists. A failed read leaves the ids out, which the
/// answer then reports as zero.
async fn topic_ids(bridge: &IggyBridge, names: &[TopicName]) -> HashMap<String, Uuid> {
    let names: Vec<StrBytes> = names.iter().map(|name| name.0.clone()).collect();
    match tokio::time::timeout(REQUEST_DEADLINE, bridge.get_kafka_topics(&names)).await {
        Ok(Ok(found)) => found
            .into_iter()
            .filter_map(|(name, metadata)| {
                metadata.map(|metadata| (name.as_str().to_string(), metadata.topic_id))
            })
            .collect(),
        _ => HashMap::new(),
    }
}

/// Denies every requested topic when `principal` is not allowed to delete topic data, or when
/// its permissions could not be read. `None` means no listener of this gateway names principals,
/// so there is nothing to enforce against; once one does, a connection that proved no identity
/// carries [`AuthenticatedPrincipal::anonymous`] instead and is denied here.
fn authorize(
    principal: Option<&AuthenticatedPrincipal>,
    topic_names: &[TopicName],
) -> Option<Vec<DeletableTopicResult>> {
    let principal = principal?;
    if principal.permissions_known && principal.permissions.manage_topics {
        return None;
    }
    if !principal.permissions_known {
        // The permission read failed after a successful login, so this connection holds no real
        // answer. Denying is the fail-closed choice: granting would authorize an irreversible
        // delete off a value nothing ever actually read.
        tracing::warn!(
            principal = %principal.username,
            "DeleteTopics asked on a connection whose permissions were never read; denying"
        );
    }
    let message = StrBytes::from_static_str("the principal is not authorized to delete topics");
    Some(
        topic_names
            .iter()
            .map(|name| {
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(ERROR_TOPIC_AUTHORIZATION_FAILED)
                    .with_error_message(Some(message.clone()))
            })
            .collect(),
    )
}

/// Every topic name that appears more than once in `topic_names` - mirrors
/// `create_topics::find_duplicate_names`.
fn find_duplicate_names(topic_names: &[TopicName]) -> HashSet<TopicName> {
    let mut seen = HashSet::with_capacity(topic_names.len());
    let mut duplicates = HashSet::new();
    for name in topic_names {
        if !seen.insert(name.clone()) {
            duplicates.insert(name.clone());
        }
    }
    duplicates
}

/// Deletes every requested topic, independently of the others.
///
/// `deadline` bounds each topic's own bridge work individually (`timeout_at`), not the whole
/// loop - same reasoning as `create_topics::create_all_topics`: a single `timeout` around the
/// entire call would discard every already-resolved result the moment one topic's call ran
/// long, answering a retriable code even for topics that had already deleted cleanly.
///
/// `tokio::time::timeout_at` polls the inner future before it ever checks the deadline (see
/// `Timeout::poll`), so once `deadline` has already passed, wrapping a fresh bridge call in it
/// still starts that call - a name past the deadline would both answer `REQUEST_TIMED_OUT` *and*
/// actually delete the topic. The `Instant::now() >= deadline` check below must run and must
/// skip the call itself, the same pattern `list_offsets.rs` uses, not rely on `timeout_at` to
/// skip it.
async fn delete_all_topics(
    bridge: &IggyBridge,
    partition_growth: &Mutex<()>,
    topic_names: &[TopicName],
    duplicate_names: &HashSet<TopicName>,
    deadline: Instant,
) -> Vec<DeletableTopicResult> {
    let mut results = Vec::with_capacity(topic_names.len());
    let mut deadline_exceeded = false;
    for name in topic_names {
        if duplicate_names.contains(name) {
            results.push(
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(ERROR_INVALID_REQUEST)
                    .with_error_message(None),
            );
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "DeleteTopics deadline passed; answering remaining topics retriable \
                     instead of starting new Iggy calls"
                );
            }
            results.push(
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(ERROR_REQUEST_TIMED_OUT)
                    .with_error_message(None),
            );
            continue;
        }

        let result = match tokio::time::timeout_at(
            deadline,
            delete_one_topic(bridge, partition_growth, name),
        )
        .await
        {
            Ok(result) => result,
            Err(_elapsed) => {
                deadline_exceeded = true;
                tracing::warn!(
                    kafka_topic = name.as_str(),
                    "DeleteTopics: this topic's bridge work exceeded the request deadline; \
                     answering retriable instead of blocking further"
                );
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(ERROR_REQUEST_TIMED_OUT)
                    .with_error_message(None)
            }
        };
        results.push(result);
    }
    results
}

async fn delete_one_topic(
    bridge: &IggyBridge,
    partition_growth: &Mutex<()>,
    name: &TopicName,
) -> DeletableTopicResult {
    let result = DeletableTopicResult::default()
        .with_name(Some(name.clone()))
        .with_error_message(None);
    // `CreatePartitions` reads a topic and then adds to it by name. Holding its lock here keeps
    // the topic from being deleted between its read and its write.
    let _growth = partition_growth.lock().await;
    match bridge.delete_kafka_topic(name.as_str()).await {
        // The second arm: the write committed on its first attempt, the SDK's own reconnect path
        // replayed it, and the server's client-table dedup caught the replay - not a fault. Same
        // reasoning as `create_topics::create_one_topic`'s identical arm.
        Ok(()) | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => {
            result.with_error_code(ERROR_NONE)
        }
        Err(err) => bridge_error_result(result, name.as_str(), &err),
    }
}

/// Maps a bridge failure to a Kafka result, logging the real cause server-side.
///
/// Same caution as `create_topics::bridge_error_result` on forwarding `err.to_string()`: a
/// server-side rejection can reconstruct with default-valued fields, so only the client-caused
/// variant (an invalid name) forwards its own text. A missing topic and an unauthorized reply
/// are routine, so they log at `debug!` with fixed text. Everything else gets a fixed message
/// and logs at `error!`.
fn bridge_error_result(
    result: DeletableTopicResult,
    kafka_topic: &str,
    err: &BridgeError,
) -> DeletableTopicResult {
    let error_code = err.to_kafka_error_code();
    match err {
        BridgeError::InvalidKafkaTopicName { reason, .. } => {
            tracing::debug!(
                kafka_topic,
                reason,
                "DeleteTopics rejected an invalid topic name"
            );
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(reason.clone())))
        }
        BridgeError::Iggy(
            IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::TopicNameNotFound(_, _)
            | IggyError::PartitionNotFound(_, _, _)
            | IggyError::ResourceNotFound(_),
        ) => {
            tracing::debug!(kafka_topic, error = %err, "DeleteTopics: topic does not exist");
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(
                    "this topic does not exist".to_string(),
                )))
        }
        BridgeError::Iggy(IggyError::Unauthorized) => {
            tracing::debug!(
                kafka_topic,
                error = %err,
                "DeleteTopics: not authorized to delete this topic"
            );
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(
                    "not authorized to delete this topic".to_string(),
                )))
        }
        other => {
            tracing::error!(kafka_topic, %other, "DeleteTopics failed against the Iggy bridge");
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(
                    "internal error deleting this topic".to_string(),
                )))
        }
    }
}

/// Well-formed `DeleteTopics` response naming every requested topic.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_inner(version, &[], error_code)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &DeleteTopicsRequest) -> Result<Bytes> {
    if version >= TOPIC_ID_MIN_VERSION {
        let results = req
            .topics
            .iter()
            .map(|topic| {
                DeletableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_topic_id(topic.topic_id)
                    .with_error_code(ERROR_NOT_CONTROLLER)
                    .with_error_message(None)
            })
            .collect();
        let resp = DeleteTopicsResponse::default().with_responses(results);
        return encode_message(&resp, version, 256);
    }
    encode_inner(version, &req.topic_names, ERROR_NOT_CONTROLLER)
}

fn encode_inner(version: i16, topic_names: &[TopicName], forced_error: i16) -> Result<Bytes> {
    let results = topic_names
        .iter()
        .map(|name| {
            DeletableTopicResult::default()
                .with_name(Some(name.clone()))
                .with_error_code(forced_error)
                .with_error_message(None)
        })
        .collect();
    let resp = DeleteTopicsResponse::default().with_responses(results);
    encode_message(&resp, version, 256)
}

#[cfg(test)]
mod tests {
    use iggy::prelude::Identifier;

    use super::*;

    #[test]
    fn bridge_error_result_names_a_missing_topic_and_an_unauthorized_reply() {
        let missing_stream = BridgeError::Iggy(IggyError::StreamIdNotFound(Identifier::default()));
        let missing_topic = BridgeError::Iggy(IggyError::TopicIdNotFound(
            Identifier::default(),
            Identifier::default(),
        ));
        let unauthorized = BridgeError::Iggy(IggyError::Unauthorized);
        let cases = [
            (&missing_stream, "this topic does not exist"),
            (&missing_topic, "this topic does not exist"),
            (&unauthorized, "not authorized to delete this topic"),
        ];
        for (err, expected) in cases {
            let result = bridge_error_result(DeletableTopicResult::default(), "orders", err);
            assert_eq!(result.error_code, err.to_kafka_error_code());
            assert_eq!(result.error_message.as_deref(), Some(expected));
        }
    }
}
