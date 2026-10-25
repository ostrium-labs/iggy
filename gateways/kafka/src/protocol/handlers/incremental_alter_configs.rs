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

//! `IncrementalAlterConfigs` (API key 44).
//!
//! `kafka-configs --alter` on a topic has sent this key since Kafka 2.5. `AlterConfigs` (33)
//! comes from Java `Admin.alterConfigs` before 4.0, which removed it, and from other clients'
//! non-incremental alter calls such as librdkafka's `rd_kafka_AlterConfigs`. Each entry carries
//! one operation on one key, where 33 carries a whole new set. Both end in the same write of the
//! resulting configuration set onto the Iggy topic, behind the same checks. See `docs/CONFIGS.md`.

use std::collections::HashSet;

use bytes::Bytes;
use kafka_protocol::messages::IncrementalAlterConfigsRequest;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{IggyBridge, StreamTopicCache, TopicLoad};
use crate::protocol::api::{
    API_KEY_INCREMENTAL_ALTER_CONFIGS, ApiVersionRange, ERROR_INVALID_REQUEST,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION,
    ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_AUTHORIZATION_FAILED, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    GatewayState, HandleOutcome, REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_incremental_alter_configs_shape;
use crate::protocol::handlers::kafka_topic_configs::{
    TopicConfigSnapshot, TopicWrite, apply_operation, plan_alter,
};
use crate::protocol::handlers::topic_config::{
    ConfigFault, ConfigOp, ONLY_TOPIC_RESOURCES, RESOURCE_TYPE_TOPIC, bridge_failure,
    fault_response, incremental_ops, may_alter, name_reason, not_authorized_to_alter,
    topic_cap_and_duplicates, topic_cap_message, write_configs,
};
use crate::protocol::handlers::{
    decode_guarded, encode_message, is_supported_version, respond_or_close,
    unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_INCREMENTAL_ALTER_CONFIGS,
    min_version: 0,
    max_version: 1,
};

const API_NAME: &str = "IncrementalAlterConfigs";

/// Kafka's own text for a topic that more than one resource of the request names.
const DUPLICATE_RESOURCE: &str = "Each resource must appear at most once.";

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_INCREMENTAL_ALTER_CONFIGS, api_version) {
        return unsupported_version_response(
            API_KEY_INCREMENTAL_ALTER_CONFIGS,
            api_version,
            |version| encode_error_response(version, ERROR_INVALID_REQUEST),
        );
    }

    let mut req = match decode_guarded::<IncrementalAlterConfigsRequest>(
        api_version,
        body,
        |version, body| {
            validate_incremental_alter_configs_shape(version, body, state.max_frame_size)
        },
    ) {
        Ok(req) => req,
        Err(error) => {
            tracing::debug!(%error, "Failed to decode IncrementalAlterConfigs request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                API_NAME,
            );
        }
    };

    // Authorization and the cap come before the bridge-availability stub: neither depends on
    // the controller being reachable.
    let denied = authorize(state.bridge.as_deref(), principal, &mut req.resources);
    if req.resources.is_empty() && !denied.is_empty() {
        return respond_or_close(
            encode_message(&response(denied), api_version, 256),
            API_NAME,
        );
    }
    let (exceeds_cap, duplicate_names) = topic_cap_and_duplicates(&req.resources);
    if exceeds_cap {
        let message = StrBytes::from(topic_cap_message(API_NAME));
        tracing::warn!(
            "IncrementalAlterConfigs request addresses too many distinct topics; rejecting"
        );
        let responses = req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_POLICY_VIOLATION, Some(message.clone())))
            .collect();
        return respond_or_close(
            encode_message(&response([responses, denied].concat()), api_version, 256),
            API_NAME,
        );
    }

    let Some(bridge) = &state.bridge else {
        let responses = req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_NOT_CONTROLLER, None))
            .collect();
        return respond_or_close(
            encode_message(&response([responses, denied].concat()), api_version, 256),
            API_NAME,
        );
    };

    let deadline = Instant::now() + REQUEST_DEADLINE;
    let responses = alter_all(bridge, &req, &duplicate_names, deadline).await;
    respond_or_close(
        encode_message(&response([responses, denied].concat()), api_version, 256),
        API_NAME,
    )
}

/// Takes out of `resources`, and answers, each one [`may_alter`] denies `principal`, so the
/// others are still altered.
fn authorize(
    bridge: Option<&IggyBridge>,
    principal: Option<&AuthenticatedPrincipal>,
    resources: &mut Vec<AlterConfigsResource>,
) -> Vec<AlterConfigsResourceResponse> {
    let mut denied = Vec::new();
    resources.retain(|resource| {
        if may_alter(bridge, principal, resource, API_NAME) {
            return true;
        }
        denied.push(resource_error(
            resource,
            ERROR_TOPIC_AUTHORIZATION_FAILED,
            Some(not_authorized_to_alter()),
        ));
        false
    });
    denied
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> crate::error::Result<Bytes> {
    let responses = vec![
        AlterConfigsResourceResponse::default()
            .with_error_code(error_code)
            .with_error_message(None)
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_static_str("")),
    ];
    encode_message(&response(responses), version, 128)
}

fn response(responses: Vec<AlterConfigsResourceResponse>) -> IncrementalAlterConfigsResponse {
    IncrementalAlterConfigsResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(responses)
}

async fn alter_all(
    bridge: &IggyBridge,
    req: &IncrementalAlterConfigsRequest,
    duplicate_names: &HashSet<&str>,
    deadline: Instant,
) -> Vec<AlterConfigsResourceResponse> {
    let mut cache = StreamTopicCache::default();
    let mut responses = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    for resource in &req.resources {
        if resource.resource_type == RESOURCE_TYPE_TOPIC
            && duplicate_names.contains(resource.resource_name.as_str())
        {
            responses.push(resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                Some(StrBytes::from_static_str(DUPLICATE_RESOURCE)),
            ));
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "IncrementalAlterConfigs deadline passed; answering remaining resources \
                     retriable instead of starting new Iggy calls"
                );
            }
            responses.push(resource_error(resource, ERROR_REQUEST_TIMED_OUT, None));
            continue;
        }

        let result = alter_one(bridge, &mut cache, resource, req.validate_only, deadline).await;
        if Instant::now() >= deadline {
            deadline_exceeded = true;
        }
        responses.push(result);
    }
    responses
}

async fn alter_one(
    bridge: &IggyBridge,
    cache: &mut StreamTopicCache,
    resource: &AlterConfigsResource,
    validate_only: bool,
    deadline: Instant,
) -> AlterConfigsResourceResponse {
    if resource.resource_type != RESOURCE_TYPE_TOPIC {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from_static_str(ONLY_TOPIC_RESOURCES)),
        );
    }
    let name = resource.resource_name.as_str();
    if let Err(error) = validate_kafka_topic_name("kafka_topic", name) {
        return resource_error(
            resource,
            ERROR_INVALID_TOPIC_EXCEPTION,
            Some(StrBytes::from(name_reason(&error))),
        );
    }

    // Request faults need no Iggy call, so they are answered before the topic lookup. Config
    // faults come after it, as in `AlterConfigs`: a missing topic is never `INVALID_CONFIG`.
    let ops = match incremental_ops(resource.configs.iter().map(|config| {
        (
            config.name.as_str(),
            config.config_operation,
            config.value.as_ref().map(StrBytes::as_str),
        )
    })) {
        Ok(ops) => ops,
        Err(fault) => {
            return resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                Some(StrBytes::from(fault.message())),
            );
        }
    };

    let current = match cache.lookup(bridge, name, deadline, API_NAME).await {
        TopicLoad::Found(snapshot) => snapshot.clone(),
        TopicLoad::Missing => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None);
        }
        TopicLoad::Failed(error) => {
            let (code, message) = bridge_failure(error, API_NAME, "altering");
            return resource_error(resource, code, message);
        }
        TopicLoad::NotStarted | TopicLoad::TimedOut => {
            return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None);
        }
    };

    let planned = plan_incremental(&current, &ops);
    let write = match planned {
        Ok(write) => write,
        Err(fault) => {
            let (code, message) = fault_response(&fault);
            return resource_error(resource, code, message);
        }
    };
    if validate_only {
        return resource_error(resource, ERROR_NONE, None);
    }
    // An empty operation list, and a delete of an override that was never set, change nothing,
    // and `write_configs` then calls nothing: Kafka answers both `NONE`.
    let (code, message) = write_configs(bridge, name, &write, deadline, API_NAME).await;
    resource_error(resource, code, message)
}

/// The write that applies `ops`, in request order, to the topic's current configuration.
fn plan_incremental(
    current: &TopicConfigSnapshot,
    ops: &[ConfigOp<'_>],
) -> Result<TopicWrite, ConfigFault> {
    let mut set = current.explicit().map_err(|()| {
        ConfigFault::Unsupported(
            "stored message expiry is not a whole number of milliseconds".to_string(),
        )
    })?;
    for op in ops {
        apply_operation(&mut set, op.name, op.operation, op.value)?;
    }
    plan_alter(current, &set)
}

fn resource_error(
    resource: &AlterConfigsResource,
    error_code: i16,
    error_message: Option<StrBytes>,
) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse::default()
        .with_error_code(error_code)
        .with_error_message(error_message)
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(manage_topics: bool, permissions_known: bool) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal::with_global(
            "alice",
            iggy::prelude::GlobalPermissions {
                manage_topics,
                ..iggy::prelude::GlobalPermissions::default()
            },
            permissions_known,
        )
    }

    fn resource(name: &str) -> AlterConfigsResource {
        AlterConfigsResource::default()
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from(name.to_string()))
    }

    #[test]
    fn no_principal_is_not_gated() {
        assert_eq!(
            authorize(None, None, &mut vec![resource("orders")]),
            Vec::new()
        );
    }

    #[test]
    fn a_principal_with_manage_topics_is_allowed() {
        let principal = principal(true, true);
        assert_eq!(
            authorize(None, Some(&principal), &mut vec![resource("orders")]),
            Vec::new()
        );
    }

    #[test]
    fn a_principal_without_manage_topics_is_denied_every_resource() {
        let principal = principal(false, true);
        let responses = authorize(
            None,
            Some(&principal),
            &mut vec![resource("orders"), resource("payments")],
        );
        assert_eq!(responses.len(), 2);
        assert!(
            responses
                .iter()
                .all(|r| r.error_code == ERROR_TOPIC_AUTHORIZATION_FAILED)
        );
    }

    #[test]
    fn unread_permissions_fail_closed() {
        let principal = principal(true, false);
        let responses = authorize(None, Some(&principal), &mut vec![resource("orders")]);
        assert_eq!(responses[0].error_code, ERROR_TOPIC_AUTHORIZATION_FAILED);
    }
}
