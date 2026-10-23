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

//! `FindCoordinator` (API key 10).
//!
//! A gateway that runs alone always answers itself, matching the single broker entry Metadata
//! advertises. Among several gateways the answer is the one that owns the group
//! (`crate::membership`), on the security protocol of the asking connection. No group state is
//! consulted: where the coordinator lives depends only on the group id and the live gateways.

use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
use crate::listener::SecurityProtocol;
use crate::membership::Snapshot;
use crate::protocol::api::{
    API_KEY_FIND_COORDINATOR, ApiVersionRange, BrokerAdvertise, ConnectionState,
    ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED, ERROR_UNSUPPORTED_VERSION, GatewayState,
    HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_find_coordinator_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_FIND_COORDINATOR,
    min_version: 0,
    max_version: 4,
};

/// `key_type` 0. Types 1 (transaction) and 2 (share) have no coordinator here.
const COORDINATOR_TYPE_GROUP: i8 = 0;

const COORDINATOR_TYPE_TRANSACTION: i8 = 1;

const UNSUPPORTED_KEY_TYPE_MESSAGE: &str = "only group coordination is supported";

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_FIND_COORDINATOR, api_version) {
        return unsupported_version_response(API_KEY_FIND_COORDINATOR, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    match decode_guarded::<FindCoordinatorRequest>(api_version, body, |version, body| {
        validate_find_coordinator_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => respond_or_close(
            encode_shared_response(
                api_version,
                &request,
                &state.membership.snapshot(),
                connection.security_protocol(),
                &connection.broker,
            ),
            "FindCoordinator",
        ),
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode FindCoordinator request");
            if api_version >= 4 {
                // v4 carries its error per requested key and the keys are exactly what failed to
                // decode, so there is no honest body to send.
                return HandleOutcome::Close;
            }
            respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "FindCoordinator",
            )
        }
    }
}

/// The answer of a gateway that runs alone, as node 1.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(
    version: i16,
    request: &FindCoordinatorRequest,
    broker: &BrokerAdvertise,
) -> Result<Bytes> {
    encode_shared_response(
        version,
        request,
        &Snapshot::single(crate::membership::SINGLE_NODE_ID),
        SecurityProtocol::Plaintext,
        broker,
    )
}

/// The answer of a gateway in `snapshot`: each group's coordinator, at its address on
/// `protocol`. This gateway answers `own`, the address of the listener the request came
/// through.
///
/// A coordinator with no listener of `protocol` is `COORDINATOR_NOT_AVAILABLE` (15), retriable:
/// every gateway sharing a bootstrap list needs the same listener protocols.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_shared_response(
    version: i16,
    request: &FindCoordinatorRequest,
    snapshot: &Snapshot,
    protocol: SecurityProtocol,
    own: &BrokerAdvertise,
) -> Result<Bytes> {
    let keys = if version >= 4 {
        request.coordinator_keys.clone()
    } else {
        vec![request.key.clone()]
    };
    let error_code = match request.key_type {
        COORDINATOR_TYPE_GROUP => {
            let answers: Vec<Answer> = keys
                .iter()
                .map(|key| {
                    let node_id = snapshot.coordinator(key.as_str());
                    snapshot.broker(node_id, protocol, own).map_or_else(
                        || Answer {
                            key: key.clone(),
                            error_code: ERROR_COORDINATOR_NOT_AVAILABLE,
                            error_message: Some(StrBytes::from_string(format!(
                                "the coordinator, node {node_id}, has no {protocol} listener"
                            ))),
                            coordinator: None,
                        },
                        |broker| Answer {
                            key: key.clone(),
                            error_code: ERROR_NONE,
                            error_message: None,
                            coordinator: Some((node_id, broker)),
                        },
                    )
                })
                .collect();
            return encode_answers(version, &answers);
        }
        // Transactions are out of scope for good, so the producer must fail rather than retry.
        COORDINATOR_TYPE_TRANSACTION => ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        _ => ERROR_INVALID_REQUEST,
    };
    encode_inner(
        version,
        &keys,
        error_code,
        Some(&StrBytes::from_static_str(UNSUPPORTED_KEY_TYPE_MESSAGE)),
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_inner(version, &[StrBytes::new()], error_code, None)
}

/// One key's answer.
struct Answer {
    key: StrBytes,
    error_code: i16,
    error_message: Option<StrBytes>,
    coordinator: Option<(i32, BrokerAdvertise)>,
}

/// Every key with the same answer and no coordinator.
fn encode_inner(
    version: i16,
    keys: &[StrBytes],
    error_code: i16,
    error_message: Option<&StrBytes>,
) -> Result<Bytes> {
    let answers: Vec<Answer> = keys
        .iter()
        .map(|key| Answer {
            key: key.clone(),
            error_code,
            error_message: error_message.cloned(),
            coordinator: None,
        })
        .collect();
    encode_answers(version, &answers)
}

/// v4 moved every field into `coordinators[]` and the encoder refuses a response that sets both
/// shapes, so each one is built on its own. Below v4 there is exactly one answer.
fn encode_answers(version: i16, answers: &[Answer]) -> Result<Bytes> {
    let fields = |answer: &Answer| {
        answer.coordinator.as_ref().map_or_else(
            || (-1, StrBytes::new(), -1),
            |(node_id, broker)| {
                (
                    *node_id,
                    StrBytes::from_string(broker.host.clone()),
                    broker.port,
                )
            },
        )
    };

    let response = if version >= 4 {
        let coordinators = answers
            .iter()
            .map(|answer| {
                let (node_id, host, port) = fields(answer);
                Coordinator::default()
                    .with_key(answer.key.clone())
                    .with_node_id(BrokerId(node_id))
                    .with_host(host)
                    .with_port(port)
                    .with_error_code(answer.error_code)
                    .with_error_message(answer.error_message.clone())
            })
            .collect();
        FindCoordinatorResponse::default().with_coordinators(coordinators)
    } else {
        let Some(answer) = answers.first() else {
            return encode_message(&FindCoordinatorResponse::default(), version, 128);
        };
        let (node_id, host, port) = fields(answer);
        FindCoordinatorResponse::default()
            .with_error_code(answer.error_code)
            .with_error_message(answer.error_message.clone())
            .with_node_id(BrokerId(node_id))
            .with_host(host)
            .with_port(port)
    };
    encode_message(&response, version, 128)
}
