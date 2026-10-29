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

//! `AddOffsetsToTxn` (API key 25): a group's offsets join the producer's transaction, so its
//! `TxnOffsetCommit` commits with it (KW-T step 3).

use bytes::Bytes;
use kafka_protocol::messages::{AddOffsetsToTxnRequest, AddOffsetsToTxnResponse};

use crate::auth::AuthenticatedPrincipal;
use crate::authz::GroupScope;
use crate::error::Result;
use crate::protocol::api::{
    ApiVersionRange, ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
    GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_add_offsets_to_txn_shape;
use crate::protocol::handlers::txn::{
    coordinator_key, decode_txn_request, may_use_transactional_ids,
};
use crate::protocol::handlers::{encode_message, respond_or_close};
use crate::transactions::codes;

pub const API_KEY: i16 = 25;

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY,
    min_version: 0,
    max_version: 4,
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
    let request = match decode_txn_request::<AddOffsetsToTxnRequest>(
        RANGE,
        api_version,
        body,
        validate_add_offsets_to_txn_shape,
        encode_error_response,
        "AddOffsetsToTxn",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    // KW-TXP: the transactional id is the principal's own too, in its namespace.
    let transactional_id = coordinator_key(principal, request.transactional_id.as_str());
    // KW-10: the group is the principal's own, in its namespace, and group access follows read
    // access to some topic, as for every group API.
    let group = principal.map_or_else(
        || request.group_id.to_string(),
        |principal| GroupScope::of(&principal.username).scope(request.group_id.as_str()),
    );
    let may_use_group = principal
        .is_none_or(|principal| principal.permissions_known && principal.grants.reads_any_topic());
    // As Kafka's `KafkaApis`: WRITE on the transactional id (53) before READ on the group (30).
    let code = if !may_use_transactional_ids(principal) {
        ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    } else if transactional_id.is_empty() || request.group_id.is_empty() {
        codes::INVALID_REQUEST
    } else if !may_use_group {
        ERROR_GROUP_AUTHORIZATION_FAILED
    } else {
        coordinator
            .add_offsets(
                &transactional_id,
                request.producer_id.0,
                request.producer_epoch,
                &group,
            )
            .await
            .err()
            .unwrap_or(codes::NONE)
    };
    respond_or_close(
        encode_error_response(
            api_version,
            codes::fenced_for(code, api_version >= PRODUCER_FENCED_VERSION),
        ),
        "AddOffsetsToTxn",
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_message(
        &AddOffsetsToTxnResponse::default().with_error_code(error_code),
        version,
        16,
    )
}
