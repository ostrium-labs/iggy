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

//! `EndTxn` (API key 26): commit or abort (KW-T). Served to v4; v5 is transaction version 2,
//! which this gateway does not advertise.

use bytes::Bytes;
use kafka_protocol::messages::{EndTxnRequest, EndTxnResponse, ProducerId};

use crate::error::Result;
use crate::protocol::api::{ApiVersionRange, GatewayState, HandleOutcome};
use crate::protocol::bounds_guard::validate_end_txn_shape;
use crate::protocol::handlers::txn::decode_txn_request;
use crate::protocol::handlers::{encode_message, respond_or_close};
use crate::transactions::{Outcome, codes};

pub const API_KEY: i16 = 26;

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY,
    min_version: 0,
    max_version: 4,
};

/// The first version whose clients know `PRODUCER_FENCED` (KIP-588).
const PRODUCER_FENCED_VERSION: i16 = 2;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let Some(coordinator) = state.transactions.as_ref() else {
        return HandleOutcome::Close;
    };
    let request = match decode_txn_request::<EndTxnRequest>(
        RANGE,
        api_version,
        body,
        validate_end_txn_shape,
        encode_error_response,
        "EndTxn",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let transactional_id = request.transactional_id.to_string();
    let code = if transactional_id.is_empty() {
        codes::INVALID_REQUEST
    } else {
        let outcome = if request.committed {
            Outcome::Commit
        } else {
            Outcome::Abort
        };
        coordinator
            .end_txn(
                &transactional_id,
                request.producer_id.0,
                request.producer_epoch,
                outcome,
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
        "EndTxn",
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let response = EndTxnResponse::default()
        .with_error_code(error_code)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    encode_message(&response, version, 32)
}
