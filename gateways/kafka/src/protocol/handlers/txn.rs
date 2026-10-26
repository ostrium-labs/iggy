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

//! What the transaction APIs share (KW-T).
//!
//! Their keys are advertised and served only while transactions are on, so they live outside
//! `api::SUPPORTED_RANGES`, as the SASL and SCRAM keys do, and check their own version range.

use bytes::Bytes;
use kafka_protocol::protocol::Decodable;

use crate::error::Result;
use crate::protocol::api::{ApiVersionRange, ERROR_INVALID_REQUEST, HandleOutcome};
use crate::protocol::handlers::{
    add_offsets_to_txn, add_partitions_to_txn, describe_producers, describe_transactions, end_txn,
    txn_offset_commit, write_txn_markers,
};
use crate::protocol::handlers::{decode_guarded, respond_or_close};

/// The transaction keys, advertised only while transactions are on.
pub static TXN_ADVERTISED_RANGES: &[ApiVersionRange] = &[
    add_partitions_to_txn::RANGE,
    add_offsets_to_txn::RANGE,
    end_txn::RANGE,
    txn_offset_commit::RANGE,
    describe_producers::RANGE,
    describe_transactions::DESCRIBE_RANGE,
    describe_transactions::LIST_RANGE,
    write_txn_markers::RANGE,
];

/// Decodes a request of a key in [`TXN_ADVERTISED_RANGES`], or the outcome to send instead: a
/// close above the range (no response schema to answer in) and below it, and an
/// `INVALID_REQUEST` answer for a body that does not decode.
pub(crate) fn decode_txn_request<T: Decodable>(
    range: ApiVersionRange,
    api_version: i16,
    body: Bytes,
    validate: impl FnOnce(i16, &Bytes) -> Result<()>,
    encode_error: impl Fn(i16, i16) -> Result<Bytes>,
    api_name: &str,
) -> core::result::Result<T, HandleOutcome> {
    if !(range.min_version..=range.max_version).contains(&api_version) {
        tracing::debug!(
            api_version,
            "{api_name} at a version this gateway does not serve"
        );
        return Err(HandleOutcome::Close);
    }
    decode_guarded::<T>(api_version, body, validate).map_err(|error| {
        tracing::debug!(%error, "Failed to decode {api_name} request");
        respond_or_close(encode_error(api_version, ERROR_INVALID_REQUEST), api_name)
    })
}
