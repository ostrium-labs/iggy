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

use crate::auth::AuthenticatedPrincipal;
use crate::authz::TransactionalIdScope;
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

/// Whether `principal` may use the transactional ids of its namespace: Kafka's `WRITE` on
/// `TRANSACTIONAL_ID` (and the `DESCRIBE` it implies), which every transaction API asks for
/// before anything else, answering `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) otherwise.
///
/// Iggy has no transactional id resource, so, as group access follows topic read access
/// (KW-10), transactional id access follows topic write access: a transaction that may write no
/// topic has nothing to commit. `None` means no listener of this gateway names principals, with
/// nothing to enforce against. A principal whose permissions were never read is refused.
pub(crate) fn may_use_transactional_ids(principal: Option<&AuthenticatedPrincipal>) -> bool {
    principal
        .is_none_or(|principal| principal.permissions_known && principal.grants.writes_any_topic())
}

/// The id the transaction coordinator keeps `transactional_id` under for `principal` (KW-TXP):
/// the principal's scoped id, or the id itself when no listener names principals. An empty id
/// stays empty, so the handlers still refuse it as `INVALID_REQUEST`, and a non-transactional
/// request still reads as one.
pub(crate) fn coordinator_key(
    principal: Option<&AuthenticatedPrincipal>,
    transactional_id: &str,
) -> String {
    match principal {
        Some(principal) if !transactional_id.is_empty() => {
            TransactionalIdScope::of(&principal.username).scope(transactional_id)
        }
        _ => transactional_id.to_string(),
    }
}

/// The id `principal` knows the coordinator key `key` by, or `None` when the key is in another
/// principal's namespace and must not reach the answer.
pub(crate) fn visible_id<'a>(
    principal: Option<&AuthenticatedPrincipal>,
    key: &'a str,
) -> Option<&'a str> {
    match principal {
        Some(principal) => TransactionalIdScope::of(&principal.username).unscope(key),
        None => Some(key),
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::GlobalPermissions;

    use super::*;

    fn writer(name: &str) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal::with_global(
            name,
            GlobalPermissions {
                send_messages: true,
                ..GlobalPermissions::default()
            },
            true,
        )
    }

    fn reader(name: &str) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal::with_global(
            name,
            GlobalPermissions {
                poll_messages: true,
                ..GlobalPermissions::default()
            },
            true,
        )
    }

    #[test]
    fn a_principal_that_may_write_may_use_transactional_ids() {
        assert!(may_use_transactional_ids(None));
        assert!(may_use_transactional_ids(Some(&writer("alice"))));
        assert!(!may_use_transactional_ids(Some(&reader("alice"))));
        let unread = AuthenticatedPrincipal::with_global(
            "alice",
            GlobalPermissions {
                send_messages: true,
                ..GlobalPermissions::default()
            },
            false,
        );
        assert!(
            !may_use_transactional_ids(Some(&unread)),
            "permissions never read"
        );
        assert!(!may_use_transactional_ids(Some(
            &AuthenticatedPrincipal::anonymous()
        )));
    }

    #[test]
    fn each_principal_names_its_own_transactional_ids() {
        let alice = writer("alice");
        let bob = writer("bob");
        let key = coordinator_key(Some(&alice), "orders-tx");
        assert_ne!(key, coordinator_key(Some(&bob), "orders-tx"));
        assert_eq!(visible_id(Some(&alice), &key), Some("orders-tx"));
        assert_eq!(visible_id(Some(&bob), &key), None);
        // Bob naming Alice's key verbatim lands in his own namespace.
        let crafted = coordinator_key(Some(&bob), &key);
        assert_ne!(crafted, key);
        assert_eq!(visible_id(Some(&alice), &crafted), None);
    }

    #[test]
    fn without_principals_and_for_an_empty_id_nothing_is_scoped() {
        assert_eq!(coordinator_key(None, "orders-tx"), "orders-tx");
        assert_eq!(visible_id(None, "orders-tx"), Some("orders-tx"));
        assert_eq!(coordinator_key(Some(&writer("alice")), ""), "");
    }
}
