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

//! `WriteTxnMarkers` (API key 27), as an operator sends it (KW-T step 4).
//!
//! Kafka's coordinators send it to partition leaders; this gateway's coordinator writes its
//! markers in process and never needs it. What reaches here is the Admin client's
//! `abortTransaction` (`kafka-transactions.sh abort`, KIP-664): end a transaction left hanging on
//! a partition. Kafka asks for `CLUSTER_ACTION` on the cluster; the gateway asks for what
//! `DeleteRecords` asks, `manage_topics`.

use bytes::Bytes;
use kafka_protocol::messages::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
};
use kafka_protocol::messages::{WriteTxnMarkersRequest, WriteTxnMarkersResponse};

use crate::auth::AuthenticatedPrincipal;
use crate::protocol::api::{ApiVersionRange, GatewayState, HandleOutcome};
use crate::protocol::bounds_guard::validate_write_txn_markers_shape;
use crate::protocol::handlers::txn::decode_txn_request;
use crate::protocol::handlers::{encode_message, respond_or_close};
use crate::transactions::codes;

pub const API_KEY: i16 = 27;

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY,
    min_version: 1,
    max_version: 1,
};

/// `CLUSTER_AUTHORIZATION_FAILED`.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let Some(coordinator) = state.transactions.as_ref() else {
        return HandleOutcome::Close;
    };
    let request = match decode_txn_request::<WriteTxnMarkersRequest>(
        RANGE,
        api_version,
        body,
        |v, b| validate_write_txn_markers_shape(v, b, state.max_frame_size),
        |v, _| encode_message(&WriteTxnMarkersResponse::default(), v, 16),
        "WriteTxnMarkers",
    ) {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let refused = refusal(principal);
    let mut markers = Vec::with_capacity(request.markers.len());
    for marker in &request.markers {
        let mut topics = Vec::with_capacity(marker.topics.len());
        for topic in &marker.topics {
            let mut partitions = Vec::with_capacity(topic.partition_indexes.len());
            for &index in &topic.partition_indexes {
                let code = match refused {
                    Some(code) => code,
                    None => coordinator
                        .write_admin_marker(
                            marker.producer_id.0,
                            marker.producer_epoch,
                            marker.coordinator_epoch,
                            marker.transaction_result,
                            &(topic.name.to_string(), index),
                        )
                        .await
                        .err()
                        .unwrap_or(codes::NONE),
                };
                partitions.push(
                    WritableTxnMarkerPartitionResult::default()
                        .with_partition_index(index)
                        .with_error_code(code),
                );
            }
            topics.push(
                WritableTxnMarkerTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            );
        }
        markers.push(
            WritableTxnMarkerResult::default()
                .with_producer_id(marker.producer_id)
                .with_topics(topics),
        );
    }
    respond_or_close(
        encode_message(
            &WriteTxnMarkersResponse::default().with_markers(markers),
            api_version,
            256,
        ),
        "WriteTxnMarkers",
    )
}

/// The code that refuses every marker to `principal`, if any. `None` means no listener names
/// principals. A principal whose permissions were never read is refused.
fn refusal(principal: Option<&AuthenticatedPrincipal>) -> Option<i16> {
    let principal = principal?;
    (!(principal.permissions_known && principal.permissions.manage_topics))
        .then_some(CLUSTER_AUTHORIZATION_FAILED)
}
