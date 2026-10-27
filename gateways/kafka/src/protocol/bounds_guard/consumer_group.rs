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

//! Shape guards for the KIP-848 consumer group keys (68 and 69).

use bytes::Bytes;

use super::ShapeCursor;
use crate::error::Result;

/// Mirrors the field order `ConsumerGroupHeartbeatRequest::decode` walks (v0-1, all flexible).
///
/// The response echoes nothing of size from the request, so no response budget applies.
///
/// # Errors
///
/// Returns an error when a declared array/string length cannot fit in the bytes remaining in the
/// frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_consumer_group_heartbeat_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), usize::MAX);
    c.compact_string(false)?; // group_id
    c.compact_string(false)?; // member_id
    let _member_epoch = c.read_i32()?;
    c.compact_string(true)?; // instance_id
    c.compact_string(true)?; // rack_id
    let _rebalance_timeout_ms = c.read_i32()?;
    let names = c.compact_array_count_nullable()?;
    for _ in 0..names {
        c.compact_string(false)?;
    }
    if version >= 1 {
        c.compact_string(true)?; // subscribed_topic_regex
    }
    c.compact_string(true)?; // server_assignor
    let owned = c.compact_array_count_nullable()?;
    for _ in 0..owned {
        c.skip(16)?; // topic_id
        let partitions = c.compact_array_count()?;
        for _ in 0..partitions {
            let _partition = c.read_i32()?;
        }
        c.tagged_fields()?;
    }
    c.tagged_fields()?;
    Ok(())
}

/// Mirrors the field order `ConsumerGroupDescribeRequest::decode` walks (v0-1, all flexible).
///
/// # Errors
///
/// Returns an error when a declared array/string length cannot fit in the bytes remaining in the
/// frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_consumer_group_describe_shape(
    _version: i16,
    body: &Bytes,
    max_frame_size: usize,
) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
    let groups = c.compact_array_count()?;
    for _ in 0..groups {
        c.compact_string(false)?;
    }
    let _include_authorized_operations = c.read_bool()?;
    c.tagged_fields()?;
    Ok(())
}
