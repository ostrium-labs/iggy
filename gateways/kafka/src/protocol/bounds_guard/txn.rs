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

//! The bounds guard of the transaction APIs (KW-T), as the parent module guards the others.

use bytes::Bytes;

use super::ShapeCursor;
use crate::error::Result;

/// Mirrors the field order `AddPartitionsToTxnRequest::decode` walks at v0 to v3, the client
/// form. Topic names are echoed, so they count toward the projected response.
///
/// # Errors
///
/// Returns an error when a declared array or string length cannot fit in the bytes remaining in
/// the frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_add_partitions_to_txn_shape(
    version: i16,
    body: &Bytes,
    max_frame_size: usize,
) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
    let flexible = version >= 3;
    if flexible {
        c.compact_string(false)?;
    } else {
        c.legacy_string(false)?;
    }
    let _producer_id = c.read_i64()?;
    let _producer_epoch = c.read_i16()?;
    let topics = if flexible {
        c.compact_array_count()?
    } else {
        c.legacy_array_count()?
    };
    for _ in 0..topics {
        if flexible {
            c.compact_string(false)?;
        } else {
            c.legacy_string(false)?;
        }
        let partitions = if flexible {
            c.compact_array_count()?
        } else {
            c.legacy_array_count()?
        };
        for _ in 0..partitions {
            let _partition = c.read_i32()?;
        }
        if flexible {
            c.tagged_fields()?;
        }
    }
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `EndTxnRequest::decode` walks. The response echoes nothing.
///
/// # Errors
///
/// Returns an error when the declared transactional id cannot fit in the bytes remaining in the
/// frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_end_txn_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), usize::MAX);
    let flexible = version >= 3;
    if flexible {
        c.compact_string(false)?;
    } else {
        c.legacy_string(false)?;
    }
    let _producer_id = c.read_i64()?;
    let _producer_epoch = c.read_i16()?;
    let _committed = c.read_bool()?;
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `AddOffsetsToTxnRequest::decode` walks. The response echoes nothing.
///
/// # Errors
///
/// Returns an error when a declared string length cannot fit in the bytes remaining in the
/// frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_add_offsets_to_txn_shape(version: i16, body: &Bytes) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), usize::MAX);
    let flexible = version >= 3;
    let string = |c: &mut ShapeCursor| {
        if flexible {
            c.compact_string(false)
        } else {
            c.legacy_string(false)
        }
    };
    string(&mut c)?;
    let _producer_id = c.read_i64()?;
    let _producer_epoch = c.read_i16()?;
    string(&mut c)?;
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

/// Mirrors the field order `TxnOffsetCommitRequest::decode` walks. Topic names are echoed.
///
/// # Errors
///
/// Returns an error when a declared array or string length cannot fit in the bytes remaining in
/// the frame, or the body is truncated or malformed in a way that cannot be walked.
pub fn validate_txn_offset_commit_shape(
    version: i16,
    body: &Bytes,
    max_frame_size: usize,
) -> Result<()> {
    let mut c = ShapeCursor::new(body.clone(), max_frame_size);
    let flexible = version >= 3;
    let string = |c: &mut ShapeCursor, nullable: bool| {
        if flexible {
            c.compact_string(nullable)
        } else {
            c.legacy_string(nullable)
        }
    };
    string(&mut c, false)?;
    string(&mut c, false)?;
    let _producer_id = c.read_i64()?;
    let _producer_epoch = c.read_i16()?;
    if version >= 3 {
        let _generation_id = c.read_i32()?;
        string(&mut c, false)?;
        string(&mut c, true)?;
    }
    let topics = if flexible {
        c.compact_array_count()?
    } else {
        c.legacy_array_count()?
    };
    for _ in 0..topics {
        string(&mut c, false)?;
        let partitions = if flexible {
            c.compact_array_count()?
        } else {
            c.legacy_array_count()?
        };
        for _ in 0..partitions {
            let _partition = c.read_i32()?;
            let _offset = c.read_i64()?;
            if version >= 2 {
                let _leader_epoch = c.read_i32()?;
            }
            string(&mut c, true)?;
            if flexible {
                c.tagged_fields()?;
            }
        }
        if flexible {
            c.tagged_fields()?;
        }
    }
    if flexible {
        c.tagged_fields()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::{
        AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, EndTxnRequest, GroupId, ProducerId,
        TopicName, TransactionalId, TxnOffsetCommitRequest,
    };
    use kafka_protocol::protocol::{Encodable, StrBytes};

    fn encoded<T: Encodable>(message: &T, version: i16) -> Bytes {
        let mut buf = BytesMut::new();
        message.encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    #[test]
    fn given_real_requests_the_guard_should_walk_them_whole() {
        let add = AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str("tx")))
            .with_v3_and_below_producer_id(ProducerId(1))
            .with_v3_and_below_topics(vec![
                AddPartitionsToTxnTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("t")))
                    .with_partitions(vec![0, 1, 2]),
            ]);
        let end = EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("tx")))
            .with_committed(true);
        for version in 0..=3 {
            validate_add_partitions_to_txn_shape(version, &encoded(&add, version), 1 << 20)
                .unwrap();
        }
        for version in 0..=4 {
            validate_end_txn_shape(version, &encoded(&end, version)).unwrap();
        }
        let add_offsets = AddOffsetsToTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("tx")))
            .with_group_id(GroupId(StrBytes::from_static_str("g")));
        for version in 0..=4 {
            validate_add_offsets_to_txn_shape(version, &encoded(&add_offsets, version)).unwrap();
        }
        let commit = |version: i16| {
            let request = TxnOffsetCommitRequest::default()
                .with_transactional_id(TransactionalId(StrBytes::from_static_str("tx")))
                .with_group_id(GroupId(StrBytes::from_static_str("g")));
            let request = if version >= 3 {
                request
                    .with_member_id(StrBytes::from_static_str("m"))
                    .with_group_instance_id(Some(StrBytes::from_static_str("i")))
            } else {
                request.with_group_instance_id(None)
            };
            request.with_topics(vec![
                TxnOffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("t")))
                    .with_partitions(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .with_committed_offset(9)
                            .with_committed_metadata(Some(StrBytes::from_static_str("md"))),
                    ]),
            ])
        };
        for version in 0..=4 {
            validate_txn_offset_commit_shape(version, &encoded(&commit(version), version), 1 << 20)
                .unwrap();
        }
    }

    #[test]
    fn given_a_huge_declared_count_the_guard_should_refuse_it() {
        // v0: transactional id "tx", producer id, epoch, then 2^31-1 topics in 4 bytes.
        let mut body = BytesMut::new();
        body.extend_from_slice(&[0, 2, b't', b'x']);
        body.extend_from_slice(&[0; 10]);
        body.extend_from_slice(&i32::MAX.to_be_bytes());
        assert!(validate_add_partitions_to_txn_shape(0, &body.freeze(), 1 << 20).is_err());
    }
}
