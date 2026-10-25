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

//! Transactional records (KW-T): the `kafka.t` mark on every record of a transactional batch,
//! and the control records that end a transaction on a partition.
//!
//! A Kafka consumer tells transactional data from plain data by the batch it arrives in: the
//! batch's transactional bit and producer id. Fetch rebuilds batches from single Iggy messages and
//! may start a read inside a batch the producer sent (after a partial read, or at a committed
//! offset), so the producer id, epoch and sequence travel on every record, not only on the
//! batch's first, which `kafka.p` marks.
//!
//! `kafka.t` is 15 bytes, big-endian: a layout byte (1), the producer id (`i64`), the epoch
//! (`i16`) and the record's own sequence (`i32`, -1 on a control record). A data record keeps its
//! mapping version (`kafka.v` 1, or 2 on the batch's first record, which also carries `kafka.p`
//! with [`FLAG_TRANSACTIONAL`](super::producer_mark::FLAG_TRANSACTIONAL)).
//!
//! A control record is its own mapping version, `kafka.v` 3, so a gateway that predates
//! transactions refuses it at that offset instead of serving it as a one-record value. Its
//! payload is Kafka's `EndTransactionMarker` value (version `i16` 0, coordinator epoch `i32`), its
//! `kafka.ctl` header the control type (0 abort, 1 commit), and its `kafka.t` the producer and the
//! epoch it ends. Fetch serves it as a control batch, which every Kafka client skips.

use std::sync::LazyLock;

use bytes::{BufMut, Bytes, BytesMut};
use iggy::prelude::{HeaderKey, HeaderValue, IggyMessage};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::records::{NO_PARTITION_LEADER_EPOCH, NO_SEQUENCE, Record, TimestampType};

use super::producer_mark::{FLAG_TRANSACTIONAL, PRODUCER_KEY};
use super::{
    PRODUCER_MAPPING_VERSION, ProducerMark, RecordCodecError, Result, VERSION_KEY, header_key,
    header_value, user_headers,
};

/// Iggy header holding a transactional record's [`TxnMark`].
pub const TXN_HEADER: &str = "kafka.t";
/// Iggy header holding a control record's [`ControlType`].
pub const CONTROL_HEADER: &str = "kafka.ctl";
/// The mapping version of a control record.
pub const CONTROL_MAPPING_VERSION: u8 = 3;

const TXN_LAYOUT: u8 = 1;
const TXN_BYTES: usize = 15;
/// Version of the control record key and of the `EndTransactionMarker` value.
const CONTROL_VERSION: i16 = 0;

pub(super) static TXN_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(TXN_HEADER));
pub(super) static CONTROL_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(CONTROL_HEADER));

/// Which producer wrote a transactional record, at which epoch, and its sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxnMark {
    pub producer_id: i64,
    pub epoch: i16,
    pub sequence: i32,
}

impl TxnMark {
    fn encode(self) -> [u8; TXN_BYTES] {
        let mut value = [0; TXN_BYTES];
        value[0] = TXN_LAYOUT;
        value[1..9].copy_from_slice(&self.producer_id.to_be_bytes());
        value[9..11].copy_from_slice(&self.epoch.to_be_bytes());
        value[11..15].copy_from_slice(&self.sequence.to_be_bytes());
        value
    }

    fn decode(value: &[u8]) -> Option<Self> {
        if value.len() != TXN_BYTES || value[0] != TXN_LAYOUT {
            return None;
        }
        Some(Self {
            producer_id: i64::from_be_bytes(value[1..9].try_into().ok()?),
            epoch: i16::from_be_bytes(value[9..11].try_into().ok()?),
            sequence: i32::from_be_bytes(value[11..15].try_into().ok()?),
        })
    }
}

/// How a transaction ended on a partition: Kafka's `ControlRecordType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlType {
    Abort,
    Commit,
}

impl ControlType {
    const fn code(self) -> i16 {
        match self {
            Self::Abort => 0,
            Self::Commit => 1,
        }
    }

    const fn from_code(code: i16) -> Option<Self> {
        match code {
            0 => Some(Self::Abort),
            1 => Some(Self::Commit),
            _ => None,
        }
    }
}

/// Marks `message`, one record of a transactional batch, with `mark`; the batch's first record
/// also gets `first`, its `kafka.p`, flagged transactional, and `kafka.v` 2.
///
/// # Errors
///
/// When the headers no longer fit Iggy's budget, or the message's headers do not parse. Unlike
/// `kafka.p` on an idempotent batch, a transactional batch must not be written unmarked: its
/// records would read as committed data whatever the transaction's outcome.
pub fn mark_transactional(
    message: &mut IggyMessage,
    mark: TxnMark,
    first: Option<ProducerMark>,
) -> Result<()> {
    let mut headers = user_headers(message)?;
    if let Some(first) = first {
        headers.insert(
            VERSION_KEY.clone(),
            header_value(&[PRODUCER_MAPPING_VERSION]),
        );
        headers.insert(
            PRODUCER_KEY.clone(),
            header_value(&first.encode_flagged(FLAG_TRANSACTIONAL)),
        );
    }
    headers.insert(TXN_KEY.clone(), header_value(&mark.encode()));
    let mut marked = IggyMessage::builder()
        .payload(message.payload.clone())
        .user_headers(headers)
        .build()
        .map_err(RecordCodecError::Iggy)?;
    marked.header.origin_timestamp = message.header.origin_timestamp;
    *message = marked;
    Ok(())
}

/// The control record that ends the transaction of `producer_id` at `epoch` on one partition.
///
/// # Errors
///
/// When Iggy refuses to build the message, which a payload of six bytes never makes it do.
pub fn control_message(
    control: ControlType,
    producer_id: i64,
    epoch: i16,
    coordinator_epoch: i32,
    timestamp_ms: i64,
) -> Result<IggyMessage> {
    let mut value = BytesMut::with_capacity(6);
    value.put_i16(CONTROL_VERSION);
    value.put_i32(coordinator_epoch);
    let mut headers = std::collections::BTreeMap::new();
    headers.insert(
        VERSION_KEY.clone(),
        header_value(&[CONTROL_MAPPING_VERSION]),
    );
    headers.insert(
        CONTROL_KEY.clone(),
        header_value(&control.code().to_be_bytes()),
    );
    let mark = TxnMark {
        producer_id,
        epoch,
        sequence: NO_SEQUENCE,
    };
    headers.insert(TXN_KEY.clone(), header_value(&mark.encode()));
    let mut message = IggyMessage::builder()
        .payload(value.freeze())
        .user_headers(headers)
        .build()
        .map_err(RecordCodecError::Iggy)?;
    message.header.origin_timestamp = u64::try_from(timestamp_ms)
        .unwrap_or(0)
        .saturating_mul(1000);
    Ok(message)
}

/// The `kafka.t` mark in `stored`, the headers of a message this gateway wrote.
pub(super) fn txn_mark_in(
    stored: &std::collections::BTreeMap<HeaderKey, HeaderValue>,
) -> Option<TxnMark> {
    TxnMark::decode(stored.get(&TXN_KEY)?.as_bytes())
}

/// Whether `stored` are the headers of a control record.
pub(super) fn is_control(stored: &std::collections::BTreeMap<HeaderKey, HeaderValue>) -> bool {
    stored
        .get(&VERSION_KEY)
        .is_some_and(|version| version.as_bytes() == [CONTROL_MAPPING_VERSION])
}

/// Applies a transactional record's mark to the record Fetch serves: its batch is then
/// transactional, with the producer's id and epoch, and the record keeps its sequence.
pub(super) const fn apply(record: &mut Record, mark: TxnMark) {
    record.transactional = true;
    record.producer_id = mark.producer_id;
    record.producer_epoch = mark.epoch;
    record.sequence = mark.sequence;
}

/// The Kafka control record a stored control message stands for.
///
/// # Errors
///
/// When the message has no valid `kafka.ctl` or `kafka.t`, or its payload is not an
/// `EndTransactionMarker` value: a control record this build cannot read is refused, never served
/// as data.
pub(super) fn control_record(
    message: &IggyMessage,
    stored: &std::collections::BTreeMap<HeaderKey, HeaderValue>,
    offset: i64,
    timestamp: i64,
) -> Result<Record> {
    let malformed = || RecordCodecError::ControlRecord(offset);
    let control = stored
        .get(&CONTROL_KEY)
        .and_then(|value| <[u8; 2]>::try_from(value.as_bytes()).ok())
        .map(i16::from_be_bytes)
        .and_then(ControlType::from_code)
        .ok_or_else(malformed)?;
    let mark = txn_mark_in(stored).ok_or_else(malformed)?;
    if message.payload.len() != 6 {
        return Err(malformed());
    }
    let mut key = BytesMut::with_capacity(4);
    key.put_i16(CONTROL_VERSION);
    key.put_i16(control.code());
    Ok(Record {
        transactional: true,
        control: true,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: mark.producer_id,
        producer_epoch: mark.epoch,
        timestamp_type: TimestampType::Creation,
        offset,
        sequence: NO_SEQUENCE,
        timestamp,
        key: Some(key.freeze()),
        value: Some(Bytes::clone(&message.payload)),
        headers: IndexMap::new(),
    })
}

/// The control type and producer of a stored control message, for readers of the log (the
/// coordinator's recovery and tests). `None` for any other message.
#[must_use]
pub fn control_of(message: &IggyMessage) -> Option<(ControlType, TxnMark)> {
    if message.header.user_headers_length == 0 {
        return None;
    }
    let stored = user_headers(message).ok()?;
    if !is_control(&stored) {
        return None;
    }
    let control = stored
        .get(&CONTROL_KEY)
        .and_then(|value| <[u8; 2]>::try_from(value.as_bytes()).ok())
        .map(i16::from_be_bytes)
        .and_then(ControlType::from_code)?;
    Some((control, txn_mark_in(&stored)?))
}

/// The `kafka.t` mark of a stored data record, if it is transactional.
#[must_use]
pub fn txn_mark_of(message: &IggyMessage) -> Option<TxnMark> {
    if message.header.user_headers_length == 0 {
        return None;
    }
    let stored = user_headers(message).ok()?;
    if is_control(&stored) {
        return None;
    }
    txn_mark_in(&stored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{TimestampWindow, from_iggy, mark_of, to_iggy};
    use kafka_protocol::records::{NO_PRODUCER_EPOCH, NO_PRODUCER_ID};

    fn data(value: &'static [u8]) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: NO_SEQUENCE,
            timestamp: 1_700_000_000_123,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(value)),
            headers: IndexMap::new(),
        }
    }

    const MARK: TxnMark = TxnMark {
        producer_id: 77,
        epoch: 3,
        sequence: 10,
    };

    #[test]
    fn given_a_marked_first_record_when_read_should_be_transactional_and_keep_its_kafka_p() {
        let original = data(b"v");
        let mut message = to_iggy(&original, TimestampWindow::of(&[])).unwrap();
        let first = ProducerMark {
            producer_id: 77,
            epoch: 3,
            first_sequence: 10,
            last_sequence: 12,
        };
        mark_transactional(&mut message, MARK, Some(first)).unwrap();
        assert_eq!(
            mark_of(&message),
            Some(first),
            "KW-3 recovery reads it as before"
        );
        assert_eq!(txn_mark_of(&message), Some(MARK));
        let read = from_iggy(&message, 40).unwrap();
        assert!(read.transactional && !read.control);
        assert_eq!(
            (read.producer_id, read.producer_epoch, read.sequence),
            (77, 3, 10)
        );
        assert_eq!(read.key, original.key);
        assert_eq!(read.value, original.value);
        assert!(
            read.headers.is_empty(),
            "no kafka.* header reaches the consumer"
        );
    }

    #[test]
    fn given_a_marked_later_record_when_read_should_be_transactional_without_kafka_p() {
        let mut message = to_iggy(&data(b"v"), TimestampWindow::of(&[])).unwrap();
        let later = TxnMark {
            sequence: 11,
            ..MARK
        };
        mark_transactional(&mut message, later, None).unwrap();
        assert_eq!(mark_of(&message), None);
        let read = from_iggy(&message, 41).unwrap();
        assert!(read.transactional);
        assert_eq!(read.sequence, 11);
    }

    #[test]
    fn given_a_control_message_when_read_should_be_kafkas_control_record() {
        let message = control_message(ControlType::Commit, 77, 4, 9, 1_700_000_000_000).unwrap();
        assert_eq!(
            control_of(&message),
            Some((
                ControlType::Commit,
                TxnMark {
                    producer_id: 77,
                    epoch: 4,
                    sequence: NO_SEQUENCE
                }
            ))
        );
        assert_eq!(txn_mark_of(&message), None, "not a data record");
        assert_eq!(mark_of(&message), None, "KW-3 recovery skips it");
        let read = from_iggy(&message, 50).unwrap();
        assert!(read.control && read.transactional);
        assert_eq!((read.producer_id, read.producer_epoch), (77, 4));
        assert_eq!(read.key.as_deref(), Some(&[0, 0, 0, 1][..]));
        assert_eq!(read.value.as_deref(), Some(&[0, 0, 0, 0, 0, 9][..]));
        assert_eq!(read.timestamp, 1_700_000_000_000);
    }

    #[test]
    fn given_a_control_message_with_a_bad_type_should_refuse_it() {
        let mut message = control_message(ControlType::Abort, 1, 0, 0, 0).unwrap();
        let mut headers = user_headers(&message).unwrap();
        headers.insert(CONTROL_KEY.clone(), header_value(&7_i16.to_be_bytes()));
        message = IggyMessage::builder()
            .payload(message.payload.clone())
            .user_headers(headers)
            .build()
            .unwrap();
        assert!(matches!(
            from_iggy(&message, 3),
            Err(RecordCodecError::ControlRecord(3))
        ));
    }

    #[test]
    fn given_a_mark_value_of_another_layout_should_read_none() {
        let mut value = MARK.encode();
        assert_eq!(TxnMark::decode(&value), Some(MARK));
        value[0] = 2;
        assert_eq!(TxnMark::decode(&value), None);
        assert_eq!(TxnMark::decode(&value[..14]), None);
    }
}
