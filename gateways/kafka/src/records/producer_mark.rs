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

//! The `kafka.p` marker: an idempotent batch's producer id, epoch and sequences, on its first
//! record, so the state of a producer can be rebuilt from the log (KW-3 part 3).
//!
//! The value is 20 bytes, big-endian: a layout byte (1), the producer id (`i64`), the epoch
//! (`i16`), the first and the last sequence (`i32` each), and a flags byte. No flag is set today;
//! bit 0 is kept for a transactional batch (KW-T). A message that carries the marker says
//! `kafka.v` 2: mapping version 1 plus this header. Every other message stays at version 1.

use iggy::prelude::IggyMessage;

use super::{RecordCodecError, Result, VERSION_KEY, header_key, header_value, user_headers};

/// Iggy header holding a batch's [`ProducerMark`], on the batch's first record only.
pub const PRODUCER_HEADER: &str = "kafka.p";
/// The mapping version of a message that carries [`PRODUCER_HEADER`].
pub const PRODUCER_MAPPING_VERSION: u8 = 2;

/// Layout version of the marker value.
const MARK_LAYOUT: u8 = 1;
const MARK_BYTES: usize = 20;

static PRODUCER_KEY: std::sync::LazyLock<iggy::prelude::HeaderKey> =
    std::sync::LazyLock::new(|| header_key(PRODUCER_HEADER));

/// Which producer wrote a batch, and its sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProducerMark {
    pub producer_id: i64,
    pub epoch: i16,
    pub first_sequence: i32,
    pub last_sequence: i32,
}

impl ProducerMark {
    fn encode(self) -> [u8; MARK_BYTES] {
        let mut value = [0; MARK_BYTES];
        value[0] = MARK_LAYOUT;
        value[1..9].copy_from_slice(&self.producer_id.to_be_bytes());
        value[9..11].copy_from_slice(&self.epoch.to_be_bytes());
        value[11..15].copy_from_slice(&self.first_sequence.to_be_bytes());
        value[15..19].copy_from_slice(&self.last_sequence.to_be_bytes());
        // value[19]: flags, none set.
        value
    }

    /// `None` for a value of another length or layout. Flags are ignored: none changes what a
    /// batch's sequences are.
    fn decode(value: &[u8]) -> Option<Self> {
        if value.len() != MARK_BYTES || value[0] != MARK_LAYOUT {
            return None;
        }
        Some(Self {
            producer_id: i64::from_be_bytes(value[1..9].try_into().ok()?),
            epoch: i16::from_be_bytes(value[9..11].try_into().ok()?),
            first_sequence: i32::from_be_bytes(value[11..15].try_into().ok()?),
            last_sequence: i32::from_be_bytes(value[15..19].try_into().ok()?),
        })
    }
}

/// Puts `mark` on `message`, a message this gateway built, and raises its `kafka.v` to 2.
///
/// # Errors
///
/// When the headers no longer fit Iggy's budget with the marker (a record whose headers are at
/// the limit), or when the message's headers do not parse. The caller sends the message
/// unmarked then: the batch is written, and only its recovery after a restart is lost.
pub fn mark(message: &mut IggyMessage, mark: ProducerMark) -> Result<()> {
    let mut headers = user_headers(message)?;
    headers.insert(
        VERSION_KEY.clone(),
        header_value(&[PRODUCER_MAPPING_VERSION]),
    );
    headers.insert(PRODUCER_KEY.clone(), header_value(&mark.encode()));
    let mut marked = IggyMessage::builder()
        .payload(message.payload.clone())
        .user_headers(headers)
        .build()
        .map_err(RecordCodecError::Iggy)?;
    marked.header.origin_timestamp = message.header.origin_timestamp;
    *message = marked;
    Ok(())
}

/// The marker on `message`, if this gateway wrote one there.
#[must_use]
pub fn mark_of(message: &IggyMessage) -> Option<ProducerMark> {
    if message.header.user_headers_length == 0 {
        return None;
    }
    let headers = user_headers(message).ok()?;
    let version = headers.get(&VERSION_KEY)?;
    if version.as_bytes() != [PRODUCER_MAPPING_VERSION] {
        return None;
    }
    ProducerMark::decode(headers.get(&PRODUCER_KEY)?.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{MAPPING_VERSION, TimestampWindow, from_iggy, to_iggy};
    use bytes::Bytes;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
        TimestampType,
    };

    const MARK: ProducerMark = ProducerMark {
        producer_id: 0x0102_0304_0506_0708,
        epoch: 3,
        first_sequence: i32::MAX - 1,
        last_sequence: 0,
    };

    fn record(key: Option<&[u8]>, headers: &[(&str, &[u8])]) -> Record {
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
            key: key.map(Bytes::copy_from_slice),
            value: Some(Bytes::from_static(b"value")),
            headers: headers
                .iter()
                .map(|(name, value)| {
                    (
                        StrBytes::from_string((*name).to_string()),
                        Some(Bytes::copy_from_slice(value)),
                    )
                })
                .collect::<IndexMap<_, _>>(),
        }
    }

    #[test]
    fn given_a_marked_message_when_read_should_give_the_mark_and_the_same_record() {
        for original in [record(None, &[]), record(Some(b"k"), &[("trace", b"t1")])] {
            let mut message = to_iggy(&original, TimestampWindow::of(&[])).unwrap();
            let before = from_iggy(&message, 4).unwrap();
            assert_eq!(mark_of(&message), None, "unmarked");
            mark(&mut message, MARK).unwrap();
            assert_eq!(mark_of(&message), Some(MARK));
            let after = from_iggy(&message, 4).unwrap();
            assert_eq!(after, before, "Fetch shows a marked record unchanged");
            assert!(after.headers.keys().all(|name| !name.starts_with("kafka.")));
        }
    }

    #[test]
    fn given_an_unmarked_message_its_mapping_version_stays_one() {
        let message = to_iggy(&record(Some(b"k"), &[]), TimestampWindow::of(&[])).unwrap();
        let headers = user_headers(&message).unwrap();
        assert_eq!(
            headers
                .get(&VERSION_KEY)
                .map(|value| value.as_bytes().to_vec()),
            Some(vec![MAPPING_VERSION])
        );
    }

    #[test]
    fn given_a_value_of_another_layout_should_read_no_mark() {
        let mut value = MARK.encode();
        assert_eq!(ProducerMark::decode(&value), Some(MARK));
        value[0] = 2;
        assert_eq!(ProducerMark::decode(&value), None);
        assert_eq!(ProducerMark::decode(&value[..19]), None);
    }
}
