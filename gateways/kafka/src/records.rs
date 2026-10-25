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

//! One Kafka record to and from one Iggy message.
//!
//! `docs/BRIDGE_MAPPING.md` is the specification. This module implements it and nothing else:
//! no Iggy calls, no handler wiring.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::{LazyLock, OnceLock};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::prelude::{HeaderKey, HeaderValue, IggyError, IggyMessage, MAX_PAYLOAD_SIZE};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use thiserror::Error;

mod headers;

pub use headers::{Header, HeaderList, RepeatedHeaders, headers_at};
use headers::{Order, fold, order_of, read_list, reorder, repeats_a_name, splice};

/// Iggy header whose one-byte value is the storage mapping version.
///
/// Written on every message this gateway produces and on no other, which is what lets the read
/// path tell its own messages from an Iggy client's. The `kafka.` namespace is reserved by
/// convention only, so presence of one namespaced header proves nothing on its own.
pub const VERSION_HEADER: &str = "kafka.v";
/// Storage mapping version this build writes and reads.
pub const MAPPING_VERSION: u8 = 1;

mod producer_mark;
mod txn_mark;

pub use producer_mark::{
    FLAG_TRANSACTIONAL, PRODUCER_HEADER, PRODUCER_MAPPING_VERSION, ProducerMark,
    mark as mark_producer, mark_of,
};
pub use txn_mark::{
    CONTROL_HEADER, CONTROL_MAPPING_VERSION, ControlType, TXN_HEADER, TxnMark, control_message,
    control_of, mark_transactional, txn_mark_of,
};

/// Iggy header carrying the Kafka record key.
pub const KEY_HEADER: &str = "kafka.key";
/// Iggy header naming which of null or empty a placeholder payload stands for.
pub const VALUE_MARKER_HEADER: &str = "kafka.value";
/// Iggy header holding the Kafka timestamp, `Int64` ms, when `origin_timestamp` cannot.
pub const TIMESTAMP_HEADER: &str = "kafka.ts";
/// Prefix every Kafka record header name is stored under.
pub const HEADER_PREFIX: &str = "kafka.h.";
/// Iggy header holding the producer's header order, when it is not the sorted one Iggy keeps.
pub const ORDER_HEADER: &str = "kafka.ho";
/// Iggy header whose one-byte value is the envelope byte layout version.
pub const ENVELOPE_HEADER: &str = "kafka.envelope";
/// Envelope byte layout version this build writes and reads.
pub const ENVELOPE_VERSION: u8 = 1;

/// Kafka sends this for a record with no timestamp.
const NO_TIMESTAMP: i64 = -1;
/// The one Kafka timestamp an `origin_timestamp` of zero cannot be told apart from.
const EPOCH_TIMESTAMP: i64 = 0;
/// Widest `origin_timestamp` span one Iggy send holds (`MAX_TIMESTAMP_DELTA_MICROS`).
const MAX_SEND_SPAN_MICROS: u64 = u32::MAX as u64;
/// Stored in place of a null or empty value, discarded on the way back.
const PLACEHOLDER: &[u8] = &[0x00];
/// Iggy caps one header name and one header value at this many bytes.
const MAX_FIELD: usize = 255;

const MARKER_NULL: &[u8] = b"null";
const MARKER_EMPTY: &[u8] = b"empty";

const FLAG_KEY: u8 = 0b01;
const FLAG_VALUE: u8 = 0b10;

/// Flags byte, key length, value length and header count, per `BRIDGE_MAPPING.md`.
const ENVELOPE_OVERHEAD: usize = 13;
/// Name length, value-present byte and value length, before either field's own bytes.
const ENVELOPE_HEADER_OVERHEAD: usize = 9;

/// Record batch version this gateway writes. v2 is the only shape `kafka_protocol` encodes.
const BATCH_VERSION: i8 = 2;

/// Smallest v2 record: a length, an attributes byte, two deltas, two field lengths and a header
/// count, each a one-byte varint at least.
const MIN_RECORD_BYTES: usize = 7;
/// Smallest v2 record header: a name length varint and a value length varint, both empty.
const MIN_HEADER_BYTES: usize = 2;
/// Bytes a zigzag varint occupies at most, which is what `kafka_protocol` reads.
const MAX_VARINT_BYTES: usize = 5;
/// Bytes a zigzag varlong occupies at most, on the same terms.
const MAX_VARLONG_BYTES: usize = 10;
/// Base offset, batch length, leader epoch, magic, CRC, attributes, last offset delta, first and
/// max timestamp, producer id, producer epoch, base sequence and record count.
pub(crate) const BATCH_HEADER_BYTES: usize = 61;
/// Widest v2 record framing before the key, the value and the header bytes: the length, offset
/// delta, key length, value length and header count varints, the timestamp delta varlong, and the
/// attributes byte.
const RECORD_FRAMING_BYTES: usize = 5 * MAX_VARINT_BYTES + MAX_VARLONG_BYTES + 1;
/// Widest per-header framing inside a v2 record: a name length and a value length varint.
const HEADER_FRAMING_BYTES: usize = 10;

/// Marks a snappy stream written by Kafka's own framing rather than raw snappy.
///
/// Kafka producers write xerial-framed snappy, which raw snappy decoders reject, and the Java
/// broker falls back to raw when the magic is absent. Both shapes therefore reach a broker.
const SNAPPY_MAGIC: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

/// Why a record or a batch could not cross.
#[derive(Debug, Error)]
pub enum RecordCodecError {
    #[error("Iggy rejected the message: {0}")]
    Iggy(#[from] IggyError),
    #[error("record timestamp {0} ms does not fit Iggy's microsecond field")]
    TimestampOutOfRange(i64),
    #[error("{0} bytes of stored user headers did not parse")]
    UserHeadersUnreadable(u32),
    #[error(
        "stored mapping version {0} is not {MAPPING_VERSION}, {PRODUCER_MAPPING_VERSION} or {CONTROL_MAPPING_VERSION}"
    )]
    MappingVersion(u8),
    #[error("value marker {0:?} is neither null nor empty")]
    ValueMarker(Bytes),
    #[error("two stored header keys both name {0}")]
    HeaderNameCollision(String),
    #[error("timestamp header {0:?} is not a Kafka timestamp")]
    TimestampHeader(Bytes),
    #[error("envelope for this record is {size} bytes, over Iggy's {MAX_PAYLOAD_SIZE} byte limit")]
    EnvelopeTooLarge { size: usize },
    #[error("envelope is truncated: needed {needed} bytes, {remaining} remain")]
    EnvelopeTruncated { needed: usize, remaining: usize },
    #[error("envelope has {0} bytes left after its last header")]
    EnvelopeTrailingBytes(usize),
    #[error("envelope format version {0} is not {ENVELOPE_VERSION}")]
    EnvelopeVersion(u8),
    #[error("envelope header name is not UTF-8")]
    EnvelopeHeaderName,
    #[error("record batch is malformed: {0}")]
    Batch(String),
    #[error("{0} record batches in one partition, Kafka allows 1")]
    SeveralBatches(usize),
    #[error("{0} bytes follow the record batch")]
    BatchTrailingBytes(usize),
    #[error("batch declares {count} records, and {limit} bytes can hold fewer")]
    RecordCountTooLarge { count: i32, limit: usize },
    #[error("batch declares {declared} records and holds {walked}")]
    RecordCountMismatch { declared: usize, walked: usize },
    #[error("record declares {count} headers, and {limit} bytes can hold fewer")]
    HeaderCountTooLarge { count: i32, limit: usize },
    #[error("record batch ends inside a record")]
    RecordTruncated,
    #[error("record field declares {0} bytes")]
    RecordFieldLength(i32),
    #[error("transactional batches are not supported")]
    TransactionalBatch,
    #[error("control batches are not supported")]
    ControlBatch,
    #[error("a transactional Produce request carries a batch that is not transactional")]
    NotTransactional,
    #[error("stored control record at offset {0} does not parse")]
    ControlRecord(i64),
    #[error("partition decompresses to at least {size} bytes, over its {limit} byte cap")]
    BudgetExceeded { size: usize, limit: usize },
    #[error("partition needs {count} record slots, over its {limit} slot cap")]
    RecordBudgetExceeded { count: usize, limit: usize },
    #[error("the request budget ran out, retry")]
    RequestBudgetSpent,
    #[error("zstd batch in a request older than Produce v7")]
    ZstdTooEarly,
    #[error("header order {0:?} does not fit the stored headers")]
    HeaderOrder(Bytes),
}

type Result<T> = std::result::Result<T, RecordCodecError>;

/// Encodes one Kafka record as one Iggy message, for a send whose timestamps fit `window`.
///
/// Takes the native path when Iggy can hold every field, and the envelope otherwise. A caller
/// cannot tell which from the return value, which is the point: `from_iggy` reverses both.
///
/// # Errors
///
/// Returns an error when the timestamp does not fit, when the envelope would exceed
/// `MAX_PAYLOAD_SIZE`, or when Iggy rejects the message for a reason the envelope does not fix.
pub fn to_iggy(record: &Record, window: TimestampWindow) -> Result<IggyMessage> {
    to_iggy_with(record, None, window)
}

/// [`to_iggy`] for a record whose headers `Record::headers` may not hold in full.
///
/// `headers` is the record's full list when it repeats a name ([`DecodedBatch::headers`]), and
/// then replaces `Record::headers`. Such a record takes the envelope, which stores a list.
///
/// # Errors
///
/// As [`to_iggy`].
pub fn to_iggy_with(
    record: &Record,
    headers: Option<&[Header]>,
    window: TimestampWindow,
) -> Result<IggyMessage> {
    let stamp = window.stamp(record.timestamp)?;
    if let Some(list) = headers {
        return envelope_message(
            record,
            list.iter().map(|(name, value)| (name, value)),
            stamp,
        );
    }
    if let Some(value) = plain_value(record, stamp) {
        return plain_message(value.clone(), stamp.origin);
    }
    let order = order_of(record.headers.keys());
    if matches!(order, Order::TooMany) || needs_envelope(record) {
        return envelope_message(record, record.headers.iter(), stamp);
    }
    let (payload, marker) = split_value(record.value.as_ref());
    let mut headers = gateway_headers(stamp.header);
    if let Some(marker) = marker {
        headers.insert(header_key(VALUE_MARKER_HEADER), header_value(marker));
    }
    if let Some(key) = record.key.as_ref() {
        headers.insert(header_key(KEY_HEADER), header_value(key));
    }
    for (name, value) in &record.headers {
        // `needs_envelope` rejected the shapes that cannot be built here, so both are infallible.
        let Some(value) = value.as_ref() else {
            continue;
        };
        headers.insert(
            header_key(&format!("{HEADER_PREFIX}{}", name.as_str())),
            header_value(value),
        );
    }
    if let Order::Ranks(ranks) = order {
        headers.insert(header_key(ORDER_HEADER), header_value(&ranks));
    }

    // The only limit left is the 100 KB budget over all headers together, which no per-field
    // check can see. Let the constructor rule on it rather than duplicating its arithmetic.
    build(payload, headers, stamp.origin)?.map_or_else(
        || envelope_message(record, record.headers.iter(), stamp),
        Ok,
    )
}

/// The `origin_timestamp` range one Iggy send holds: `u32::MAX` µs, about 71.6 min.
///
/// Starts at the batch's earliest real timestamp. A timestamp outside it is clamped into it, and
/// `kafka.ts` keeps the real one, so a Kafka batch always fits one send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimestampWindow {
    start: u64,
}

impl TimestampWindow {
    /// The window for `records`, the batch one send carries. `-1` and the epoch do not move it.
    #[must_use]
    pub fn of(records: &[Record]) -> Self {
        let start = records
            .iter()
            .filter(|record| record.timestamp > EPOCH_TIMESTAMP)
            .filter_map(|record| timestamp_in(record.timestamp).ok())
            .min()
            .unwrap_or(0);
        Self { start }
    }

    fn stamp(self, millis: i64) -> Result<Stamp> {
        let native = timestamp_in(millis)?;
        let origin = native.clamp(self.start, self.start.saturating_add(MAX_SEND_SPAN_MICROS));
        // A zero origin reads back as "no timestamp", so the epoch needs the header unclamped too.
        let header = (origin != native || millis == EPOCH_TIMESTAMP).then_some(millis);
        Ok(Stamp { origin, header })
    }
}

/// Where one record's timestamp is stored.
#[derive(Clone, Copy)]
struct Stamp {
    origin: u64,
    /// The `kafka.ts` value, when `origin` does not read back as the record's timestamp.
    header: Option<i64>,
}

/// Decodes one Iggy message as one Kafka record at `offset`.
///
/// A message without `kafka.v` was written by an Iggy client, not through this gateway. It gets a
/// null key, its own user headers under their own names, and its payload as the record value.
/// None of the `kafka.` headers carries meaning on such a message, because the namespace is
/// reserved by `BRIDGE_MAPPING.md` and by nothing the server enforces.
///
/// A message with `kafka.v` is taken as written here, so every marker on it is authoritative and
/// one this build does not recognize is an error rather than a guess. Nothing on the server
/// enforces the namespace, so that is a claim the message makes and not one the server keeps: an
/// Iggy writer can set `kafka.v` to a version this build does not implement, and every read of
/// that message then fails. Fetch cannot serve a record it cannot decode and a Kafka consumer
/// cannot step over one, so Fetch answers -1 at that offset and the consumer stops there.
/// `BRIDGE_MAPPING.md` records that as the open end of the provenance design.
///
/// # Errors
///
/// Returns an error when the stored user headers do not parse, when `kafka.v` names a mapping
/// version this build does not implement, or when a marker or an envelope is malformed.
pub fn from_iggy(message: &IggyMessage, offset: i64) -> Result<Record> {
    from_iggy_with(message, offset).map(|(record, _)| record)
}

/// [`from_iggy`], plus the record's full header list when `Record::headers` cannot hold it
/// because two headers share a name. [`encode_batch_with`] writes that list.
///
/// # Errors
///
/// As [`from_iggy`].
pub fn from_iggy_with(message: &IggyMessage, offset: i64) -> Result<(Record, Option<HeaderList>)> {
    // The common record: a value alone, which `plain_message` wrote. Skips the header map.
    if message.user_headers.as_deref() == Some(version_only()) {
        let value = Some(message.payload.clone());
        let record = record(None, value, IndexMap::new(), offset, timestamp_out(message));
        return Ok((record, None));
    }
    let stored = user_headers(message)?;
    let timestamp = stored_timestamp(message, &stored)?;
    if !stored.contains_key(&VERSION_KEY) {
        let (key, value, headers, list) = foreign_fields(message, &stored);
        return Ok((record(key, value, headers, offset, timestamp), list));
    }
    // KW-T: a control record ends a transaction, and a transactional record says whose it is.
    if txn_mark::is_control(&stored) {
        let control = txn_mark::control_record(message, &stored, offset, timestamp)?;
        return Ok((control, None));
    }
    let (key, value, headers, list) = if let Some(envelope) = stored.get(&ENVELOPE_KEY) {
        decode_envelope(envelope.as_bytes(), &message.payload)?
    } else {
        gateway_fields(message, &stored)?
    };
    let mut record = record(key, value, headers, offset, timestamp);
    if let Some(mark) = txn_mark::txn_mark_in(&stored) {
        txn_mark::apply(&mut record, mark);
    }
    Ok((record, list))
}

/// The timestamp [`from_iggy`] reports for `message`, read from its headers alone.
///
/// # Errors
///
/// Fails where `from_iggy` fails before it reaches the key, the value or the envelope: unreadable
/// user headers, an unknown `kafka.v`, or a `kafka.ts` that is not a Kafka timestamp. A malformed
/// envelope or value marker does not fail here.
pub fn record_timestamp(message: &IggyMessage) -> Result<i64> {
    if message.user_headers.as_deref() == Some(version_only()) {
        return Ok(timestamp_out(message));
    }
    stored_timestamp(message, &user_headers(message)?)
}

/// Checks `kafka.v` and reads `kafka.ts`, which only a gateway-written message has.
fn stored_timestamp(
    message: &IggyMessage,
    stored: &BTreeMap<HeaderKey, HeaderValue>,
) -> Result<i64> {
    let Some(version) = stored.get(&VERSION_KEY) else {
        return Ok(timestamp_out(message));
    };
    // Version 2 is version 1 plus the `kafka.p` marker, which no Kafka field reads. Version 3 is
    // a control record (KW-T).
    if ![
        MAPPING_VERSION,
        PRODUCER_MAPPING_VERSION,
        CONTROL_MAPPING_VERSION,
    ]
    .iter()
    .any(|known| version.as_bytes() == [*known])
    {
        let version = version.as_bytes().first().copied().unwrap_or_default();
        return Err(RecordCodecError::MappingVersion(version));
    }
    stored.get(&TIMESTAMP_KEY).map_or_else(
        || Ok(timestamp_out(message)),
        |header| match header.as_int64() {
            Ok(NO_TIMESTAMP) => Ok(server_timestamp(message)),
            Ok(millis) if millis >= EPOCH_TIMESTAMP => Ok(millis),
            _ => Err(RecordCodecError::TimestampHeader(header.value())),
        },
    )
}

/// The stored user headers, with an unreadable block told apart from an absent one.
///
/// `IggyMessage::user_headers_map` folds a header block it cannot parse into `Ok(None)`, which
/// reads the same as a message that carries no headers at all. Taken at face value that turns an
/// enveloped message into its own envelope bytes served as the record value.
fn user_headers(message: &IggyMessage) -> Result<BTreeMap<HeaderKey, HeaderValue>> {
    match message.user_headers_map()? {
        Some(stored) => Ok(stored),
        None if message.header.user_headers_length > 0 => Err(
            RecordCodecError::UserHeadersUnreadable(message.header.user_headers_length),
        ),
        None => Ok(BTreeMap::new()),
    }
}

/// Kafka counts milliseconds, Iggy counts microseconds, and `-1` means the broker assigns one.
fn timestamp_in(millis: i64) -> Result<u64> {
    if millis == NO_TIMESTAMP {
        return Ok(0);
    }
    millis
        .checked_mul(1000)
        .and_then(|micros| u64::try_from(micros).ok())
        .ok_or(RecordCodecError::TimestampOutOfRange(millis))
}

/// Zero means the producer sent no timestamp, so the server-assigned one stands in.
///
/// `from_iggy` reads `kafka.ts` first: an epoch or clamped record stores a value that does not
/// read back as its own.
fn timestamp_out(message: &IggyMessage) -> i64 {
    match message.header.origin_timestamp {
        0 => server_timestamp(message),
        micros => i64::try_from(micros / 1000).unwrap_or(NO_TIMESTAMP),
    }
}

fn server_timestamp(message: &IggyMessage) -> i64 {
    i64::try_from(message.header.timestamp / 1000).unwrap_or(NO_TIMESTAMP)
}

/// Whether any field of `record` is one Iggy refuses to hold natively.
///
/// A record that repeats a header name never reaches here: its full list goes straight to the
/// envelope.
fn needs_envelope(record: &Record) -> bool {
    let key_unholdable = record
        .key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > MAX_FIELD);
    if key_unholdable {
        return true;
    }
    record.headers.iter().any(|(name, value)| {
        HEADER_PREFIX.len() + name.as_str().len() > MAX_FIELD
            || value
                .as_ref()
                .is_none_or(|value| value.is_empty() || value.len() > MAX_FIELD)
    })
}

/// The value, when `kafka.v` is the only header the record needs.
fn plain_value(record: &Record, stamp: Stamp) -> Option<&Bytes> {
    if record.key.is_some() || !record.headers.is_empty() || stamp.header.is_some() {
        return None;
    }
    record.value.as_ref().filter(|value| !value.is_empty())
}

/// Reuses one encoded `kafka.v` header block, so the common record skips a map and an encode.
///
/// Static bytes: a clone touches no refcount shared across workers.
fn plain_message(value: Bytes, origin: u64) -> Result<IggyMessage> {
    let headers = version_only();
    let mut message = IggyMessage::builder().payload(value).build()?;
    message.header.user_headers_length =
        u32::try_from(headers.len()).unwrap_or_else(|_| unreachable!("the kafka.v header is tiny"));
    message.user_headers = Some(Bytes::from_static(headers));
    message.header.origin_timestamp = origin;
    Ok(message)
}

/// Payload to store, and the marker naming what the original value was when it is not the payload.
fn split_value(value: Option<&Bytes>) -> (Bytes, Option<&'static [u8]>) {
    match value {
        None => (Bytes::from_static(PLACEHOLDER), Some(MARKER_NULL)),
        Some(value) if value.is_empty() => (Bytes::from_static(PLACEHOLDER), Some(MARKER_EMPTY)),
        Some(value) => (value.clone(), None),
    }
}

/// The headers every gateway-written message carries, whichever path it takes, plus `kafka.ts`
/// when `timestamp` holds one.
fn gateway_headers(timestamp: Option<i64>) -> BTreeMap<HeaderKey, HeaderValue> {
    let mut headers = BTreeMap::new();
    headers.insert(header_key(VERSION_HEADER), header_value(&[MAPPING_VERSION]));
    if let Some(millis) = timestamp {
        headers.insert(header_key(TIMESTAMP_HEADER), HeaderValue::from(millis));
    }
    headers
}

/// `Ok(None)` when the headers together pass Iggy's budget, which the envelope then carries.
fn build(
    payload: Bytes,
    headers: BTreeMap<HeaderKey, HeaderValue>,
    origin: u64,
) -> Result<Option<IggyMessage>> {
    let mut message = match IggyMessage::builder()
        .payload(payload)
        .user_headers(headers)
        .build()
    {
        Ok(message) => message,
        Err(IggyError::TooBigUserHeaders) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    message.header.origin_timestamp = origin;
    Ok(Some(message))
}

/// Headers as the envelope reads them, whichever of a `Record`'s map or a full list holds them.
trait HeaderIter<'a>: Iterator<Item = (&'a StrBytes, &'a Option<Bytes>)> + Clone {}

impl<'a, I: Iterator<Item = (&'a StrBytes, &'a Option<Bytes>)> + Clone> HeaderIter<'a> for I {}

fn envelope_message<'a>(
    record: &Record,
    headers: impl HeaderIter<'a>,
    stamp: Stamp,
) -> Result<IggyMessage> {
    // The envelope moves the key and the headers into the payload, so a record whose value alone
    // clears `MAX_PAYLOAD_SIZE` can be one the fallback cannot hold. Say so before spending the
    // allocation, since the native path has already been ruled out and nothing else is left.
    let size = envelope_size(record, headers.clone());
    if size > MAX_PAYLOAD_SIZE as usize {
        return Err(RecordCodecError::EnvelopeTooLarge { size });
    }

    let payload = encode_envelope(record, headers, size);
    let mut headers = gateway_headers(stamp.header);
    headers.insert(
        header_key(ENVELOPE_HEADER),
        header_value(&[ENVELOPE_VERSION]),
    );
    build(payload, headers, stamp.origin)?
        .ok_or(IggyError::TooBigUserHeaders)
        .map_err(Into::into)
}

/// Exactly what `encode_envelope` writes for `record` with `headers`.
fn envelope_size<'a>(record: &Record, headers: impl HeaderIter<'a>) -> usize {
    let field = |field: Option<&Bytes>| field.map_or(0, Bytes::len);
    ENVELOPE_OVERHEAD
        + field(record.key.as_ref())
        + field(record.value.as_ref())
        + headers
            .map(|(name, value)| {
                ENVELOPE_HEADER_OVERHEAD + name.as_str().len() + field(value.as_ref())
            })
            .sum::<usize>()
}

/// 13 bytes of fixed overhead plus 9 per header, little-endian throughout.
fn encode_envelope<'a>(record: &Record, headers: impl HeaderIter<'a>, size: usize) -> Bytes {
    let mut flags = 0u8;
    if record.key.is_some() {
        flags |= FLAG_KEY;
    }
    if record.value.is_some() {
        flags |= FLAG_VALUE;
    }

    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(flags);
    put_field(&mut buf, record.key.as_ref());
    put_field(&mut buf, record.value.as_ref());
    buf.put_u32_le(u32::try_from(headers.clone().count()).unwrap_or(u32::MAX));
    for (name, value) in headers {
        let name = name.as_str().as_bytes();
        buf.put_u32_le(u32::try_from(name.len()).unwrap_or(u32::MAX));
        buf.put_slice(name);
        buf.put_u8(u8::from(value.is_some()));
        put_field(&mut buf, value.as_ref());
    }
    buf.freeze()
}

/// Key, value, the headers a `Record` holds, and the full list when that map cannot hold it.
type RecordFields = (
    Option<Bytes>,
    Option<Bytes>,
    IndexMap<StrBytes, Option<Bytes>>,
    Option<HeaderList>,
);

fn decode_envelope(version: &[u8], payload: &Bytes) -> Result<RecordFields> {
    match version.first() {
        Some(&ENVELOPE_VERSION) => {}
        Some(&other) => return Err(RecordCodecError::EnvelopeVersion(other)),
        None => return Err(RecordCodecError::EnvelopeVersion(0)),
    }

    let mut buf = payload.clone();
    let flags = take(&mut buf, 1)?[0];
    let key = take_field(&mut buf)?;
    let value = take_field(&mut buf)?;
    let count =
        u32::from_le_bytes(take(&mut buf, 4)?.as_ref().try_into().unwrap_or_default()) as usize;

    // The count is four bytes of a payload anyone can write and every header costs at least nine,
    // so reserving before reading lets a 13-byte message ask for four billion entries. Charge the
    // floor against what is left and the reserve below is bounded by the input.
    let needed = count.saturating_mul(ENVELOPE_HEADER_OVERHEAD);
    if buf.remaining() < needed {
        return Err(RecordCodecError::EnvelopeTruncated {
            needed,
            remaining: buf.remaining(),
        });
    }

    let mut list = Vec::with_capacity(count);
    for _ in 0..count {
        let name = take_field(&mut buf)?;
        let name = StrBytes::try_from(name).map_err(|_| RecordCodecError::EnvelopeHeaderName)?;
        let present = take(&mut buf, 1)?[0] != 0;
        let value = take_field(&mut buf)?;
        list.push((name, present.then_some(value)));
    }

    // Every byte of an envelope is accounted for above, so a leftover means this payload is not
    // one. Accepting it would turn a stray `kafka.envelope` header plus junk into a null record.
    if buf.has_remaining() {
        return Err(RecordCodecError::EnvelopeTrailingBytes(buf.remaining()));
    }

    let (headers, list) = fold(list);
    Ok((
        (flags & FLAG_KEY != 0).then_some(key),
        (flags & FLAG_VALUE != 0).then_some(value),
        headers,
        list,
    ))
}

/// The native reading of a message this gateway wrote, so every marker on it is authoritative.
fn gateway_fields(
    message: &IggyMessage,
    stored: &BTreeMap<HeaderKey, HeaderValue>,
) -> Result<RecordFields> {
    let key = stored.get(&KEY_KEY).map(HeaderValue::value);
    let value = match stored.get(&VALUE_MARKER_KEY) {
        None => Some(message.payload.clone()),
        Some(marker) => match marker.as_bytes() {
            MARKER_NULL => None,
            MARKER_EMPTY => Some(Bytes::new()),
            // A marker this build does not write, on a message that says this build wrote it.
            // Guessing loses a payload or invents one, and a later version that adds a third
            // marker is read wrongly here rather than refused.
            _ => return Err(RecordCodecError::ValueMarker(marker.value())),
        },
    };

    let mut headers = IndexMap::new();
    for (name, stored_value) in stored {
        let Some(name) = header_name(name).and_then(|name| name.strip_prefix(HEADER_PREFIX)) else {
            continue;
        };
        // Two stored keys can hold the same bytes under different kinds, because an Iggy header
        // key orders on kind before bytes (`user_headers.rs:209`). `to_iggy` writes one kind, so
        // on a message this build wrote the names cannot collide, and a collision here says the
        // message is not what its `kafka.v` claims. Overwriting would drop a header quietly.
        let name = StrBytes::from_string(name.to_string());
        if headers
            .insert(name.clone(), Some(stored_value.value()))
            .is_some()
        {
            return Err(RecordCodecError::HeaderNameCollision(name.to_string()));
        }
    }
    // Iggy hands the headers back sorted by name. `kafka.ho` is there when the producer's order
    // was another one.
    if let Some(order) = stored.get(&ORDER_KEY) {
        headers = reorder(&headers, &order.value())?;
    }
    Ok((key, value, headers, None))
}

/// The reading of a message an Iggy client wrote, which no marker on it can change.
///
/// Every header passes through under its own name, including one in the `kafka.` namespace. The
/// alternative was to treat any namespaced header as gateway metadata, which made a single
/// `kafka.`-prefixed header hide every other header on the message.
///
/// Two keys holding the same bytes under different kinds are two stored headers and one Kafka
/// name. Kafka carries headers as a list, so both reach the consumer, in key order, through the
/// full list `Record::headers` cannot hold.
fn foreign_fields(
    message: &IggyMessage,
    stored: &BTreeMap<HeaderKey, HeaderValue>,
) -> RecordFields {
    let list = stored
        .iter()
        .filter_map(|(name, stored_value)| {
            let name = header_name(name)?;
            Some((
                StrBytes::from_string(name.to_string()),
                Some(stored_value.value()),
            ))
        })
        .collect();
    let (headers, list) = fold(list);
    (None, Some(message.payload.clone()), headers, list)
}

/// A Kafka header name for an Iggy header key, or `None` when the key is not text.
///
/// An Iggy key is bytes plus a kind, and `HeaderField::as_str` refuses every kind but `String`
/// (`user_headers.rs:372`). Other SDKs hand out `Raw` and `Int32` key constructors, so a key
/// holding a perfectly good Kafka name arrives under a kind this one would reject. Read the bytes
/// and let UTF-8 decide.
fn header_name(key: &HeaderKey) -> Option<&str> {
    std::str::from_utf8(key.as_bytes()).ok()
}

const fn record(
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: IndexMap<StrBytes, Option<Bytes>>,
    offset: i64,
    timestamp: i64,
) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset,
        sequence: NO_SEQUENCE,
        timestamp,
        key,
        value,
        headers,
    }
}

fn put_field(buf: &mut BytesMut, field: Option<&Bytes>) {
    let field = field.map_or(&[][..], |field| field.as_ref());
    buf.put_u32_le(u32::try_from(field.len()).unwrap_or(u32::MAX));
    buf.put_slice(field);
}

fn take(buf: &mut Bytes, needed: usize) -> Result<Bytes> {
    if buf.remaining() < needed {
        return Err(RecordCodecError::EnvelopeTruncated {
            needed,
            remaining: buf.remaining(),
        });
    }
    Ok(buf.split_to(needed))
}

/// Length-prefixed bytes, possibly empty. Presence is the flags byte's job, not the length's,
/// so that an empty key stays distinct from a null one.
fn take_field(buf: &mut Bytes) -> Result<Bytes> {
    let len = u32::from_le_bytes(take(buf, 4)?.as_ref().try_into().unwrap_or_default()) as usize;
    take(buf, len)
}

/// The encoded header block that holds `kafka.v` alone.
fn version_only() -> &'static [u8] {
    static VERSION_ONLY: OnceLock<&'static [u8]> = OnceLock::new();
    VERSION_ONLY.get_or_init(|| {
        let block = IggyMessage::builder()
            .payload(Bytes::from_static(PLACEHOLDER))
            .user_headers(gateway_headers(None))
            .build()
            .ok()
            .and_then(|message| message.user_headers)
            .unwrap_or_else(|| unreachable!("the kafka.v header always fits"));
        Box::leak(block.to_vec().into_boxed_slice())
    })
}

/// Lookup keys for the headers `from_iggy` reads, built once.
static VERSION_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(VERSION_HEADER));
static ENVELOPE_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(ENVELOPE_HEADER));
static TIMESTAMP_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(TIMESTAMP_HEADER));
static KEY_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(KEY_HEADER));
static VALUE_MARKER_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(VALUE_MARKER_HEADER));
static ORDER_KEY: LazyLock<HeaderKey> = LazyLock::new(|| header_key(ORDER_HEADER));

/// Both are infallible for the names and values this module builds: every one is non-empty and
/// within `MAX_FIELD`, which `needs_envelope` guarantees for caller-supplied bytes.
fn header_key(name: &str) -> HeaderKey {
    HeaderKey::try_from(name).unwrap_or_else(|_| unreachable!("header name {name} is out of range"))
}

fn header_value(value: &[u8]) -> HeaderValue {
    HeaderValue::try_from(value).unwrap_or_else(|_| unreachable!("header value is out of range"))
}

/// Decompressed bytes and record slots, the two things a decode spends.
///
/// A record slot costs a `Record` and an `IggyMessage` in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Allowance {
    pub bytes: usize,
    pub records: usize,
}

impl Allowance {
    #[must_use]
    pub const fn times(self, factor: usize) -> Self {
        Self {
            bytes: self.bytes.saturating_mul(factor),
            records: self.records.saturating_mul(factor),
        }
    }
}

/// What one Produce request may decode.
///
/// - One partition may take `partition`. Past it, it is too large alone (10).
/// - All partitions together may take `request`. It counts work done, refused partitions too, so
///   no refusal lets the same inflate run again for free. Once it runs out, the partition that
///   ran it out and every later one answer 6, undecoded. A retry that comes first in its request
///   gets the full allowance, so a partition too large alone still ends at 10.
///
/// Writes go through `BudgetedWriter`, so no partition inflates past either one in memory.
pub struct DecompressionBudget {
    partition: Allowance,
    /// What the request has left.
    bytes_left: Cell<usize>,
    records_left: Cell<usize>,
    /// What the current partition took.
    entry_bytes: Cell<usize>,
    entry_records: Cell<usize>,
    spent: Cell<bool>,
    /// Why this module refused a batch, when it did. Everything this module reports from inside
    /// the decoder's decompression hook leaves as an `io::Error` or an `anyhow::Error`, which the
    /// decoder stringifies, so the typed reason is parked here and taken by `decode_batch`.
    reason: Cell<Option<RecordCodecError>>,
}

impl DecompressionBudget {
    #[must_use]
    pub const fn new(partition: Allowance, request: Allowance) -> Self {
        Self {
            partition,
            bytes_left: Cell::new(request.bytes),
            records_left: Cell::new(request.records),
            entry_bytes: Cell::new(0),
            entry_records: Cell::new(0),
            spent: Cell::new(false),
            reason: Cell::new(None),
        }
    }

    /// Whether the request allowance ran out, so nothing more decodes.
    #[must_use]
    pub const fn is_spent(&self) -> bool {
        self.spent.get()
    }

    fn start_entry(&self) -> Result<()> {
        if self.is_spent() {
            return Err(RecordCodecError::RequestBudgetSpent);
        }
        self.entry_bytes.set(0);
        self.entry_records.set(0);
        Ok(())
    }

    /// Takes `len` decoded bytes for the current partition, or refuses.
    fn charge(&self, len: usize) -> io::Result<()> {
        let entry = self.entry_bytes.get().saturating_add(len);
        if entry > self.partition.bytes {
            return Err(self.refuse(RecordCodecError::BudgetExceeded {
                size: entry,
                limit: self.partition.bytes,
            }));
        }
        let Some(left) = self.bytes_left.get().checked_sub(len) else {
            return Err(self.refuse(self.spend()));
        };
        self.bytes_left.set(left);
        self.entry_bytes.set(entry);
        Ok(())
    }

    fn charge_records(&self, count: usize) -> Result<()> {
        let entry = self.entry_records.get().saturating_add(count);
        if entry > self.partition.records {
            return Err(RecordCodecError::RecordBudgetExceeded {
                count: entry,
                limit: self.partition.records,
            });
        }
        let Some(left) = self.records_left.get().checked_sub(count) else {
            return Err(self.spend());
        };
        self.records_left.set(left);
        self.entry_records.set(entry);
        Ok(())
    }

    fn spend(&self) -> RecordCodecError {
        self.spent.set(true);
        RecordCodecError::RequestBudgetSpent
    }

    /// Parks the typed reason and returns the error the decoder stringifies.
    fn refuse(&self, reason: RecordCodecError) -> io::Error {
        let error = io::Error::other(reason.to_string());
        self.reason.set(Some(reason));
        error
    }

    /// The typed reason for a decoder error, when this module is what caused it.
    ///
    /// Taken rather than read, because the budget outlives one batch. Left in place, the first
    /// refusal would reclassify every later error on the same request as that same refusal.
    fn reason(&self, error: &str) -> RecordCodecError {
        self.reason
            .take()
            .unwrap_or_else(|| RecordCodecError::Batch(error.to_string()))
    }
}

/// An `io::Write` sink that stops at the budget rather than after it.
///
/// Charging the output once it exists is too late: the decompressor has already allocated it.
struct BudgetedWriter<'a> {
    out: BytesMut,
    budget: &'a DecompressionBudget,
}

impl<'a> BudgetedWriter<'a> {
    fn new(budget: &'a DecompressionBudget) -> Self {
        Self {
            out: BytesMut::new(),
            budget,
        }
    }

    fn into_output(self) -> Bytes {
        self.out.freeze()
    }
}

impl Write for BudgetedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.budget.charge(buf.len())?;
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Whether a partition may hold zstd. Produce allows it from v7.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Zstd {
    Allowed,
    Refused,
}

/// Whether the first batch in `blob` is compressed. Byte 22 is the low byte of its attributes.
#[must_use]
pub fn is_compressed(blob: &[u8]) -> bool {
    blob.get(22)
        .is_some_and(|attributes| attributes & 0b111 != 0)
}

/// The producer id of the first batch in `blob`, bytes 43 to 50. `None` when it has none.
#[must_use]
pub fn producer_id(blob: &[u8]) -> Option<i64> {
    let id = i64::from_be_bytes(blob.get(43..51)?.try_into().ok()?);
    (id != NO_PRODUCER_ID).then_some(id)
}

/// The record count the first batch in `blob` declares, bytes 57 to 60. `None` when it has none.
#[must_use]
pub fn declared_records(blob: &[u8]) -> Option<usize> {
    let count = i32::from_be_bytes(blob.get(57..BATCH_HEADER_BYTES)?.try_into().ok()?);
    usize::try_from(count).ok()
}

/// Record slots the first batch in `blob` takes to decode, when it is not compressed: one per
/// declared record, plus its headers as `scan_records` charges them. Stops at `most`.
///
/// Counts the records it can walk. The decode refuses a batch that does not walk, so that batch
/// holds nothing.
#[must_use]
pub fn record_slots(blob: &[u8], most: usize) -> usize {
    let mut slots = declared_records(blob).unwrap_or(0);
    let mut records = blob.get(BATCH_HEADER_BYTES..).unwrap_or_default();
    while slots < most && !records.is_empty() {
        let Ok((headers, _)) = take_record(&mut records) else {
            break;
        };
        slots = slots.saturating_add(headers / HEADERS_PER_SLOT);
    }
    slots.min(most)
}

/// One decoded Produce batch.
#[derive(Debug, Default)]
pub struct DecodedBatch {
    /// The records, in batch order.
    pub records: Vec<Record>,
    /// The full header list of each record that repeats a name, which `Record::headers` folds.
    pub repeated: RepeatedHeaders,
}

impl DecodedBatch {
    /// The full header list of record `index`, when its `Record::headers` does not hold it all.
    #[must_use]
    pub fn headers(&self, index: usize) -> Option<&[Header]> {
        headers_at(&self.repeated, index)
    }
}

/// Decodes the one record batch a Produce partition carries.
///
/// A record may repeat a header name, as Kafka allows. `kafka_protocol` keeps the last value of
/// each name only, so the record's full list comes back in [`DecodedBatch::repeated`].
///
/// # Errors
///
/// Returns an error when the blob holds more than one batch or bytes after it, when the batch is
/// malformed or holds a record count other than the one it declares, when it is a control or
/// transactional batch, when it is zstd and `zstd` refuses it, or when it passes `budget` or the
/// budget is already spent.
pub fn decode_batch(
    buf: &mut Bytes,
    budget: &DecompressionBudget,
    zstd: Zstd,
) -> Result<DecodedBatch> {
    decode_batch_as(buf, budget, zstd, Transactional::Refused)
}

/// Whether a Produce partition may carry a transactional batch: only in a request that names a
/// transactional id, on a gateway that serves transactions (KW-T).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transactional {
    /// Refused with [`RecordCodecError::TransactionalBatch`].
    Refused,
    /// Required: a batch that is not transactional is refused with
    /// [`RecordCodecError::NotTransactional`].
    Required,
}

/// [`decode_batch`], taking a transactional batch when `transactional` requires one.
///
/// # Errors
///
/// As [`decode_batch`], and [`RecordCodecError::NotTransactional`] for a plain batch where a
/// transactional one is required.
pub fn decode_batch_as(
    buf: &mut Bytes,
    budget: &DecompressionBudget,
    zstd: Zstd,
    transactional: Transactional,
) -> Result<DecodedBatch> {
    budget.start_entry()?;
    preflight(buf, budget, zstd, transactional)?;
    if !buf.has_remaining() {
        return Ok(DecodedBatch::default());
    }

    let walked = Cell::new(0);
    let repeated = Cell::new(Vec::new());
    let set = RecordBatchDecoder::decode_with_custom_compression(
        buf,
        Some(
            |compressed: &mut Bytes, compression| -> anyhow::Result<Bytes> {
                let (records, scan) = decompress(compressed, compression, budget)?;
                walked.set(scan.records);
                repeated.set(scan.repeated);
                Ok(records)
            },
        ),
    )
    .map_err(|error| budget.reason(&error.to_string()))?;
    // The decoder stops at the declared count and drops the rest.
    if set.records.len() != walked.get() {
        return Err(RecordCodecError::RecordCountMismatch {
            declared: set.records.len(),
            walked: walked.get(),
        });
    }
    Ok(DecodedBatch {
        records: set.records,
        repeated: repeated.take(),
    })
}

/// Checks the batch header before anything decodes a record.
///
/// - One batch per partition, as Kafka requires from Produce v3, and no bytes after it.
/// - The decoder reserves from `record_count` (`kafka-protocol-0.18.0/src/records.rs:517`), so
///   the count must fit the blob, plus the partition cap when compressed.
/// - Control and transactional batches are refused: a stored message cannot carry their flags.
///   An idempotent batch passes, its producer id, epoch and sequence ignored, because a stock
///   Java producer is idempotent. `IDEMPOTENCE.md` has why a retry is then not deduplicated.
fn preflight(
    buf: &Bytes,
    budget: &DecompressionBudget,
    zstd: Zstd,
    transactional: Transactional,
) -> Result<()> {
    let mut rest = buf.clone();
    let infos = RecordBatchDecoder::decode_batch_info(&mut rest)
        .map_err(|error| RecordCodecError::Batch(error.to_string()))?;
    let info = match infos.as_slice() {
        [] => return Ok(()),
        [info] => info,
        several => return Err(RecordCodecError::SeveralBatches(several.len())),
    };
    // `decode_batch_info` stops at a magic byte other than 2 and leaves the rest unread, and so
    // does the decoder. Unchecked, the tail is dropped and the partition answers success.
    if rest.has_remaining() {
        return Err(RecordCodecError::BatchTrailingBytes(rest.remaining()));
    }
    if info.control {
        return Err(RecordCodecError::ControlBatch);
    }
    match (transactional, info.transactional) {
        (Transactional::Refused, true) => return Err(RecordCodecError::TransactionalBatch),
        (Transactional::Required, false) => return Err(RecordCodecError::NotTransactional),
        _ => {}
    }
    if zstd == Zstd::Refused && info.compression == Compression::Zstd {
        return Err(RecordCodecError::ZstdTooEarly);
    }

    let limit = if info.compression == Compression::None {
        buf.len()
    } else {
        buf.len().saturating_add(budget.partition.bytes)
    };
    let declared = usize::try_from(info.record_count).unwrap_or(0);
    if declared.saturating_mul(MIN_RECORD_BYTES) > limit {
        return Err(RecordCodecError::RecordCountTooLarge {
            count: info.record_count,
            limit,
        });
    }
    budget.charge_records(declared)
}

/// Encodes records as one uncompressed v2 batch.
///
/// Fetch always emits uncompressed, so the read path spends no CPU on a codec the client did
/// not ask for. `BRIDGE_MAPPING.md` records that as a default open to revisiting.
///
/// Takes the records by mutable reference to normalize `sequence`. The encoder groups records
/// while `offset - sequence` holds (`kafka-protocol-0.18.0/src/records.rs:277`), and `record()`
/// pins `sequence` at `NO_SEQUENCE` while offsets advance, which breaks the group on every record
/// and spends a 61-byte batch header on each one. Numbering each sequence from the first offset
/// keeps one batch and leaves the encoded `base_sequence` at `NO_SEQUENCE`. Fetch reads a
/// partition forward, so the first record carries the lowest offset, and that is what makes the
/// encoded `base_sequence` come out at `NO_SEQUENCE` rather than at an offset.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the batch.
pub fn encode_batch(records: &mut [Record]) -> Result<Bytes> {
    encode_batch_with(records, &RepeatedHeaders::new())
}

/// [`encode_batch`], writing the full header list in `repeated` for each record it names in place
/// of that record's `Record::headers`.
///
/// # Errors
///
/// As [`encode_batch`].
pub fn encode_batch_with(records: &mut [Record], repeated: &RepeatedHeaders) -> Result<Bytes> {
    if records.is_empty() {
        return Ok(Bytes::new());
    }
    // One batch per run of records that share a producer (KW-T): a transactional run keeps its
    // producer id, epoch and sequences, a control record stands alone, and everything else is one
    // batch as before. Without transactional records the whole slice is one run.
    let mut out: Option<BytesMut> = None;
    let mut start = 0;
    while start < records.len() {
        let end = start + run_length(&records[start..]);
        let run_repeated: RepeatedHeaders = repeated
            .iter()
            .filter(|(at, _)| (start..end).contains(at))
            .map(|(at, list)| (at - start, list.clone()))
            .collect();
        let batch = encode_one_batch(&mut records[start..end], &run_repeated)?;
        if start == 0 && end == records.len() {
            return Ok(batch);
        }
        out.get_or_insert_with(BytesMut::new).put_slice(&batch);
        start = end;
    }
    Ok(out.map_or_else(Bytes::new, BytesMut::freeze))
}

/// How many records from the start of `records` share its first record's batch.
fn run_length(records: &[Record]) -> usize {
    if records.is_empty() {
        return 0;
    }
    1 + records
        .windows(2)
        .take_while(|pair| continues_batch(&pair[0], &pair[1]))
        .count()
}

/// Whether `next` goes in the same encoded batch as `previous`, the record before it (KW-T): two
/// records without a producer do, a control record never shares one, and a producer's records do
/// while the producer, epoch and transactional flag hold and their sequences follow their offsets.
#[must_use]
pub(crate) fn continues_batch(previous: &Record, next: &Record) -> bool {
    if previous.control || next.control {
        return false;
    }
    if previous.producer_id == NO_PRODUCER_ID || next.producer_id == NO_PRODUCER_ID {
        return previous.producer_id == next.producer_id;
    }
    let gap = |record: &Record| record.offset.wrapping_sub(i64::from(record.sequence));
    previous.producer_id == next.producer_id
        && previous.producer_epoch == next.producer_epoch
        && previous.transactional == next.transactional
        && gap(previous) == gap(next)
}

/// Encodes one run as one batch. A run without a producer gets the sequences that make its
/// `base_sequence` come out at `NO_SEQUENCE`; a producer's run keeps its own.
fn encode_one_batch(records: &mut [Record], repeated: &RepeatedHeaders) -> Result<Bytes> {
    let Some(base) = records.first().map(|record| record.offset) else {
        return Ok(Bytes::new());
    };
    for record in records
        .iter_mut()
        .filter(|record| record.producer_id == NO_PRODUCER_ID)
    {
        let delta = record.offset.saturating_sub(base);
        record.sequence = i32::try_from(delta)
            .unwrap_or(i32::MAX)
            .saturating_add(NO_SEQUENCE);
    }

    let mut buf = BytesMut::with_capacity(batch_size(records));
    let options = RecordEncodeOptions {
        version: BATCH_VERSION,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut buf, records.iter(), &options)
        .map_err(|error| RecordCodecError::Batch(error.to_string()))?;
    let batch = buf.freeze();
    if repeated.is_empty() {
        return Ok(batch);
    }
    splice(&batch, repeated)
}

/// An upper bound on the bytes `record` adds to a batch [`encode_batch`] writes.
///
/// Fetch sums these against its byte budget before it encodes.
#[must_use]
pub(crate) fn record_size_bound(record: &Record) -> usize {
    record_size_bound_with(record, None)
}

/// [`record_size_bound`] for a record written with `headers`, its full list, when there is one.
#[must_use]
pub(crate) fn record_size_bound_with(record: &Record, headers: Option<&[Header]>) -> usize {
    let field = |field: Option<&Bytes>| field.map_or(0, Bytes::len);
    let headers: &mut dyn Iterator<Item = (&StrBytes, &Option<Bytes>)> = match headers {
        Some(list) => &mut list.iter().map(|(name, value)| (name, value)),
        None => &mut record.headers.iter(),
    };
    RECORD_FRAMING_BYTES
        + field(record.key.as_ref())
        + field(record.value.as_ref())
        + headers
            .map(|(name, value)| HEADER_FRAMING_BYTES + name.as_str().len() + field(value.as_ref()))
            .sum::<usize>()
}

/// An upper bound on the encoded batch, so the Fetch path reserves once instead of doubling.
///
/// `kafka_protocol` never reserves: it writes every field with `put_slice` into whatever buffer
/// it is handed. Every length needed here is already in hand.
fn batch_size(records: &[Record]) -> usize {
    BATCH_HEADER_BYTES + records.iter().map(record_size_bound).sum::<usize>()
}

/// Decompresses one batch, refusing the write that would pass the budget.
///
/// `kafka_protocol`'s own decompressors write the whole stream into a growing buffer before they
/// hand it over, so the four codecs are driven from here instead. Each one writes through
/// `BudgetedWriter`, which bounds the peak rather than reporting an overrun after the fact.
///
/// The decoder calls this for every batch, uncompressed ones included, which is what makes it the
/// one place that sees the record bytes before anything reserves from what they declare.
///
/// Returns the records and what the scan found in them.
fn decompress(
    compressed: &mut Bytes,
    compression: Compression,
    budget: &DecompressionBudget,
) -> anyhow::Result<(Bytes, Scan)> {
    let body = compressed.copy_to_bytes(compressed.remaining());
    let mut writer = BudgetedWriter::new(budget);
    let records = match compression {
        Compression::None => {
            budget.charge(body.len())?;
            body
        }
        Compression::Gzip => {
            let mut decoder = flate2::write::GzDecoder::new(&mut writer);
            decoder.write_all(&body)?;
            decoder.finish()?;
            writer.into_output()
        }
        Compression::Zstd => {
            zstd::stream::copy_decode(body.as_ref(), &mut writer)?;
            writer.into_output()
        }
        Compression::Lz4 => {
            let mut decoder = lz4::Decoder::new(body.as_ref())?;
            io::copy(&mut decoder, &mut writer)?;
            decoder.finish().1?;
            writer.into_output()
        }
        Compression::Snappy => {
            inflate_snappy(&body, &mut writer)?;
            writer.into_output()
        }
    };

    match scan_records(&records, budget) {
        Ok(scan) => Ok((records, scan)),
        Err(reason) => Err(budget.refuse(reason).into()),
    }
}

/// Headers that cost one record slot. A decoded header takes about a third of a slot's memory.
const HEADERS_PER_SLOT: usize = 3;

/// What [`scan_records`] found in one batch's records.
struct Scan {
    /// How many there are, which `decode_batch` checks against the batch header.
    records: usize,
    /// The full header list of each record that repeats a name.
    repeated: RepeatedHeaders,
}

/// Walks the records of one batch before the decoder reserves for them.
///
/// - Refuses a header count the record cannot hold. The decoder reserves an `IndexMap` from it
///   (`kafka-protocol-0.18.0/src/records.rs:896`), and that memory is resident.
/// - Charges headers to the record budget, [`HEADERS_PER_SLOT`] per slot.
/// - Reads the full header list of a record that repeats a name, which the decoder folds.
fn scan_records(records: &Bytes, budget: &DecompressionBudget) -> Result<Scan> {
    let mut blob = records.as_ref();
    let mut walked = 0;
    let mut slots = 0;
    let mut names = Vec::new();
    let mut repeated = RepeatedHeaders::new();
    while !blob.is_empty() {
        let (headers, record) = take_record(&mut blob)?;
        if headers > 1 && repeats_a_name(record, headers, &mut names)? {
            repeated.push((walked, read_list(records, record, headers)?));
        }
        slots += headers / HEADERS_PER_SLOT;
        walked += 1;
    }
    budget.charge_records(slots)?;
    Ok(Scan {
        records: walked,
        repeated,
    })
}

/// Steps over one record of `blob`. Returns its header count and its header bytes, and refuses a
/// count those bytes cannot hold.
///
/// The framing mirrors `Record::decode_new`, field for field.
fn take_record<'a>(blob: &mut &'a [u8]) -> Result<(usize, &'a [u8])> {
    let size = take_varint(blob)?;
    let size = usize::try_from(size).map_err(|_| RecordCodecError::RecordFieldLength(size))?;
    let mut record = take_bytes(blob, size)?;

    take_bytes(&mut record, 1)?; // attributes
    skip_varint(&mut record, MAX_VARLONG_BYTES)?; // timestamp delta
    skip_varint(&mut record, MAX_VARINT_BYTES)?; // offset delta
    skip_field(&mut record)?; // key
    skip_field(&mut record)?; // value

    let count = take_varint(&mut record)?;
    let limit = record.len();
    let headers = usize::try_from(count)
        .ok()
        .filter(|headers| headers.saturating_mul(MIN_HEADER_BYTES) <= limit)
        .ok_or(RecordCodecError::HeaderCountTooLarge { count, limit })?;
    Ok((headers, record))
}

/// Reads a zigzag varint, five bytes at most, the way `kafka_protocol` reads one.
fn take_varint(buf: &mut &[u8]) -> Result<i32> {
    let mut value = 0u32;
    for (index, &byte) in buf.iter().enumerate().take(MAX_VARINT_BYTES) {
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte < 0x80 || index + 1 == MAX_VARINT_BYTES {
            *buf = &buf[index + 1..];
            return Ok((value >> 1).cast_signed() ^ -(value & 1).cast_signed());
        }
    }
    Err(RecordCodecError::RecordTruncated)
}

/// Steps over a zigzag varint of up to `most` bytes, for the fields the scan does not read.
fn skip_varint(buf: &mut &[u8], most: usize) -> Result<()> {
    for (index, &byte) in buf.iter().enumerate().take(most) {
        if byte < 0x80 || index + 1 == most {
            *buf = &buf[index + 1..];
            return Ok(());
        }
    }
    Err(RecordCodecError::RecordTruncated)
}

fn take_bytes<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    let (head, rest) = buf
        .split_at_checked(len)
        .ok_or(RecordCodecError::RecordTruncated)?;
    *buf = rest;
    Ok(head)
}

/// A length-prefixed record field, where `-1` is the absent one Kafka writes for a null.
fn skip_field(buf: &mut &[u8]) -> Result<()> {
    let len = take_varint(buf)?;
    if len < -1 {
        return Err(RecordCodecError::RecordFieldLength(len));
    }
    if len > 0 {
        take_bytes(buf, usize::try_from(len).unwrap_or_default())?;
    }
    Ok(())
}

/// Kafka's snappy, and raw snappy for the producers that send it.
///
/// Snappy is the one codec that states its output size up front, which `snap` reads without
/// allocating. Charge that number before the decoder runs, because the decoder needs the whole
/// block laid out to write into and cannot be fed a bounded sink.
fn inflate_snappy(body: &Bytes, writer: &mut BudgetedWriter<'_>) -> anyhow::Result<()> {
    let mut decoder = snap::raw::Decoder::new();
    let mut inflate = |block: &[u8], writer: &mut BudgetedWriter<'_>| -> anyhow::Result<()> {
        let declared = snap::raw::decompress_len(block)?;
        writer.budget.charge(declared)?;
        let start = writer.out.len();
        writer.out.resize(start.saturating_add(declared), 0);
        decoder.decompress(block, &mut writer.out[start..])?;
        Ok(())
    };

    let Some(mut blocks) = body.strip_prefix(SNAPPY_MAGIC) else {
        return inflate(body.as_ref(), writer);
    };
    while !blocks.is_empty() {
        let (length, rest) = blocks
            .split_at_checked(4)
            .ok_or_else(|| anyhow::anyhow!("snappy block length is truncated"))?;
        let length = u32::from_be_bytes(length.try_into()?) as usize;
        let (block, rest) = rest
            .split_at_checked(length)
            .ok_or_else(|| anyhow::anyhow!("snappy block of {length} bytes is truncated"))?;
        inflate(block, writer)?;
        blocks = rest;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::HeaderKind;

    const CREATE_TIME: i64 = 1_700_000_000_123;

    fn record_with(
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> Record {
        let headers = headers
            .iter()
            .map(|(name, value)| {
                (
                    StrBytes::from_string((*name).to_string()),
                    value.map(Bytes::copy_from_slice),
                )
            })
            .collect();
        record(
            key.map(Bytes::copy_from_slice),
            value.map(Bytes::copy_from_slice),
            headers,
            0,
            CREATE_TIME,
        )
    }

    fn record_at(timestamp: i64) -> Record {
        record(
            Some(Bytes::from_static(b"k")),
            Some(Bytes::from_static(b"v")),
            IndexMap::new(),
            0,
            timestamp,
        )
    }

    fn message_with(payload: &'static [u8], headers: &[(&str, &[u8])]) -> IggyMessage {
        let headers = headers
            .iter()
            .map(|(name, value)| (header_key(name), header_value(value)))
            .collect();
        IggyMessage::builder()
            .payload(Bytes::from_static(payload))
            .user_headers(headers)
            .build()
            .unwrap()
    }

    /// The same, plus the version header that marks a message as gateway-written.
    fn gateway_message(payload: &'static [u8], headers: &[(&str, &[u8])]) -> IggyMessage {
        let mut all = vec![(VERSION_HEADER, &[MAPPING_VERSION][..])];
        all.extend_from_slice(headers);
        message_with(payload, &all)
    }

    /// `to_iggy` for a record sent on its own, so nothing clamps it.
    fn to_iggy_alone(record: &Record) -> Result<IggyMessage> {
        to_iggy(record, TimestampWindow::of(std::slice::from_ref(record)))
    }

    /// A request allowance of one partition's.
    fn partition_budget(bytes: usize, records: usize) -> DecompressionBudget {
        let allowance = Allowance { bytes, records };
        DecompressionBudget::new(allowance, allowance)
    }

    #[test]
    fn given_a_value_only_record_when_encoded_should_match_the_general_path() {
        let plain = to_iggy_alone(&record_with(None, Some(b"v"), &[])).unwrap();
        let general = gateway_message(b"v", &[]);

        assert_eq!(plain.user_headers, general.user_headers);
        assert_eq!(
            plain.header.user_headers_length,
            general.header.user_headers_length
        );
        assert_eq!(plain.header.origin_timestamp, 1_700_000_000_123_000);
        // Fetch reads this block without a header map. It must match the full read.
        let back = from_iggy(&plain, 7).unwrap();
        assert_eq!(back.value.as_deref(), Some(&b"v"[..]));
        assert_eq!(back.timestamp, CREATE_TIME);
        assert_eq!(back.key, None);
        assert!(back.headers.is_empty());
        assert_eq!(back.offset, 7);
    }

    fn envelope_bytes(count: u32, trailing: &[u8]) -> Bytes {
        let mut payload = BytesMut::new();
        payload.put_u8(0);
        payload.put_u32_le(0);
        payload.put_u32_le(0);
        payload.put_u32_le(count);
        payload.put_slice(trailing);
        payload.freeze()
    }

    fn is_enveloped(message: &IggyMessage) -> bool {
        message
            .user_headers_map()
            .unwrap()
            .unwrap_or_default()
            .contains_key(&header_key(ENVELOPE_HEADER))
    }

    #[test]
    fn given_a_plain_record_when_round_tripped_should_keep_key_value_and_headers() {
        let original = record_with(Some(b"k"), Some(b"v"), &[("trace", Some(b"abc"))]);
        let message = to_iggy_alone(&original).unwrap();
        assert!(!is_enveloped(&message));
        assert_eq!(message.payload.as_ref(), b"v");

        let back = from_iggy(&message, 7).unwrap();
        assert_eq!(back.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(back.value.as_deref(), Some(&b"v"[..]));
        assert_eq!(back.offset, 7);
        assert_eq!(
            back.headers.get(&StrBytes::from_static_str("trace")),
            Some(&Some(Bytes::from_static(b"abc")))
        );
        assert_eq!(back.headers.len(), 1, "no gateway header reaches Kafka");
    }

    #[test]
    fn given_a_null_value_when_round_tripped_should_stay_null() {
        let message = to_iggy_alone(&record_with(Some(b"k"), None, &[])).unwrap();
        assert!(
            !is_enveloped(&message),
            "a tombstone stays on the fast path"
        );
        assert_eq!(message.payload.as_ref(), PLACEHOLDER);
        assert_eq!(from_iggy(&message, 0).unwrap().value, None);
    }

    #[test]
    fn given_an_empty_value_when_round_tripped_should_stay_empty_and_not_null() {
        let message = to_iggy_alone(&record_with(Some(b"k"), Some(b""), &[])).unwrap();
        assert_eq!(message.payload.as_ref(), PLACEHOLDER);
        assert_eq!(
            from_iggy(&message, 0).unwrap().value.as_deref(),
            Some(&[][..])
        );
    }

    #[test]
    fn given_an_empty_key_when_stored_should_take_the_envelope() {
        let original = record_with(Some(b""), Some(b"v"), &[]);
        let message = to_iggy_alone(&original).unwrap();
        assert!(is_enveloped(&message));
        let back = from_iggy(&message, 0).unwrap();
        assert_eq!(back.key.as_deref(), Some(&[][..]), "empty, not null");
        assert_eq!(back.value.as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn given_an_oversized_key_when_stored_should_take_the_envelope() {
        let key = vec![b'x'; MAX_FIELD + 1];
        let message = to_iggy_alone(&record_with(Some(&key), Some(b"v"), &[])).unwrap();
        assert!(is_enveloped(&message));
        assert_eq!(
            from_iggy(&message, 0).unwrap().key.as_deref(),
            Some(&key[..])
        );
    }

    #[test]
    fn given_a_null_header_value_when_stored_should_take_the_envelope() {
        let message =
            to_iggy_alone(&record_with(Some(b"k"), Some(b"v"), &[("flag", None)])).unwrap();
        assert!(is_enveloped(&message));
        assert_eq!(
            from_iggy(&message, 0)
                .unwrap()
                .headers
                .get(&StrBytes::from_static_str("flag")),
            Some(&None),
            "a null header value survives the envelope as null"
        );
    }

    #[test]
    fn given_an_oversized_header_name_when_stored_should_take_the_envelope() {
        let name = "n".repeat(MAX_FIELD - HEADER_PREFIX.len() + 1);
        let message =
            to_iggy_alone(&record_with(Some(b"k"), Some(b"v"), &[(&name, Some(b"v"))])).unwrap();
        assert!(is_enveloped(&message));
        assert!(
            from_iggy(&message, 0)
                .unwrap()
                .headers
                .contains_key(&StrBytes::from_string(name))
        );
    }

    #[test]
    fn given_an_iggy_written_message_when_encoded_should_have_a_null_key_and_its_own_headers() {
        let message = message_with(b"{}", &[("source", b"connector")]);

        let record = from_iggy(&message, 3).unwrap();
        assert_eq!(record.key, None);
        assert_eq!(record.value.as_deref(), Some(&b"{}"[..]));
        assert_eq!(
            record.headers.get(&StrBytes::from_static_str("source")),
            Some(&Some(Bytes::from_static(b"connector")))
        );
    }

    #[test]
    fn given_an_iggy_message_with_a_reserved_header_when_read_should_keep_every_header() {
        // No `kafka.v`, so nothing here was written by the gateway and the reserved namespace
        // carries no meaning. The payload is the value and `own` must not disappear.
        let message = message_with(
            b"payload",
            &[(VALUE_MARKER_HEADER, MARKER_NULL), ("own", b"1")],
        );

        let record = from_iggy(&message, 0).unwrap();
        assert_eq!(
            record.value.as_deref(),
            Some(&b"payload"[..]),
            "not a tombstone"
        );
        assert_eq!(
            record.headers.get(&StrBytes::from_static_str("own")),
            Some(&Some(Bytes::from_static(b"1")))
        );
        assert!(
            record
                .headers
                .contains_key(&StrBytes::from_static_str(VALUE_MARKER_HEADER)),
            "a reserved name an Iggy client chose passes through under that name"
        );
    }

    #[test]
    fn given_a_non_string_key_kind_when_read_should_use_the_bytes_as_the_name() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_raw(HeaderKind::Raw, b"trace").unwrap(),
            header_value(b"abc"),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();

        assert_eq!(
            from_iggy(&message, 0)
                .unwrap()
                .headers
                .get(&StrBytes::from_static_str("trace")),
            Some(&Some(Bytes::from_static(b"abc"))),
            "other SDKs hand out Raw key constructors, and the bytes are a valid Kafka name"
        );
    }

    #[test]
    fn given_unreadable_user_headers_when_read_should_fail() {
        let mut message = to_iggy_alone(&record_with(Some(b"k"), Some(b"v"), &[])).unwrap();
        message.user_headers = Some(Bytes::from_static(b"not a header block"));

        assert!(
            matches!(
                from_iggy(&message, 0),
                Err(RecordCodecError::UserHeadersUnreadable(_))
            ),
            "a block Iggy cannot parse must not read as a message with no headers"
        );
    }

    #[test]
    fn given_an_unknown_mapping_version_when_read_should_fail() {
        let message = message_with(b"v", &[(VERSION_HEADER, &[CONTROL_MAPPING_VERSION + 1])]);
        assert!(matches!(
            from_iggy(&message, 0),
            Err(RecordCodecError::MappingVersion(4))
        ));
    }

    #[test]
    fn given_an_unknown_value_marker_on_a_gateway_message_when_read_should_fail() {
        let message = gateway_message(b"v", &[(VALUE_MARKER_HEADER, b"neither")]);
        assert!(
            matches!(
                from_iggy(&message, 0),
                Err(RecordCodecError::ValueMarker(_))
            ),
            "a marker this build does not write cannot stand for null or empty"
        );
    }

    #[test]
    fn given_a_timestamp_header_that_is_not_int64_on_a_gateway_message_when_read_should_fail() {
        let message = gateway_message(b"v", &[(TIMESTAMP_HEADER, b"epoch")]);
        assert!(matches!(
            from_iggy(&message, 0),
            Err(RecordCodecError::TimestampHeader(_))
        ));
    }

    #[test]
    fn given_a_timestamp_header_below_minus_one_when_read_should_fail() {
        let mut headers = BTreeMap::new();
        headers.insert(header_key(VERSION_HEADER), header_value(&[MAPPING_VERSION]));
        headers.insert(header_key(TIMESTAMP_HEADER), HeaderValue::from(-2i64));
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();

        assert!(
            matches!(
                from_iggy(&message, 0),
                Err(RecordCodecError::TimestampHeader(_))
            ),
            "to_iggy refuses these, so no gateway message holds one"
        );
    }

    #[test]
    fn given_timestamps_past_one_send_when_stored_should_clamp_and_keep_the_real_one() {
        let later = CREATE_TIME + 72 * 60 * 1000;
        let records = [record_at(CREATE_TIME), record_at(later)];
        let window = TimestampWindow::of(&records);
        let first = to_iggy(&records[0], window).unwrap();
        let second = to_iggy(&records[1], window).unwrap();

        assert_eq!(
            second.header.origin_timestamp - first.header.origin_timestamp,
            MAX_SEND_SPAN_MICROS,
            "clamped to the window end, so one send holds both"
        );
        assert_eq!(from_iggy(&first, 0).unwrap().timestamp, CREATE_TIME);
        assert_eq!(from_iggy(&second, 1).unwrap().timestamp, later);
    }

    #[test]
    fn given_no_timestamp_among_real_ones_when_round_tripped_should_read_the_server_timestamp() {
        let records = [record_at(CREATE_TIME), record_at(NO_TIMESTAMP)];
        let mut message = to_iggy(&records[1], TimestampWindow::of(&records)).unwrap();
        assert_eq!(
            message.header.origin_timestamp,
            CREATE_TIME.cast_unsigned() * 1000,
            "clamped to the window start"
        );
        message.header.timestamp = 5_000_000;
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, 5_000);
    }

    #[test]
    fn given_an_epoch_timestamp_among_real_ones_when_round_tripped_should_stay_at_the_epoch() {
        let records = [record_at(CREATE_TIME), record_at(EPOCH_TIMESTAMP)];
        let message = to_iggy(&records[1], TimestampWindow::of(&records)).unwrap();
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, EPOCH_TIMESTAMP);
    }

    #[test]
    fn given_a_batch_when_windowed_should_start_at_its_earliest_real_timestamp() {
        let records = [
            record_at(NO_TIMESTAMP),
            record_at(EPOCH_TIMESTAMP),
            record_at(CREATE_TIME + 5),
            record_at(CREATE_TIME),
        ];
        assert_eq!(
            TimestampWindow::of(&records).start,
            CREATE_TIME.cast_unsigned() * 1000
        );
        assert_eq!(TimestampWindow::of(&[record_at(NO_TIMESTAMP)]).start, 0);
    }

    #[test]
    fn given_a_create_time_when_round_tripped_should_come_back_unchanged() {
        let message = to_iggy_alone(&record_at(CREATE_TIME)).unwrap();
        assert_eq!(
            message.header.origin_timestamp,
            CREATE_TIME.cast_unsigned() * 1000
        );
        assert_eq!(
            from_iggy(&message, 0).unwrap().timestamp,
            CREATE_TIME,
            "a record that arrived through Produce survives the round trip exactly"
        );
    }

    #[test]
    fn given_no_timestamp_when_stored_should_store_zero() {
        assert_eq!(timestamp_in(NO_TIMESTAMP).unwrap(), 0);
    }

    #[test]
    fn given_an_out_of_range_timestamp_when_stored_should_fail() {
        assert!(matches!(
            timestamp_in(i64::MAX),
            Err(RecordCodecError::TimestampOutOfRange(_))
        ));
    }

    #[test]
    fn given_an_epoch_timestamp_when_round_tripped_should_stay_at_the_epoch() {
        let mut message = to_iggy_alone(&record_at(EPOCH_TIMESTAMP)).unwrap();
        message.header.timestamp = 5_000_000;
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, EPOCH_TIMESTAMP);
    }

    #[test]
    fn given_an_epoch_timestamp_in_an_envelope_when_read_should_stay_at_the_epoch() {
        let original = record(
            Some(Bytes::new()),
            Some(Bytes::from_static(b"v")),
            IndexMap::new(),
            0,
            EPOCH_TIMESTAMP,
        );
        let mut message = to_iggy_alone(&original).unwrap();
        assert!(is_enveloped(&message));
        message.header.timestamp = 5_000_000;
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, EPOCH_TIMESTAMP);
    }

    #[test]
    fn given_every_record_shape_when_its_timestamp_is_read_alone_should_match_from_iggy() {
        let later = CREATE_TIME + 72 * 60 * 1000;
        let clamped = [record_at(CREATE_TIME), record_at(later)];
        let unset_among_real = [record_at(CREATE_TIME), record_at(NO_TIMESTAMP)];
        let epoch_among_real = [record_at(CREATE_TIME), record_at(EPOCH_TIMESTAMP)];
        let enveloped = |timestamp| {
            record(
                Some(Bytes::new()),
                Some(Bytes::from_static(b"v")),
                IndexMap::new(),
                0,
                timestamp,
            )
        };
        let mut unreadable = to_iggy_alone(&record_at(CREATE_TIME)).unwrap();
        unreadable.user_headers = Some(Bytes::from_static(b"not a header block"));
        let mut foreign_with_ts = BTreeMap::new();
        foreign_with_ts.insert(header_key(TIMESTAMP_HEADER), HeaderValue::from(CREATE_TIME));
        let foreign_with_ts = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(foreign_with_ts)
            .build()
            .unwrap();
        let mut messages = vec![
            to_iggy_alone(&record_with(None, Some(b"v"), &[])).unwrap(),
            to_iggy_alone(&record_with(Some(b"k"), Some(b"v"), &[("h", Some(b"x"))])).unwrap(),
            to_iggy_alone(&record_at(NO_TIMESTAMP)).unwrap(),
            to_iggy_alone(&record_at(EPOCH_TIMESTAMP)).unwrap(),
            to_iggy_alone(&enveloped(CREATE_TIME)).unwrap(),
            to_iggy_alone(&enveloped(EPOCH_TIMESTAMP)).unwrap(),
            to_iggy_alone(&enveloped(NO_TIMESTAMP)).unwrap(),
            to_iggy(&clamped[1], TimestampWindow::of(&clamped)).unwrap(),
            to_iggy(&unset_among_real[1], TimestampWindow::of(&unset_among_real)).unwrap(),
            to_iggy(&epoch_among_real[1], TimestampWindow::of(&epoch_among_real)).unwrap(),
            message_with(b"v", &[]),
            message_with(b"v", &[("trace", b"abc")]),
            foreign_with_ts,
            gateway_message(b"v", &[(TIMESTAMP_HEADER, b"epoch")]),
            message_with(b"v", &[(VERSION_HEADER, &[CONTROL_MAPPING_VERSION + 1])]),
            unreadable,
        ];
        for (index, message) in messages.iter_mut().enumerate() {
            message.header.timestamp = 5_000_000;
            let expected = from_iggy(message, 0).map(|record| record.timestamp);
            let actual = record_timestamp(message);
            match (&expected, &actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(actual, expected, "fixture {index}"),
                (Err(_), Err(_)) => {}
                _ => panic!("fixture {index}: from_iggy {expected:?}, alone {actual:?}"),
            }
        }
    }

    #[test]
    fn given_no_timestamp_when_read_should_use_the_server_timestamp() {
        let mut message = to_iggy_alone(&record_at(NO_TIMESTAMP)).unwrap();
        message.header.timestamp = 5_000_000;
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, 5_000);
    }

    #[test]
    fn given_a_truncated_envelope_when_decoded_should_fail() {
        let message = to_iggy_alone(&record_with(Some(b""), Some(b"v"), &[])).unwrap();
        assert!(matches!(
            decode_envelope(&[ENVELOPE_VERSION], &message.payload.slice(0..3)),
            Err(RecordCodecError::EnvelopeTruncated { .. })
        ));
    }

    #[test]
    fn given_more_headers_than_the_payload_holds_when_decoded_should_fail() {
        // Thirteen bytes claiming four billion headers. Reserving for them is the whole risk.
        assert!(matches!(
            decode_envelope(&[ENVELOPE_VERSION], &envelope_bytes(u32::MAX, b"")),
            Err(RecordCodecError::EnvelopeTruncated { .. })
        ));
    }

    #[test]
    fn given_bytes_after_the_last_header_when_decoded_should_fail() {
        assert!(matches!(
            decode_envelope(&[ENVELOPE_VERSION], &envelope_bytes(0, b"junk")),
            Err(RecordCodecError::EnvelopeTrailingBytes(4))
        ));
    }

    #[test]
    fn given_a_gateway_envelope_that_does_not_parse_when_read_should_fail() {
        let message = gateway_message(
            b"not an envelope",
            &[(ENVELOPE_HEADER, &[ENVELOPE_VERSION])],
        );
        assert!(
            from_iggy(&message, 0).is_err(),
            "the gateway writes only envelopes that parse, so this one is corrupt"
        );
    }

    #[test]
    fn given_an_envelope_header_without_a_version_header_when_read_should_read_it_as_iggy() {
        let message = message_with(
            b"not an envelope",
            &[(ENVELOPE_HEADER, &[ENVELOPE_VERSION])],
        );

        let record = from_iggy(&message, 0).unwrap();
        assert_eq!(record.key, None, "no Kafka producer wrote this");
        assert_eq!(record.value.as_deref(), Some(&b"not an envelope"[..]));
    }

    #[test]
    fn given_a_value_filling_the_payload_when_the_envelope_is_needed_should_fail() {
        // An empty key is the cheapest field Iggy cannot hold, so this record has to take the
        // envelope, and the envelope has to carry the key alongside a value already at the cap.
        let oversized = record(
            Some(Bytes::new()),
            Some(Bytes::from(vec![b'x'; MAX_PAYLOAD_SIZE as usize])),
            IndexMap::new(),
            0,
            CREATE_TIME,
        );
        assert!(matches!(
            to_iggy_alone(&oversized),
            Err(RecordCodecError::EnvelopeTooLarge { size })
                if size == MAX_PAYLOAD_SIZE as usize + ENVELOPE_OVERHEAD
        ));
    }

    fn encode_with(records: &[Record], compression: Compression) -> Bytes {
        let mut buf = BytesMut::new();
        let options = RecordEncodeOptions {
            version: BATCH_VERSION,
            compression,
        };
        RecordBatchEncoder::encode(&mut buf, records, &options).unwrap();
        buf.freeze()
    }

    fn record_at_offset(offset: i64, value: &[u8]) -> Record {
        record(
            Some(Bytes::from_static(b"k")),
            Some(Bytes::copy_from_slice(value)),
            IndexMap::new(),
            offset,
            CREATE_TIME,
        )
    }

    fn batch_count(batch: &Bytes) -> usize {
        RecordBatchDecoder::decode_batch_info(&mut batch.clone())
            .unwrap()
            .len()
    }

    /// Rewrites a batch header field and repairs the CRC, so the decoder reaches the field.
    ///
    /// The CRC covers every byte after it, and it is checked before the record count is read, so
    /// a patched count without a repaired CRC only ever tests CRC verification.
    fn patch_header(batch: &Bytes, offset: usize, value: &[u8]) -> Bytes {
        let mut bytes = batch.to_vec();
        bytes[offset..offset + value.len()].copy_from_slice(value);
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(bytes)
    }

    #[test]
    fn given_an_uncompressed_batch_when_round_tripped_should_keep_every_record() {
        let mut records = vec![record_at_offset(0, b"1"), record_at_offset(1, b"2")];
        let mut encoded = encode_batch(&mut records).unwrap();
        let budget = partition_budget(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed)
            .unwrap()
            .records;
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].value.as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn given_records_at_distinct_offsets_when_encoded_should_make_one_batch() {
        let mut records = (0..4)
            .map(|offset| record_at_offset(offset, b"v"))
            .collect::<Vec<_>>();
        let encoded = encode_batch(&mut records).unwrap();

        assert_eq!(
            batch_count(&encoded),
            1,
            "a batch header per record costs 61 bytes of framing on every Fetch"
        );
        assert_eq!(
            records[0].sequence, NO_SEQUENCE,
            "the encoded base sequence follows the first record's"
        );
    }

    #[test]
    fn given_no_records_when_encoded_should_write_nothing() {
        assert!(encode_batch(&mut []).unwrap().is_empty());
    }

    #[test]
    fn given_two_batches_in_one_blob_when_decoded_should_reject() {
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(&mut [record_at_offset(0, b"1")]).unwrap());
        blob.extend_from_slice(&encode_batch(&mut [record_at_offset(1, b"2")]).unwrap());

        let budget = partition_budget(1024, usize::MAX);
        assert!(
            matches!(
                decode_batch(&mut blob.freeze(), &budget, Zstd::Allowed),
                Err(RecordCodecError::SeveralBatches(2))
            ),
            "Kafka allows one batch per partition from Produce v3"
        );
    }

    #[test]
    fn given_bytes_after_the_batch_when_decoded_should_reject() {
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(&mut [record_at_offset(0, b"1")]).unwrap());
        // Byte 16 of the tail is its magic. Not 2, so `decode_batch_info` stops there, no error.
        blob.extend_from_slice(&[0; 32]);
        let budget = partition_budget(1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut blob.freeze(), &budget, Zstd::Allowed),
                Err(RecordCodecError::BatchTrailingBytes(32))
            ),
            "the decoder never reads the tail, so the partition would answer success"
        );
    }

    #[test]
    fn given_more_records_than_the_batch_declares_when_decoded_should_reject() {
        let batch =
            encode_batch(&mut [record_at_offset(0, b"1"), record_at_offset(1, b"2")]).unwrap();
        let mut patched = patch_header(&batch, 57, &1i32.to_be_bytes());
        let budget = partition_budget(1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut patched, &budget, Zstd::Allowed),
                Err(RecordCodecError::RecordCountMismatch {
                    declared: 1,
                    walked: 2
                })
            ),
            "the decoder drops the second record, so storing the rest would lose it"
        );
    }

    #[test]
    fn given_a_batch_when_checked_for_compression_should_read_its_attributes() {
        let records = [record_at_offset(0, b"v")];
        assert!(is_compressed(&encode_with(&records, Compression::Gzip)));
        assert!(!is_compressed(&encode_with(&records, Compression::None)));
        assert!(!is_compressed(&[]));
    }

    #[test]
    fn given_a_batch_when_its_records_are_counted_should_read_the_declared_count() {
        let mut records = [record_at_offset(0, b"a"), record_at_offset(1, b"b")];
        assert_eq!(
            declared_records(&encode_batch(&mut records).unwrap()),
            Some(2)
        );
        // `encode_batch` set the sequences that keep both records in one batch.
        assert_eq!(
            declared_records(&encode_with(&records, Compression::Gzip)),
            Some(2),
            "the header counts records before compression"
        );
        assert_eq!(declared_records(&[0; BATCH_HEADER_BYTES - 1]), None);
    }

    #[test]
    fn given_a_gzip_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = partition_budget(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed)
            .unwrap()
            .records;
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_snappy_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Snappy);
        let budget = partition_budget(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed)
            .unwrap()
            .records;
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_an_lz4_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Lz4);
        let budget = partition_budget(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed)
            .unwrap()
            .records;
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_zstd_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Zstd);
        let budget = partition_budget(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed)
            .unwrap()
            .records;
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_budget_smaller_than_the_batch_when_decoded_should_reject() {
        let records = vec![record_at_offset(0, &[b'x'; 512])];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = partition_budget(8, usize::MAX);
        assert!(matches!(
            decode_batch(&mut encoded, &budget, Zstd::Allowed),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));
    }

    #[test]
    fn given_a_spent_budget_when_a_partition_fits_alone_should_ask_for_a_retry() {
        // Enough for one partition, not for both: the budget is per request.
        let budget = partition_budget(400, usize::MAX);
        let big = || encode_batch(&mut [record_at_offset(0, &[b'x'; 256])]).unwrap();

        assert!(decode_batch(&mut big(), &budget, Zstd::Allowed).is_ok());
        assert!(matches!(
            decode_batch(&mut big(), &budget, Zstd::Allowed),
            Err(RecordCodecError::RequestBudgetSpent)
        ));
    }

    #[test]
    fn given_a_spent_budget_when_a_later_partition_arrives_should_refuse_it_undecoded() {
        let budget = partition_budget(400, usize::MAX);
        let big = || encode_batch(&mut [record_at_offset(0, &[b'x'; 256])]).unwrap();
        assert!(decode_batch(&mut big(), &budget, Zstd::Allowed).is_ok());
        assert!(decode_batch(&mut big(), &budget, Zstd::Allowed).is_err());

        // Malformed, so a decode would refuse it as a bad batch instead.
        let mut garbage = Bytes::from_static(b"not a record batch");
        assert!(
            matches!(
                decode_batch(&mut garbage, &budget, Zstd::Allowed),
                Err(RecordCodecError::RequestBudgetSpent)
            ),
            "one spent request inflates nothing more, so it cannot hold a slot inflating"
        );
    }

    #[test]
    fn given_a_partition_too_large_alone_when_others_follow_should_leave_them_room() {
        let partition = Allowance {
            bytes: 64 * 1024,
            records: usize::MAX,
        };
        let budget = DecompressionBudget::new(partition, partition.times(2));
        let mut bomb = encode_with(
            &[record_at_offset(0, &vec![0; 1024 * 1024])],
            Compression::Gzip,
        );
        let normal = || {
            encode_with(
                &[record_at_offset(0, &vec![b'n'; 32 * 1024])],
                Compression::Gzip,
            )
        };

        assert!(matches!(
            decode_batch(&mut bomb, &budget, Zstd::Allowed),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));
        assert!(
            decode_batch(&mut normal(), &budget, Zstd::Allowed).is_ok(),
            "the bomb stops at its own cap, so the request has room left"
        );
    }

    #[test]
    fn given_a_partition_too_large_alone_when_decoded_should_refuse_it() {
        // gzip writes 32 KiB at a time, so the refusal comes several writes before the end.
        let partition = Allowance {
            bytes: 64 * 1024,
            records: usize::MAX,
        };
        let budget = DecompressionBudget::new(partition, partition.times(8));
        let mut first = encode_with(
            &[record_at_offset(0, &vec![b'a'; 60 * 1024])],
            Compression::Gzip,
        );
        let mut second = encode_with(
            &[record_at_offset(0, &vec![b'b'; 200 * 1024])],
            Compression::Gzip,
        );

        assert!(decode_batch(&mut first, &budget, Zstd::Allowed).is_ok());
        assert!(
            matches!(
                decode_batch(&mut second, &budget, Zstd::Allowed),
                Err(RecordCodecError::BudgetExceeded { limit, .. }) if limit == 64 * 1024
            ),
            "a retry cannot help a partition over its own cap"
        );
    }

    #[test]
    fn given_a_compression_bomb_when_decoded_should_stop_before_it_is_whole() {
        let bomb = vec![0u8; 4 * 1024 * 1024];
        let mut compressed = Bytes::from(encode_gzip(&bomb));
        let budget = partition_budget(1024, usize::MAX);

        let Err(error) = decompress(&mut compressed, Compression::Gzip, &budget) else {
            panic!("a 4 MB output against a 1 KB budget has to be refused");
        };
        drop(error);
        let Some(RecordCodecError::BudgetExceeded { size, limit }) = budget.reason.take() else {
            panic!("the budget is what refused it");
        };
        assert_eq!(limit, 1024);
        assert!(
            size < bomb.len(),
            "refused at the write that passed the budget: {size} of {} bytes",
            bomb.len()
        );
    }

    #[test]
    fn given_a_snappy_block_declaring_more_than_the_budget_when_decoded_should_reject() {
        // A raw snappy stream whose leading varint claims u32::MAX bytes of output. `snap` reads
        // that length without allocating, so the declared size is checkable before the decode.
        let mut compressed = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0x0f, 0x00]);
        let budget = partition_budget(1024, usize::MAX);
        assert!(decompress(&mut compressed, Compression::Snappy, &budget).is_err());
        assert!(matches!(
            budget.reason.take(),
            Some(RecordCodecError::BudgetExceeded { size, .. }) if size == u32::MAX as usize
        ));
    }

    #[test]
    fn given_a_record_count_past_the_frame_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"v")]).unwrap();
        let mut patched = patch_header(&batch, 57, &i32::MAX.to_be_bytes());
        let budget = partition_budget(1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut patched, &budget, Zstd::Allowed),
                Err(RecordCodecError::RecordCountTooLarge { .. })
            ),
            "the decoder reserves from this count before it reads a record"
        );
    }

    #[test]
    fn given_a_compressed_batch_when_preflighting_should_grant_it_the_partition_cap() {
        // 100 tiny records declare more than the blob and the bytes left can hold, and fit the
        // partition cap. Earlier partitions spending the request must not make them look
        // malformed. One offset for all, so the encoder writes one batch.
        let records: Vec<_> = (0..100).map(|_| record_at_offset(0, b"")).collect();
        let mut batch = encode_with(&records, Compression::Gzip);
        let budget = partition_budget(4096, usize::MAX);
        let mut spend = encode_batch(&mut [record_at_offset(0, &[b'x'; 4000])]).unwrap();

        assert!(decode_batch(&mut spend, &budget, Zstd::Allowed).is_ok());
        assert!(matches!(
            decode_batch(&mut batch, &budget, Zstd::Allowed),
            Err(RecordCodecError::RequestBudgetSpent)
        ));
    }

    #[test]
    fn given_many_headers_when_decoded_should_charge_them_as_records() {
        let headers: Vec<_> = (0..6)
            .map(|index| (format!("h{index}"), Some(&b"v"[..])))
            .collect();
        let headers: Vec<_> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), *value))
            .collect();
        let mut batch = encode_batch(&mut [record_with(None, Some(b"v"), &headers)]).unwrap();

        // One slot for the record and two for its six headers.
        let tight = partition_budget(1024, 2);
        assert!(matches!(
            decode_batch(&mut batch.clone(), &tight, Zstd::Allowed),
            Err(RecordCodecError::RecordBudgetExceeded { count: 3, limit: 2 })
        ));
        let enough = partition_budget(1024, 3);
        assert!(decode_batch(&mut batch, &enough, Zstd::Allowed).is_ok());
    }

    /// A record at `offset` with `count` headers.
    fn record_with_headers(offset: i64, count: usize) -> Record {
        let names: Vec<String> = (0..count).map(|index| format!("h{index}")).collect();
        let headers: Vec<_> = names
            .iter()
            .map(|name| (name.as_str(), Some(&b"v"[..])))
            .collect();
        let mut record = record_with(None, Some(b"v"), &headers);
        record.offset = offset;
        record
    }

    #[test]
    fn given_headers_when_slots_are_counted_should_match_what_the_decode_charges() {
        let mut records = [
            record_with_headers(0, 6),
            record_with_headers(1, 3),
            record_with_headers(2, 2),
        ];
        let batch = encode_batch(&mut records).unwrap();

        // Three records, two slots for six headers and one for three.
        assert_eq!(record_slots(&batch, usize::MAX), 6);
        let exact = partition_budget(4096, 6);
        assert!(decode_batch(&mut batch.clone(), &exact, Zstd::Allowed).is_ok());
        let short = partition_budget(4096, 5);
        assert!(matches!(
            decode_batch(&mut batch.clone(), &short, Zstd::Allowed),
            Err(RecordCodecError::RecordBudgetExceeded { count: 6, limit: 5 })
        ));

        assert_eq!(record_slots(&batch, 4), 4, "the walk stops at the cap");
        assert_eq!(
            record_slots(&batch[..BATCH_HEADER_BYTES + 3], usize::MAX),
            3,
            "a batch that does not walk counts its declared records"
        );
    }

    /// A one-record v2 batch whose record body declares `headers` headers and carries none.
    ///
    /// Built by hand because no encoder writes that, and it is the shape that reaches
    /// `IndexMap::with_capacity` inside kafka-protocol.
    fn batch_declaring_headers(headers: i32) -> Bytes {
        fn put_varint(buf: &mut BytesMut, value: i32) {
            let mut zigzag = ((value << 1) ^ (value >> 31)).cast_unsigned();
            while zigzag >= 0x80 {
                buf.put_u8(u8::try_from(zigzag & 0x7f).unwrap() | 0x80);
                zigzag >>= 7;
            }
            buf.put_u8(u8::try_from(zigzag).unwrap());
        }

        let mut record = BytesMut::new();
        record.put_u8(0); // attributes
        put_varint(&mut record, 0); // timestamp delta
        put_varint(&mut record, 0); // offset delta
        put_varint(&mut record, -1); // null key
        put_varint(&mut record, -1); // null value
        put_varint(&mut record, headers);

        let mut records = BytesMut::new();
        put_varint(&mut records, i32::try_from(record.len()).unwrap());
        records.extend_from_slice(&record);

        // Everything from the attributes field on, which is what the CRC covers.
        let mut body = BytesMut::new();
        body.put_i16(0); // attributes: no compression, CreateTime
        body.put_i32(0); // last offset delta
        body.put_i64(CREATE_TIME); // first timestamp
        body.put_i64(CREATE_TIME); // max timestamp
        body.put_i64(NO_PRODUCER_ID);
        body.put_i16(NO_PRODUCER_EPOCH);
        body.put_i32(NO_SEQUENCE);
        body.put_i32(1); // record count
        body.extend_from_slice(&records);

        let mut batch = BytesMut::new();
        batch.put_i64(0); // base offset
        batch.put_i32(i32::try_from(body.len() + 9).unwrap()); // batch length, from leader epoch on
        batch.put_i32(NO_PARTITION_LEADER_EPOCH);
        batch.put_i8(BATCH_VERSION);
        batch.put_u32(crc32c::crc32c(&body));
        batch.extend_from_slice(&body);
        batch.freeze()
    }

    /// A one-record batch whose headers are `a=x, b=y`, with `b` renamed `a` on the wire.
    fn batch_repeating_a_name() -> Bytes {
        let headers = [("a", Some(&b"x"[..])), ("b", Some(&b"y"[..]))];
        let mut records = [record_with(Some(b"k"), Some(b"v"), &headers)];
        let batch = encode_batch(&mut records).unwrap();
        // Same length, so only the CRC needs repair.
        let at = batch
            .windows(4)
            .position(|window| window == [0x02, b'b', 0x02, b'y'])
            .unwrap();
        patch_header(&batch, at + 1, b"a")
    }

    fn list(headers: &[(&str, Option<&[u8]>)]) -> HeaderList {
        headers
            .iter()
            .map(|(name, value)| {
                (
                    StrBytes::from_string((*name).to_string()),
                    value.map(Bytes::copy_from_slice),
                )
            })
            .collect()
    }

    #[test]
    fn given_a_repeated_header_name_when_decoded_should_keep_every_header() {
        let mut batch = batch_repeating_a_name();
        let budget = partition_budget(1024, usize::MAX);

        let decoded = decode_batch(&mut batch, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(
            decoded.records[0].headers.len(),
            1,
            "kafka_protocol folds the pair, which is why the list travels beside the record"
        );
        assert_eq!(
            decoded.headers(0),
            Some(list(&[("a", Some(b"x")), ("a", Some(b"y"))]).as_slice())
        );
    }

    #[test]
    fn given_distinct_header_names_when_decoded_should_carry_no_list() {
        let headers = [("b", Some(&b"x"[..])), ("a", Some(&b"y"[..]))];
        let mut batch = encode_batch(&mut [record_with(None, Some(b"v"), &headers)]).unwrap();
        let budget = partition_budget(1024, usize::MAX);

        let decoded = decode_batch(&mut batch, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded.repeated, RepeatedHeaders::new());
        assert_eq!(decoded.headers(0), None);
    }

    #[test]
    fn given_a_repeated_header_name_when_produced_and_fetched_should_return_the_same_bytes() {
        let original = batch_repeating_a_name();
        let budget = partition_budget(1024, usize::MAX);
        let decoded = decode_batch(&mut original.clone(), &budget, Zstd::Allowed).unwrap();
        let window = TimestampWindow::of(&decoded.records);
        let message = to_iggy_with(&decoded.records[0], decoded.headers(0), window).unwrap();
        assert!(is_enveloped(&message), "the envelope is what stores a list");

        let (record, headers) = from_iggy_with(&message, 0).unwrap();
        assert_eq!(headers, Some(list(&[("a", Some(b"x")), ("a", Some(b"y"))])));
        let repeated = vec![(0, headers.unwrap())];
        let fetched = encode_batch_with(&mut [record], &repeated).unwrap();
        assert_eq!(fetched, original, "key, value and headers byte for byte");
    }

    #[test]
    fn given_a_repeated_name_among_null_values_when_round_tripped_should_keep_the_nulls() {
        let headers = list(&[
            ("t", None),
            ("s", Some(b"1")),
            ("t", Some(b"2")),
            ("t", None),
        ]);
        let record = record_with(None, Some(b"v"), &[]);
        let message = to_iggy_with(&record, Some(&headers), TimestampWindow::of(&[])).unwrap();

        let (back, list) = from_iggy_with(&message, 0).unwrap();
        assert_eq!(list.as_ref(), Some(&headers));
        assert_eq!(back.headers.len(), 2);

        let mut records = [back];
        let mut batch = encode_batch_with(&mut records, &vec![(0, list.unwrap())]).unwrap();
        let budget = partition_budget(1024, usize::MAX);
        let decoded = decode_batch(&mut batch, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded.headers(0), Some(headers.as_slice()));
    }

    #[test]
    fn given_spliced_records_when_batched_should_leave_the_others_untouched() {
        let mut records = vec![
            record_at_offset(0, b"first"),
            record_at_offset(1, b"second"),
            record_at_offset(2, b"third"),
        ];
        let plain = encode_batch(&mut records).unwrap();
        let headers = list(&[("h", Some(b"1")), ("h", Some(b"2"))]);
        let spliced = encode_batch_with(&mut records, &vec![(1, headers.clone())]).unwrap();

        let budget = partition_budget(4096, usize::MAX);
        let decoded = decode_batch(&mut spliced.clone(), &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded.records.len(), 3);
        assert_eq!(decoded.headers(1), Some(headers.as_slice()));
        assert_eq!(decoded.repeated.len(), 1);
        let values: Vec<_> = decoded
            .records
            .iter()
            .map(|record| record.value.clone().unwrap())
            .collect();
        assert_eq!(values, [&b"first"[..], b"second", b"third"]);
        assert_eq!(
            spliced.len(),
            plain.len() + headers_section_len(&headers) - 1,
            "only the middle record grows, by its new header section over the empty one"
        );
    }

    /// Bytes a header section of `list` takes: its count and each name and value, all short.
    fn headers_section_len(list: &[Header]) -> usize {
        1 + list
            .iter()
            .map(|(name, value)| 2 + name.len() + value.as_ref().map_or(0, Bytes::len))
            .sum::<usize>()
    }

    #[test]
    fn given_headers_out_of_name_order_when_round_tripped_should_keep_their_order() {
        let sent = [
            ("zulu", Some(&b"1"[..])),
            ("alpha", Some(&b"2"[..])),
            ("mike", Some(&b"3"[..])),
        ];
        let message = to_iggy_alone(&record_with(Some(b"k"), Some(b"v"), &sent)).unwrap();
        assert!(
            !is_enveloped(&message),
            "order alone does not need the envelope"
        );
        let stored = message.user_headers_map().unwrap().unwrap();
        assert_eq!(
            stored
                .get(&header_key(ORDER_HEADER))
                .map(HeaderValue::value),
            Some(Bytes::from_static(&[2, 0, 1])),
            "the sorted rank of each header, in send order"
        );

        let back = from_iggy(&message, 0).unwrap();
        let names: Vec<&str> = back.headers.keys().map(StrBytes::as_str).collect();
        assert_eq!(names, ["zulu", "alpha", "mike"]);
    }

    #[test]
    fn given_headers_in_name_order_when_encoded_should_store_no_order() {
        let sent = [("alpha", Some(&b"1"[..])), ("beta", Some(&b"2"[..]))];
        let message = to_iggy_alone(&record_with(None, Some(b"v"), &sent)).unwrap();
        let stored = message.user_headers_map().unwrap().unwrap();
        assert!(!stored.contains_key(&header_key(ORDER_HEADER)));
    }

    #[test]
    fn given_more_unsorted_headers_than_one_order_byte_ranks_when_encoded_should_envelope() {
        let names: Vec<String> = (0..300).rev().map(|at| format!("h{at:03}")).collect();
        let sent: Vec<(&str, Option<&[u8]>)> = names
            .iter()
            .map(|name| (name.as_str(), Some(&b"v"[..])))
            .collect();
        let message = to_iggy_alone(&record_with(None, Some(b"v"), &sent)).unwrap();
        assert!(is_enveloped(&message));

        let back = from_iggy(&message, 0).unwrap();
        let order: Vec<&str> = back.headers.keys().map(StrBytes::as_str).collect();
        assert_eq!(order, names);
    }

    #[test]
    fn given_an_order_that_is_not_a_permutation_when_read_should_fail() {
        for order in [&[0, 0][..], &[0][..], &[0, 2][..], &[1, 0, 2][..]] {
            let message = gateway_message(
                b"v",
                &[
                    ("kafka.h.a", b"1"),
                    ("kafka.h.b", b"2"),
                    (ORDER_HEADER, order),
                ],
            );
            assert!(
                matches!(
                    from_iggy(&message, 0),
                    Err(RecordCodecError::HeaderOrder(_))
                ),
                "{order:?}"
            );
        }
        let message = gateway_message(
            b"v",
            &[
                ("kafka.h.a", b"1"),
                ("kafka.h.b", b"2"),
                (ORDER_HEADER, &[1, 0]),
            ],
        );
        let names: Vec<String> = from_iggy(&message, 0)
            .unwrap()
            .headers
            .keys()
            .map(ToString::to_string)
            .collect();
        assert_eq!(names, ["b", "a"]);
    }

    #[test]
    fn given_a_header_count_past_the_record_when_decoded_should_reject() {
        // The batch header says one record, so the record count bound passes. The count that
        // matters is the one inside the record, and no batch header reports it.
        let mut batch = batch_declaring_headers(i32::MAX);
        let budget = partition_budget(8 * 1024 * 1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut batch, &budget, Zstd::Allowed),
                Err(RecordCodecError::HeaderCountTooLarge { count, .. }) if count == i32::MAX
            ),
            "this reserve is resident memory, not address space"
        );
    }

    #[test]
    fn given_a_hand_built_batch_when_its_header_count_fits_should_decode() {
        // The same builder with a count the record can hold, so the scan cannot be passing the
        // test above by rejecting every hand-built batch.
        let mut batch = batch_declaring_headers(0);
        let budget = partition_budget(1024, usize::MAX);
        assert_eq!(
            decode_batch(&mut batch, &budget, Zstd::Allowed)
                .unwrap()
                .records
                .len(),
            1
        );
    }

    #[test]
    fn given_records_carrying_headers_when_round_tripped_should_keep_them() {
        // Exercises the header framing the scan walks, on a batch an encoder wrote.
        let mut with_headers = vec![record_with(
            Some(b"k"),
            Some(b"v"),
            &[("trace", Some(b"abc"))],
        )];
        let mut encoded = encode_batch(&mut with_headers).unwrap();
        let budget = partition_budget(1024, usize::MAX);

        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed)
            .unwrap()
            .records;
        assert_eq!(
            decoded[0].headers.get(&StrBytes::from_static_str("trace")),
            Some(&Some(Bytes::from_static(b"abc")))
        );
    }

    #[test]
    fn given_an_uncompressed_batch_when_preflighting_should_not_grant_it_the_budget() {
        // One record's worth of bytes, declaring far more records than it holds. The budget is
        // the documented 8 MiB, which an uncompressed batch has no claim on.
        let batch = encode_batch(&mut [record_at_offset(0, b"v")]).unwrap();
        let mut patched = patch_header(&batch, 57, &100_000i32.to_be_bytes());
        let budget = partition_budget(8 * 1024 * 1024, usize::MAX);

        assert!(matches!(
            decode_batch(&mut patched, &budget, Zstd::Allowed),
            Err(RecordCodecError::RecordCountTooLarge { limit, .. }) if limit == batch.len()
        ));
    }

    #[test]
    fn given_two_key_kinds_naming_one_header_on_a_gateway_message_when_read_should_fail() {
        let mut headers = BTreeMap::new();
        headers.insert(header_key(VERSION_HEADER), header_value(&[MAPPING_VERSION]));
        headers.insert(header_key("kafka.h.trace"), header_value(b"string"));
        headers.insert(
            HeaderKey::from_raw(HeaderKind::Raw, b"kafka.h.trace").unwrap(),
            header_value(b"raw"),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();

        assert!(
            matches!(
                from_iggy(&message, 0),
                Err(RecordCodecError::HeaderNameCollision(_))
            ),
            "to_iggy writes one key kind, so a collision means the message is not what it claims"
        );
    }

    #[test]
    fn given_two_key_kinds_naming_one_header_on_an_iggy_message_when_read_should_keep_both() {
        let mut headers = BTreeMap::new();
        headers.insert(header_key("trace"), header_value(b"string"));
        headers.insert(
            HeaderKey::from_raw(HeaderKind::Raw, b"trace").unwrap(),
            header_value(b"raw"),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();

        let (record, list) = from_iggy_with(&message, 0).unwrap();
        assert_eq!(
            record.headers.len(),
            1,
            "a Record keys headers in an IndexMap, so the pair cannot both survive there"
        );
        assert_eq!(
            list.map(|list| list.len()),
            Some(2),
            "Kafka carries a list, so both reach the consumer"
        );
        assert!(
            record
                .headers
                .contains_key(&StrBytes::from_static_str("trace")),
            "refusing the message instead would stall the partition for every Kafka consumer"
        );
    }

    #[test]
    fn given_a_transactional_batch_when_decoded_should_reject() {
        let mut transactional = record_at_offset(0, b"v");
        transactional.transactional = true;
        let mut encoded = encode_with(&[transactional], Compression::None);
        let budget = partition_budget(1024, usize::MAX);

        assert!(matches!(
            decode_batch(&mut encoded, &budget, Zstd::Allowed),
            Err(RecordCodecError::TransactionalBatch)
        ));
    }

    #[test]
    fn given_a_control_batch_when_decoded_should_reject() {
        let mut control = record_at_offset(0, b"v");
        control.control = true;
        let mut encoded = encode_with(&[control], Compression::None);
        let budget = partition_budget(1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut encoded, &budget, Zstd::Allowed),
                Err(RecordCodecError::ControlBatch)
            ),
            "consumers filter control records by a flag no stored message can carry"
        );
    }

    #[test]
    fn given_a_truncated_batch_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut truncated = batch.slice(0..batch.len() - 4);
        let budget = partition_budget(1024, usize::MAX);
        assert!(decode_batch(&mut truncated, &budget, Zstd::Allowed).is_err());
    }

    #[test]
    fn given_a_corrupt_batch_crc_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut bytes = batch.to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let mut corrupt = Bytes::from(bytes);

        let budget = partition_budget(1024, usize::MAX);
        assert!(matches!(
            decode_batch(&mut corrupt, &budget, Zstd::Allowed),
            Err(RecordCodecError::Batch(_))
        ));
    }

    #[test]
    fn given_an_earlier_overrun_when_a_later_batch_fails_should_not_reuse_the_budget_reason() {
        // A failed charge deducts nothing, so the budget still has room for the batch below.
        let budget = partition_budget(64, usize::MAX);
        let mut bomb = encode_with(&[record_at_offset(0, &[b'x'; 512])], Compression::Gzip);
        assert!(matches!(
            decode_batch(&mut bomb, &budget, Zstd::Allowed),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));

        // One record more than the batch holds. The count clears the preflight bound, so the
        // failure comes out of the record decoder, which is the path that reads the overflow.
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut short = patch_header(&batch, 57, &2i32.to_be_bytes());

        assert!(
            matches!(
                decode_batch(&mut short, &budget, Zstd::Allowed),
                Err(RecordCodecError::Batch(_))
            ),
            "a stale overflow reports a malformed batch as MESSAGE_TOO_LARGE"
        );
    }

    fn encode_gzip(body: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }
}
