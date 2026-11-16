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

//! Kafka record headers as the list Kafka carries, not the map a `Record` holds.
//!
//! `kafka_protocol` keys a record's headers in an `IndexMap`, which keeps their order but folds a
//! repeated name into its last value. Kafka allows the repeat (`KafkaJS` array headers, Spring's
//! trace headers), so a record that repeats a name travels with its full list beside the
//! `Record`: read from the raw record bytes on Produce, written into the encoded batch on Fetch.
//!
//! Order is the other half. Iggy keys user headers in a `BTreeMap`, so a header stored natively
//! comes back sorted by name. `kafka.ho` holds the producer's order when it is not already the
//! sorted one, one byte per header. `BRIDGE_MAPPING.md` has both rules.

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;

use super::{
    BATCH_HEADER_BYTES, MAX_FIELD, MAX_VARINT_BYTES, MAX_VARLONG_BYTES, RecordCodecError, Result,
    skip_field, skip_varint, take_bytes, take_varint,
};

/// One Kafka record header: a name and a value Kafka may leave null.
pub type Header = (StrBytes, Option<Bytes>);

/// Every header of one record, in the order the producer sent them.
pub type HeaderList = Vec<Header>;

/// The full header lists of the records in one batch whose names repeat, by record index,
/// ascending. Every other record's `Record::headers` already holds all of its headers.
pub type RepeatedHeaders = Vec<(usize, HeaderList)>;

/// The list `repeated` holds for record `index`, when that record repeats a name.
#[must_use]
pub fn headers_at(repeated: &RepeatedHeaders, index: usize) -> Option<&[Header]> {
    repeated
        .binary_search_by_key(&index, |(at, _)| *at)
        .ok()
        .map(|at| repeated[at].1.as_slice())
}

/// The map a `Record` holds for `list`, and `list` itself when the map cannot hold all of it.
///
/// A repeated name keeps its first position and its last value in the map, which is what
/// `kafka_protocol` decodes the same record to.
pub(super) fn fold(list: HeaderList) -> (IndexMap<StrBytes, Option<Bytes>>, Option<HeaderList>) {
    let mut map = IndexMap::with_capacity(list.len());
    for (name, value) in &list {
        map.insert(name.clone(), value.clone());
    }
    if map.len() == list.len() {
        (map, None)
    } else {
        (map, Some(list))
    }
}

/// Whether two of the `count` headers starting at `record` share a name.
///
/// `names` is scratch space, reused across records.
pub(super) fn repeats_a_name<'a>(
    mut record: &'a [u8],
    count: usize,
    names: &mut Vec<&'a [u8]>,
) -> Result<bool> {
    names.clear();
    for _ in 0..count {
        let len = take_varint(&mut record)?;
        let len = usize::try_from(len).map_err(|_| RecordCodecError::RecordFieldLength(len))?;
        names.push(take_bytes(&mut record, len)?);
        skip_field(&mut record)?; // value
    }
    names.sort_unstable();
    Ok(names.windows(2).any(|pair| pair[0] == pair[1]))
}

/// The `count` headers starting at `record`, which lies inside `records`, as a list.
///
/// Slices `records` rather than copying, so the list shares the decompressed batch.
pub(super) fn read_list(records: &Bytes, mut record: &[u8], count: usize) -> Result<HeaderList> {
    let mut list = Vec::with_capacity(count);
    for _ in 0..count {
        let len = take_varint(&mut record)?;
        let len = usize::try_from(len).map_err(|_| RecordCodecError::RecordFieldLength(len))?;
        let name = records.slice_ref(take_bytes(&mut record, len)?);
        let name = StrBytes::try_from(name)
            .map_err(|_| RecordCodecError::Batch("record header name is not UTF-8".into()))?;
        let len = take_varint(&mut record)?;
        let value = match len {
            -1 => None,
            len if len < -1 => return Err(RecordCodecError::RecordFieldLength(len)),
            len => Some(records.slice_ref(take_bytes(
                &mut record,
                usize::try_from(len).unwrap_or_default(),
            )?)),
        };
        list.push((name, value));
    }
    Ok(list)
}

/// Where the producer's header order is kept for a record stored natively.
pub(super) enum Order {
    /// The names are already sorted, which is how Iggy hands them back: nothing to store, so the
    /// common record pays nothing.
    Sorted,
    /// The `kafka.ho` value: byte `i` is the sorted rank of the `i`th header sent.
    Ranks(Vec<u8>),
    /// More headers than one byte can rank, or one header value can hold. The envelope keeps
    /// their order instead.
    TooMany,
}

/// The order of headers named `names`, in send order, against the sorted one Iggy stores.
pub(super) fn order_of<'a>(names: impl ExactSizeIterator<Item = &'a StrBytes> + Clone) -> Order {
    let sorted = names
        .clone()
        .zip(names.clone().skip(1))
        .all(|(left, right)| left.as_bytes() <= right.as_bytes());
    if sorted {
        return Order::Sorted;
    }
    if names.len() > MAX_FIELD {
        return Order::TooMany;
    }
    let names: Vec<&[u8]> = names.map(|name| name.as_bytes()).collect();
    let mut by_name: Vec<usize> = (0..names.len()).collect();
    by_name.sort_unstable_by_key(|&at| names[at]);
    let mut ranks = vec![0u8; names.len()];
    for (rank, at) in (0u8..=u8::MAX).zip(by_name) {
        ranks[at] = rank;
    }
    Order::Ranks(ranks)
}

/// `sorted`, headers in name order as Iggy stores them, back in the order `kafka.ho` records.
///
/// # Errors
///
/// `HeaderOrder` when `order` is not a permutation of the headers: on a message that says this
/// build wrote it, a wrong order is a wrong message, not one to guess at.
pub(super) fn reorder(
    sorted: &IndexMap<StrBytes, Option<Bytes>>,
    order: &Bytes,
) -> Result<IndexMap<StrBytes, Option<Bytes>>> {
    let refuse = || RecordCodecError::HeaderOrder(order.clone());
    if order.len() != sorted.len() {
        return Err(refuse());
    }
    let mut seen = vec![false; sorted.len()];
    let mut reordered = IndexMap::with_capacity(sorted.len());
    for &rank in order {
        let rank = usize::from(rank);
        if seen.get(rank).copied().unwrap_or(true) {
            return Err(refuse());
        }
        seen[rank] = true;
        let (name, value) = sorted.get_index(rank).ok_or_else(refuse)?;
        reordered.insert(name.clone(), value.clone());
    }
    Ok(reordered)
}

/// `batch`, one uncompressed v2 batch `encode_batch` wrote, with the header section of each
/// record in `repeated` replaced by that record's full list.
///
/// Every other record is copied byte for byte. The batch length and the CRC are rewritten, since
/// both cover the records.
pub(super) fn splice(batch: &Bytes, repeated: &RepeatedHeaders) -> Result<Bytes> {
    let malformed = || RecordCodecError::Batch("encoded batch does not walk".into());
    let (head, mut records) = batch
        .split_at_checked(BATCH_HEADER_BYTES)
        .ok_or_else(malformed)?;
    let extra: usize = repeated
        .iter()
        .flat_map(|(_, list)| list)
        .map(|(name, value)| {
            2 * MAX_VARINT_BYTES + name.len() + value.as_ref().map_or(0, Bytes::len)
        })
        .sum();
    let mut out = BytesMut::with_capacity(batch.len() + extra);
    out.put_slice(head);

    let mut index = 0;
    while !records.is_empty() {
        let start = records;
        let size = take_varint(&mut records)?;
        let size = usize::try_from(size).map_err(|_| RecordCodecError::RecordFieldLength(size))?;
        let record = take_bytes(&mut records, size)?;
        let Some(list) = headers_at(repeated, index) else {
            out.put_slice(&start[..start.len() - records.len()]);
            index += 1;
            continue;
        };

        let mut fields = record;
        take_bytes(&mut fields, 1)?; // attributes
        skip_varint(&mut fields, MAX_VARLONG_BYTES)?; // timestamp delta
        skip_varint(&mut fields, MAX_VARINT_BYTES)?; // offset delta
        skip_field(&mut fields)?; // key
        skip_field(&mut fields)?; // value
        let before_headers = &record[..record.len() - fields.len()];

        let mut section = BytesMut::new();
        put_varint(&mut section, list.len())?;
        for (name, value) in list {
            put_varint(&mut section, name.len())?;
            section.put_slice(name.as_bytes());
            match value {
                Some(value) => {
                    put_varint(&mut section, value.len())?;
                    section.put_slice(value);
                }
                None => put_signed_varint(&mut section, -1),
            }
        }
        put_varint(&mut out, before_headers.len() + section.len())?;
        out.put_slice(before_headers);
        out.put_slice(&section);
        index += 1;
    }

    // Base offset and batch length come first; the length counts everything after itself.
    let length = i32::try_from(out.len() - 12).map_err(|_| malformed())?;
    out[8..12].copy_from_slice(&length.to_be_bytes());
    // Leader epoch and magic follow, then the CRC over everything after it.
    let crc = crc32c::crc32c(&out[21..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());
    Ok(out.freeze())
}

fn put_varint(buf: &mut BytesMut, value: usize) -> Result<()> {
    let value = i32::try_from(value).map_err(|_| {
        RecordCodecError::Batch(format!("record field of {value} bytes is too large"))
    })?;
    put_signed_varint(buf, value);
    Ok(())
}

/// A zigzag varint, as Kafka writes one.
fn put_signed_varint(buf: &mut BytesMut, value: i32) {
    let mut zigzag = ((value << 1) ^ (value >> 31)).cast_unsigned();
    while zigzag >= 0x80 {
        buf.put_u8(u8::try_from(zigzag & 0x7f).unwrap_or_default() | 0x80);
        zigzag >>= 7;
    }
    buf.put_u8(u8::try_from(zigzag).unwrap_or_default());
}
