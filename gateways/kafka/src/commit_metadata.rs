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

//! The metadata string of a Kafka offset commit (KW-12).
//!
//! Iggy keeps a consumer offset as a `u64` and nothing else, so the string a client commits with
//! its offset is kept beside it, in a gateway-owned Iggy topic that every gateway appends to and
//! replays, the way Kafka keeps it in `__consumer_offsets` (owner ruling, 2026-10-24: no server
//! change). Each record names a group, a topic, a partition, the offset it was committed with and
//! the string. A record with an empty string clears the entry, and a group record clears every
//! entry of the group.
//!
//! The log is the only writer of the in-memory index: a commit appends, and the index learns the
//! record when it next replays the log. `OffsetFetch` replays the records it has not read yet
//! before it answers, so a commit through another gateway is seen as well. The string is reported
//! only while the offset in Iggy is still the one it was committed with. An offset stored some
//! other way (an Iggy-native consumer, a commit whose record did not land, a reset) then reads
//! back with `""`, as a commit without metadata does, rather than with a stale string.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::bridge::BridgeError;

/// The topic of the commit metadata log, in the stream `cluster_id_stream` names.
pub const OFFSET_METADATA_TOPIC: &str = "offset_metadata";

/// Version of the record layout.
const RECORD_VERSION: u8 = 1;
/// A record for one partition.
const KIND_PARTITION: u8 = 0;
/// A record that clears every entry of a group.
const KIND_GROUP_DELETED: u8 = 1;

/// One record of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRecord {
    /// `metadata` committed with `offset` on one partition. Empty clears the entry.
    Partition {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
        metadata: String,
    },
    /// The group was deleted: every entry of it goes.
    GroupDeleted { group: String },
}

impl MetadataRecord {
    /// The record's bytes.
    ///
    /// Version, kind, then length-prefixed strings: `u16` for the group and the topic (both
    /// at most 255 bytes here), `u32` for the metadata.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(RECORD_VERSION);
        match self {
            Self::Partition {
                group,
                topic,
                partition,
                offset,
                metadata,
            } => {
                buf.put_u8(KIND_PARTITION);
                put_short(&mut buf, group);
                put_short(&mut buf, topic);
                buf.put_i32(*partition);
                buf.put_i64(*offset);
                let len = u32::try_from(metadata.len()).unwrap_or(u32::MAX);
                buf.put_u32(len);
                buf.put_slice(&metadata.as_bytes()[..len as usize]);
            }
            Self::GroupDeleted { group } => {
                buf.put_u8(KIND_GROUP_DELETED);
                put_short(&mut buf, group);
            }
        }
        buf.freeze()
    }

    /// The record `bytes` holds, or `None` when they do not parse: another layout version, a
    /// truncated record, or a string that is not UTF-8.
    #[must_use]
    pub fn decode(mut bytes: &[u8]) -> Option<Self> {
        if bytes.remaining() < 2 || bytes.get_u8() != RECORD_VERSION {
            return None;
        }
        let record = match bytes.get_u8() {
            KIND_PARTITION => {
                let group = get_short(&mut bytes)?;
                let topic = get_short(&mut bytes)?;
                if bytes.remaining() < 4 + 8 + 4 {
                    return None;
                }
                let partition = bytes.get_i32();
                let offset = bytes.get_i64();
                let len = bytes.get_u32() as usize;
                if bytes.remaining() < len {
                    return None;
                }
                let metadata = String::from_utf8(bytes[..len].to_vec()).ok()?;
                bytes.advance(len);
                Self::Partition {
                    group,
                    topic,
                    partition,
                    offset,
                    metadata,
                }
            }
            KIND_GROUP_DELETED => Self::GroupDeleted {
                group: get_short(&mut bytes)?,
            },
            _ => return None,
        };
        bytes.is_empty().then_some(record)
    }
}

fn put_short(buf: &mut BytesMut, value: &str) {
    let len = u16::try_from(value.len()).unwrap_or(u16::MAX);
    buf.put_u16(len);
    buf.put_slice(&value.as_bytes()[..usize::from(len)]);
}

fn get_short(bytes: &mut &[u8]) -> Option<String> {
    if bytes.remaining() < 2 {
        return None;
    }
    let len = usize::from(bytes.get_u16());
    if bytes.remaining() < len {
        return None;
    }
    let value = String::from_utf8(bytes[..len].to_vec()).ok()?;
    bytes.advance(len);
    Some(value)
}

/// The log as the store uses it: the Iggy topic in production, memory in unit tests.
#[async_trait]
pub trait MetadataLog: Send + Sync + 'static {
    /// Appends `records` in order, and returns once they are durable.
    ///
    /// # Errors
    ///
    /// The bridge's error. A timeout may still land.
    async fn append(&self, records: &[MetadataRecord]) -> Result<(), BridgeError>;

    /// The records from position `from` on, in order, and the position after the last one read.
    /// Returns fewer than all when there are many; the caller reads on until the position stops
    /// moving. A record that does not parse is skipped but still moves it.
    ///
    /// # Errors
    ///
    /// The bridge's error. A log that does not exist yet has no records.
    async fn read_from(&self, from: u64) -> Result<(Vec<MetadataRecord>, u64), BridgeError>;
}

/// One partition's entry: the offset the string was committed with, and the string.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    offset: i64,
    metadata: Arc<str>,
}

/// The entries of one group, by topic and partition.
type GroupEntries = HashMap<(String, i32), Entry>;

/// The commit metadata of every group, replayed from the log.
pub struct CommitMetadata {
    log: Arc<dyn MetadataLog>,
    /// Entries with a non-empty string, by group.
    index: Mutex<HashMap<String, GroupEntries>>,
    /// The log position the next replay starts from. Held for the whole replay, so two replays
    /// never apply the same records out of order.
    next: tokio::sync::Mutex<u64>,
}

impl std::fmt::Debug for CommitMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitMetadata").finish_non_exhaustive()
    }
}

/// One partition of an `OffsetCommit` as the store sees it.
#[derive(Debug, Clone, Copy)]
pub struct Committed<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    /// `None` for a null string, which Kafka stores as `""`.
    pub metadata: Option<&'a str>,
}

impl CommitMetadata {
    #[must_use]
    pub fn new(log: Arc<dyn MetadataLog>) -> Self {
        Self {
            log,
            index: Mutex::default(),
            next: tokio::sync::Mutex::new(0),
        }
    }

    /// Reads the records appended since the last replay, by any gateway, into the index.
    ///
    /// # Errors
    ///
    /// The bridge's error. The records read before it are kept, and the next replay starts after
    /// them.
    pub async fn replay(&self) -> Result<usize, BridgeError> {
        let mut next = self.next.lock().await;
        let mut applied = 0;
        loop {
            let (records, after) = self.log.read_from(*next).await?;
            applied += records.len();
            self.apply(records);
            if after <= *next {
                return Ok(applied);
            }
            *next = after;
        }
    }

    fn apply(&self, records: Vec<MetadataRecord>) {
        let mut index = self.index.lock().unwrap_or_else(PoisonError::into_inner);
        for record in records {
            match record {
                MetadataRecord::Partition {
                    group,
                    topic,
                    partition,
                    offset,
                    metadata,
                } => {
                    if metadata.is_empty() {
                        if let Some(entries) = index.get_mut(&group) {
                            entries.remove(&(topic, partition));
                            if entries.is_empty() {
                                index.remove(&group);
                            }
                        }
                    } else {
                        index.entry(group).or_default().insert(
                            (topic, partition),
                            Entry {
                                offset,
                                metadata: Arc::from(metadata),
                            },
                        );
                    }
                }
                MetadataRecord::GroupDeleted { group } => {
                    index.remove(&group);
                }
            }
        }
    }

    /// The records a commit of `partitions` by `group` needs: one for each non-empty string, and
    /// one clearing each entry the index holds where the commit carries none. A commit without
    /// metadata over a partition that never had any needs no record, which keeps the common
    /// case (the Java consumer commits `""`) free of a log write.
    #[must_use]
    pub fn records_for(&self, group: &str, partitions: &[Committed<'_>]) -> Vec<MetadataRecord> {
        let index = self.index.lock().unwrap_or_else(PoisonError::into_inner);
        let entries = index.get(group);
        let held: Vec<bool> = partitions
            .iter()
            .map(|committed| {
                entries.is_some_and(|entries| {
                    entries.contains_key(&(committed.topic.to_string(), committed.partition))
                })
            })
            .collect();
        drop(index);
        partitions
            .iter()
            .zip(held)
            .filter(|(committed, held)| {
                *held
                    || committed
                        .metadata
                        .is_some_and(|metadata| !metadata.is_empty())
            })
            .map(|(committed, _)| MetadataRecord::Partition {
                group: group.to_string(),
                topic: committed.topic.to_string(),
                partition: committed.partition,
                offset: committed.offset,
                metadata: committed.metadata.unwrap_or_default().to_string(),
            })
            .collect()
    }

    /// Appends `records`.
    ///
    /// # Errors
    ///
    /// The bridge's error.
    pub async fn append(&self, records: &[MetadataRecord]) -> Result<(), BridgeError> {
        if records.is_empty() {
            return Ok(());
        }
        self.log.append(records).await
    }

    /// The string committed with `offset` on `topic` and `partition` by `group`, or `None` when
    /// there is none for that offset.
    #[must_use]
    pub fn metadata(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Option<Arc<str>> {
        let index = self.index.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = index.get(group)?.get(&(topic.to_string(), partition))?;
        let metadata = (entry.offset == offset).then(|| Arc::clone(&entry.metadata));
        drop(index);
        metadata
    }

    /// Whether `group` holds any entry.
    #[must_use]
    pub fn holds(&self, group: &str) -> bool {
        self.index
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(group)
    }
}

/// Kafka's measure of a metadata string against `offset.metadata.max.bytes`: Java's
/// `String.length()`, in UTF-16 code units.
#[must_use]
pub fn kafka_length(metadata: &str) -> usize {
    metadata.encode_utf16().count()
}

/// A log held in memory, for tests.
#[derive(Debug, Default)]
pub struct MemoryLog {
    records: tokio::sync::Mutex<Vec<MetadataRecord>>,
}

#[async_trait]
impl MetadataLog for MemoryLog {
    async fn append(&self, records: &[MetadataRecord]) -> Result<(), BridgeError> {
        self.records.lock().await.extend_from_slice(records);
        Ok(())
    }

    async fn read_from(&self, from: u64) -> Result<(Vec<MetadataRecord>, u64), BridgeError> {
        let records = self.records.lock().await;
        let from = usize::try_from(from)
            .unwrap_or(usize::MAX)
            .min(records.len());
        // Two at a time, so a test exercises the replay's loop.
        let to = (from + 2).min(records.len());
        Ok((records[from..to].to_vec(), to as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(group: &str, partition: i32, offset: i64, metadata: &str) -> MetadataRecord {
        MetadataRecord::Partition {
            group: group.to_string(),
            topic: "orders".to_string(),
            partition,
            offset,
            metadata: metadata.to_string(),
        }
    }

    fn committed(partition: i32, offset: i64, metadata: Option<&str>) -> Committed<'_> {
        Committed {
            topic: "orders",
            partition,
            offset,
            metadata,
        }
    }

    #[test]
    fn given_each_record_kind_should_decode_what_it_encoded() {
        for record in [
            partition("g", 3, 42, "task=0_1;ts=17"),
            partition("g", 0, -1, ""),
            partition("g", 1, 7, "ünïcödé"),
            MetadataRecord::GroupDeleted {
                group: "g".to_string(),
            },
        ] {
            assert_eq!(MetadataRecord::decode(&record.encode()), Some(record));
        }
    }

    #[test]
    fn given_a_truncated_or_foreign_record_should_not_decode_it() {
        let encoded = partition("g", 3, 42, "meta").encode();
        for end in 0..encoded.len() {
            assert_eq!(MetadataRecord::decode(&encoded[..end]), None, "{end}");
        }
        let mut longer = encoded.to_vec();
        longer.push(0);
        assert_eq!(MetadataRecord::decode(&longer), None);
        let mut other_version = encoded.to_vec();
        other_version[0] = 2;
        assert_eq!(MetadataRecord::decode(&other_version), None);
    }

    #[tokio::test]
    async fn given_a_commit_should_report_its_string_only_for_its_offset() {
        let store = CommitMetadata::new(Arc::new(MemoryLog::default()));
        let records = store.records_for("g", &[committed(0, 5, Some("m")), committed(1, 6, None)]);
        assert_eq!(records, vec![partition("g", 0, 5, "m")]);
        store.append(&records).await.unwrap();
        assert_eq!(
            store.metadata("g", "orders", 0, 5),
            None,
            "not replayed yet"
        );

        assert_eq!(store.replay().await.unwrap(), 1);
        assert_eq!(store.metadata("g", "orders", 0, 5).as_deref(), Some("m"));
        assert_eq!(store.metadata("g", "orders", 0, 6), None);
        assert_eq!(store.metadata("g", "orders", 1, 6), None);
        assert_eq!(store.metadata("h", "orders", 0, 5), None);
    }

    #[tokio::test]
    async fn given_a_later_commit_without_a_string_should_clear_the_entry() {
        let store = CommitMetadata::new(Arc::new(MemoryLog::default()));
        store
            .append(&store.records_for("g", &[committed(0, 5, Some("m"))]))
            .await
            .unwrap();
        store.replay().await.unwrap();

        let records = store.records_for("g", &[committed(0, 9, Some(""))]);
        assert_eq!(records, vec![partition("g", 0, 9, "")]);
        store.append(&records).await.unwrap();
        store.replay().await.unwrap();
        assert_eq!(store.metadata("g", "orders", 0, 5), None);
        assert!(!store.holds("g"));
    }

    /// The log is the truth: a second store over the same log (another gateway, or this one
    /// after a restart) replays every entry, in order.
    #[tokio::test]
    async fn given_a_restart_should_replay_every_entry_from_the_log() {
        let log = Arc::new(MemoryLog::default());
        let before = CommitMetadata::new(Arc::clone(&log) as Arc<dyn MetadataLog>);
        for (offset, metadata) in [(1, "a"), (2, "b"), (3, "c")] {
            before
                .append(&before.records_for("g", &[committed(0, offset, Some(metadata))]))
                .await
                .unwrap();
        }
        before
            .append(&before.records_for("g", &[committed(1, 4, Some("d"))]))
            .await
            .unwrap();
        before
            .append(&[MetadataRecord::GroupDeleted {
                group: "gone".to_string(),
            }])
            .await
            .unwrap();
        before
            .append(&before.records_for("gone", &[committed(0, 1, Some("x"))]))
            .await
            .unwrap();

        let after = CommitMetadata::new(log);
        assert_eq!(after.replay().await.unwrap(), 6);
        assert_eq!(after.metadata("g", "orders", 0, 3).as_deref(), Some("c"));
        assert_eq!(after.metadata("g", "orders", 1, 4).as_deref(), Some("d"));
        assert_eq!(after.metadata("gone", "orders", 0, 1).as_deref(), Some("x"));
        assert_eq!(after.replay().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn given_a_deleted_group_or_partition_should_clear_its_entries() {
        let store = CommitMetadata::new(Arc::new(MemoryLog::default()));
        store
            .append(&store.records_for(
                "g",
                &[committed(0, 5, Some("m")), committed(1, 6, Some("n"))],
            ))
            .await
            .unwrap();
        store.replay().await.unwrap();

        store.append(&[partition("g", 1, -1, "")]).await.unwrap();
        store.replay().await.unwrap();
        assert_eq!(store.metadata("g", "orders", 1, 6), None);
        assert_eq!(store.metadata("g", "orders", 0, 5).as_deref(), Some("m"));

        store
            .append(&[MetadataRecord::GroupDeleted {
                group: "g".to_string(),
            }])
            .await
            .unwrap();
        store.replay().await.unwrap();
        assert!(!store.holds("g"));
    }

    #[test]
    fn given_a_string_should_measure_it_as_java_does() {
        assert_eq!(kafka_length("abc"), 3);
        assert_eq!(kafka_length("é"), 1);
        assert_eq!(kafka_length("😀"), 2);
    }
}
