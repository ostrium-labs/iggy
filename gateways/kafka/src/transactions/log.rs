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

//! The transaction log: Kafka's `__transaction_state`, kept in the Iggy topic
//! `<default stream>.meta/txn_state` (KW-T).
//!
//! Each record is one transactional id's whole metadata after a transition, as JSON, so the
//! current state of an id is its last record: replaying the log in order rebuilds the
//! coordinator. A `CompleteCommit` or `CompleteAbort` record also lists, per partition of that
//! transaction, the range it spanned up to its marker: the aborted ones are what `read_committed`
//! Fetch needs, and the markers bound where the producer's next transaction may start. Every such
//! record counts, not only the last per id.
//!
//! An id lives on partition `hash(transactional_id) mod partitions` (Java's `String.hashCode`,
//! as Kafka places it), so a later gateway that owns only some partitions (KW-5) replays only
//! those. The log is never compacted (KW-6 brings compaction); see `docs/TRANSACTIONS.md`.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::metadata::{PartitionStart, TopicPartition, TxnMetadata, TxnState};
use crate::bridge::BridgeError;

/// Partitions of the transaction log.
pub const LOG_PARTITIONS: u32 = 16;

/// The format version every record carries.
const RECORD_VERSION: u8 = 1;

/// The partition of the transaction log that holds `transactional_id`.
#[must_use]
pub fn partition_for(transactional_id: &str, partitions: u32) -> u32 {
    // Java's String.hashCode over UTF-16 code units, then Kafka's Utils.abs.
    let mut hash: i32 = 0;
    for unit in transactional_id.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(unit));
    }
    let positive = if hash == i32::MIN { 0 } else { hash.abs() };
    positive.unsigned_abs() % partitions.max(1)
}

/// The range of an ended transaction on one partition.
///
/// From the first offset it may hold to its marker. `first` may be lower than the transaction's
/// first record, never higher, and is above the producer's previous marker on the partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndedRange {
    pub topic: String,
    pub partition: i32,
    pub producer_id: i64,
    pub first: u64,
    /// The marker's offset.
    pub last: u64,
    /// Whether `last` is the marker's own offset; when Iggy did not name it, `last` is the high
    /// watermark read after the marker landed, which is past it.
    #[serde(default = "exact_default", skip_serializing_if = "is_true")]
    pub exact: bool,
}

const fn exact_default() -> bool {
    true
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's skip_serializing_if signature"
)]
const fn is_true(value: &bool) -> bool {
    *value
}

/// One partition of a transaction in a record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LoggedPartition {
    t: String,
    p: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
}

/// One record of the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    v: u8,
    tid: String,
    pid: i64,
    prev_pid: i64,
    epoch: i16,
    last_epoch: i16,
    timeout_ms: i32,
    state: TxnState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    partitions: Vec<LoggedPartition>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    groups: BTreeSet<String>,
    start_ms: i64,
    update_ms: i64,
    /// On a `CompleteCommit` or `CompleteAbort`: the range of each partition.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ended: Vec<EndedRange>,
}

impl LogRecord {
    #[must_use]
    pub fn of(metadata: &TxnMetadata, ended: Vec<EndedRange>) -> Self {
        Self {
            v: RECORD_VERSION,
            tid: metadata.transactional_id.clone(),
            pid: metadata.producer_id,
            prev_pid: metadata.prev_producer_id,
            epoch: metadata.producer_epoch,
            last_epoch: metadata.last_producer_epoch,
            timeout_ms: metadata.timeout_ms,
            state: metadata.state,
            partitions: metadata
                .partitions
                .iter()
                .map(|((topic, partition), start)| LoggedPartition {
                    t: topic.clone(),
                    p: *partition,
                    start: start.start,
                })
                .collect(),
            groups: metadata.groups.clone(),
            start_ms: metadata.start_ms,
            update_ms: metadata.update_ms,
            ended,
        }
    }

    #[must_use]
    pub fn transactional_id(&self) -> &str {
        &self.tid
    }

    #[must_use]
    pub fn metadata(&self) -> TxnMetadata {
        TxnMetadata {
            transactional_id: self.tid.clone(),
            producer_id: self.pid,
            prev_producer_id: self.prev_pid,
            producer_epoch: self.epoch,
            last_producer_epoch: self.last_epoch,
            timeout_ms: self.timeout_ms,
            state: self.state,
            partitions: self
                .partitions
                .iter()
                .map(|logged| {
                    let partition: TopicPartition = (logged.t.clone(), logged.p);
                    (
                        partition,
                        PartitionStart {
                            start: logged.start,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            groups: self.groups.clone(),
            start_ms: self.start_ms,
            update_ms: self.update_ms,
        }
    }

    #[must_use]
    pub fn ended(&self) -> &[EndedRange] {
        &self.ended
    }

    /// # Errors
    ///
    /// When the record does not serialize, which a record of strings and numbers never does.
    pub fn encode(&self) -> Result<Vec<u8>, BridgeError> {
        serde_json::to_vec(self).map_err(|error| BridgeError::InvalidConfig(error.to_string()))
    }

    /// `None` for a record of another format; it is skipped with a warning.
    #[must_use]
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let record: Self = serde_json::from_slice(payload).ok()?;
        (record.v == RECORD_VERSION).then_some(record)
    }
}

/// The log as the coordinator uses it: the Iggy topic in production, memory in unit tests.
#[async_trait]
pub trait TxnLog: Send + Sync + 'static {
    /// Appends `record` to `partition`, and returns once it is durable.
    ///
    /// # Errors
    ///
    /// The bridge's error. An error with an unknown outcome (a timeout) may still land; the next
    /// record for the same id supersedes it, since each record holds the whole state.
    async fn append(&self, partition: u32, record: &LogRecord) -> Result<(), BridgeError>;

    /// Every record of every partition, in order per partition.
    ///
    /// # Errors
    ///
    /// The bridge's error. A log that does not exist yet is empty.
    async fn read_all(&self) -> Result<Vec<LogRecord>, BridgeError>;
}

/// What replaying the log rebuilt.
#[derive(Debug, Default)]
pub struct Replayed {
    /// The last state of each transactional id.
    pub metadata: BTreeMap<String, TxnMetadata>,
    /// Every aborted range, in log order per id.
    pub aborted: Vec<EndedRange>,
    /// Every ended range, aborted or committed, in log order per id.
    pub ended: Vec<EndedRange>,
}

/// Replays `records`, oldest first within each partition.
#[must_use]
pub fn replay(records: Vec<LogRecord>) -> Replayed {
    let mut replayed = Replayed::default();
    for record in records {
        if record.state == TxnState::CompleteAbort {
            replayed.aborted.extend(record.ended.iter().cloned());
        }
        replayed.ended.extend(record.ended.iter().cloned());
        replayed
            .metadata
            .insert(record.tid.clone(), record.metadata());
    }
    replayed
}

/// The log in memory, for unit tests.
#[derive(Debug, Default)]
pub struct MemoryLog {
    records: std::sync::Mutex<Vec<(u32, LogRecord)>>,
    /// Appends that fail from now on, as an unreachable Iggy would.
    pub failing: std::sync::atomic::AtomicBool,
}

impl MemoryLog {
    #[must_use]
    pub fn records(&self) -> Vec<LogRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(_, record)| record.clone())
            .collect()
    }
}

#[async_trait]
impl TxnLog for MemoryLog {
    async fn append(&self, partition: u32, record: &LogRecord) -> Result<(), BridgeError> {
        if self.failing.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BridgeError::Timeout);
        }
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((partition, record.clone()));
        Ok(())
    }

    async fn read_all(&self) -> Result<Vec<LogRecord>, BridgeError> {
        if self.failing.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BridgeError::Timeout);
        }
        Ok(self.records())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactions::metadata::TxnMetadata;

    #[test]
    fn given_kafkas_hash_when_placing_an_id_should_match_javas_string_hash_code() {
        // "transactional-id".hashCode() == 510_590_431 in Java; mod 50 == 31.
        assert_eq!(partition_for("transactional-id", 50), 31);
        assert_eq!(partition_for("", 16), 0);
        // A hash of i32::MIN maps to 0, as Kafka's Utils.abs does: "polygenelubricants".
        assert_eq!(partition_for("polygenelubricants", 7), 0);
        assert!(partition_for("loams-oracle-x", LOG_PARTITIONS) < LOG_PARTITIONS);
    }

    #[test]
    fn given_records_when_replayed_should_keep_the_last_state_and_every_abort() {
        let mut first = TxnMetadata::new("a", 1, 1000, 5);
        first.producer_epoch = 0;
        let mut second = first.clone();
        second.state = TxnState::CompleteAbort;
        let abort = EndedRange {
            topic: "t".into(),
            partition: 0,
            producer_id: 1,
            first: 3,
            last: 9,
            exact: true,
        };
        let records = vec![
            LogRecord::of(&first, Vec::new()),
            LogRecord::of(&second, vec![abort.clone()]),
            LogRecord::of(&TxnMetadata::new("b", 2, 1000, 5), Vec::new()),
        ];
        let decoded: Vec<LogRecord> = records
            .iter()
            .map(|record| LogRecord::decode(&record.encode().unwrap()).unwrap())
            .collect();
        assert_eq!(decoded, records, "records round-trip through JSON");
        let replayed = replay(decoded);
        assert_eq!(replayed.metadata.len(), 2);
        assert_eq!(replayed.metadata["a"].state, TxnState::CompleteAbort);
        assert_eq!(replayed.aborted, vec![abort.clone()]);
        assert_eq!(replayed.ended, vec![abort]);
    }

    #[test]
    fn given_a_record_of_another_version_should_skip_it() {
        let mut record = LogRecord::of(&TxnMetadata::new("a", 1, 1000, 5), Vec::new());
        record.v = 9;
        assert_eq!(LogRecord::decode(&record.encode().unwrap()), None);
        assert_eq!(LogRecord::decode(b"not json"), None);
    }
}
