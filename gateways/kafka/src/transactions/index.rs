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

//! What each data partition knows of transactions.
//!
//! The open ones (their lower bounds hold the last stable offset), the aborted ranges
//! (`read_committed` Fetch lists them), and where each producer's last marker lies (the next
//! transaction of that producer starts above it).
//!
//! Kafka keeps this beside each partition's log (`ProducerStateManager`, the transaction index).
//! Here it is rebuilt from the transaction log, which every `CompleteAbort` and `CompleteCommit`
//! record feeds, so no data partition is scanned on start.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, PoisonError};

use super::log::EndedRange;
use super::metadata::TopicPartition;

/// One open transaction on one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Open {
    /// No record of the transaction lies below this.
    bound: u64,
    /// The first offset it wrote, once a send named it.
    first_written: Option<u64>,
    /// Whether it may have written here: a send started, or the transaction was recovered from
    /// the log, where whether it wrote is not known. Only such a transaction holds the last stable
    /// offset.
    holds: bool,
}

#[derive(Debug, Default)]
struct PartitionTxns {
    open: BTreeMap<i64, Open>,
    /// Sorted by `last`.
    aborted: Vec<EndedRange>,
    /// One past each producer's last marker.
    after_marker: HashMap<i64, u64>,
}

/// The transactions of every data partition, by Kafka topic and index.
#[derive(Debug, Default)]
pub struct TxnIndex {
    partitions: Mutex<HashMap<TopicPartition, PartitionTxns>>,
}

/// An aborted transaction as Fetch lists it: the producer and the first offset it may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    pub producer_id: i64,
    pub first_offset: u64,
}

impl TxnIndex {
    fn with<T>(&self, partition: &TopicPartition, work: impl FnOnce(&mut PartitionTxns) -> T) -> T {
        let mut partitions = self
            .partitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        work(partitions.entry(partition.clone()).or_default())
    }

    fn read<T>(
        &self,
        partition: &TopicPartition,
        read: impl FnOnce(&PartitionTxns) -> T,
    ) -> Option<T> {
        let partitions = self
            .partitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        partitions.get(partition).map(read)
    }

    /// Opens the transaction of `producer_id` on `partition`, below which it holds no record.
    /// `recovered` when it comes from the log, so it may already have written.
    pub fn open(&self, partition: &TopicPartition, producer_id: i64, bound: u64, recovered: bool) {
        self.with(partition, |txns| {
            txns.open.entry(producer_id).or_insert(Open {
                bound,
                first_written: None,
                holds: recovered,
            });
        });
    }

    /// A send of the open transaction of `producer_id` starts on `partition`.
    pub fn writing(&self, partition: &TopicPartition, producer_id: i64) {
        self.with(partition, |txns| {
            if let Some(open) = txns.open.get_mut(&producer_id) {
                open.holds = true;
            }
        });
    }

    /// A send of that transaction landed at `base_offset`.
    pub fn written(&self, partition: &TopicPartition, producer_id: i64, base_offset: u64) {
        self.with(partition, |txns| {
            if let Some(open) = txns.open.get_mut(&producer_id) {
                open.first_written = Some(
                    open.first_written
                        .map_or(base_offset, |first| first.min(base_offset)),
                );
            }
        });
    }

    /// The first offset the open transaction of `producer_id` may hold on `partition`: what it
    /// wrote first, else its bound. `None` when none is open.
    #[must_use]
    pub fn first_of(&self, partition: &TopicPartition, producer_id: i64) -> Option<u64> {
        self.read(partition, |txns| {
            txns.open
                .get(&producer_id)
                .map(|open| open.first_written.unwrap_or(open.bound).max(open.bound))
        })
        .flatten()
    }

    /// One past the last marker of `producer_id` on `partition`, if one is known.
    #[must_use]
    pub fn after_marker(&self, partition: &TopicPartition, producer_id: i64) -> Option<u64> {
        self.read(partition, |txns| {
            txns.after_marker.get(&producer_id).copied()
        })
        .flatten()
    }

    /// Ends the transaction of `ended.producer_id` on its partition: it no longer holds the last
    /// stable offset, its marker bounds the producer's next one, and an abort is listed.
    pub fn end(&self, ended: &EndedRange, aborted: bool) {
        let partition = (ended.topic.clone(), ended.partition);
        self.with(&partition, |txns| {
            txns.open.remove(&ended.producer_id);
            if ended.exact {
                let next = ended.last.saturating_add(1);
                let after = txns.after_marker.entry(ended.producer_id).or_insert(next);
                *after = (*after).max(next);
            } else {
                txns.after_marker
                    .entry(ended.producer_id)
                    .and_modify(|after| *after = (*after).max(ended.last))
                    .or_insert(ended.last);
            }
            if aborted {
                let at = txns
                    .aborted
                    .partition_point(|range| range.last <= ended.last);
                txns.aborted.insert(at, ended.clone());
            }
        });
    }

    /// The last stable offset of `partition` whose high watermark is `high_watermark`: the first
    /// offset of its earliest open transaction that may have written, else the high watermark.
    #[must_use]
    pub fn last_stable_offset(&self, partition: &TopicPartition, high_watermark: u64) -> u64 {
        self.read(partition, |txns| {
            txns.open
                .values()
                .filter(|open| open.holds)
                .map(|open| open.first_written.unwrap_or(open.bound).max(open.bound))
                .min()
        })
        .flatten()
        .map_or(high_watermark, |first| first.min(high_watermark))
    }

    /// The aborted transactions of `partition` that overlap `from..upper`, as Kafka's
    /// `collectAbortedTransactions` lists them: ending at or after `from` and starting before
    /// `upper`.
    #[must_use]
    pub fn aborted_between(
        &self,
        partition: &TopicPartition,
        from: u64,
        upper: u64,
    ) -> Vec<AbortedTxn> {
        self.read(partition, |txns| {
            let start = txns.aborted.partition_point(|range| range.last < from);
            txns.aborted[start..]
                .iter()
                .filter(|range| range.first < upper)
                .map(|range| AbortedTxn {
                    producer_id: range.producer_id,
                    first_offset: range.first,
                })
                .collect()
        })
        .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp() -> TopicPartition {
        ("t".to_string(), 0)
    }

    fn ended(producer_id: i64, first: u64, last: u64) -> EndedRange {
        EndedRange {
            topic: "t".into(),
            partition: 0,
            producer_id,
            first,
            last,
            exact: true,
        }
    }

    #[test]
    fn given_open_transactions_the_last_stable_offset_should_be_the_earliest_that_wrote() {
        let index = TxnIndex::default();
        assert_eq!(index.last_stable_offset(&tp(), 10), 10);
        index.open(&tp(), 1, 4, false);
        assert_eq!(
            index.last_stable_offset(&tp(), 10),
            10,
            "added, nothing sent yet"
        );
        index.writing(&tp(), 1);
        assert_eq!(index.last_stable_offset(&tp(), 10), 4);
        index.written(&tp(), 1, 6);
        assert_eq!(index.last_stable_offset(&tp(), 10), 6);
        index.open(&tp(), 2, 3, true);
        assert_eq!(
            index.last_stable_offset(&tp(), 10),
            3,
            "recovered ones hold"
        );
        index.end(&ended(2, 3, 8), false);
        index.end(&ended(1, 6, 9), true);
        assert_eq!(index.last_stable_offset(&tp(), 10), 10);
        assert_eq!(index.after_marker(&tp(), 1), Some(10));
    }

    #[test]
    fn given_aborted_ranges_should_list_those_that_overlap_the_read() {
        let index = TxnIndex::default();
        index.end(&ended(1, 0, 4), true);
        index.end(&ended(2, 5, 9), true);
        index.end(&ended(3, 2, 7), false);
        let ids = |from, upper| {
            index
                .aborted_between(&tp(), from, upper)
                .iter()
                .map(|txn| txn.producer_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(0, 100), vec![1, 2]);
        assert_eq!(ids(5, 100), vec![2]);
        assert_eq!(ids(0, 5), vec![1], "starts before the upper bound only");
        assert_eq!(ids(10, 100), Vec::<i64>::new());
    }

    #[test]
    fn given_an_inexact_marker_the_next_start_should_be_the_watermark_after_it() {
        let index = TxnIndex::default();
        index.end(
            &EndedRange {
                exact: false,
                ..ended(1, 0, 12)
            },
            false,
        );
        assert_eq!(index.after_marker(&tp(), 1), Some(12));
    }
}
