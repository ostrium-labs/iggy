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

//! One transactional id's state, and the transitions Kafka's coordinator allows.
//!
//! KIP-98, KIP-360 and KIP-588, as pure functions: each takes the current metadata and returns
//! the next, or the Kafka error to answer. The coordinator writes the next metadata to the
//! transaction log and only then makes it current.
//!
//! The rules follow Apache Kafka 4.1's `TransactionCoordinator` under transaction version 1, the
//! version this gateway serves (it never advertises `transaction.version` 2, see
//! `docs/IDEMPOTENCE.md`): `prepareInitProducerIdTransit`, `handleAddPartitionsToTransaction`,
//! `endTransactionWithTV1` and `abortTimedOutTransactions`. Kafka's pending `PrepareEpochFence`
//! state is not kept: fencing goes straight to `PrepareAbort` at the bumped epoch, which is the
//! record Kafka writes for it.

use std::collections::{BTreeMap, BTreeSet};

use kafka_protocol::error::ResponseError;
use serde::{Deserialize, Serialize};

pub const NO_PRODUCER_ID: i64 = -1;
pub const NO_PRODUCER_EPOCH: i16 = -1;

/// An epoch at or past this is exhausted: the next `InitProducerId` gets a new producer id, so the
/// coordinator can always fence with one more bump (Kafka's `isEpochExhausted`).
pub const EXHAUSTED_EPOCH: i16 = i16::MAX - 1;

/// The Kafka error codes the coordinator answers.
pub mod codes {
    use super::ResponseError;

    pub const NONE: i16 = 0;
    pub const NOT_ENOUGH_REPLICAS: i16 = ResponseError::NotEnoughReplicas.code();
    pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = ResponseError::CoordinatorLoadInProgress.code();
    pub const COORDINATOR_NOT_AVAILABLE: i16 = ResponseError::CoordinatorNotAvailable.code();
    pub const NOT_COORDINATOR: i16 = ResponseError::NotCoordinator.code();
    pub const INVALID_REQUEST: i16 = ResponseError::InvalidRequest.code();
    pub const INVALID_PRODUCER_EPOCH: i16 = ResponseError::InvalidProducerEpoch.code();
    pub const INVALID_TXN_STATE: i16 = ResponseError::InvalidTxnState.code();
    pub const INVALID_PRODUCER_ID_MAPPING: i16 = ResponseError::InvalidProducerIdMapping.code();
    pub const INVALID_TRANSACTION_TIMEOUT: i16 = ResponseError::InvalidTransactionTimeout.code();
    pub const CONCURRENT_TRANSACTIONS: i16 = ResponseError::ConcurrentTransactions.code();
    pub const OPERATION_NOT_ATTEMPTED: i16 = ResponseError::OperationNotAttempted.code();
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = ResponseError::UnknownTopicOrPartition.code();
    pub const PRODUCER_FENCED: i16 = ResponseError::ProducerFenced.code();
    pub const TRANSACTIONAL_ID_NOT_FOUND: i16 = ResponseError::TransactionalIdNotFound.code();
    pub const UNKNOWN_SERVER_ERROR: i16 = ResponseError::UnknownServerError.code();

    /// `PRODUCER_FENCED` (90) for a client that knows it, else `INVALID_PRODUCER_EPOCH` (47), as
    /// Kafka's `KafkaApis` answers request versions from before KIP-588.
    #[must_use]
    pub const fn fenced_for(code: i16, knows_producer_fenced: bool) -> i16 {
        if code == PRODUCER_FENCED && !knows_producer_fenced {
            INVALID_PRODUCER_EPOCH
        } else {
            code
        }
    }
}

/// Kafka's `TransactionState`, with its names as `DescribeTransactions` reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxnState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
}

impl TxnState {
    /// The name `DescribeTransactions` and `ListTransactions` use.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::Ongoing => "Ongoing",
            Self::PrepareCommit => "PrepareCommit",
            Self::PrepareAbort => "PrepareAbort",
            Self::CompleteCommit => "CompleteCommit",
            Self::CompleteAbort => "CompleteAbort",
            Self::Dead => "Dead",
        }
    }

    /// Markers are being written: the next transaction waits.
    #[must_use]
    pub const fn is_preparing(self) -> bool {
        matches!(self, Self::PrepareCommit | Self::PrepareAbort)
    }

    /// A transaction is open: its partitions hold their last stable offset.
    #[must_use]
    pub const fn is_open(self) -> bool {
        matches!(
            self,
            Self::Ongoing | Self::PrepareCommit | Self::PrepareAbort
        )
    }
}

/// How a transaction ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    Commit,
    Abort,
}

/// A data partition of a transaction, by Kafka topic name and index.
pub type TopicPartition = (String, i32);

/// What the coordinator knows of one data partition of an open transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionStart {
    /// The partition's high watermark when the transaction added it: no record of this
    /// transaction lies below it, and none of the producer's earlier transactions above it (that
    /// one's marker landed before this one could start). The bound of the last stable offset and
    /// of the aborted range when the first offset is not known. `None` when it could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
}

/// One transactional id's metadata: Kafka's `TransactionMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnMetadata {
    pub transactional_id: String,
    pub producer_id: i64,
    /// The producer id before the last rotation, so a retried `InitProducerId` of an exhausted
    /// epoch is recognized.
    pub prev_producer_id: i64,
    pub producer_epoch: i16,
    /// The epoch before the last bump by `InitProducerId` with the producer's own id and epoch, so
    /// its retry gets the same answer (KIP-360). -1 after a fence.
    pub last_producer_epoch: i16,
    pub timeout_ms: i32,
    pub state: TxnState,
    pub partitions: BTreeMap<TopicPartition, PartitionStart>,
    /// The groups whose offsets this transaction commits (`AddOffsetsToTxn`).
    pub groups: BTreeSet<String>,
    /// The offsets `TxnOffsetCommit` stored for each group, visible once the transaction commits.
    pub offsets: BTreeMap<String, BTreeMap<TopicPartition, i64>>,
    /// When the transaction's first partition was added, -1 when none is.
    pub start_ms: i64,
    pub update_ms: i64,
}

/// What `InitProducerId` leads to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitTransit {
    /// Write this metadata, then answer its producer id and epoch.
    Ready(TxnMetadata),
    /// The id had a transaction open: write this abort at the bumped epoch, then answer
    /// `CONCURRENT_TRANSACTIONS`, so the producer retries and gets the epoch after it.
    FenceAndAbort(TxnMetadata),
}

impl TxnMetadata {
    /// A new transactional id with `producer_id`, before its first epoch.
    #[must_use]
    pub fn new(transactional_id: &str, producer_id: i64, timeout_ms: i32, now_ms: i64) -> Self {
        Self {
            transactional_id: transactional_id.to_string(),
            producer_id,
            prev_producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            last_producer_epoch: NO_PRODUCER_EPOCH,
            timeout_ms,
            state: TxnState::Empty,
            partitions: BTreeMap::new(),
            groups: BTreeSet::new(),
            offsets: BTreeMap::new(),
            start_ms: -1,
            update_ms: now_ms,
        }
    }

    /// `InitProducerId` with a transactional id. `expected` is the producer's own id and epoch
    /// (`InitProducerId` v3+, KIP-360), `None` for a new producer. `new_producer_id` is called when
    /// the epoch is exhausted and the id rotates.
    ///
    /// # Errors
    ///
    /// `CONCURRENT_TRANSACTIONS` while markers are written, `PRODUCER_FENCED` for an `expected`
    /// that a newer producer replaced.
    pub fn init_producer_id(
        &self,
        timeout_ms: i32,
        expected: Option<(i64, i16)>,
        now_ms: i64,
        new_producer_id: impl FnOnce() -> Option<i64>,
    ) -> Result<InitTransit, i16> {
        let valid_expected = |(id, epoch): (i64, i16)| {
            self.producer_epoch == NO_PRODUCER_EPOCH
                || id == self.producer_id
                || (id == self.prev_producer_id && epoch >= EXHAUSTED_EPOCH)
        };
        if !expected.is_none_or(valid_expected) {
            return Err(codes::PRODUCER_FENCED);
        }
        match self.state {
            TxnState::PrepareCommit | TxnState::PrepareAbort => Err(codes::CONCURRENT_TRANSACTIONS),
            TxnState::Ongoing => Ok(InitTransit::FenceAndAbort(self.fence(now_ms)?)),
            TxnState::Empty
            | TxnState::CompleteCommit
            | TxnState::CompleteAbort
            | TxnState::Dead => {
                let exhausted = self.producer_epoch >= EXHAUSTED_EPOCH;
                if exhausted && expected.is_none_or(|(_, epoch)| epoch == self.producer_epoch) {
                    let rotated = new_producer_id().ok_or(codes::UNKNOWN_SERVER_ERROR)?;
                    return Ok(InitTransit::Ready(Self {
                        prev_producer_id: self.producer_id,
                        producer_id: rotated,
                        producer_epoch: 0,
                        last_producer_epoch: if expected.is_some() {
                            self.producer_epoch
                        } else {
                            NO_PRODUCER_EPOCH
                        },
                        ..self.reset(timeout_ms, now_ms)
                    }));
                }
                let bumped = self.producer_epoch.saturating_add(1);
                let (epoch, last) = match expected {
                    None => (bumped, NO_PRODUCER_EPOCH),
                    Some((_, epoch))
                        if self.producer_epoch == NO_PRODUCER_EPOCH
                            || epoch == self.producer_epoch =>
                    {
                        (bumped, self.producer_epoch)
                    }
                    // A retry of a bump whose answer was lost: the same answer again.
                    Some((_, epoch)) if epoch == self.last_producer_epoch => {
                        (self.producer_epoch, self.last_producer_epoch)
                    }
                    Some(_) => return Err(codes::PRODUCER_FENCED),
                };
                Ok(InitTransit::Ready(Self {
                    producer_epoch: epoch,
                    last_producer_epoch: last,
                    ..self.reset(timeout_ms, now_ms)
                }))
            }
        }
    }

    /// Empty, at `timeout_ms`, with no partition: what `InitProducerId` leaves.
    fn reset(&self, timeout_ms: i32, now_ms: i64) -> Self {
        Self {
            timeout_ms,
            state: TxnState::Empty,
            partitions: BTreeMap::new(),
            groups: BTreeSet::new(),
            offsets: BTreeMap::new(),
            start_ms: -1,
            update_ms: now_ms,
            ..self.clone()
        }
    }

    /// Bumps the epoch and aborts the open transaction: Kafka's `prepareFenceProducerEpoch`
    /// followed by the abort it leads to. The fenced producer's next call is refused.
    ///
    /// # Errors
    ///
    /// `UNKNOWN_SERVER_ERROR` at an epoch of `i16::MAX`, which the exhaustion rule never lets a
    /// producer reach.
    pub fn fence(&self, now_ms: i64) -> Result<Self, i16> {
        if self.producer_epoch == i16::MAX {
            return Err(codes::UNKNOWN_SERVER_ERROR);
        }
        Ok(Self {
            producer_epoch: self.producer_epoch + 1,
            last_producer_epoch: NO_PRODUCER_EPOCH,
            state: TxnState::PrepareAbort,
            update_ms: now_ms,
            ..self.clone()
        })
    }

    /// Checks that `producer_id` at `epoch` is this id's current producer.
    const fn check_producer(&self, producer_id: i64, epoch: i16) -> Result<(), i16> {
        if self.producer_id != producer_id {
            return Err(codes::INVALID_PRODUCER_ID_MAPPING);
        }
        if self.producer_epoch != epoch {
            return Err(codes::PRODUCER_FENCED);
        }
        Ok(())
    }

    /// `AddPartitionsToTxn` and `AddOffsetsToTxn`: `Ok(None)` when every partition and group is
    /// in the transaction already, so nothing needs writing.
    ///
    /// `starts` gives each new partition's high watermark, read before the call.
    ///
    /// # Errors
    ///
    /// `INVALID_PRODUCER_ID_MAPPING`, `PRODUCER_FENCED`, or `CONCURRENT_TRANSACTIONS` while the
    /// last transaction's markers are written.
    pub fn add(
        &self,
        producer_id: i64,
        epoch: i16,
        partitions: &BTreeMap<TopicPartition, PartitionStart>,
        groups: &BTreeSet<String>,
        now_ms: i64,
    ) -> Result<Option<Self>, i16> {
        self.check_producer(producer_id, epoch)?;
        if self.state.is_preparing() {
            return Err(codes::CONCURRENT_TRANSACTIONS);
        }
        if self.state == TxnState::Dead {
            return Err(codes::INVALID_PRODUCER_ID_MAPPING);
        }
        let known = partitions
            .keys()
            .all(|partition| self.partitions.contains_key(partition))
            && groups.is_subset(&self.groups);
        if self.state == TxnState::Ongoing && known {
            return Ok(None);
        }
        let mut next = self.clone();
        if !matches!(self.state, TxnState::Ongoing) {
            next.start_ms = now_ms;
            next.partitions.clear();
            next.groups.clear();
            next.offsets.clear();
        }
        for (partition, start) in partitions {
            next.partitions.entry(partition.clone()).or_insert(*start);
        }
        next.groups.extend(groups.iter().cloned());
        next.state = TxnState::Ongoing;
        next.update_ms = now_ms;
        Ok(Some(next))
    }

    /// `EndTxn` from the producer: `Ok(None)` for a retry of an end that completed, answered
    /// `NONE` without a write.
    ///
    /// # Errors
    ///
    /// `INVALID_PRODUCER_ID_MAPPING`, `PRODUCER_FENCED`, `CONCURRENT_TRANSACTIONS` for a retry
    /// while markers are written, `INVALID_TXN_STATE` for an end that does not fit the state.
    pub fn end(
        &self,
        producer_id: i64,
        epoch: i16,
        outcome: Outcome,
        now_ms: i64,
    ) -> Result<Option<Self>, i16> {
        self.check_producer(producer_id, epoch)?;
        match (self.state, outcome) {
            (TxnState::Ongoing, _) => Ok(Some(Self {
                state: match outcome {
                    Outcome::Commit => TxnState::PrepareCommit,
                    Outcome::Abort => TxnState::PrepareAbort,
                },
                update_ms: now_ms,
                ..self.clone()
            })),
            (TxnState::CompleteCommit, Outcome::Commit)
            | (TxnState::CompleteAbort, Outcome::Abort) => Ok(None),
            (TxnState::PrepareCommit, Outcome::Commit)
            | (TxnState::PrepareAbort, Outcome::Abort) => Err(codes::CONCURRENT_TRANSACTIONS),
            _ => Err(codes::INVALID_TXN_STATE),
        }
    }

    /// The state after every marker of a `Prepare*` landed.
    #[must_use]
    pub fn complete(&self, now_ms: i64) -> Self {
        Self {
            state: if self.state == TxnState::PrepareCommit {
                TxnState::CompleteCommit
            } else {
                TxnState::CompleteAbort
            },
            partitions: BTreeMap::new(),
            groups: BTreeSet::new(),
            offsets: BTreeMap::new(),
            update_ms: now_ms,
            ..self.clone()
        }
    }

    /// `TxnOffsetCommit`: stores `offsets` of `group` in the transaction, to be committed with it.
    ///
    /// # Errors
    ///
    /// `INVALID_PRODUCER_EPOCH` for a fenced producer (the code the group coordinator answers for
    /// a stale epoch), `INVALID_PRODUCER_ID_MAPPING`, `CONCURRENT_TRANSACTIONS` while markers are
    /// written, and `INVALID_TXN_STATE` for a group the transaction did not add.
    pub fn commit_offsets(
        &self,
        producer_id: i64,
        epoch: i16,
        group: &str,
        offsets: &BTreeMap<TopicPartition, i64>,
        now_ms: i64,
    ) -> Result<Self, i16> {
        match self.check_producer(producer_id, epoch) {
            Err(codes::PRODUCER_FENCED) => return Err(codes::INVALID_PRODUCER_EPOCH),
            other => other?,
        }
        if self.state.is_preparing() {
            return Err(codes::CONCURRENT_TRANSACTIONS);
        }
        if self.state != TxnState::Ongoing || !self.groups.contains(group) {
            return Err(codes::INVALID_TXN_STATE);
        }
        let mut next = self.clone();
        next.offsets.entry(group.to_string()).or_default().extend(
            offsets
                .iter()
                .map(|(partition, offset)| (partition.clone(), *offset)),
        );
        next.update_ms = now_ms;
        Ok(next)
    }

    /// Whether an open transaction ran past its timeout at `now_ms`.
    #[must_use]
    pub const fn timed_out(&self, now_ms: i64) -> bool {
        matches!(self.state, TxnState::Ongoing)
            && self.start_ms >= 0
            && self.start_ms.saturating_add(self.timeout_ms as i64) < now_ms
    }

    /// Checks that a transactional Produce batch of `producer_id` at `epoch` may land on
    /// `partition`. Kafka's partition leader asks the coordinator the same (KIP-890 part 1) and
    /// answers with these codes, mapped for Produce by the caller.
    ///
    /// # Errors
    ///
    /// `INVALID_PRODUCER_ID_MAPPING`, `PRODUCER_FENCED`, `CONCURRENT_TRANSACTIONS` while markers
    /// are written, and `INVALID_TXN_STATE` for a partition the transaction did not add.
    pub fn verify_write(
        &self,
        producer_id: i64,
        epoch: i16,
        partition: &TopicPartition,
    ) -> Result<(), i16> {
        if self.producer_id != producer_id {
            return Err(codes::INVALID_PRODUCER_ID_MAPPING);
        }
        if self.producer_epoch != epoch {
            return Err(codes::PRODUCER_FENCED);
        }
        if self.state.is_preparing() {
            return Err(codes::CONCURRENT_TRANSACTIONS);
        }
        if self.state != TxnState::Ongoing || !self.partitions.contains_key(partition) {
            return Err(codes::INVALID_TXN_STATE);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_000_000;

    fn fresh() -> TxnMetadata {
        TxnMetadata::new("tx", 100, 60_000, NOW)
    }

    fn ready(transit: Result<InitTransit, i16>) -> TxnMetadata {
        match transit {
            Ok(InitTransit::Ready(next)) => next,
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        (topic.to_string(), partition)
    }

    fn starts(list: &[(&str, i32)]) -> BTreeMap<TopicPartition, PartitionStart> {
        list.iter()
            .map(|(topic, partition)| (tp(topic, *partition), PartitionStart { start: Some(5) }))
            .collect()
    }

    fn ongoing() -> TxnMetadata {
        let init = ready(fresh().init_producer_id(60_000, None, NOW, || None));
        init.add(100, 0, &starts(&[("t", 0)]), &BTreeSet::new(), NOW + 1)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn given_a_new_id_when_initialized_should_start_at_epoch_zero() {
        let init = ready(fresh().init_producer_id(60_000, None, NOW, || None));
        assert_eq!((init.producer_id, init.producer_epoch), (100, 0));
        assert_eq!(init.state, TxnState::Empty);
        assert_eq!(init.last_producer_epoch, NO_PRODUCER_EPOCH);
    }

    #[test]
    fn given_a_second_producer_when_initialized_should_bump_the_epoch_and_fence_the_first() {
        let first = ready(fresh().init_producer_id(60_000, None, NOW, || None));
        let second = ready(first.init_producer_id(60_000, None, NOW, || None));
        assert_eq!(second.producer_epoch, 1);
        assert_eq!(
            second.verify_write(100, 0, &tp("t", 0)),
            Err(codes::PRODUCER_FENCED)
        );
        assert_eq!(
            second.add(100, 0, &starts(&[("t", 0)]), &BTreeSet::new(), NOW),
            Err(codes::PRODUCER_FENCED)
        );
    }

    #[test]
    fn given_an_open_transaction_when_initialized_should_fence_and_abort_it() {
        let open = ongoing();
        let Ok(InitTransit::FenceAndAbort(abort)) =
            open.init_producer_id(60_000, None, NOW + 2, || None)
        else {
            panic!("an open transaction is fenced and aborted first");
        };
        assert_eq!(abort.state, TxnState::PrepareAbort);
        assert_eq!(abort.producer_epoch, 1);
        assert_eq!(
            abort.partitions, open.partitions,
            "its partitions get the markers"
        );
        let done = abort.complete(NOW + 3);
        let next = ready(done.init_producer_id(60_000, None, NOW + 4, || None));
        assert_eq!(next.producer_epoch, 2, "the retry bumps a second time");
    }

    #[test]
    fn given_markers_being_written_when_initialized_should_answer_concurrent_transactions() {
        let preparing = ongoing()
            .end(100, 0, Outcome::Commit, NOW)
            .unwrap()
            .unwrap();
        assert_eq!(
            preparing.init_producer_id(60_000, None, NOW, || None),
            Err(codes::CONCURRENT_TRANSACTIONS)
        );
    }

    #[test]
    fn given_the_producers_own_epoch_when_reinitialized_should_bump_once_and_answer_a_retry_alike()
    {
        let init = ready(fresh().init_producer_id(60_000, None, NOW, || None));
        let bumped = ready(init.init_producer_id(60_000, Some((100, 0)), NOW, || None));
        assert_eq!((bumped.producer_epoch, bumped.last_producer_epoch), (1, 0));
        let retried = ready(bumped.init_producer_id(60_000, Some((100, 0)), NOW, || None));
        assert_eq!(retried.producer_epoch, 1, "a lost answer is answered again");
        assert_eq!(
            bumped.init_producer_id(60_000, Some((100, 7)), NOW, || None),
            Err(codes::PRODUCER_FENCED)
        );
        assert_eq!(
            bumped.init_producer_id(60_000, Some((555, 1)), NOW, || None),
            Err(codes::PRODUCER_FENCED)
        );
    }

    #[test]
    fn given_an_exhausted_epoch_when_initialized_should_rotate_the_producer_id() {
        let mut init = ready(fresh().init_producer_id(60_000, None, NOW, || None));
        init.producer_epoch = EXHAUSTED_EPOCH;
        let rotated =
            ready(init.init_producer_id(60_000, Some((100, EXHAUSTED_EPOCH)), NOW, || Some(200)));
        assert_eq!(
            (
                rotated.producer_id,
                rotated.producer_epoch,
                rotated.prev_producer_id
            ),
            (200, 0, 100)
        );
        let retried =
            ready(rotated.init_producer_id(60_000, Some((100, EXHAUSTED_EPOCH)), NOW, || None));
        assert_eq!(
            retried.producer_id, 200,
            "the old id with an exhausted epoch is a retry"
        );
    }

    #[test]
    fn given_partitions_when_added_should_open_and_skip_a_repeat() {
        let open = ongoing();
        assert_eq!(open.state, TxnState::Ongoing);
        assert_eq!(open.start_ms, NOW + 1);
        assert_eq!(
            open.add(100, 0, &starts(&[("t", 0)]), &BTreeSet::new(), NOW + 5),
            Ok(None)
        );
        let wider = open
            .add(100, 0, &starts(&[("t", 1)]), &BTreeSet::new(), NOW + 5)
            .unwrap()
            .unwrap();
        assert_eq!(wider.partitions.len(), 2);
        assert_eq!(wider.start_ms, NOW + 1, "the start stays the first add's");
    }

    #[test]
    fn given_an_end_when_applied_should_follow_kafkas_table() {
        let open = ongoing();
        let commit = open.end(100, 0, Outcome::Commit, NOW).unwrap().unwrap();
        assert_eq!(commit.state, TxnState::PrepareCommit);
        assert_eq!(
            commit.end(100, 0, Outcome::Commit, NOW),
            Err(codes::CONCURRENT_TRANSACTIONS)
        );
        assert_eq!(
            commit.end(100, 0, Outcome::Abort, NOW),
            Err(codes::INVALID_TXN_STATE)
        );
        let done = commit.complete(NOW);
        assert_eq!(done.state, TxnState::CompleteCommit);
        assert!(done.partitions.is_empty());
        assert_eq!(done.end(100, 0, Outcome::Commit, NOW), Ok(None), "a retry");
        assert_eq!(
            done.end(100, 0, Outcome::Abort, NOW),
            Err(codes::INVALID_TXN_STATE)
        );
        let empty = ready(fresh().init_producer_id(60_000, None, NOW, || None));
        assert_eq!(
            empty.end(100, 0, Outcome::Abort, NOW),
            Err(codes::INVALID_TXN_STATE)
        );
        assert_eq!(
            open.end(100, 1, Outcome::Commit, NOW),
            Err(codes::PRODUCER_FENCED)
        );
        assert_eq!(
            open.end(9, 0, Outcome::Commit, NOW),
            Err(codes::INVALID_PRODUCER_ID_MAPPING)
        );
    }

    #[test]
    fn given_a_write_when_verified_should_need_the_partition_in_an_open_transaction() {
        let open = ongoing();
        assert_eq!(open.verify_write(100, 0, &tp("t", 0)), Ok(()));
        assert_eq!(
            open.verify_write(100, 0, &tp("t", 1)),
            Err(codes::INVALID_TXN_STATE)
        );
        assert_eq!(
            open.verify_write(101, 0, &tp("t", 0)),
            Err(codes::INVALID_PRODUCER_ID_MAPPING)
        );
        let preparing = open.end(100, 0, Outcome::Abort, NOW).unwrap().unwrap();
        assert_eq!(
            preparing.verify_write(100, 0, &tp("t", 0)),
            Err(codes::CONCURRENT_TRANSACTIONS)
        );
    }

    #[test]
    fn given_an_open_transaction_past_its_timeout_should_read_as_timed_out() {
        let open = ongoing();
        assert!(!open.timed_out(NOW + 60_001));
        assert!(open.timed_out(NOW + 60_002));
        assert!(!open.complete(NOW).timed_out(i64::MAX));
    }

    #[test]
    fn given_an_old_client_should_get_invalid_producer_epoch_for_fenced() {
        assert_eq!(codes::fenced_for(codes::PRODUCER_FENCED, false), 47);
        assert_eq!(codes::fenced_for(codes::PRODUCER_FENCED, true), 90);
        assert_eq!(codes::fenced_for(codes::INVALID_TXN_STATE, false), 48);
    }
}
