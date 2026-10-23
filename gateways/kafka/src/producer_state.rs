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

//! Idempotent producer state, as Kafka's `ProducerStateManager` keeps it.
//!
//! Per producer id and partition, so a retried batch is answered instead of written twice, and a
//! batch that would land out of order is refused.
//!
//! The rules are Kafka's (`ProducerAppendInfo.checkSequence` and `findDuplicateBatch`):
//!
//! - A batch that equals one of the last [`CACHED_BATCHES`] of its producer and epoch is a
//!   duplicate. It is answered with the base offset it landed at, and not written again.
//! - Otherwise its first sequence must follow the last one, wrapping from `i32::MAX` to 0, or it
//!   is `OUT_OF_ORDER_SEQUENCE_NUMBER` (45).
//! - An epoch below the producer's is `INVALID_PRODUCER_EPOCH` (47). A higher epoch starts over
//!   and must begin at sequence 0.
//! - A producer with no state here is accepted at any sequence, as Kafka accepts one whose state
//!   expired (KIP-360). After a gateway restart that is at-least-once, the same as before.
//!
//! The produce path writes the batches of one producer one at a time (its turn on its Iggy
//! session), so a check and the send that follows it never race another batch of that producer.
//!
//! A send that Iggy never answered leaves its batch [`Landing::InDoubt`]: the SDK keeps the
//! request, and a retry of the batch replays it ([`Verdict::Resolve`]) instead of writing it
//! again, so Iggy's own deduplication decides. A send whose request nothing keeps (the connection
//! was cut off at the slot limit, or the session was replaced) leaves it [`Landing::Unknown`]: a
//! retry of it is written again rather than refused, so delivery stays at-least-once in that
//! case only.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::bridge::InDoubt;

/// Batches kept per producer and partition. Kafka keeps five, one per request a producer may have
/// in flight (`max.in.flight.requests.per.connection` is at most 5 with idempotence).
pub const CACHED_BATCHES: usize = 5;

/// Producer-partition entries kept at most, before the least recently used is forgotten.
pub const DEFAULT_MAX_ENTRIES: usize = 100_000;

/// How long an idle entry is kept: Kafka's `producer.id.expiration.ms` default, one day.
pub const DEFAULT_EXPIRY: Duration = Duration::from_hours(24);

/// Where a batch's write stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Landing {
    /// Its send runs.
    Pending,
    /// It landed. `None` when Iggy named no offset.
    Landed(Option<u64>),
    /// The send ended without an answer, and the SDK keeps its request for a replay.
    InDoubt(InDoubt),
    /// The send ended without saying whether it landed, and nothing keeps its request.
    Unknown,
}

#[derive(Debug, Clone)]
struct Batch {
    first: i32,
    last: i32,
    landing: Landing,
}

#[derive(Debug)]
struct Entry {
    epoch: i16,
    batches: VecDeque<Batch>,
    used: Instant,
}

/// What to do with a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Write it. Call [`ProducerStates::begin`] before the send and [`ProducerStates::finish`]
    /// with its outcome.
    Append,
    /// A retry of a batch that landed. Answer its base offset; do not write it.
    Duplicate(Option<u64>),
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER` (45).
    OutOfOrder,
    /// `INVALID_PRODUCER_EPOCH` (47).
    InvalidEpoch,
    /// A retry of a batch whose send still runs. Answer retriable `REQUEST_TIMED_OUT` (7).
    InFlight,
    /// A retry of a batch whose send Iggy never answered. Replay that send
    /// (`IggyBridge::resolve_send`) and record what it proved with [`ProducerStates::resolved`].
    Resolve(InDoubt),
    /// The next batch after one whose send Iggy never answered. Replay that send first: this
    /// batch follows it if it landed, and is out of sequence (45) if it was refused.
    ResolveFirst(InDoubt),
}

/// One idempotent batch: its producer, epoch and sequence range.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchKey {
    pub producer_id: i64,
    pub topic: String,
    pub partition: i32,
}

/// Sequence `next` follows `last`, wrapping as Kafka's sequences do.
#[must_use]
pub const fn in_sequence(last: i32, next: i32) -> bool {
    (last == i32::MAX && next == 0) || (last != i32::MAX && next == last + 1)
}

/// The last sequence of a batch of `first` whose last offset delta is `delta`.
#[must_use]
pub const fn last_sequence(first: i32, delta: i32) -> i32 {
    // Kafka's `DefaultRecordBatch.incrementSequence`: wraps past `i32::MAX` to 0.
    if delta > i32::MAX - first {
        delta - (i32::MAX - first) - 1
    } else {
        first + delta
    }
}

/// Producer id, epoch, first and last sequence of an idempotent batch.
///
/// Read from bytes 43 to 50, 51 to 52, 53 to 56 and 23 to 26 of the v2 batch header. `None` for a
/// batch that is not idempotent (no producer id, or no sequence).
#[must_use]
pub fn idempotence(blob: &[u8]) -> Option<(i64, i16, i32, i32)> {
    let producer_id = i64::from_be_bytes(blob.get(43..51)?.try_into().ok()?);
    let epoch = i16::from_be_bytes(blob.get(51..53)?.try_into().ok()?);
    let first = i32::from_be_bytes(blob.get(53..57)?.try_into().ok()?);
    let delta = i32::from_be_bytes(blob.get(23..27)?.try_into().ok()?);
    (producer_id >= 0 && epoch >= 0 && first >= 0 && delta >= 0)
        .then(|| (producer_id, epoch, first, last_sequence(first, delta)))
}

/// Producer state for every producer id and partition this gateway writes.
#[derive(Debug)]
pub struct ProducerStates {
    entries: Mutex<HashMap<BatchKey, Entry>>,
    max_entries: usize,
    expiry: Duration,
}

impl Default for ProducerStates {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, DEFAULT_EXPIRY)
    }
}

impl ProducerStates {
    #[must_use]
    pub fn new(max_entries: usize, expiry: Duration) -> Self {
        Self {
            entries: Mutex::default(),
            max_entries: max_entries.max(1),
            expiry,
        }
    }

    /// The verdict on a batch of `key` at `epoch` with sequences `first..=last`.
    #[must_use]
    pub fn check(&self, key: &BatchKey, epoch: i16, first: i32, last: i32) -> Verdict {
        verdict(self.lock().get(key), epoch, first, last)
    }

    /// Records that a batch's send starts. A new epoch replaces the old state.
    pub fn begin(&self, key: &BatchKey, epoch: i16, first: i32, last: i32) {
        let (max_entries, expiry) = (self.max_entries, self.expiry);
        begin_in(
            &mut self.lock(),
            key,
            max_entries,
            expiry,
            (epoch, first, last),
        );
    }

    /// Records how a batch's send ended. A batch that did not land is forgotten, so its retry is
    /// checked against the batch before it.
    pub fn finish(&self, key: &BatchKey, epoch: i16, first: i32, landing: Option<Landing>) {
        finish_in(&mut self.lock(), key, epoch, first, landing);
    }

    /// Records what the replay of a batch in doubt proved: `Some(landing)` replaces its
    /// [`Landing::InDoubt`], and `None` (it was refused, so nothing was written) forgets it.
    ///
    /// Only the batch that `in_doubt` was for: a batch sent again since is left alone.
    pub fn resolved(&self, key: &BatchKey, in_doubt: &InDoubt, landing: Option<Landing>) {
        let mut entries = self.lock();
        let Some(entry) = entries.get_mut(key) else {
            return;
        };
        let Some(position) = entry
            .batches
            .iter()
            .position(|batch| matches!(&batch.landing, Landing::InDoubt(held) if held == in_doubt))
        else {
            return;
        };
        if let Some(landing) = landing {
            entry.batches[position].landing = landing;
            return;
        }
        entry.batches.remove(position);
        if entry.batches.is_empty() {
            entries.remove(key);
        }
    }

    /// Entries held, for tests and metrics.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<BatchKey, Entry>> {
        // The map stays consistent across a panic: every update is a single insert or field write.
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn verdict(entry: Option<&Entry>, epoch: i16, first: i32, last: i32) -> Verdict {
    let Some(entry) = entry else {
        return Verdict::Append;
    };
    if epoch < entry.epoch {
        return Verdict::InvalidEpoch;
    }
    if epoch > entry.epoch {
        return if first == 0 {
            Verdict::Append
        } else {
            Verdict::OutOfOrder
        };
    }
    if let Some(batch) = entry
        .batches
        .iter()
        .find(|batch| batch.first == first && batch.last == last)
    {
        return match &batch.landing {
            Landing::Landed(offset) => Verdict::Duplicate(*offset),
            Landing::Pending => Verdict::InFlight,
            Landing::InDoubt(in_doubt) => Verdict::Resolve(in_doubt.clone()),
            // Whether it landed is unknown: write it again rather than refuse a batch that may
            // not exist. At-least-once, as before producer state.
            Landing::Unknown => Verdict::Append,
        };
    }
    match entry.batches.back() {
        Some(newest) if !in_sequence(newest.last, first) => Verdict::OutOfOrder,
        // A send in doubt keeps its lane in Iggy's SDK, so it is always the newest batch.
        Some(Batch {
            landing: Landing::InDoubt(in_doubt),
            ..
        }) => Verdict::ResolveFirst(in_doubt.clone()),
        _ => Verdict::Append,
    }
}

fn begin_in(
    entries: &mut HashMap<BatchKey, Entry>,
    key: &BatchKey,
    max_entries: usize,
    expiry: Duration,
    (epoch, first, last): (i16, i32, i32),
) {
    let now = Instant::now();
    if !entries.contains_key(key) && entries.len() >= max_entries {
        evict(entries, now, expiry);
    }
    let entry = entries.entry(key.clone()).or_insert_with(|| Entry {
        epoch,
        batches: VecDeque::with_capacity(CACHED_BATCHES),
        used: now,
    });
    if epoch != entry.epoch {
        entry.epoch = epoch;
        entry.batches.clear();
    }
    entry.used = now;
    // A retry of a batch whose outcome was unknown replaces it.
    entry
        .batches
        .retain(|batch| !(batch.first == first && batch.last == last));
    if entry.batches.len() == CACHED_BATCHES {
        entry.batches.pop_front();
    }
    entry.batches.push_back(Batch {
        first,
        last,
        landing: Landing::Pending,
    });
}

fn finish_in(
    entries: &mut HashMap<BatchKey, Entry>,
    key: &BatchKey,
    epoch: i16,
    first: i32,
    landing: Option<Landing>,
) {
    let Some(entry) = entries.get_mut(key) else {
        return;
    };
    if entry.epoch != epoch {
        return;
    }
    if let Some(landing) = landing {
        if let Some(batch) = entry
            .batches
            .iter_mut()
            .rev()
            .find(|batch| batch.first == first)
        {
            batch.landing = landing;
        }
        return;
    }
    if let Some(position) = entry.batches.iter().rposition(|batch| batch.first == first) {
        entry.batches.remove(position);
    }
    if entry.batches.is_empty() {
        entries.remove(key);
    }
}

/// Drops expired entries, and the least recently used one if none had expired.
fn evict(entries: &mut HashMap<BatchKey, Entry>, now: Instant, expiry: Duration) {
    let before = entries.len();
    entries.retain(|_, entry| now.duration_since(entry.used) < expiry);
    if entries.len() < before {
        return;
    }
    if let Some(oldest) = entries
        .iter()
        .min_by_key(|(_, entry)| entry.used)
        .map(|(key, _)| key.clone())
    {
        entries.remove(&oldest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(partition: i32) -> BatchKey {
        BatchKey {
            producer_id: 7,
            topic: "orders".to_string(),
            partition,
        }
    }

    fn land(states: &ProducerStates, epoch: i16, first: i32, last: i32, offset: u64) {
        assert_eq!(states.check(&key(0), epoch, first, last), Verdict::Append);
        states.begin(&key(0), epoch, first, last);
        states.finish(&key(0), epoch, first, Some(Landing::Landed(Some(offset))));
    }

    #[test]
    fn given_an_unknown_producer_when_checked_should_accept_any_sequence() {
        let states = ProducerStates::default();
        assert_eq!(states.check(&key(0), 0, 42, 44), Verdict::Append);
    }

    #[test]
    fn given_a_landed_batch_when_retried_should_answer_its_offset() {
        let states = ProducerStates::default();
        land(&states, 0, 0, 2, 10);
        assert_eq!(states.check(&key(0), 0, 0, 2), Verdict::Duplicate(Some(10)));
        assert_eq!(
            states.check(&key(1), 0, 0, 2),
            Verdict::Append,
            "per partition"
        );
    }

    #[test]
    fn given_a_gap_when_checked_should_answer_out_of_order() {
        let states = ProducerStates::default();
        land(&states, 0, 0, 2, 10);
        assert_eq!(states.check(&key(0), 0, 4, 4), Verdict::OutOfOrder);
        assert_eq!(states.check(&key(0), 0, 3, 5), Verdict::Append);
    }

    #[test]
    fn given_an_older_epoch_when_checked_should_answer_invalid_epoch() {
        let states = ProducerStates::default();
        land(&states, 3, 0, 0, 1);
        assert_eq!(states.check(&key(0), 2, 1, 1), Verdict::InvalidEpoch);
    }

    #[test]
    fn given_a_new_epoch_when_checked_should_require_sequence_zero() {
        let states = ProducerStates::default();
        land(&states, 0, 0, 4, 1);
        assert_eq!(states.check(&key(0), 1, 5, 5), Verdict::OutOfOrder);
        assert_eq!(states.check(&key(0), 1, 0, 0), Verdict::Append);
        land(&states, 1, 0, 0, 6);
        assert_eq!(states.check(&key(0), 0, 5, 5), Verdict::InvalidEpoch);
    }

    #[test]
    fn given_six_batches_when_the_first_is_retried_should_no_longer_find_it() {
        let states = ProducerStates::default();
        for index in 0..6 {
            land(&states, 0, index, index, u64::try_from(index).unwrap());
        }
        assert_eq!(states.check(&key(0), 0, 5, 5), Verdict::Duplicate(Some(5)));
        assert_eq!(states.check(&key(0), 0, 1, 1), Verdict::Duplicate(Some(1)));
        assert_eq!(
            states.check(&key(0), 0, 0, 0),
            Verdict::OutOfOrder,
            "older than the cache, as Kafka answers"
        );
    }

    #[test]
    fn given_a_send_running_when_its_batch_is_retried_should_answer_in_flight() {
        let states = ProducerStates::default();
        states.begin(&key(0), 0, 0, 0);
        assert_eq!(states.check(&key(0), 0, 0, 0), Verdict::InFlight);
    }

    #[test]
    fn given_a_send_that_did_not_land_when_retried_should_accept_it() {
        let states = ProducerStates::default();
        land(&states, 0, 0, 0, 1);
        states.begin(&key(0), 0, 1, 1);
        states.finish(&key(0), 0, 1, None);
        assert_eq!(states.check(&key(0), 0, 1, 1), Verdict::Append);
        assert_eq!(
            states.check(&key(0), 0, 2, 2),
            Verdict::OutOfOrder,
            "the batch after it must wait for it"
        );
    }

    #[test]
    fn given_a_first_send_that_did_not_land_should_forget_the_producer() {
        let states = ProducerStates::default();
        states.begin(&key(0), 0, 0, 0);
        states.finish(&key(0), 0, 0, None);
        assert!(states.is_empty());
    }

    #[test]
    fn given_an_unknown_outcome_when_retried_should_write_it_again() {
        let states = ProducerStates::default();
        states.begin(&key(0), 0, 0, 1);
        states.finish(&key(0), 0, 0, Some(Landing::Unknown));
        assert_eq!(states.check(&key(0), 0, 0, 1), Verdict::Append);
        assert_eq!(states.check(&key(0), 0, 2, 2), Verdict::Append);
    }

    fn in_doubt(states: &ProducerStates, first: i32, last: i32, operation: u64) -> InDoubt {
        let in_doubt = InDoubt::detached(operation);
        states.begin(&key(0), 0, first, last);
        states.finish(&key(0), 0, first, Some(Landing::InDoubt(in_doubt.clone())));
        in_doubt
    }

    #[test]
    fn given_a_send_in_doubt_when_its_batch_is_retried_should_replay_it() {
        let states = ProducerStates::default();
        land(&states, 0, 0, 0, 1);
        let held = in_doubt(&states, 1, 2, 9);
        assert_eq!(states.check(&key(0), 0, 1, 2), Verdict::Resolve(held));
        assert_eq!(
            states.check(&key(0), 0, 3, 3),
            Verdict::ResolveFirst(InDoubt::detached(9)),
            "the batch after it waits for what the one in doubt did"
        );
    }

    #[test]
    fn given_a_replay_that_landed_when_retried_again_should_answer_a_duplicate() {
        let states = ProducerStates::default();
        let held = in_doubt(&states, 0, 1, 9);
        states.resolved(&key(0), &held, Some(Landing::Landed(None)));
        assert_eq!(states.check(&key(0), 0, 0, 1), Verdict::Duplicate(None));
    }

    #[test]
    fn given_a_replay_that_was_refused_should_accept_the_batch_again() {
        let states = ProducerStates::default();
        land(&states, 0, 0, 0, 1);
        let held = in_doubt(&states, 1, 1, 9);
        states.resolved(&key(0), &held, None);
        assert_eq!(states.check(&key(0), 0, 1, 1), Verdict::Append);
        assert_eq!(states.check(&key(0), 0, 2, 2), Verdict::OutOfOrder);
    }

    #[test]
    fn given_a_batch_sent_again_since_when_an_old_replay_ends_should_keep_the_new_send() {
        let states = ProducerStates::default();
        let held = in_doubt(&states, 0, 0, 9);
        states.begin(&key(0), 0, 0, 0);
        states.resolved(&key(0), &held, Some(Landing::Landed(Some(4))));
        assert_eq!(states.check(&key(0), 0, 0, 0), Verdict::InFlight);
    }

    #[test]
    fn given_sequences_at_the_limit_should_wrap_to_zero() {
        assert!(in_sequence(i32::MAX, 0));
        assert!(!in_sequence(i32::MAX, i32::MIN));
        assert_eq!(last_sequence(i32::MAX - 1, 3), 1);
        assert_eq!(last_sequence(5, 0), 5);
        let states = ProducerStates::default();
        land(&states, 0, i32::MAX - 1, i32::MAX, 1);
        assert_eq!(states.check(&key(0), 0, 0, 0), Verdict::Append);
    }

    #[test]
    fn given_a_full_table_when_a_producer_arrives_should_forget_the_least_recently_used() {
        let states = ProducerStates::new(2, DEFAULT_EXPIRY);
        for partition in 0..3 {
            states.begin(&key(partition), 0, 0, 0);
            states.finish(&key(partition), 0, 0, Some(Landing::Landed(Some(0))));
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(states.len(), 2);
        assert_eq!(states.check(&key(0), 0, 0, 0), Verdict::Append, "forgotten");
        assert_eq!(states.check(&key(2), 0, 0, 0), Verdict::Duplicate(Some(0)));
    }
}
