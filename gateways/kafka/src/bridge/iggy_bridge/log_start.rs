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

//! The `DeleteRecords` watermark of each partition (apache/iggy#4525).
//!
//! Iggy has no low watermark at offset precision that a client can set and read. So the gateway
//! keeps one per partition, as an offset of the Iggy consumer group [`LOG_START_GROUP`] on the
//! topic, in the external group kind that Kafka group offsets use (`OFFSET_STORAGE.md`): Iggy
//! replicates it, does not range-check it, does not hold retention back for it, and deletes it
//! with the topic.
//!
//! Fetch refuses an offset below the watermark with `OFFSET_OUT_OF_RANGE`, and `ListOffsets`
//! answers it for EARLIEST when retention kept records below it. Iggy keeps those records on disk
//! until retention removes their segment. See `docs/SCOPE.md`.
//!
//! Each probe stamps the watermarks onto its partitions ([`PartitionProbe::deleted_below`]),
//! reading them from Iggy at most every [`FRESH_FOR`] per topic. One call tells a topic without
//! the group, which is every topic that never saw a `DeleteRecords`. A write through this gateway
//! raises the copy here at once. A write through another gateway shows within [`FRESH_FOR`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use iggy::prelude::{Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, IggyClient};
use tokio::time::{Instant, timeout_at};
use tracing::debug;

use super::fetch::TopicProbe;
use super::group_offsets::{OffsetCalls, OffsetNames, commit_on, slot_of};
use super::{IggyBridge, REQUEST_TIMEOUT, TopicTarget};
use crate::bridge::error::BridgeError;

/// The Iggy consumer group whose offsets hold the `DeleteRecords` watermarks of a topic.
///
/// Not `kafka.cg.` prefixed, so no Kafka group lists it. An Iggy client sees it among the groups of
/// the topic.
pub const LOG_START_GROUP: &str = "kafka.logstart";

/// How long the watermarks read for a topic stand before a probe reads them again.
const FRESH_FOR: Duration = Duration::from_secs(5);

/// The watermarks read lately, per Kafka topic.
#[derive(Default)]
pub(super) struct LogStarts {
    known: Mutex<HashMap<String, Known>>,
    /// Held by each write, so two writes to one partition cannot lower its watermark.
    writes: tokio::sync::Mutex<()>,
}

/// One topic's watermarks, as last read or written.
#[derive(Debug, Clone)]
struct Known {
    /// The topic they belong to. See [`TopicProbe::created_at`].
    created_at: u64,
    read_at: Instant,
    /// By partition id. A partition left out has none.
    below: HashMap<u32, u64>,
}

impl LogStarts {
    /// The watermarks of `topic`, when read since `fresh_since` for the topic created at
    /// `created_at`.
    fn fresh(&self, topic: &str, created_at: u64, fresh_since: Instant) -> Option<Known> {
        let known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        known
            .get(topic)
            .filter(|known| known.created_at == created_at && known.read_at >= fresh_since)
            .cloned()
    }

    /// The watermarks of `topic` held for the topic created at `created_at`, whatever their age.
    fn any_age(&self, topic: &str, created_at: u64) -> HashMap<u32, u64> {
        let known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        known
            .get(topic)
            .filter(|known| known.created_at == created_at)
            .map(|known| known.below.clone())
            .unwrap_or_default()
    }

    /// Keeps what a read found. A watermark only rises, so a read that raced a write keeps the
    /// write's.
    fn store(&self, topic: &str, created_at: u64, read_at: Instant, mut below: HashMap<u32, u64>) {
        let mut known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(old) = known.get(topic)
            && old.created_at == created_at
        {
            for (&partition, &old) in &old.below {
                let entry = below.entry(partition).or_default();
                *entry = (*entry).max(old);
            }
        }
        known.insert(
            topic.to_owned(),
            Known {
                created_at,
                read_at,
                below,
            },
        );
    }

    /// Raises the watermark of `partition` of `topic` after a write, when a read of the topic is
    /// held. Otherwise the next probe reads it.
    fn raise(&self, topic: &str, partition: u32, offset: u64) {
        let mut known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(known) = known.get_mut(topic) {
            let entry = known.below.entry(partition).or_default();
            *entry = (*entry).max(offset);
        }
    }

    /// The watermark of `partition` of `topic` held here, whatever its age. 0 when none is.
    fn held(&self, topic: &str, partition: u32) -> u64 {
        let known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        known
            .get(topic)
            .and_then(|known| known.below.get(&partition).copied())
            .unwrap_or(0)
    }

    /// Forgets the topics past `max` entries, the oldest first, so client-named topics cannot
    /// grow the map without bound.
    fn trim(&self, max: usize) {
        let mut known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        if known.len() <= max {
            return;
        }
        let mut ages: Vec<(Instant, String)> = known
            .iter()
            .map(|(topic, known)| (known.read_at, topic.clone()))
            .collect();
        ages.sort_unstable();
        for (_, topic) in ages.into_iter().take(known.len() - max) {
            known.remove(&topic);
        }
    }
}

/// Topics whose watermarks the bridge holds at most. Past it, the oldest go.
const MAX_KNOWN_TOPICS: usize = 4096;

impl IggyBridge {
    /// Sets `PartitionProbe::deleted_below` on every partition of `probe`, from watermarks read
    /// within [`FRESH_FOR`], or read now on the probe client.
    ///
    /// A read that fails keeps the watermarks held for the topic, or none: Fetch then serves below
    /// a watermark rather than failing every read of the topic.
    pub(super) async fn stamp_deleted_below(
        &self,
        kafka_topic: &str,
        target: &TopicTarget,
        probe: &mut TopicProbe,
    ) {
        let now = Instant::now();
        let created_at = probe.created_at();
        let fresh_since = now.checked_sub(FRESH_FOR).unwrap_or(now);
        let below = if let Some(known) = self.log_starts.fresh(kafka_topic, created_at, fresh_since)
        {
            known.below
        } else {
            let partitions: Vec<u32> = probe.ids().collect();
            let read = |client: Arc<IggyClient>| async move {
                read_watermarks(&client, target, &partitions).await
            };
            match self
                .probe_client
                .until(&self.config, now + REQUEST_TIMEOUT, read)
                .await
            {
                Ok(below) => {
                    self.log_starts.store(kafka_topic, created_at, now, below);
                    self.log_starts.trim(MAX_KNOWN_TOPICS);
                    self.log_starts
                        .fresh(kafka_topic, created_at, now)
                        .map(|known| known.below)
                        .unwrap_or_default()
                }
                Err(error) => {
                    debug!(
                        kafka_topic,
                        %error,
                        "could not read the DeleteRecords watermarks; keeping the ones held"
                    );
                    self.log_starts.any_age(kafka_topic, created_at)
                }
            }
        };
        probe.set_deleted_below(|partition| below.get(&partition).copied().unwrap_or(0));
    }

    /// The `DeleteRecords` watermark of `partition` of `kafka_topic` this gateway holds, read or
    /// written. 0 when it holds none. Fetch checks it on top of its probe, which can predate a
    /// write.
    #[must_use]
    pub fn deleted_below(&self, kafka_topic: &str, partition: u32) -> u64 {
        self.log_starts.held(kafka_topic, partition)
    }

    /// Raises the `DeleteRecords` watermark of `partition` of `kafka_topic` to `offset`, and
    /// returns the watermark it then has. A lower `offset` leaves it as it is, as Kafka does. The
    /// caller checks `offset` against the high watermark.
    ///
    /// Writes run one at a time on this gateway, each reading the stored watermark first, so none
    /// lowers it. The store runs on the commit lane of the partition's offset slot, and creates
    /// the Iggy group [`LOG_START_GROUP`] on its first write to a topic.
    ///
    /// # Errors
    ///
    /// [`BridgeError::Iggy`] if Iggy refuses the read or the store, [`BridgeError::Timeout`] past
    /// the deadline of `calls`. A store that timed out can still land.
    pub async fn delete_records_below(
        &self,
        kafka_topic: &str,
        partition: u32,
        offset: u64,
        calls: &OffsetCalls,
    ) -> Result<u64, BridgeError> {
        let target = self.topic_target(kafka_topic)?;
        let _write = timeout_at(calls.deadline(), self.log_starts.writes.lock())
            .await
            .map_err(|_elapsed| BridgeError::Timeout)?;
        let slot = slot_of(&target, partition);
        let read_target = target.clone();
        let read = move |client: Arc<IggyClient>| async move {
            read_watermarks(&client, &read_target, &[partition]).await
        };
        let stored = self
            .read_in_slot(slot, calls, read)
            .await?
            .get(&partition)
            .copied()
            .unwrap_or(0);
        if offset <= stored {
            self.log_starts.raise(kafka_topic, partition, stored);
            return Ok(stored);
        }
        let names =
            Arc::new(OffsetNames::exact(LOG_START_GROUP.to_owned()).map_err(BridgeError::Iggy)?);
        let stored_offset = i64::try_from(offset).unwrap_or(i64::MAX);
        let commit = move |client: Arc<IggyClient>| async move {
            let created = |id| debug!(id, "created the Iggy group of DeleteRecords watermarks");
            commit_on(&client, &names, &target, partition, stored_offset, created).await
        };
        self.commit_in_slot(slot, calls, commit, ()).await?;
        self.log_starts.raise(kafka_topic, partition, offset);
        Ok(offset)
    }
}

/// The stored watermark of each of `partitions` of `target`, read on `client`. Empty when the
/// topic has no group [`LOG_START_GROUP`], which costs one call.
async fn read_watermarks(
    client: &IggyClient,
    target: &TopicTarget,
    partitions: &[u32],
) -> Result<HashMap<u32, u64>, BridgeError> {
    let group = Identifier::named(LOG_START_GROUP).map_err(BridgeError::Iggy)?;
    let (stream_id, topic_id) = (&target.stream_id, &target.topic_id);
    let found = client
        .get_consumer_group(stream_id, topic_id, &group)
        .await
        .map_err(BridgeError::Iggy)?;
    let mut below = HashMap::new();
    if found.is_none() {
        return Ok(below);
    }
    let consumer = Consumer::external_group(group);
    for &partition in partitions {
        let stored = client
            .get_consumer_offset(&consumer, stream_id, topic_id, Some(partition))
            .await
            .map_err(BridgeError::Iggy)?;
        if let Some(stored) = stored {
            below.insert(partition, stored.stored_offset);
        }
    }
    Ok(below)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "orders";

    fn below(pairs: &[(u32, u64)]) -> HashMap<u32, u64> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn given_a_read_for_the_same_topic_the_watermarks_should_stand_until_they_age() {
        let starts = LogStarts::default();
        let now = Instant::now();
        starts.store(TOPIC, 7, now, below(&[(0, 5)]));
        let fresh = starts.fresh(TOPIC, 7, now).expect("just read");
        assert_eq!(fresh.below.get(&0), Some(&5));
        let later = now + FRESH_FOR;
        assert!(starts.fresh(TOPIC, 7, later).is_none(), "aged out");
    }

    #[test]
    fn given_a_topic_created_again_its_old_watermarks_should_not_count() {
        let starts = LogStarts::default();
        let now = Instant::now();
        starts.store(TOPIC, 7, now, below(&[(0, 5)]));
        assert!(starts.fresh(TOPIC, 8, now).is_none());
        starts.store(TOPIC, 8, now, below(&[]));
        let fresh = starts.fresh(TOPIC, 8, now).expect("read for the new topic");
        assert!(fresh.below.is_empty(), "the new topic starts with none");
    }

    #[test]
    fn given_a_read_that_raced_a_write_the_write_should_win() {
        let starts = LogStarts::default();
        let now = Instant::now();
        starts.store(TOPIC, 7, now, below(&[(0, 5)]));
        starts.raise(TOPIC, 0, 9);
        // A read that started before the write lands after it.
        starts.store(TOPIC, 7, now, below(&[(0, 5), (1, 2)]));
        assert_eq!(starts.held(TOPIC, 0), 9);
        assert_eq!(starts.held(TOPIC, 1), 2);
    }

    #[test]
    fn given_no_read_of_a_topic_a_raise_should_wait_for_the_next_probe() {
        let starts = LogStarts::default();
        starts.raise(TOPIC, 0, 9);
        assert_eq!(starts.held(TOPIC, 0), 0);
    }

    #[test]
    fn given_more_topics_than_the_cap_the_oldest_should_go() {
        let starts = LogStarts::default();
        let now = Instant::now();
        starts.store("old", 1, now, below(&[(0, 1)]));
        starts.store("new", 1, now + Duration::from_secs(1), below(&[(0, 2)]));
        starts.trim(1);
        assert_eq!(starts.held("old", 0), 0);
        assert_eq!(starts.held("new", 0), 2);
    }
}
