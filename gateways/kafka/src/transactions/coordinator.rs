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

//! The transaction coordinator: Kafka's `TransactionCoordinator` and `TransactionStateManager`
//! over the transaction log ([`super::log`]) and the data partitions ([`DataPlane`]).
//!
//! Every transition is written to the log before it becomes current, one at a time per
//! transactional id. `EndTxn` answers once its `Prepare*` record is durable; the markers are then
//! written to every partition of the transaction by a task of their own, and a `Complete*` record
//! follows them. A gateway that stops in between finds the `Prepare*` record on start and writes
//! the markers again: a second marker of the same outcome is harmless to every Kafka consumer.
//!
//! A marker is sent on the producer's own Iggy session, after the producer's turn there
//! (`IggyBridge::produce_turn`): every batch of the transaction that a Produce request still holds
//! lands before it, and a send that Iggy never answered is replayed ahead of it by the SDK's lane.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::index::TxnIndex;
use super::log::{EndedRange, LOG_PARTITIONS, LogRecord, TxnLog, partition_for, replay};
use super::metadata::{
    InitTransit, Outcome, PartitionStart, TopicPartition, TxnMetadata, TxnState, codes,
};
use crate::bridge::BridgeError;
use crate::records::ControlType;

/// Kafka's `transaction.max.timeout.ms` default: 15 minutes.
pub const DEFAULT_MAX_TIMEOUT: Duration = Duration::from_mins(15);

/// Kafka's `transaction.abort.timed.out.transaction.cleanup.interval.ms` default: 10 seconds.
pub const DEFAULT_ABORT_INTERVAL: Duration = Duration::from_secs(10);

/// How long one marker write may take before it is tried again.
const MARKER_ATTEMPT: Duration = Duration::from_secs(30);

/// The pause between attempts at a marker or a log write that failed.
const RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// The pause between attempts to load the log at start.
const LOAD_BACKOFF: Duration = Duration::from_secs(2);

/// The coordinator's settings.
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Kafka's `transaction.max.timeout.ms`: an `InitProducerId` asking for more is refused with
    /// `INVALID_TRANSACTION_TIMEOUT`.
    pub max_timeout: Duration,
    /// How often transactions past their timeout are aborted.
    pub abort_interval: Duration,
    /// The coordinator epoch every marker carries. 0 until several gateways share the log
    /// (KW-5), when it becomes the owner's lease epoch.
    pub coordinator_epoch: i32,
    /// Kafka's `transactional.id.expiration.ms`: an id with no transaction open and no change for
    /// this long is forgotten (a `Dead` record), with its producer id.
    pub id_expiration: Duration,
}

/// Kafka's `transactional.id.expiration.ms` default: 7 days.
pub const DEFAULT_ID_EXPIRATION: Duration = Duration::from_hours(24 * 7);

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            max_timeout: DEFAULT_MAX_TIMEOUT,
            abort_interval: DEFAULT_ABORT_INTERVAL,
            coordinator_epoch: 0,
            id_expiration: DEFAULT_ID_EXPIRATION,
        }
    }
}

/// One marker to write: the control record that ends `producer_id`'s transaction on a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    pub topic: String,
    pub partition: i32,
    pub producer_id: i64,
    /// The user the producer's Produce requests ran as, which keys its Iggy session.
    pub user: Option<String>,
    pub epoch: i16,
    pub control: ControlType,
    pub coordinator_epoch: i32,
}

/// Where a marker landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Landed {
    /// At this offset.
    At(u64),
    /// Below this high watermark, read after it landed: Iggy did not name its offset.
    Below(u64),
}

/// The data partitions, as the coordinator uses them: the bridge in production, memory in tests.
#[async_trait]
pub trait DataPlane: Send + Sync + 'static {
    /// One past the last committed offset of `partition` of `topic`.
    ///
    /// # Errors
    ///
    /// The bridge's error; `UNKNOWN_TOPIC_OR_PARTITION` for a partition that does not exist.
    async fn high_watermark(&self, topic: &str, partition: i32) -> Result<u64, BridgeError>;

    /// Writes `marker` after every earlier send of its producer.
    ///
    /// # Errors
    ///
    /// The bridge's error. The marker may still land; it is written again.
    async fn write_marker(&self, marker: &Marker) -> Result<Landed, BridgeError>;

    /// Stores `offsets` as `group`'s committed offsets: what a committed transaction's
    /// `TxnOffsetCommit` stored becomes visible. Writing the same offsets again changes nothing.
    ///
    /// # Errors
    ///
    /// The bridge's error for any partition; the whole set is written again.
    async fn commit_offsets(
        &self,
        group: &str,
        offsets: &BTreeMap<TopicPartition, i64>,
    ) -> Result<(), BridgeError>;
}

/// Hands out a producer id for a new transactional id, or `None` once the space is spent.
pub type AllocateProducerId = Arc<dyn Fn() -> Option<i64> + Send + Sync>;

/// One transactional id: its current metadata, and the lock its transitions take in turn.
struct Entry {
    /// Held across a transition and its log write.
    transition: AsyncMutex<()>,
    metadata: Mutex<TxnMetadata>,
    /// The user of the producer's last `AddPartitionsToTxn`, which keys its Iggy session.
    user: Mutex<Option<String>>,
}

impl Entry {
    fn new(metadata: TxnMetadata) -> Arc<Self> {
        Arc::new(Self {
            transition: AsyncMutex::new(()),
            metadata: Mutex::new(metadata),
            user: Mutex::new(None),
        })
    }

    fn metadata(&self) -> TxnMetadata {
        self.metadata
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// What an `AddPartitionsToTxn` answers, per partition when one of them is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddError {
    /// Every partition gets this code.
    All(i16),
    /// The codes of the partitions that do not exist (or cannot be read); every other partition
    /// gets `OPERATION_NOT_ATTEMPTED`, as Kafka answers.
    Partitions(BTreeMap<TopicPartition, i16>),
}

/// A transaction as `DescribeTransactions` and `ListTransactions` show it.
pub type TxnView = TxnMetadata;

/// The transaction coordinator of this gateway.
pub struct TransactionCoordinator {
    config: TransactionConfig,
    log: Arc<dyn TxnLog>,
    data: Arc<dyn DataPlane>,
    allocate: AllocateProducerId,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    by_producer: Mutex<HashMap<i64, String>>,
    index: Arc<TxnIndex>,
    loaded: AtomicBool,
    cancel: CancellationToken,
    clock: fn() -> i64,
}

impl std::fmt::Debug for TransactionCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionCoordinator")
            .field("loaded", &self.loaded.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn wall_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

impl TransactionCoordinator {
    #[must_use]
    pub fn new(
        config: TransactionConfig,
        log: Arc<dyn TxnLog>,
        data: Arc<dyn DataPlane>,
        allocate: AllocateProducerId,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            log,
            data,
            allocate,
            entries: Mutex::default(),
            by_producer: Mutex::default(),
            index: Arc::default(),
            loaded: AtomicBool::new(false),
            cancel,
            clock: wall_clock_ms,
        })
    }

    /// The same coordinator with another clock, for tests of timeouts.
    #[doc(hidden)]
    #[must_use]
    pub fn with_clock(
        config: TransactionConfig,
        log: Arc<dyn TxnLog>,
        data: Arc<dyn DataPlane>,
        allocate: AllocateProducerId,
        clock: fn() -> i64,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            log,
            data,
            allocate,
            entries: Mutex::default(),
            by_producer: Mutex::default(),
            index: Arc::default(),
            loaded: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            clock,
        })
    }

    /// The per-partition view Fetch and Produce read.
    #[must_use]
    pub const fn index(&self) -> &Arc<TxnIndex> {
        &self.index
    }

    #[must_use]
    pub const fn config(&self) -> &TransactionConfig {
        &self.config
    }

    /// Whether the log has been replayed, so the coordinator answers.
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        self.loaded.load(Ordering::Acquire)
    }

    fn now(&self) -> i64 {
        (self.clock)()
    }

    fn entry(&self, transactional_id: &str) -> Option<Arc<Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(transactional_id)
            .cloned()
    }

    /// Whether `producer_id` is a transactional producer this coordinator knows.
    #[must_use]
    pub fn knows_producer(&self, producer_id: i64) -> bool {
        self.by_producer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&producer_id)
    }

    /// Replays the log, then resumes every transaction whose markers were not all written. Retries
    /// until it succeeds or the coordinator is cancelled; the coordinator answers
    /// `COORDINATOR_LOAD_IN_PROGRESS` meanwhile.
    pub async fn load(self: &Arc<Self>) {
        loop {
            match self.log.read_all().await {
                Ok(records) => {
                    self.load_records(records);
                    return;
                }
                Err(error) => {
                    warn!(%error, "could not read the Kafka transaction log yet; retrying");
                }
            }
            tokio::select! {
                () = self.cancel.cancelled() => return,
                () = tokio::time::sleep(LOAD_BACKOFF) => {}
            }
        }
    }

    fn load_records(self: &Arc<Self>, records: Vec<LogRecord>) {
        let count = records.len();
        let replayed = replay(records);
        for ended in &replayed.ended {
            self.index.end(ended, false);
        }
        for aborted in &replayed.aborted {
            self.index.end(aborted, true);
        }
        let mut resume = Vec::new();
        let mut producers = Vec::with_capacity(replayed.metadata.len());
        let mut loaded = Vec::with_capacity(replayed.metadata.len());
        for (transactional_id, metadata) in replayed.metadata {
            // An expired id is forgotten; its last record is the `Dead` one.
            if metadata.state == TxnState::Dead {
                continue;
            }
            if metadata.state.is_open() {
                for (partition, start) in &metadata.partitions {
                    self.index.open(
                        partition,
                        metadata.producer_id,
                        start.start.unwrap_or(0),
                        true,
                    );
                }
            }
            if metadata.state.is_preparing() {
                resume.push(metadata.clone());
            }
            producers.push((metadata.producer_id, transactional_id.clone()));
            loaded.push((transactional_id, Entry::new(metadata)));
        }
        self.by_producer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(producers);
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(loaded);
        self.loaded.store(true, Ordering::Release);
        info!(
            records = count,
            transactional_ids = self
                .entries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len(),
            resumed = resume.len(),
            "Kafka transaction log loaded"
        );
        for prepared in resume {
            self.spawn_completion(prepared);
        }
    }

    /// Aborts the transactions past their timeout every [`TransactionConfig::abort_interval`],
    /// until cancelled.
    pub async fn abort_timed_out_forever(self: &Arc<Self>) {
        let mut tick = tokio::time::interval(self.config.abort_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => return,
                _ = tick.tick() => {
                    self.abort_timed_out().await;
                    self.expire_idle().await;
                }
            }
        }
    }

    /// Fences and aborts every transaction past its timeout, as Kafka's
    /// `abortTimedOutTransactions` does: the producer's next call is refused at the old epoch.
    pub async fn abort_timed_out(self: &Arc<Self>) {
        if !self.is_loaded() {
            return;
        }
        let now = self.now();
        let candidates: Vec<(String, Arc<Entry>)> = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, entry)| entry.metadata().timed_out(now))
            .map(|(id, entry)| (id.clone(), Arc::clone(entry)))
            .collect();
        for (transactional_id, entry) in candidates {
            let _turn = entry.transition.lock().await;
            let current = entry.metadata();
            if !current.timed_out(self.now()) {
                continue;
            }
            let Ok(fenced) = current.fence(self.now()) else {
                continue;
            };
            match self.append(&fenced, Vec::new()).await {
                Ok(()) => {
                    info!(
                        transactional_id,
                        producer_id = fenced.producer_id,
                        epoch = fenced.producer_epoch,
                        "aborting a transaction past its timeout"
                    );
                    self.publish(&entry, fenced.clone());
                    self.spawn_completion(fenced);
                }
                Err(error) => {
                    debug!(%error, transactional_id, "could not abort a timed-out transaction yet");
                }
            }
        }
    }

    /// Forgets every transactional id idle past [`TransactionConfig::id_expiration`], as Kafka's
    /// `removeExpiredTransactionalIds` does: no transaction open, no change for that long. A
    /// `Dead` record goes to the log first; a later `InitProducerId` starts the id over with a new
    /// producer id.
    pub async fn expire_idle(self: &Arc<Self>) {
        if !self.is_loaded() {
            return;
        }
        let expiry = i64::try_from(self.config.id_expiration.as_millis()).unwrap_or(i64::MAX);
        let idle = |metadata: &TxnMetadata, now: i64| {
            matches!(
                metadata.state,
                TxnState::Empty | TxnState::CompleteCommit | TxnState::CompleteAbort
            ) && metadata.update_ms.saturating_add(expiry) < now
        };
        let now = self.now();
        let candidates: Vec<(String, Arc<Entry>)> = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(_, entry)| idle(&entry.metadata(), now))
            .map(|(id, entry)| (id.clone(), Arc::clone(entry)))
            .collect();
        for (transactional_id, entry) in candidates {
            let _turn = entry.transition.lock().await;
            let current = entry.metadata();
            if !idle(&current, self.now()) {
                continue;
            }
            let dead = TxnMetadata {
                state: TxnState::Dead,
                update_ms: self.now(),
                ..current.clone()
            };
            if let Err(error) = self.append(&dead, Vec::new()).await {
                debug!(%error, transactional_id, "could not expire a transactional id yet");
                continue;
            }
            self.entries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&transactional_id);
            self.by_producer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&current.producer_id);
            info!(transactional_id, "expired an idle transactional id");
        }
    }

    /// `WriteTxnMarkers` from an operator (`kafka-transactions.sh abort`, KIP-664): writes the
    /// marker of `producer_id` at `epoch` to `partition` and records its range, so a transaction
    /// left hanging on a partition (its records with no marker) is ended. The range starts after
    /// the producer's last marker there, or at its open transaction's first offset when this
    /// coordinator knows one.
    ///
    /// # Errors
    ///
    /// `UNKNOWN_TOPIC_OR_PARTITION`, or `NOT_ENOUGH_REPLICAS` (retriable) when Iggy cannot take
    /// the marker or the log record now.
    pub async fn write_admin_marker(
        &self,
        producer_id: i64,
        epoch: i16,
        coordinator_epoch: i32,
        commit: bool,
        partition: &TopicPartition,
    ) -> Result<(), i16> {
        let control = if commit {
            ControlType::Commit
        } else {
            ControlType::Abort
        };
        let marker = Marker {
            topic: partition.0.clone(),
            partition: partition.1,
            producer_id,
            user: None,
            epoch,
            control,
            coordinator_epoch,
        };
        let refusal = |error: BridgeError| match error.to_kafka_error_code() {
            codes::UNKNOWN_TOPIC_OR_PARTITION => codes::UNKNOWN_TOPIC_OR_PARTITION,
            _ => codes::NOT_ENOUGH_REPLICAS,
        };
        let landed = tokio::time::timeout(MARKER_ATTEMPT, self.data.write_marker(&marker))
            .await
            .map_err(|_| codes::NOT_ENOUGH_REPLICAS)?
            .map_err(refusal)?;
        let (last, exact) = match landed {
            Landed::At(offset) => (offset, true),
            Landed::Below(watermark) => (watermark, false),
        };
        let first = self
            .index
            .first_of(partition, producer_id)
            .or_else(|| self.index.after_marker(partition, producer_id))
            .unwrap_or(0)
            .min(last);
        let range = EndedRange {
            topic: partition.0.clone(),
            partition: partition.1,
            producer_id,
            first,
            last,
            exact,
        };
        let record = LogRecord::admin(producer_id, epoch, !commit, vec![range.clone()], self.now());
        let log_partition =
            u32::try_from(producer_id.rem_euclid(i64::from(LOG_PARTITIONS))).unwrap_or(0);
        self.log
            .append(log_partition, &record)
            .await
            .map_err(|_| codes::NOT_ENOUGH_REPLICAS)?;
        self.index.end(&range, !commit);
        info!(
            producer_id,
            epoch,
            topic = partition.0,
            partition = partition.1,
            ?control,
            "wrote an operator's transaction marker"
        );
        Ok(())
    }

    /// Writes `metadata` to the log.
    async fn append(
        &self,
        metadata: &TxnMetadata,
        ended: Vec<EndedRange>,
    ) -> Result<(), BridgeError> {
        let partition = partition_for(&metadata.transactional_id, LOG_PARTITIONS);
        self.log
            .append(partition, &LogRecord::of(metadata, ended))
            .await
    }

    /// Makes `metadata` current, after its log write.
    fn publish(&self, entry: &Entry, metadata: TxnMetadata) {
        let (producer_id, transactional_id) =
            (metadata.producer_id, metadata.transactional_id.clone());
        let previous = {
            let mut current = entry
                .metadata
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            std::mem::replace(&mut *current, metadata)
        };
        let mut by_producer = self
            .by_producer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if previous.producer_id != producer_id {
            by_producer.remove(&previous.producer_id);
        }
        by_producer.insert(producer_id, transactional_id);
    }

    /// `InitProducerId` with a transactional id: the producer id and epoch to answer.
    ///
    /// # Errors
    ///
    /// The Kafka error to answer, `PRODUCER_FENCED` included (the handler maps it for old
    /// versions).
    pub async fn init_producer_id(
        self: &Arc<Self>,
        transactional_id: &str,
        timeout_ms: i32,
        expected: Option<(i64, i16)>,
    ) -> Result<(i64, i16), i16> {
        if !self.is_loaded() {
            return Err(codes::COORDINATOR_LOAD_IN_PROGRESS);
        }
        let max = i64::try_from(self.config.max_timeout.as_millis()).unwrap_or(i64::MAX);
        if timeout_ms <= 0 || i64::from(timeout_ms) > max {
            return Err(codes::INVALID_TRANSACTION_TIMEOUT);
        }
        let entry = self.entry_or_new(transactional_id, timeout_ms)?;
        let _turn = entry.transition.lock().await;
        let current = entry.metadata();
        let now = self.now();
        let transit =
            current.init_producer_id(timeout_ms, expected, now, || self.new_producer_id())?;
        match transit {
            InitTransit::Ready(next) => {
                self.append(&next, Vec::new())
                    .await
                    .map_err(|error| log_failed(&error, transactional_id))?;
                let answer = (next.producer_id, next.producer_epoch);
                self.publish(&entry, next);
                Ok(answer)
            }
            InitTransit::FenceAndAbort(abort) => {
                self.append(&abort, Vec::new())
                    .await
                    .map_err(|error| log_failed(&error, transactional_id))?;
                info!(
                    transactional_id,
                    epoch = abort.producer_epoch,
                    "a new producer fenced the open transaction; aborting it"
                );
                self.publish(&entry, abort.clone());
                self.spawn_completion(abort);
                Err(codes::CONCURRENT_TRANSACTIONS)
            }
        }
    }

    /// A producer id no transactional id holds.
    fn new_producer_id(&self) -> Option<i64> {
        // The allocator's ids are unique per run; an id from the log of an earlier run is skipped.
        (0..16).find_map(|_| (self.allocate)().filter(|id| !self.knows_producer(*id)))
    }

    fn entry_or_new(&self, transactional_id: &str, timeout_ms: i32) -> Result<Arc<Entry>, i16> {
        if let Some(entry) = self.entry(transactional_id) {
            return Ok(entry);
        }
        let producer_id = self.new_producer_id().ok_or_else(|| {
            tracing::error!(
                "producer id space exhausted; restart the gateway with a free IGGY_KAFKA_INSTANCE_ID"
            );
            codes::UNKNOWN_SERVER_ERROR
        })?;
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(Arc::clone(
            entries
                .entry(transactional_id.to_string())
                .or_insert_with(|| {
                    Entry::new(TxnMetadata::new(
                        transactional_id,
                        producer_id,
                        timeout_ms,
                        (self.clock)(),
                    ))
                }),
        ))
    }

    /// `AddPartitionsToTxn`: adds `partitions` to the producer's transaction, opening it.
    ///
    /// # Errors
    ///
    /// The Kafka error for every partition, or per partition when some do not exist.
    pub async fn add_partitions(
        self: &Arc<Self>,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        partitions: &[TopicPartition],
        user: Option<&str>,
    ) -> Result<(), AddError> {
        self.add(
            transactional_id,
            producer_id,
            epoch,
            partitions,
            &BTreeSet::new(),
            user,
        )
        .await
    }

    /// `AddOffsetsToTxn`: adds `group`'s offsets to the producer's transaction.
    ///
    /// # Errors
    ///
    /// The Kafka error to answer.
    pub async fn add_offsets(
        self: &Arc<Self>,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        group: &str,
    ) -> Result<(), i16> {
        let groups = BTreeSet::from([group.to_string()]);
        self.add(transactional_id, producer_id, epoch, &[], &groups, None)
            .await
            .map_err(|error| match error {
                AddError::All(code) => code,
                AddError::Partitions(_) => codes::UNKNOWN_SERVER_ERROR,
            })
    }

    async fn add(
        self: &Arc<Self>,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        partitions: &[TopicPartition],
        groups: &BTreeSet<String>,
        user: Option<&str>,
    ) -> Result<(), AddError> {
        if !self.is_loaded() {
            return Err(AddError::All(codes::COORDINATOR_LOAD_IN_PROGRESS));
        }
        let entry = self
            .entry(transactional_id)
            .ok_or(AddError::All(codes::INVALID_PRODUCER_ID_MAPPING))?;
        let _turn = entry.transition.lock().await;
        let current = entry.metadata();
        // Refuse before reading any partition: a fenced producer costs no Iggy call.
        let probe = current
            .add(
                producer_id,
                epoch,
                &BTreeMap::new(),
                &BTreeSet::new(),
                self.now(),
            )
            .map_err(AddError::All)?;
        let reopening = !matches!(current.state, TxnState::Ongoing);
        let mut starts = BTreeMap::new();
        let mut refused = BTreeMap::new();
        for partition in partitions {
            if !reopening && current.partitions.contains_key(partition) {
                continue;
            }
            match self.start_of(partition, producer_id).await {
                Ok(start) => {
                    starts.insert(partition.clone(), PartitionStart { start: Some(start) });
                }
                Err(code) => {
                    refused.insert(partition.clone(), code);
                }
            }
        }
        if !refused.is_empty() {
            return Err(AddError::Partitions(refused));
        }
        drop(probe);
        let Some(next) = current
            .add(producer_id, epoch, &starts, groups, self.now())
            .map_err(AddError::All)?
        else {
            Self::remember_user(&entry, user);
            return Ok(());
        };
        self.append(&next, Vec::new())
            .await
            .map_err(|error| AddError::All(log_failed(&error, transactional_id)))?;
        for (partition, start) in &starts {
            self.index
                .open(partition, producer_id, start.start.unwrap_or(0), false);
        }
        Self::remember_user(&entry, user);
        self.publish(&entry, next);
        Ok(())
    }

    fn remember_user(entry: &Entry, user: Option<&str>) {
        if let Some(user) = user {
            *entry.user.lock().unwrap_or_else(PoisonError::into_inner) = Some(user.to_string());
        }
    }

    /// The first offset a new transaction of `producer_id` may write on `partition`: the
    /// partition's high watermark, and never at or below the producer's last marker there.
    async fn start_of(&self, partition: &TopicPartition, producer_id: i64) -> Result<u64, i16> {
        let watermark = self
            .data
            .high_watermark(&partition.0, partition.1)
            .await
            .map_err(|error| match error.to_kafka_error_code() {
                codes::UNKNOWN_TOPIC_OR_PARTITION => codes::UNKNOWN_TOPIC_OR_PARTITION,
                _ => codes::COORDINATOR_NOT_AVAILABLE,
            })?;
        Ok(self
            .index
            .after_marker(partition, producer_id)
            .map_or(watermark, |after| after.max(watermark)))
    }

    /// `EndTxn`: commits or aborts the producer's transaction. Answers once the decision is in the
    /// log; the markers follow.
    ///
    /// # Errors
    ///
    /// The Kafka error to answer.
    pub async fn end_txn(
        self: &Arc<Self>,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        outcome: Outcome,
    ) -> Result<(), i16> {
        if !self.is_loaded() {
            return Err(codes::COORDINATOR_LOAD_IN_PROGRESS);
        }
        let entry = self
            .entry(transactional_id)
            .ok_or(codes::INVALID_PRODUCER_ID_MAPPING)?;
        let _turn = entry.transition.lock().await;
        let current = entry.metadata();
        let Some(prepared) = current.end(producer_id, epoch, outcome, self.now())? else {
            return Ok(());
        };
        self.append(&prepared, Vec::new())
            .await
            .map_err(|error| log_failed(&error, transactional_id))?;
        self.publish(&entry, prepared.clone());
        self.spawn_completion(prepared);
        Ok(())
    }

    /// `TxnOffsetCommit`: stores `offsets` of `group` in the producer's transaction.
    ///
    /// # Errors
    ///
    /// The Kafka error to answer for every partition.
    pub async fn txn_offset_commit(
        self: &Arc<Self>,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        group: &str,
        offsets: &BTreeMap<TopicPartition, i64>,
    ) -> Result<(), i16> {
        if !self.is_loaded() {
            return Err(codes::COORDINATOR_LOAD_IN_PROGRESS);
        }
        let entry = self
            .entry(transactional_id)
            .ok_or(codes::INVALID_PRODUCER_ID_MAPPING)?;
        let _turn = entry.transition.lock().await;
        let next =
            entry
                .metadata()
                .commit_offsets(producer_id, epoch, group, offsets, self.now())?;
        self.append(&next, Vec::new())
            .await
            .map_err(|error| log_failed(&error, transactional_id))?;
        self.publish(&entry, next);
        Ok(())
    }

    /// Whether an open transaction holds an offset of `group` for `partition` that it has not
    /// committed yet: `OffsetFetch` with `require_stable` then answers `UNSTABLE_OFFSET_COMMIT`
    /// (KIP-447).
    #[must_use]
    pub fn has_pending_offset(&self, group: &str, partition: &TopicPartition) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .any(|entry| {
                let metadata = entry
                    .metadata
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                metadata.state.is_open()
                    && metadata
                        .offsets
                        .get(group)
                        .is_some_and(|offsets| offsets.contains_key(partition))
            })
    }

    /// Checks a transactional Produce batch against its transaction, before it is written.
    ///
    /// # Errors
    ///
    /// The coordinator's code; Produce maps it as Kafka's partition leader does.
    pub fn verify_write(
        &self,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        partition: &TopicPartition,
    ) -> Result<(), i16> {
        if !self.is_loaded() {
            return Err(codes::COORDINATOR_LOAD_IN_PROGRESS);
        }
        let entry = self
            .entry(transactional_id)
            .ok_or(codes::INVALID_PRODUCER_ID_MAPPING)?;
        let current = entry.metadata();
        current.verify_write(producer_id, epoch, partition)?;
        self.index.writing(partition, producer_id);
        Ok(())
    }

    /// The transaction of `transactional_id`, as `DescribeTransactions` shows it.
    #[must_use]
    pub fn describe(&self, transactional_id: &str) -> Option<TxnView> {
        self.entry(transactional_id).map(|entry| entry.metadata())
    }

    /// Every known transaction.
    #[must_use]
    pub fn list(&self) -> Vec<TxnView> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|entry| entry.metadata())
            .collect()
    }

    fn spawn_completion(self: &Arc<Self>, prepared: TxnMetadata) {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move { coordinator.complete(prepared).await });
    }

    /// Writes the markers of a `Prepare*` transaction to every partition, then its `Complete*`
    /// record. Retries each step until it succeeds or the coordinator stops; a gateway that stops
    /// resumes it from the log.
    async fn complete(self: Arc<Self>, prepared: TxnMetadata) {
        let transactional_id = prepared.transactional_id.clone();
        let Some(entry) = self.entry(&transactional_id) else {
            return;
        };
        let user = entry
            .user
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let control = if prepared.state == TxnState::PrepareCommit {
            ControlType::Commit
        } else {
            ControlType::Abort
        };
        let mut ended = Vec::with_capacity(prepared.partitions.len());
        for (partition, start) in &prepared.partitions {
            let marker = Marker {
                topic: partition.0.clone(),
                partition: partition.1,
                producer_id: prepared.producer_id,
                user: user.clone(),
                epoch: prepared.producer_epoch,
                control,
                coordinator_epoch: self.config.coordinator_epoch,
            };
            let Some(landed) = self.write_marker_until_done(&marker).await else {
                return;
            };
            let first = self
                .index
                .first_of(partition, prepared.producer_id)
                .or(start.start)
                .unwrap_or(0);
            let (last, exact) = match landed {
                Landed::At(offset) => (offset, true),
                Landed::Below(watermark) => (watermark, false),
            };
            ended.push(EndedRange {
                topic: partition.0.clone(),
                partition: partition.1,
                producer_id: prepared.producer_id,
                first: first.min(last),
                last,
                exact,
            });
        }
        // A committed transaction's offsets become the groups' committed offsets before it is
        // complete, as Kafka's commit marker on `__consumer_offsets` makes them visible.
        if control == ControlType::Commit {
            for (group, offsets) in &prepared.offsets {
                loop {
                    match self.data.commit_offsets(group, offsets).await {
                        Ok(()) => break,
                        Err(error) => {
                            debug!(%error, transactional_id, group, "transaction offsets not committed yet");
                        }
                    }
                    if self.sleep_or_cancelled(RETRY_BACKOFF).await {
                        return;
                    }
                }
            }
        }
        loop {
            let turn = entry.transition.lock().await;
            let current = entry.metadata();
            if current.state != prepared.state
                || current.producer_id != prepared.producer_id
                || current.producer_epoch != prepared.producer_epoch
            {
                // Another completion of the same transaction finished first.
                return;
            }
            let done = current.complete(self.now());
            match self.append(&done, ended.clone()).await {
                Ok(()) => {
                    for range in &ended {
                        self.index.end(range, control == ControlType::Abort);
                    }
                    self.publish(&entry, done);
                    debug!(transactional_id, ?control, "transaction completed");
                    return;
                }
                Err(error) => {
                    drop(turn);
                    debug!(%error, transactional_id, "could not complete a transaction yet");
                }
            }
            if self.sleep_or_cancelled(RETRY_BACKOFF).await {
                return;
            }
        }
    }

    /// `None` once the coordinator is cancelled.
    async fn write_marker_until_done(&self, marker: &Marker) -> Option<Landed> {
        loop {
            match tokio::time::timeout(MARKER_ATTEMPT, self.data.write_marker(marker)).await {
                Ok(Ok(landed)) => return Some(landed),
                Ok(Err(error)) => {
                    debug!(%error, topic = marker.topic, partition = marker.partition, "transaction marker not written yet");
                }
                Err(_) => {
                    debug!(
                        topic = marker.topic,
                        partition = marker.partition,
                        "transaction marker write timed out"
                    );
                }
            }
            if self.sleep_or_cancelled(RETRY_BACKOFF).await {
                return None;
            }
        }
    }

    async fn sleep_or_cancelled(&self, pause: Duration) -> bool {
        tokio::select! {
            () = self.cancel.cancelled() => true,
            () = tokio::time::sleep(pause) => false,
        }
    }

    /// Notes that a transactional send landed at `base_offset`, so the transaction's range on the
    /// partition starts there rather than at its bound.
    pub fn written(&self, partition: &TopicPartition, producer_id: i64, base_offset: u64) {
        self.index.written(partition, producer_id, base_offset);
    }

    /// Stops the background tasks: the timeout sweep and marker writes still running. Those
    /// resume from the log at the next start.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

/// The Kafka code for a transaction log write that failed: retriable, so the client tries again.
fn log_failed(error: &BridgeError, transactional_id: &str) -> i16 {
    warn!(%error, transactional_id, "could not write the Kafka transaction log");
    codes::COORDINATOR_NOT_AVAILABLE
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicU64};

    use super::*;
    use crate::transactions::log::MemoryLog;

    /// Data partitions in memory: every partition of every topic exists, and each marker lands at
    /// the next offset.
    #[derive(Default)]
    struct MemoryData {
        markers: Mutex<Vec<Marker>>,
        next: AtomicU64,
        committed: Mutex<BTreeMap<(String, TopicPartition), i64>>,
    }

    #[async_trait]
    impl DataPlane for MemoryData {
        async fn high_watermark(&self, topic: &str, _partition: i32) -> Result<u64, BridgeError> {
            if topic == "missing" {
                return Err(BridgeError::Iggy(
                    iggy::prelude::IggyError::TopicNameNotFound(
                        topic.to_string(),
                        "kafka".to_string(),
                    ),
                ));
            }
            Ok(self.next.load(Ordering::Relaxed))
        }

        async fn write_marker(&self, marker: &Marker) -> Result<Landed, BridgeError> {
            self.markers.lock().unwrap().push(marker.clone());
            Ok(Landed::At(self.next.fetch_add(1, Ordering::Relaxed)))
        }

        async fn commit_offsets(
            &self,
            group: &str,
            offsets: &BTreeMap<TopicPartition, i64>,
        ) -> Result<(), BridgeError> {
            self.committed.lock().unwrap().extend(
                offsets
                    .iter()
                    .map(|(partition, offset)| ((group.to_string(), partition.clone()), *offset)),
            );
            Ok(())
        }
    }

    static NOW: AtomicI64 = AtomicI64::new(1_000_000);

    fn clock() -> i64 {
        NOW.load(Ordering::Relaxed)
    }

    fn coordinator(log: Arc<MemoryLog>, data: Arc<MemoryData>) -> Arc<TransactionCoordinator> {
        let next = Arc::new(AtomicI64::new(500));
        TransactionCoordinator::with_clock(
            TransactionConfig::default(),
            log,
            data,
            Arc::new(move || Some(next.fetch_add(1, Ordering::Relaxed))),
            clock,
        )
    }

    async fn loaded(log: &Arc<MemoryLog>, data: &Arc<MemoryData>) -> Arc<TransactionCoordinator> {
        let coordinator = coordinator(Arc::clone(log), Arc::clone(data));
        coordinator.load().await;
        coordinator
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        (topic.to_string(), partition)
    }

    async fn settle(coordinator: &TransactionCoordinator, transactional_id: &str) -> TxnState {
        for _ in 0..200 {
            let state = coordinator.describe(transactional_id).unwrap().state;
            if !state.is_preparing() {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the transaction did not complete");
    }

    #[tokio::test]
    async fn given_a_commit_should_write_a_marker_per_partition_and_complete() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let coordinator = loaded(&log, &data).await;
        let (id, epoch) = coordinator
            .init_producer_id("tx", 60_000, None)
            .await
            .unwrap();
        assert_eq!(epoch, 0);
        assert!(coordinator.knows_producer(id));
        coordinator
            .add_partitions("tx", id, epoch, &[tp("t", 0), tp("t", 1)], Some("alice"))
            .await
            .unwrap();
        coordinator
            .verify_write("tx", id, epoch, &tp("t", 0))
            .unwrap();
        assert_eq!(
            coordinator.verify_write("tx", id, epoch, &tp("t", 2)),
            Err(codes::INVALID_TXN_STATE)
        );
        coordinator
            .end_txn("tx", id, epoch, Outcome::Commit)
            .await
            .unwrap();
        assert_eq!(settle(&coordinator, "tx").await, TxnState::CompleteCommit);
        let markers = data.markers.lock().unwrap().clone();
        assert_eq!(markers.len(), 2);
        assert!(
            markers
                .iter()
                .all(|marker| marker.control == ControlType::Commit
                    && marker.user.as_deref() == Some("alice"))
        );
        let states: Vec<TxnState> = log
            .records()
            .iter()
            .map(|record| record.metadata().state)
            .collect();
        assert_eq!(
            states,
            [
                TxnState::Empty,
                TxnState::Ongoing,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit
            ]
        );
        assert_eq!(
            coordinator.end_txn("tx", id, epoch, Outcome::Commit).await,
            Ok(()),
            "a retry of the end answers NONE"
        );
    }

    #[tokio::test]
    async fn given_a_second_producer_should_fence_the_first_and_abort_its_transaction() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let coordinator = loaded(&log, &data).await;
        let (id, epoch) = coordinator
            .init_producer_id("tx", 60_000, None)
            .await
            .unwrap();
        coordinator
            .add_partitions("tx", id, epoch, &[tp("t", 0)], None)
            .await
            .unwrap();
        assert_eq!(
            coordinator.init_producer_id("tx", 60_000, None).await,
            Err(codes::CONCURRENT_TRANSACTIONS)
        );
        assert_eq!(settle(&coordinator, "tx").await, TxnState::CompleteAbort);
        let (same, bumped) = coordinator
            .init_producer_id("tx", 60_000, None)
            .await
            .unwrap();
        assert_eq!((same, bumped), (id, 2));
        assert_eq!(
            coordinator.verify_write("tx", id, epoch, &tp("t", 0)),
            Err(codes::PRODUCER_FENCED)
        );
        assert_eq!(
            coordinator.end_txn("tx", id, epoch, Outcome::Commit).await,
            Err(codes::PRODUCER_FENCED)
        );
        let aborted = coordinator.index().aborted_between(&tp("t", 0), 0, 100);
        assert_eq!(
            aborted.len(),
            1,
            "the fenced transaction's range is aborted"
        );
    }

    #[tokio::test]
    async fn given_a_transaction_past_its_timeout_should_abort_it_and_fence_the_producer() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let coordinator = loaded(&log, &data).await;
        let (id, epoch) = coordinator
            .init_producer_id("slow", 1_000, None)
            .await
            .unwrap();
        coordinator
            .add_partitions("slow", id, epoch, &[tp("t", 0)], None)
            .await
            .unwrap();
        coordinator.abort_timed_out().await;
        assert_eq!(
            coordinator.describe("slow").unwrap().state,
            TxnState::Ongoing
        );
        NOW.fetch_add(2_000, Ordering::Relaxed);
        coordinator.abort_timed_out().await;
        assert_eq!(settle(&coordinator, "slow").await, TxnState::CompleteAbort);
        assert_eq!(
            coordinator
                .end_txn("slow", id, epoch, Outcome::Commit)
                .await,
            Err(codes::PRODUCER_FENCED)
        );
        NOW.fetch_sub(2_000, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn given_a_restart_after_prepare_should_resume_the_markers() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let first = loaded(&log, &data).await;
        let (id, epoch) = first.init_producer_id("tx", 60_000, None).await.unwrap();
        first
            .add_partitions("tx", id, epoch, &[tp("t", 0)], None)
            .await
            .unwrap();
        // The decision lands, the markers do not: the log stops taking writes after it.
        let entry = first.entry("tx").unwrap();
        let prepared = entry
            .metadata()
            .end(id, epoch, Outcome::Abort, clock())
            .unwrap()
            .unwrap();
        first.append(&prepared, Vec::new()).await.unwrap();
        drop(first);

        let second = loaded(&log, &data).await;
        assert_eq!(settle(&second, "tx").await, TxnState::CompleteAbort);
        assert_eq!(data.markers.lock().unwrap().len(), 1);
        assert!(second.knows_producer(id));
        let (same, next) = second.init_producer_id("tx", 60_000, None).await.unwrap();
        assert_eq!((same, next), (id, 1));
    }

    #[tokio::test]
    async fn given_errors_should_answer_as_kafka_does() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let coordinator = coordinator(Arc::clone(&log), Arc::clone(&data));
        assert_eq!(
            coordinator.init_producer_id("tx", 60_000, None).await,
            Err(codes::COORDINATOR_LOAD_IN_PROGRESS)
        );
        coordinator.load().await;
        assert_eq!(
            coordinator.init_producer_id("tx", 900_001, None).await,
            Err(codes::INVALID_TRANSACTION_TIMEOUT)
        );
        assert_eq!(
            coordinator.init_producer_id("tx", 0, None).await,
            Err(codes::INVALID_TRANSACTION_TIMEOUT)
        );
        assert_eq!(
            coordinator
                .add_partitions("nobody", 1, 0, &[tp("t", 0)], None)
                .await,
            Err(AddError::All(codes::INVALID_PRODUCER_ID_MAPPING))
        );
        let (id, epoch) = coordinator
            .init_producer_id("tx", 60_000, None)
            .await
            .unwrap();
        let Err(AddError::Partitions(refused)) = coordinator
            .add_partitions("tx", id, epoch, &[tp("t", 0), tp("missing", 0)], None)
            .await
        else {
            panic!("a missing topic is refused per partition");
        };
        assert_eq!(
            refused.get(&tp("missing", 0)),
            Some(&codes::UNKNOWN_TOPIC_OR_PARTITION)
        );
        assert_eq!(
            coordinator.end_txn("tx", id, epoch, Outcome::Commit).await,
            Err(codes::INVALID_TXN_STATE),
            "nothing was added"
        );
        log.failing.store(true, Ordering::Relaxed);
        assert_eq!(
            coordinator
                .add_partitions("tx", id, epoch, &[tp("t", 0)], None)
                .await,
            Err(AddError::All(codes::COORDINATOR_NOT_AVAILABLE))
        );
        assert_eq!(
            coordinator.describe("tx").unwrap().state,
            TxnState::Empty,
            "not current until written"
        );
    }

    #[tokio::test]
    async fn given_a_new_transaction_should_start_above_the_last_marker() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let coordinator = loaded(&log, &data).await;
        let (id, epoch) = coordinator
            .init_producer_id("tx", 60_000, None)
            .await
            .unwrap();
        coordinator
            .add_partitions("tx", id, epoch, &[tp("t", 0)], None)
            .await
            .unwrap();
        coordinator
            .end_txn("tx", id, epoch, Outcome::Commit)
            .await
            .unwrap();
        settle(&coordinator, "tx").await;
        let marker = data.next.load(Ordering::Relaxed) - 1;
        // A watermark read that lags behind the marker must not start the next range below it.
        data.next.store(0, Ordering::Relaxed);
        coordinator
            .add_partitions("tx", id, epoch, &[tp("t", 0)], None)
            .await
            .unwrap();
        let start = coordinator.describe("tx").unwrap().partitions[&tp("t", 0)].start;
        assert_eq!(start, Some(marker + 1));
    }

    #[tokio::test]
    async fn given_offsets_in_a_transaction_should_commit_them_only_when_it_commits() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let coordinator = loaded(&log, &data).await;
        let (id, epoch) = coordinator
            .init_producer_id("eos", 60_000, None)
            .await
            .unwrap();
        let offsets = BTreeMap::from([(tp("in", 0), 42)]);
        assert_eq!(
            coordinator
                .txn_offset_commit("eos", id, epoch, "g", &offsets)
                .await,
            Err(codes::INVALID_TXN_STATE),
            "the group was not added"
        );
        coordinator
            .add_offsets("eos", id, epoch, "g")
            .await
            .unwrap();
        coordinator
            .add_partitions("eos", id, epoch, &[tp("out", 0)], None)
            .await
            .unwrap();
        coordinator
            .txn_offset_commit("eos", id, epoch, "g", &offsets)
            .await
            .unwrap();
        assert!(coordinator.has_pending_offset("g", &tp("in", 0)));
        assert!(!coordinator.has_pending_offset("g", &tp("in", 1)));
        assert_eq!(
            coordinator
                .txn_offset_commit("eos", id, epoch + 1, "g", &offsets)
                .await,
            Err(codes::INVALID_PRODUCER_EPOCH)
        );
        coordinator
            .end_txn("eos", id, epoch, Outcome::Commit)
            .await
            .unwrap();
        assert_eq!(settle(&coordinator, "eos").await, TxnState::CompleteCommit);
        assert!(!coordinator.has_pending_offset("g", &tp("in", 0)));
        assert_eq!(
            data.committed
                .lock()
                .unwrap()
                .get(&("g".to_string(), tp("in", 0))),
            Some(&42)
        );

        coordinator
            .add_offsets("eos", id, epoch, "g")
            .await
            .unwrap();
        coordinator
            .txn_offset_commit("eos", id, epoch, "g", &BTreeMap::from([(tp("in", 0), 99)]))
            .await
            .unwrap();
        coordinator
            .end_txn("eos", id, epoch, Outcome::Abort)
            .await
            .unwrap();
        assert_eq!(settle(&coordinator, "eos").await, TxnState::CompleteAbort);
        assert_eq!(
            data.committed
                .lock()
                .unwrap()
                .get(&("g".to_string(), tp("in", 0))),
            Some(&42),
            "an aborted transaction's offsets are dropped"
        );
        let replayed = log
            .records()
            .iter()
            .rev()
            .map(LogRecord::metadata)
            .find(|metadata| !metadata.offsets.is_empty())
            .expect("the offsets are in the log");
        assert_eq!(replayed.offsets["g"][&tp("in", 0)], 99);
    }

    #[tokio::test]
    async fn given_an_idle_id_past_its_expiry_should_forget_it_and_stay_forgotten() {
        let (log, data) = (
            Arc::new(MemoryLog::default()),
            Arc::new(MemoryData::default()),
        );
        let config = TransactionConfig {
            id_expiration: Duration::from_millis(10),
            ..TransactionConfig::default()
        };
        let coordinator = TransactionCoordinator::with_clock(
            config.clone(),
            Arc::clone(&log) as Arc<dyn TxnLog>,
            Arc::clone(&data) as Arc<dyn DataPlane>,
            Arc::new(|| Some(900)),
            wall_clock_ms,
        );
        coordinator.load().await;
        let (id, _) = coordinator
            .init_producer_id("old", 60_000, None)
            .await
            .unwrap();
        coordinator.expire_idle().await;
        assert!(
            coordinator.describe("old").is_some(),
            "not idle long enough yet"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        coordinator.expire_idle().await;
        assert!(coordinator.describe("old").is_none());
        assert!(!coordinator.knows_producer(id));
        let replayed = TransactionCoordinator::with_clock(
            config,
            Arc::clone(&log) as Arc<dyn TxnLog>,
            data,
            Arc::new(|| Some(901)),
            wall_clock_ms,
        );
        replayed.load().await;
        assert!(
            replayed.describe("old").is_none(),
            "the Dead record keeps it forgotten"
        );
    }
}
