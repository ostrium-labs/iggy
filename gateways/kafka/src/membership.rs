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

//! Several gateways behind one bootstrap list ([#4515](https://github.com/apache/iggy/issues/4515)).
//!
//! Each gateway is one Kafka broker with a node id of its own. The gateways that front one Iggy
//! cluster find each other through a one-partition Iggy topic, `<default stream>.meta/gateways`:
//! every gateway appends an [`Announcement`] to it each heartbeat interval and reads everything
//! the others appended. Iggy stamps each message with its append time, and every judgement below
//! is made on those stamps, never on a gateway's own clock:
//!
//! - A node is **live** while its last announcement is at most one lease older than the newest
//!   announcement in the topic. A node that says goodbye on shutdown is gone at once.
//! - A live node is **eligible** to own groups and lead partitions once its first announcement is
//!   at least one join delay older than the newest. So a gateway that starts takes no group until
//!   every other gateway had the time to hear of it. When no live node is eligible yet, every
//!   live node is.
//!
//! Two gateways that have read the same prefix of the topic compute the same live and eligible
//! sets. Which group a node coordinates, and which partition it leads, is rendezvous hashing over
//! the eligible set, so a node that joins or leaves moves only the groups and partitions it
//! takes or gives up.
//!
//! A gateway with no node id configured runs alone: it is node 1, it owns every group and leads
//! every partition, and it reads and writes no announcement. That is the behaviour before #4515.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::bridge::BridgeError;
use crate::listener::SecurityProtocol;
use crate::protocol::api::BrokerAdvertise;

/// The node id of a gateway that runs alone, the one Metadata advertised before #4515.
pub const SINGLE_NODE_ID: i32 = 1;

/// The topic, in the gateway state stream, that the announcements go to.
pub const GATEWAYS_TOPIC: &str = "gateways";

/// The announcement format. A gateway skips an announcement of another version.
pub const ANNOUNCEMENT_VERSION: u32 = 1;

/// Announcements read per poll of the topic.
pub const READ_BATCH: u32 = 1000;

/// How often a gateway announces itself, and reads the others, by default.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How long a gateway stays live after its last announcement, by default.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(10);

/// How long a goodbye may take at shutdown.
const GOODBYE_DEADLINE: Duration = Duration::from_secs(2);

/// How a gateway takes part in membership: its node id and its timings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipConfig {
    /// `IGGY_KAFKA_NODE_ID`. Unique among the gateways that front one Iggy cluster.
    pub node_id: i32,
    /// `IGGY_KAFKA_MEMBERSHIP_HEARTBEAT_MS`.
    pub heartbeat_interval: Duration,
    /// `IGGY_KAFKA_MEMBERSHIP_LEASE_MS`. Longer than two heartbeat intervals.
    pub lease: Duration,
}

impl MembershipConfig {
    /// Membership for `node_id` with the default timings.
    #[must_use]
    pub const fn new(node_id: i32) -> Self {
        Self {
            node_id,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            lease: DEFAULT_LEASE,
        }
    }

    /// How long a node announces itself before it may own anything: two heartbeat intervals, so
    /// every other gateway has polled the topic at least once since its first announcement.
    #[must_use]
    pub const fn join_delay(&self) -> Duration {
        self.heartbeat_interval.saturating_mul(2)
    }

    /// # Errors
    ///
    /// A message when the node id is negative, an interval is zero, or the lease is not longer
    /// than two heartbeat intervals.
    pub fn validate(&self) -> Result<(), String> {
        if self.node_id < 0 {
            return Err(format!(
                "IGGY_KAFKA_NODE_ID {} is negative; Kafka node ids are 0 or more",
                self.node_id
            ));
        }
        if self.heartbeat_interval.is_zero() {
            return Err("IGGY_KAFKA_MEMBERSHIP_HEARTBEAT_MS must be above 0".to_string());
        }
        if self.lease <= self.heartbeat_interval.saturating_mul(2) {
            return Err(format!(
                "IGGY_KAFKA_MEMBERSHIP_LEASE_MS ({} ms) must be longer than two heartbeat \
                 intervals ({} ms each), or a gateway that misses one heartbeat drops out",
                self.lease.as_millis(),
                self.heartbeat_interval.as_millis()
            ));
        }
        Ok(())
    }
}

/// Where a node can be reached on one security protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// [`SecurityProtocol::as_str`]. A string, so a protocol a later build adds is skipped, not
    /// fatal.
    pub protocol: String,
    pub host: String,
    pub port: i32,
}

impl Endpoint {
    #[must_use]
    pub fn new(protocol: SecurityProtocol, broker: &BrokerAdvertise) -> Self {
        Self {
            protocol: protocol.as_str().to_string(),
            host: broker.host.clone(),
            port: broker.port,
        }
    }
}

/// Whether a node is serving or saying goodbye.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    Up,
    Leaving,
}

/// What one gateway appends to the topic each heartbeat interval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Announcement {
    pub version: u32,
    pub node_id: i32,
    /// Random per process, so a restart is told apart from a second process with the same id.
    pub incarnation: u64,
    /// `IGGY_KAFKA_INSTANCE_ID`, so two gateways that would hand out the same producer ids are
    /// reported.
    pub instance_id: u16,
    pub state: NodeState,
    pub endpoints: Vec<Endpoint>,
}

impl Announcement {
    #[must_use]
    pub fn new(node_id: i32, instance_id: u16, endpoints: Vec<Endpoint>) -> Self {
        Self {
            version: ANNOUNCEMENT_VERSION,
            node_id,
            incarnation: uuid::Uuid::new_v4().as_u64_pair().0,
            instance_id,
            state: NodeState::Up,
            endpoints,
        }
    }

    /// The same announcement, saying goodbye.
    #[must_use]
    pub fn leaving(&self) -> Self {
        Self {
            state: NodeState::Leaving,
            ..self.clone()
        }
    }

    /// # Errors
    ///
    /// Never in practice: every field is a TOML scalar or a table of them.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        toml::to_string(self)
            .map(String::into_bytes)
            .map_err(|error| error.to_string())
    }

    /// `None` for a payload that is not an announcement of [`ANNOUNCEMENT_VERSION`].
    #[must_use]
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(payload).ok()?;
        let announcement: Self = toml::from_str(text).ok()?;
        (announcement.version == ANNOUNCEMENT_VERSION && announcement.node_id >= 0)
            .then_some(announcement)
    }

    fn endpoint(&self, protocol: SecurityProtocol) -> Option<BrokerAdvertise> {
        self.endpoints
            .iter()
            .find(|endpoint| endpoint.protocol == protocol.as_str())
            .map(|endpoint| BrokerAdvertise {
                host: endpoint.host.clone(),
                port: endpoint.port,
            })
    }
}

/// One message read back from the topic.
#[derive(Debug, Clone)]
pub struct Heard {
    pub offset: u64,
    /// Iggy's append time, in microseconds.
    pub timestamp: u64,
    pub payload: Bytes,
}

/// What the view knows of one node id.
#[derive(Debug, Clone)]
struct Seen {
    announcement: Announcement,
    /// The append time of the first announcement of this run of the node.
    first: u64,
    /// The append time of its last announcement.
    last: u64,
}

/// What hearing one announcement found, for the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hearing {
    /// A node id that was not live, or a later announcement of a live one.
    Fresh,
    /// A new incarnation of a node id whose previous one was still live: a restart without a
    /// goodbye, or a second process with the same id.
    Restarted { previous: u64 },
    /// An incarnation that a newer one already replaced. Two processes share the node id.
    Duplicate { incarnation: u64 },
}

/// Every announcement heard, folded into what each node id last said.
#[derive(Debug, Default)]
pub struct View {
    nodes: BTreeMap<i32, Seen>,
    /// The newest append time heard. "Now", on Iggy's clock.
    newest: u64,
    /// Incarnations that a newer one of the same id replaced while they were live.
    replaced: HashSet<(i32, u64)>,
}

impl View {
    /// Folds in one announcement appended at `timestamp`.
    pub fn hear(&mut self, timestamp: u64, announcement: Announcement, lease: Duration) -> Hearing {
        self.newest = self.newest.max(timestamp);
        let node_id = announcement.node_id;
        if self.replaced.contains(&(node_id, announcement.incarnation)) {
            return Hearing::Duplicate {
                incarnation: announcement.incarnation,
            };
        }
        let lease = micros(lease);
        let Some(seen) = self.nodes.get_mut(&node_id) else {
            self.nodes.insert(
                node_id,
                Seen {
                    announcement,
                    first: timestamp,
                    last: timestamp,
                },
            );
            return Hearing::Fresh;
        };
        let was_live = seen.announcement.state == NodeState::Up
            && timestamp.saturating_sub(seen.last) <= lease;
        let hearing = if seen.announcement.incarnation == announcement.incarnation {
            Hearing::Fresh
        } else if was_live {
            // The new incarnation inherits the first time, so a quick restart keeps its groups
            // instead of handing them away for a join delay and taking them back.
            let previous = seen.announcement.incarnation;
            self.replaced.insert((node_id, previous));
            Hearing::Restarted { previous }
        } else {
            seen.first = timestamp;
            Hearing::Fresh
        };
        if !was_live && announcement.state == NodeState::Up {
            seen.first = timestamp;
        }
        seen.last = timestamp;
        seen.announcement = announcement;
        hearing
    }

    /// The live and eligible nodes as of the newest announcement heard. `me` is always live, so a
    /// gateway lists itself before its first announcement is read back.
    #[must_use]
    pub fn snapshot(&self, me: &Announcement, config: &MembershipConfig) -> Snapshot {
        let lease = micros(config.lease);
        let join_delay = micros(config.join_delay());
        let mut live = BTreeMap::new();
        let mut eligible = BTreeSet::new();
        for (&node_id, seen) in &self.nodes {
            if seen.announcement.state != NodeState::Up
                || self.newest.saturating_sub(seen.last) > lease
            {
                continue;
            }
            live.insert(node_id, seen.announcement.clone());
            if self.newest.saturating_sub(seen.first) >= join_delay {
                eligible.insert(node_id);
            }
        }
        live.insert(me.node_id, me.clone());
        if eligible.is_empty() {
            eligible.extend(live.keys().copied());
        }
        Snapshot {
            me: me.node_id,
            live,
            eligible: eligible.into_iter().collect(),
        }
    }
}

fn micros(duration: Duration) -> u64 {
    // Saturates far past any lease anyone sets.
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Who is live and who owns what, at one moment. Cheap to share; handlers read one per request.
#[derive(Debug, Clone)]
pub struct Snapshot {
    me: i32,
    /// Every live node with what it last announced, by node id.
    live: BTreeMap<i32, Announcement>,
    /// The nodes that own groups and lead partitions, ascending. Never empty.
    eligible: Vec<i32>,
}

impl Snapshot {
    /// A gateway that runs alone, as node `me`.
    #[must_use]
    pub fn single(me: i32) -> Self {
        let announcement = Announcement {
            version: ANNOUNCEMENT_VERSION,
            node_id: me,
            incarnation: 0,
            instance_id: 0,
            state: NodeState::Up,
            endpoints: Vec::new(),
        };
        Self {
            me,
            live: BTreeMap::from([(me, announcement)]),
            eligible: vec![me],
        }
    }

    /// This gateway's node id.
    #[must_use]
    pub const fn node_id(&self) -> i32 {
        self.me
    }

    /// Live node ids, ascending, this one included.
    pub fn live_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.live.keys().copied()
    }

    /// Node ids that own groups and lead partitions, ascending.
    #[must_use]
    pub fn eligible_ids(&self) -> &[i32] {
        &self.eligible
    }

    /// Whether more than one gateway is live.
    #[must_use]
    pub fn is_shared(&self) -> bool {
        self.live.len() > 1
    }

    /// The node that coordinates `group`.
    #[must_use]
    pub fn coordinator(&self, group: &str) -> i32 {
        rendezvous(group.as_bytes(), &self.eligible)
    }

    /// Whether this gateway coordinates `group`.
    #[must_use]
    pub fn coordinates(&self, group: &str) -> bool {
        self.coordinator(group) == self.me
    }

    /// The node that leads `partition` of Kafka topic `topic`.
    #[must_use]
    pub fn leader(&self, topic: &str, partition: i32) -> i32 {
        let mut key = Vec::with_capacity(topic.len() + 5);
        key.extend_from_slice(topic.as_bytes());
        key.push(0);
        key.extend_from_slice(&partition.to_be_bytes());
        rendezvous(&key, &self.eligible)
    }

    /// Whether this gateway leads `partition` of `topic`.
    #[must_use]
    pub fn leads(&self, topic: &str, partition: i32) -> bool {
        self.leader(topic, partition) == self.me
    }

    /// The controller Metadata and `DescribeCluster` name: the lowest eligible node id.
    #[must_use]
    pub fn controller(&self) -> i32 {
        self.eligible.first().copied().unwrap_or(self.me)
    }

    /// Where `node_id` is reached on `protocol`. This gateway answers `own`, the address of the
    /// listener the request came through. `None` for a node that is not live or has no listener
    /// of that protocol.
    #[must_use]
    pub fn broker(
        &self,
        node_id: i32,
        protocol: SecurityProtocol,
        own: &BrokerAdvertise,
    ) -> Option<BrokerAdvertise> {
        if node_id == self.me {
            return Some(own.clone());
        }
        self.live.get(&node_id)?.endpoint(protocol)
    }

    /// Every live node reachable on `protocol`, ascending by node id. This one is always first
    /// among equals and always present.
    #[must_use]
    pub fn brokers(
        &self,
        protocol: SecurityProtocol,
        own: &BrokerAdvertise,
    ) -> Vec<(i32, BrokerAdvertise)> {
        self.live
            .keys()
            .filter_map(|&node_id| {
                self.broker(node_id, protocol, own)
                    .map(|broker| (node_id, broker))
            })
            .collect()
    }
}

/// Rendezvous (highest random weight) hashing: the node with the highest score for `key` wins,
/// the lower id on a tie. Stable across processes and builds, unlike `std`'s hasher.
#[must_use]
pub fn rendezvous(key: &[u8], nodes: &[i32]) -> i32 {
    if let [only] = nodes {
        return *only;
    }
    let key_hash = fnv1a(key);
    nodes
        .iter()
        .copied()
        .max_by(|&left, &right| {
            score(key_hash, left)
                .cmp(&score(key_hash, right))
                .then(right.cmp(&left))
        })
        .unwrap_or(SINGLE_NODE_ID)
}

const fn score(key_hash: u64, node_id: i32) -> u64 {
    #[expect(clippy::cast_sign_loss, reason = "node ids are not negative")]
    let node = node_id as u64;
    splitmix64(key_hash ^ splitmix64(node))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

const fn splitmix64(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Where announcements are written and read: the Iggy topic, or a fake in tests.
#[async_trait]
pub trait Registry: Send + Sync {
    /// Appends one announcement.
    async fn announce(&self, payload: Vec<u8>) -> Result<(), BridgeError>;

    /// Up to `count` messages from `offset` on, or the last `count` when `offset` is `None`.
    async fn read(&self, offset: Option<u64>, count: u32) -> Result<Vec<Heard>, BridgeError>;
}

/// This gateway's membership: what it announces, what it heard, and the snapshot handlers read.
#[derive(Debug)]
pub struct Membership {
    config: Option<MembershipConfig>,
    me: Announcement,
    view: Mutex<ViewState>,
    snapshot: ArcSwap<Snapshot>,
    /// Bumped whenever the live or the eligible set changes.
    changes: watch::Sender<u64>,
}

#[derive(Debug, Default)]
struct ViewState {
    view: View,
    /// The next offset to read, once the first read found the end of the topic.
    next_offset: Option<u64>,
    /// Node ids already reported for sharing this gateway's node or instance id, so each is
    /// reported once, not every heartbeat.
    reported: HashSet<(i32, u64)>,
}

impl Membership {
    /// A gateway that runs alone, as node 1.
    #[must_use]
    pub fn single() -> Self {
        Self {
            config: None,
            me: Snapshot::single(SINGLE_NODE_ID).live[&SINGLE_NODE_ID].clone(),
            view: Mutex::default(),
            snapshot: ArcSwap::from_pointee(Snapshot::single(SINGLE_NODE_ID)),
            changes: watch::Sender::new(0),
        }
    }

    /// A gateway that announces `me` and takes part in membership as `config` says.
    #[must_use]
    pub fn shared(config: MembershipConfig, me: Announcement) -> Self {
        let view = ViewState::default();
        let snapshot = view.view.snapshot(&me, &config);
        Self {
            config: Some(config),
            me,
            view: Mutex::new(view),
            snapshot: ArcSwap::from_pointee(snapshot),
            changes: watch::Sender::new(0),
        }
    }

    /// The node id this gateway advertises.
    #[must_use]
    pub const fn node_id(&self) -> i32 {
        self.me.node_id
    }

    /// Who is live and who owns what, now.
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.load_full()
    }

    /// Changes whenever the live or the eligible set changes, so a waiter can check again what
    /// this gateway owns.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// Whether this gateway takes part in membership.
    #[must_use]
    pub const fn is_shared(&self) -> bool {
        self.config.is_some()
    }

    /// Folds `heard` into the view and publishes the new snapshot. Returns it.
    pub fn hear(&self, heard: Vec<Heard>) -> Arc<Snapshot> {
        let Some(config) = self.config else {
            return self.snapshot();
        };
        let mut state = self.view.lock().unwrap_or_else(PoisonError::into_inner);
        for message in heard {
            if state.next_offset.is_some_and(|next| message.offset < next) {
                continue;
            }
            state.next_offset = Some(message.offset + 1);
            let Some(announcement) = Announcement::decode(&message.payload) else {
                debug!(
                    offset = message.offset,
                    "skipping a message of the gateways topic that is no announcement of this \
                     version"
                );
                continue;
            };
            if state
                .reported
                .insert((announcement.node_id, announcement.incarnation))
            {
                self.report(&announcement, &state.view);
            }
            let node_id = announcement.node_id;
            match state
                .view
                .hear(message.timestamp, announcement, config.lease)
            {
                Hearing::Fresh => {}
                Hearing::Restarted { previous } => info!(
                    node_id,
                    previous, "Kafka gateway node restarted under a new incarnation"
                ),
                Hearing::Duplicate { incarnation } => error!(
                    node_id,
                    incarnation,
                    "two Kafka gateways announce node id {node_id}; give each its own \
                     IGGY_KAFKA_NODE_ID, or they split every group they share"
                ),
            }
        }
        let snapshot = Arc::new(state.view.snapshot(&self.me, &config));
        drop(state);
        let previous = self.snapshot.swap(Arc::clone(&snapshot));
        if log_change(&previous, &snapshot) {
            self.changes
                .send_modify(|changes| *changes = changes.wrapping_add(1));
        }
        snapshot
    }

    /// Warns about another node that collides with this one.
    fn report(&self, announcement: &Announcement, view: &View) {
        if announcement.node_id == self.me.node_id
            && announcement.incarnation != self.me.incarnation
            && announcement.state == NodeState::Up
            && !view
                .replaced
                .contains(&(announcement.node_id, announcement.incarnation))
        {
            warn!(
                node_id = self.me.node_id,
                "another Kafka gateway announces this gateway's node id {}, or an earlier run of \
                 this one does; if it is another process, give each its own IGGY_KAFKA_NODE_ID",
                self.me.node_id
            );
        }
        if announcement.node_id != self.me.node_id
            && announcement.instance_id == self.me.instance_id
            && announcement.state == NodeState::Up
        {
            warn!(
                node_id = announcement.node_id,
                instance_id = announcement.instance_id,
                "another Kafka gateway has this gateway's IGGY_KAFKA_INSTANCE_ID, so both hand \
                 out the same producer ids"
            );
        }
    }

    /// One heartbeat: announce, then read what is new. Returns the snapshot it published.
    ///
    /// # Errors
    ///
    /// The registry's error when the announcement or the read fails. The snapshot then stays.
    pub async fn beat(&self, registry: &dyn Registry) -> Result<Arc<Snapshot>, BridgeError> {
        let payload = self.me.encode().map_err(BridgeError::InvalidConfig)?;
        registry.announce(payload).await?;
        self.read_new(registry).await
    }

    async fn read_new(&self, registry: &dyn Registry) -> Result<Arc<Snapshot>, BridgeError> {
        loop {
            let next = self
                .view
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .next_offset;
            let heard = registry.read(next, READ_BATCH).await?;
            let full = heard.len() >= READ_BATCH as usize;
            let snapshot = self.hear(heard);
            // The first read takes the tail, and only later reads follow on from it.
            if !full || next.is_none() {
                return Ok(snapshot);
            }
        }
    }

    /// Says goodbye, so the others take this gateway's groups and partitions at once.
    pub async fn leave(&self, registry: &dyn Registry) {
        let Ok(payload) = self.me.leaving().encode() else {
            return;
        };
        match tokio::time::timeout(GOODBYE_DEADLINE, registry.announce(payload)).await {
            Ok(Ok(())) => info!(node_id = self.me.node_id, "Kafka gateway node said goodbye"),
            Ok(Err(error)) => warn!(
                %error,
                "could not say goodbye; the other gateways take this one's groups when its \
                 lease runs out"
            ),
            Err(_elapsed) => warn!(
                "saying goodbye timed out; the other gateways take this one's groups when its \
                 lease runs out"
            ),
        }
    }

    /// Beats every heartbeat interval until `cancel` fires, then says goodbye.
    pub async fn run(self: Arc<Self>, registry: Arc<dyn Registry>, cancel: CancellationToken) {
        let Some(config) = self.config else {
            return;
        };
        let mut failing = false;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(config.heartbeat_interval) => {}
            }
            match self.beat(registry.as_ref()).await {
                Ok(_) if failing => {
                    info!("Kafka gateway membership heartbeats reach Iggy again");
                    failing = false;
                }
                Ok(_) => {}
                Err(error) if !failing => {
                    warn!(
                        %error,
                        "a Kafka gateway membership heartbeat failed; the last view of the \
                         other gateways stays until one succeeds"
                    );
                    failing = true;
                }
                Err(error) => debug!(%error, "a Kafka gateway membership heartbeat failed"),
            }
        }
        self.leave(registry.as_ref()).await;
    }
}

impl Default for Membership {
    fn default() -> Self {
        Self::single()
    }
}

/// Logs a change of the live or eligible set, and says whether there was one.
fn log_change(previous: &Snapshot, next: &Snapshot) -> bool {
    if previous.live.keys().eq(next.live.keys()) && previous.eligible == next.eligible {
        return false;
    }
    info!(
        node_id = next.me,
        live = ?next.live.keys().collect::<Vec<_>>(),
        eligible = ?next.eligible,
        "Kafka gateway membership changed"
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u64 = 1_000_000;

    fn config() -> MembershipConfig {
        MembershipConfig {
            node_id: 1,
            heartbeat_interval: Duration::from_secs(1),
            lease: Duration::from_secs(5),
        }
    }

    fn node(node_id: i32) -> Announcement {
        Announcement::new(
            node_id,
            0,
            vec![Endpoint {
                protocol: "PLAINTEXT".to_string(),
                host: format!("gw{node_id}"),
                port: 9092,
            }],
        )
    }

    #[test]
    fn announcements_round_trip_and_reject_other_versions() {
        let announcement = node(3);
        let encoded = announcement.encode().unwrap();
        assert_eq!(Announcement::decode(&encoded), Some(announcement.clone()));
        let other = Announcement {
            version: 2,
            ..announcement
        };
        assert_eq!(Announcement::decode(&other.encode().unwrap()), None);
        assert_eq!(Announcement::decode(b"not toml ="), None);
    }

    #[test]
    fn rendezvous_is_stable_and_moves_only_the_keys_of_a_leaving_node() {
        let three = [1, 2, 3];
        let two = [1, 2];
        let mut moved = 0;
        for group in 0..1000 {
            let key = format!("group-{group}");
            let before = rendezvous(key.as_bytes(), &three);
            let after = rendezvous(key.as_bytes(), &two);
            if before == 3 {
                moved += 1;
            } else {
                assert_eq!(before, after, "a key of a node that stays must not move");
            }
            assert_eq!(rendezvous(key.as_bytes(), &[3, 2, 1]), before);
        }
        assert!(
            (250..420).contains(&moved),
            "{moved} of 1000 keys owned by node 3"
        );
    }

    #[test]
    fn rendezvous_answers_known_values() {
        // Pinned, so a change to the hash that would move every group in a rolling upgrade
        // fails here.
        assert_eq!(
            rendezvous(b"orders-app", &[1, 2, 3]),
            rendezvous(b"orders-app", &[1, 2, 3])
        );
        let owners: Vec<i32> = ["a", "b", "c", "d", "e", "f"]
            .iter()
            .map(|key| rendezvous(key.as_bytes(), &[1, 2, 3]))
            .collect();
        assert_eq!(owners, PINNED_OWNERS);
    }

    const PINNED_OWNERS: [i32; 6] = [3, 3, 3, 2, 2, 2];

    #[test]
    fn a_node_is_eligible_only_after_the_join_delay() {
        let config = config();
        let me = node(1);
        let other = node(2);
        let mut view = View::default();
        view.hear(0, me.clone(), config.lease);
        view.hear(0, other.clone(), config.lease);
        // Nobody is eligible yet, so everyone live is.
        let snapshot = view.snapshot(&me, &config);
        assert_eq!(snapshot.eligible_ids(), &[1, 2]);

        view.hear(3 * SECOND, me.clone(), config.lease);
        let third = node(3);
        view.hear(3 * SECOND, third, config.lease);
        let snapshot = view.snapshot(&me, &config);
        assert_eq!(snapshot.live_ids().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(snapshot.eligible_ids(), &[1, 2], "node 3 just joined");

        view.hear(5 * SECOND, me.clone(), config.lease);
        view.hear(5 * SECOND, other, config.lease);
        let snapshot = view.snapshot(&me, &config);
        assert_eq!(snapshot.eligible_ids(), &[1, 2, 3]);
    }

    #[test]
    fn a_node_expires_after_its_lease_and_leaves_at_once_on_goodbye() {
        let config = config();
        let me = node(1);
        let two = node(2);
        let three = node(3);
        let mut view = View::default();
        for node in [&me, &two, &three] {
            view.hear(0, node.clone(), config.lease);
        }
        view.hear(SECOND, three.leaving(), config.lease);
        let snapshot = view.snapshot(&me, &config);
        assert_eq!(snapshot.live_ids().collect::<Vec<_>>(), vec![1, 2]);

        view.hear(6 * SECOND, me.clone(), config.lease);
        let snapshot = view.snapshot(&me, &config);
        assert_eq!(snapshot.live_ids().collect::<Vec<_>>(), vec![1]);
        assert_eq!(snapshot.eligible_ids(), &[1]);
    }

    #[test]
    fn a_quick_restart_keeps_its_place_and_a_duplicate_is_reported() {
        let config = config();
        let me = node(1);
        let first = node(2);
        let mut view = View::default();
        view.hear(0, me.clone(), config.lease);
        view.hear(0, first.clone(), config.lease);
        view.hear(3 * SECOND, me.clone(), config.lease);
        let restarted = node(2);
        assert_eq!(
            view.hear(3 * SECOND, restarted, config.lease),
            Hearing::Restarted {
                previous: first.incarnation
            }
        );
        assert_eq!(view.snapshot(&me, &config).eligible_ids(), &[1, 2]);
        assert_eq!(
            view.hear(4 * SECOND, first.clone(), config.lease),
            Hearing::Duplicate {
                incarnation: first.incarnation
            }
        );
    }

    #[test]
    fn a_restart_after_a_goodbye_waits_the_join_delay_again() {
        let config = config();
        let me = node(1);
        let two = node(2);
        let mut view = View::default();
        view.hear(0, me.clone(), config.lease);
        view.hear(0, two.clone(), config.lease);
        view.hear(3 * SECOND, two.leaving(), config.lease);
        view.hear(3 * SECOND, me.clone(), config.lease);
        assert_eq!(view.hear(4 * SECOND, node(2), config.lease), Hearing::Fresh);
        assert_eq!(view.snapshot(&me, &config).eligible_ids(), &[1]);
    }

    #[test]
    fn brokers_list_every_live_node_on_the_protocol_and_this_one_by_its_listener() {
        let config = config();
        let me = node(1);
        let mut view = View::default();
        view.hear(0, node(2), config.lease);
        let tls_only = Announcement::new(
            3,
            0,
            vec![Endpoint {
                protocol: "SSL".to_string(),
                host: "gw3".to_string(),
                port: 9093,
            }],
        );
        view.hear(0, tls_only, config.lease);
        let snapshot = view.snapshot(&me, &config);
        let own = BrokerAdvertise {
            host: "self".to_string(),
            port: 1,
        };
        let brokers = snapshot.brokers(SecurityProtocol::Plaintext, &own);
        let listed: Vec<(i32, &str)> = brokers
            .iter()
            .map(|(id, broker)| (*id, broker.host.as_str()))
            .collect();
        assert_eq!(listed, vec![(1, "self"), (2, "gw2")]);
        assert_eq!(
            snapshot
                .broker(3, SecurityProtocol::Plaintext, &own)
                .map(|b| b.port),
            None
        );
        assert_eq!(
            snapshot
                .broker(3, SecurityProtocol::Ssl, &own)
                .map(|b| b.port),
            Some(9093)
        );
    }

    #[test]
    fn a_single_gateway_owns_everything_as_node_one() {
        let membership = Membership::single();
        let snapshot = membership.snapshot();
        assert_eq!(snapshot.node_id(), SINGLE_NODE_ID);
        assert!(snapshot.coordinates("any"));
        assert!(snapshot.leads("orders", 7));
        assert_eq!(snapshot.controller(), SINGLE_NODE_ID);
        assert!(!membership.is_shared());
    }

    /// Two memberships on one fake registry agree on every owner once both heard each other.
    #[tokio::test]
    async fn two_gateways_on_one_registry_agree() {
        let registry = FakeRegistry::default();
        let config = MembershipConfig {
            node_id: 1,
            heartbeat_interval: Duration::from_millis(10),
            lease: Duration::from_millis(100),
        };
        let one = Membership::shared(config, node(1));
        let two = Membership::shared(
            MembershipConfig {
                node_id: 2,
                ..config
            },
            node(2),
        );
        for _ in 0..3 {
            one.beat(&registry).await.unwrap();
            two.beat(&registry).await.unwrap();
            registry.advance(Duration::from_millis(15));
        }
        let (left, right) = (one.snapshot(), two.snapshot());
        assert_eq!(left.eligible_ids(), &[1, 2]);
        assert_eq!(right.eligible_ids(), &[1, 2]);
        for group in 0..50 {
            let group = format!("g{group}");
            assert_eq!(left.coordinator(&group), right.coordinator(&group));
            assert_ne!(left.coordinates(&group), right.coordinates(&group));
        }

        two.leave(&registry).await;
        let after = one.beat(&registry).await.unwrap();
        assert_eq!(after.live_ids().collect::<Vec<_>>(), vec![1]);
        assert!(after.coordinates("g1") && after.coordinates("g2"));
    }

    #[derive(Default)]
    struct FakeRegistry {
        log: Mutex<Vec<(u64, Vec<u8>)>>,
        now: Mutex<u64>,
    }

    impl FakeRegistry {
        fn advance(&self, by: Duration) {
            *self.now.lock().unwrap() += micros(by);
        }
    }

    #[async_trait]
    impl Registry for FakeRegistry {
        async fn announce(&self, payload: Vec<u8>) -> Result<(), BridgeError> {
            let now = *self.now.lock().unwrap();
            self.log.lock().unwrap().push((now, payload));
            Ok(())
        }

        async fn read(&self, offset: Option<u64>, count: u32) -> Result<Vec<Heard>, BridgeError> {
            let log = self.log.lock().unwrap();
            let count = count as usize;
            let start = offset.map_or_else(
                || log.len().saturating_sub(count),
                |offset| usize::try_from(offset).unwrap().min(log.len()),
            );
            Ok(log[start..]
                .iter()
                .take(count)
                .enumerate()
                .map(|(at, (timestamp, payload))| Heard {
                    offset: (start + at) as u64,
                    timestamp: *timestamp,
                    payload: Bytes::from(payload.clone()),
                })
                .collect())
        }
    }
}
