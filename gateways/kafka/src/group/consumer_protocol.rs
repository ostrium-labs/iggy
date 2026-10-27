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

//! KIP-848 consumer groups: the `consumer` group protocol.
//!
//! The coordinator computes every assignment itself (the `uniform` and `range` server assignors),
//! and members converge on it one heartbeat at a time, as in Kafka's `GroupMetadataManager` and
//! `CurrentAssignmentBuilder`:
//!
//! - The **group epoch** goes up whenever the membership, a subscription, or the partitions of a
//!   subscribed topic change. The **target assignment** is recomputed for the new group epoch,
//!   which becomes the **assignment epoch**.
//! - Each **member epoch** catches up with the assignment epoch through reconciliation. A member
//!   first revokes what the target takes from it, staying at its epoch (`UnrevokedPartitions`)
//!   until a heartbeat reports those partitions released; it then moves to the target epoch and
//!   receives the partitions no other member still holds (`UnreleasedPartitions`), and the rest
//!   as their owners let go (`Stable`).
//! - A partition is never owned by two members at once: ownership (the partition's epoch) is
//!   held from assignment until the owner reports it revoked.
//!
//! Everything here is synchronous and takes the time as an argument, like the classic
//! state machine in `state.rs`. Expiry runs when a request touches the group.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use regex::Regex;
use tokio::time::Instant;
use uuid::Uuid;

use crate::protocol::api::{
    ERROR_FENCED_MEMBER_EPOCH, ERROR_GROUP_MAX_SIZE_REACHED, ERROR_INVALID_REGULAR_EXPRESSION,
    ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_UNKNOWN_MEMBER_ID, ERROR_UNRELEASED_INSTANCE_ID,
    ERROR_UNSUPPORTED_ASSIGNOR,
};

/// Kafka's `group.consumer.heartbeat.interval.ms` default.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// Kafka's `group.consumer.session.timeout.ms` default.
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(45);
/// Member epoch a member sends to join, and the epoch of a member before its first assignment.
pub const JOIN_EPOCH: i32 = 0;
/// Member epoch a member sends to leave.
pub const LEAVE_EPOCH: i32 = -1;
/// Member epoch a static member sends to leave and come back as the same instance.
pub const STATIC_LEAVE_EPOCH: i32 = -2;

/// Partitions per topic id. Ordered, so assignments and responses are deterministic.
pub type Assignment = BTreeMap<Uuid, BTreeSet<i32>>;

/// A topic as the assignors see it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicMeta {
    pub name: String,
    pub id: Uuid,
    pub partitions: u32,
}

/// The topics the group subscribes to, by name, as the gateway resolved them for one request.
pub type TopicsImage = BTreeMap<String, TopicMeta>;

/// The server-side assignors, by the names Kafka gives them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Assignor {
    /// Kafka's default: balanced, sticky, any mix of subscriptions.
    #[default]
    Uniform,
    /// Contiguous ranges per topic, so co-partitioned topics are assigned alike.
    Range,
}

impl Assignor {
    /// The assignor `name` selects, or `None` when the gateway has no such assignor.
    #[must_use]
    pub fn named(name: &str) -> Option<Self> {
        match name {
            "uniform" => Some(Self::Uniform),
            "range" => Some(Self::Range),
            _ => None,
        }
    }

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Range => "range",
        }
    }
}

/// Where a member is in reconciling its assignment with the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    /// Owns its whole target assignment.
    Stable,
    /// Must give up partitions before it moves to the target epoch.
    UnrevokedPartitions,
    /// At the target epoch, waiting for other members to release partitions it is to get.
    UnreleasedPartitions,
}

/// One `ConsumerGroupHeartbeat`, decoded.
#[derive(Debug, Clone, Default)]
pub struct Heartbeat {
    pub version: i16,
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    /// -1: unchanged.
    pub rebalance_timeout_ms: i32,
    /// `None`: unchanged.
    pub subscribed_topic_names: Option<Vec<String>>,
    /// `None`: unchanged. `Some("")` clears it.
    pub subscribed_topic_regex: Option<String>,
    /// `None`: unchanged.
    pub server_assignor: Option<String>,
    /// `None`: unchanged. The partitions the member owns, by topic id.
    pub owned: Option<Assignment>,
    pub client_id: String,
    pub client_host: String,
}

/// The coordinator's answer to one heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatAnswer {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    /// `None` when the member's assignment did not change and the request was not a full one.
    pub assignment: Option<Assignment>,
}

impl HeartbeatAnswer {
    fn error(error_code: i16, message: impl Into<String>) -> Self {
        Self {
            error_code,
            error_message: Some(message.into()),
            member_id: None,
            member_epoch: 0,
            heartbeat_interval_ms: 0,
            assignment: None,
        }
    }
}

/// One member of a consumer group.
#[derive(Debug, Clone)]
pub struct Member {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub rebalance_timeout: Duration,
    pub subscribed_names: BTreeSet<String>,
    pub subscribed_regex: Option<String>,
    regex: Option<Regex>,
    pub server_assignor: Option<Assignor>,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: MemberState,
    /// What the member may use now.
    pub assigned: Assignment,
    /// What it must give up before moving on.
    pub pending_revocation: Assignment,
    session_deadline: Instant,
    /// Set while the member owes a revocation: past it, the member is fenced.
    revocation_deadline: Option<Instant>,
    /// A static member that left with epoch -2: its assignment waits for the instance to return.
    static_left: bool,
}

impl Member {
    fn subscribes_to(&self, topic: &str) -> bool {
        self.subscribed_names.contains(topic)
            || self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(topic))
    }
}

/// `ConsumerGroupDescribe` state names (`ConsumerGroup.ConsumerGroupState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    Assigning,
    Reconciling,
    Stable,
}

impl GroupState {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::Stable => "Stable",
        }
    }
}

/// One consumer group.
#[derive(Debug, Clone, Default)]
pub struct ConsumerGroup {
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub members: BTreeMap<String, Member>,
    /// Instance id to member id.
    static_members: HashMap<String, String>,
    /// The target assignment of each member, at `assignment_epoch`.
    pub target: BTreeMap<String, Assignment>,
    /// Owner and owner's epoch of every partition some member holds or still has to revoke.
    owners: HashMap<(Uuid, i32), (String, i32)>,
    /// The subscribed topics the target was computed from.
    image: TopicsImage,
    pub assignor: Assignor,
}

/// Limits the coordinator enforces.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_members: usize,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_members: 1_000,
            session_timeout: SESSION_TIMEOUT,
            heartbeat_interval: HEARTBEAT_INTERVAL,
        }
    }
}

/// What a request has to resolve before the group is touched: every topic name some member
/// (or the request) subscribes to, and whether some subscription is a regex, which needs every
/// topic.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Interest {
    pub names: BTreeSet<String>,
    pub needs_all_topics: bool,
}

impl ConsumerGroup {
    /// The topics the group saw last.
    #[must_use]
    pub const fn image(&self) -> &TopicsImage {
        &self.image
    }

    /// Whether no member is in the group, static members waiting to return included.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    #[must_use]
    pub fn state(&self) -> GroupState {
        if self.members.is_empty() {
            GroupState::Empty
        } else if self.group_epoch > self.assignment_epoch {
            GroupState::Assigning
        } else if self.members.values().any(|member| {
            member.member_epoch != self.assignment_epoch || member.state != MemberState::Stable
        }) {
            GroupState::Reconciling
        } else {
            GroupState::Stable
        }
    }

    /// The topics this group, and `request`, need resolved.
    #[must_use]
    pub fn interest(&self, request: Option<&Heartbeat>) -> Interest {
        let mut interest = Interest::default();
        for member in self.members.values() {
            interest
                .names
                .extend(member.subscribed_names.iter().cloned());
            interest.needs_all_topics |= member.regex.is_some();
        }
        if let Some(request) = request {
            if let Some(names) = &request.subscribed_topic_names {
                interest.names.extend(names.iter().cloned());
            }
            interest.needs_all_topics |= request
                .subscribed_topic_regex
                .as_ref()
                .is_some_and(|regex| !regex.is_empty());
        }
        interest
    }

    /// Fences members whose session ended or whose revocation is overdue. Returns whether any
    /// left.
    pub fn expire(&mut self, now: Instant) -> bool {
        let expired: Vec<String> = self
            .members
            .values()
            .filter(|member| {
                member.session_deadline <= now
                    || member
                        .revocation_deadline
                        .is_some_and(|deadline| deadline <= now)
            })
            .map(|member| member.member_id.clone())
            .collect();
        for member_id in &expired {
            tracing::debug!(
                member_id,
                "consumer group member fenced: session or revocation expired"
            );
            self.remove_member(member_id);
        }
        if !expired.is_empty() {
            self.group_epoch += 1;
        }
        !expired.is_empty()
    }

    fn remove_member(&mut self, member_id: &str) {
        if let Some(member) = self.members.remove(member_id) {
            if let Some(instance) = &member.instance_id
                && self.static_members.get(instance).map(String::as_str) == Some(member_id)
            {
                self.static_members.remove(instance);
            }
            self.release_all(&member);
        }
        self.target.remove(member_id);
    }

    fn release_all(&mut self, member: &Member) {
        for (topic, partitions) in member.assigned.iter().chain(&member.pending_revocation) {
            for partition in partitions {
                if self
                    .owners
                    .get(&(*topic, *partition))
                    .is_some_and(|(owner, _)| owner == &member.member_id)
                {
                    self.owners.remove(&(*topic, *partition));
                }
            }
        }
    }

    /// Handles one heartbeat against `image`, the subscribed topics resolved for it.
    pub fn heartbeat(
        &mut self,
        request: &Heartbeat,
        image: &TopicsImage,
        limits: &Limits,
        now: Instant,
    ) -> HeartbeatAnswer {
        if let Err(answer) = validate(request) {
            return answer;
        }
        self.expire(now);
        let image_changed = self.refresh_image(image);

        match request.member_epoch {
            LEAVE_EPOCH => return self.leave(request),
            STATIC_LEAVE_EPOCH => return self.static_leave(request, now),
            _ => {}
        }

        let joining = request.member_epoch == JOIN_EPOCH;
        let member_id = if request.member_id.is_empty() {
            // v0 only: validation refuses an empty id from v1, where the client mints it.
            Uuid::new_v4().to_string()
        } else {
            request.member_id.clone()
        };
        let mut bumped = image_changed;
        let full = request.owned.is_some()
            && request.subscribed_topic_names.is_some()
            && request.rebalance_timeout_ms > 0;

        if joining {
            match self.join(request, &member_id, limits, now) {
                Ok(joined) => bumped |= joined,
                Err(answer) => return answer,
            }
        } else {
            let Some(member) = self.members.get(&member_id) else {
                return HeartbeatAnswer::error(
                    ERROR_UNKNOWN_MEMBER_ID,
                    format!(
                        "Member {member_id} is not a member of group {}.",
                        request.group_id
                    ),
                );
            };
            if let Err(answer) = check_epoch(member, request) {
                return answer;
            }
        }

        let before = self
            .members
            .get(&member_id)
            .map(|member| (member.member_epoch, member.assigned.clone()));
        let member = self
            .members
            .get_mut(&member_id)
            .expect("joined or found above");
        member.session_deadline = now + limits.session_timeout;
        bumped |= update_subscription(member, request);
        if bumped {
            self.group_epoch += 1;
        }
        self.pick_assignor();
        if self.group_epoch > self.assignment_epoch {
            self.compute_target();
        }
        let owned = request.owned.clone();
        self.reconcile(&member_id, owned.as_ref(), now);

        let member = &self.members[&member_id];
        let changed = before.as_ref().is_none_or(|(epoch, assigned)| {
            *epoch != member.member_epoch || *assigned != member.assigned
        });
        HeartbeatAnswer {
            error_code: ERROR_NONE,
            error_message: None,
            member_id: Some(member_id.clone()),
            member_epoch: member.member_epoch,
            heartbeat_interval_ms: i32::try_from(limits.heartbeat_interval.as_millis())
                .unwrap_or(i32::MAX),
            assignment: (changed || joining || full).then(|| member.assigned.clone()),
        }
    }

    /// Replaces the image when a subscribed topic appeared, went, or changed its partitions or
    /// id. Returns whether that changes what the group subscribes to.
    fn refresh_image(&mut self, image: &TopicsImage) -> bool {
        let subscribed = |name: &str| {
            self.members
                .values()
                .any(|member| member.subscribes_to(name))
        };
        let relevant = |image: &TopicsImage| -> TopicsImage {
            image
                .iter()
                .filter(|(name, _)| subscribed(name))
                .map(|(name, meta)| (name.clone(), meta.clone()))
                .collect()
        };
        let changed = relevant(image) != relevant(&self.image);
        // Keep every resolved topic, so a subscription change in this same request finds them.
        self.image = image.clone();
        changed && !self.members.is_empty()
    }

    fn join(
        &mut self,
        request: &Heartbeat,
        member_id: &str,
        limits: &Limits,
        now: Instant,
    ) -> Result<bool, HeartbeatAnswer> {
        if let Some(instance) = &request.instance_id
            && let Some(holder) = self.static_members.get(instance).cloned()
            && holder != member_id
        {
            let left = self
                .members
                .get(&holder)
                .is_some_and(|member| member.static_left);
            if !left {
                return Err(HeartbeatAnswer::error(
                    ERROR_UNRELEASED_INSTANCE_ID,
                    format!(
                        "Static member {holder} with instance id {instance} is present in the group."
                    ),
                ));
            }
            // The instance returns under a new member id: it takes over the old member's
            // assignment and epoch, so the group does not rebalance.
            let mut member = self.members.remove(&holder).expect("held");
            let owned: Vec<(Uuid, i32)> = member
                .assigned
                .iter()
                .chain(&member.pending_revocation)
                .flat_map(|(topic, partitions)| partitions.iter().map(|p| (*topic, *p)))
                .collect();
            for key in owned {
                if let Some(owner) = self.owners.get_mut(&key) {
                    owner.0 = member_id.to_string();
                }
            }
            if let Some(target) = self.target.remove(&holder) {
                self.target.insert(member_id.to_string(), target);
            }
            member.member_id = member_id.to_string();
            member.static_left = false;
            member.client_id.clone_from(&request.client_id);
            member.client_host.clone_from(&request.client_host);
            self.static_members
                .insert(instance.clone(), member_id.to_string());
            self.members.insert(member_id.to_string(), member);
            return Ok(false);
        }
        if let Some(existing) = self.members.get(member_id).cloned() {
            // A known member joins again, after it was fenced or lost its state: it gives up what
            // it held and is reconciled from nothing to its target, at no cost to the others.
            self.release_all(&existing);
            let member = self.members.get_mut(member_id).expect("present");
            member.assigned.clear();
            member.pending_revocation.clear();
            member.member_epoch = JOIN_EPOCH;
            member.previous_member_epoch = JOIN_EPOCH;
            member.state = MemberState::Stable;
            member.revocation_deadline = None;
            return Ok(false);
        }
        if self.members.len() >= limits.max_members {
            return Err(HeartbeatAnswer::error(
                ERROR_GROUP_MAX_SIZE_REACHED,
                format!(
                    "The consumer group has reached its maximum capacity of {} members.",
                    limits.max_members
                ),
            ));
        }
        let member = Member {
            member_id: member_id.to_string(),
            instance_id: request.instance_id.clone(),
            rack_id: request.rack_id.clone(),
            client_id: request.client_id.clone(),
            client_host: request.client_host.clone(),
            rebalance_timeout: Duration::from_millis(
                u64::try_from(request.rebalance_timeout_ms).unwrap_or(0),
            ),
            subscribed_names: BTreeSet::new(),
            subscribed_regex: None,
            regex: None,
            server_assignor: None,
            member_epoch: JOIN_EPOCH,
            previous_member_epoch: JOIN_EPOCH,
            state: MemberState::Stable,
            assigned: Assignment::new(),
            pending_revocation: Assignment::new(),
            session_deadline: now,
            revocation_deadline: None,
            static_left: false,
        };
        if let Some(instance) = &request.instance_id {
            self.static_members
                .insert(instance.clone(), member_id.to_string());
        }
        self.members.insert(member_id.to_string(), member);
        Ok(true)
    }

    fn leave(&mut self, request: &Heartbeat) -> HeartbeatAnswer {
        let member_id = request.member_id.clone();
        if !self.members.contains_key(&member_id) {
            return HeartbeatAnswer::error(
                ERROR_UNKNOWN_MEMBER_ID,
                format!(
                    "Member {member_id} is not a member of group {}.",
                    request.group_id
                ),
            );
        }
        self.remove_member(&member_id);
        self.group_epoch += 1;
        if self.members.is_empty() {
            self.target.clear();
            self.assignment_epoch = self.group_epoch;
        } else {
            self.compute_target();
        }
        HeartbeatAnswer {
            error_code: ERROR_NONE,
            error_message: None,
            member_id: Some(member_id),
            member_epoch: LEAVE_EPOCH,
            heartbeat_interval_ms: 0,
            assignment: None,
        }
    }

    fn static_leave(&mut self, request: &Heartbeat, now: Instant) -> HeartbeatAnswer {
        let member_id = request.member_id.clone();
        let Some(member) = self.members.get_mut(&member_id) else {
            return HeartbeatAnswer::error(
                ERROR_UNKNOWN_MEMBER_ID,
                format!(
                    "Member {member_id} is not a member of group {}.",
                    request.group_id
                ),
            );
        };
        if member.instance_id.is_none() {
            return self.leave(request);
        }
        // Kept, with its assignment, until its session ends or its instance returns.
        member.static_left = true;
        member.session_deadline = now + SESSION_TIMEOUT;
        HeartbeatAnswer {
            error_code: ERROR_NONE,
            error_message: None,
            member_id: Some(member_id),
            member_epoch: STATIC_LEAVE_EPOCH,
            heartbeat_interval_ms: 0,
            assignment: None,
        }
    }

    /// The group's assignor: the one most members ask for, `uniform` when none asks.
    fn pick_assignor(&mut self) {
        let mut votes: BTreeMap<&'static str, usize> = BTreeMap::new();
        for assignor in self
            .members
            .values()
            .filter_map(|member| member.server_assignor)
        {
            *votes.entry(assignor.name()).or_default() += 1;
        }
        let chosen = votes
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .and_then(|(name, _)| Assignor::named(name))
            .unwrap_or_default();
        if chosen != self.assignor {
            self.assignor = chosen;
            if !self.members.is_empty() {
                self.group_epoch = self.group_epoch.max(self.assignment_epoch + 1);
            }
        }
    }

    /// Recomputes the target assignment for the current group epoch.
    fn compute_target(&mut self) {
        let members: Vec<(&str, BTreeSet<&TopicMeta>)> = self
            .members
            .values()
            .map(|member| {
                let topics = self
                    .image
                    .values()
                    .filter(|topic| member.subscribes_to(&topic.name))
                    .collect();
                (member.member_id.as_str(), topics)
            })
            .collect();
        let previous = &self.target;
        let target = match self.assignor {
            Assignor::Uniform => uniform(&members, previous),
            Assignor::Range => range(&members),
        };
        self.target = target;
        self.assignment_epoch = self.group_epoch;
    }

    /// Moves `member_id` towards its target, as Kafka's `CurrentAssignmentBuilder` does.
    fn reconcile(&mut self, member_id: &str, owned: Option<&Assignment>, now: Instant) {
        let target_epoch = self.assignment_epoch;
        let target = self.target.get(member_id).cloned().unwrap_or_default();
        let Some(member) = self.members.get(member_id).cloned() else {
            return;
        };
        let proceed = match member.state {
            MemberState::Stable => member.member_epoch != target_epoch,
            // Only once the member reports none of the partitions it had to give up.
            MemberState::UnrevokedPartitions => owned.is_some_and(|owned| {
                member.pending_revocation.iter().all(|(topic, partitions)| {
                    owned
                        .get(topic)
                        .is_none_or(|held| held.is_disjoint(partitions))
                })
            }),
            MemberState::UnreleasedPartitions => true,
        };
        if !proceed {
            return;
        }
        // What it keeps, what it must give up, and what it is to get.
        let current = if member.state == MemberState::UnrevokedPartitions {
            // The revoked partitions are released now.
            for (topic, partitions) in &member.pending_revocation {
                for partition in partitions {
                    if self
                        .owners
                        .get(&(*topic, *partition))
                        .is_some_and(|(owner, _)| owner == member_id)
                    {
                        self.owners.remove(&(*topic, *partition));
                    }
                }
            }
            member.assigned.clone()
        } else {
            member.assigned.clone()
        };
        let kept = intersect(&current, &target);
        let revoke = subtract(&current, &target);
        let wanted = subtract(&target, &current);

        let next = if !revoke.is_empty() {
            Next {
                epoch: member.member_epoch,
                state: MemberState::UnrevokedPartitions,
                assigned: kept,
                pending: revoke,
            }
        } else {
            let mut assigned = kept;
            let mut unreleased = false;
            for (topic, partitions) in &wanted {
                for partition in partitions {
                    let free = self
                        .owners
                        .get(&(*topic, *partition))
                        .is_none_or(|(owner, _)| owner == member_id);
                    if free {
                        assigned.entry(*topic).or_default().insert(*partition);
                    } else {
                        unreleased = true;
                    }
                }
            }
            Next {
                epoch: target_epoch,
                state: if unreleased {
                    MemberState::UnreleasedPartitions
                } else {
                    MemberState::Stable
                },
                assigned,
                pending: Assignment::new(),
            }
        };

        for (topic, partitions) in next.assigned.iter().chain(&next.pending) {
            for partition in partitions {
                self.owners
                    .insert((*topic, *partition), (member_id.to_string(), next.epoch));
            }
        }
        let member = self.members.get_mut(member_id).expect("present");
        if next.epoch != member.member_epoch {
            member.previous_member_epoch = member.member_epoch;
        }
        member.revocation_deadline = match next.state {
            MemberState::UnrevokedPartitions => Some(
                member
                    .revocation_deadline
                    .unwrap_or(now + member.rebalance_timeout),
            ),
            _ => None,
        };
        member.member_epoch = next.epoch;
        member.state = next.state;
        member.assigned = next.assigned;
        member.pending_revocation = next.pending;
    }

    /// Validates an `OffsetCommit` from `member_id` at `member_epoch` (KIP-848: the epoch must
    /// be the member's current one). A commit with epoch -1 and no member id is allowed only into
    /// an empty group, as in Kafka.
    ///
    /// # Errors
    ///
    /// `UNKNOWN_MEMBER_ID`, or `STALE_MEMBER_EPOCH` (v9+, `ILLEGAL_GENERATION` before).
    pub fn validate_commit(
        &self,
        member_id: &str,
        member_epoch: i32,
        api_version: i16,
    ) -> Result<(), i16> {
        if member_epoch < 0 && self.members.is_empty() {
            return Ok(());
        }
        let member = self.members.get(member_id).ok_or(ERROR_UNKNOWN_MEMBER_ID)?;
        if member.member_epoch == member_epoch {
            return Ok(());
        }
        Err(if api_version >= 9 {
            crate::protocol::api::ERROR_STALE_MEMBER_EPOCH
        } else {
            crate::protocol::api::ERROR_ILLEGAL_GENERATION
        })
    }

    /// Topic names the target names, for answers that carry both id and name.
    #[must_use]
    pub fn topic_name(&self, id: &Uuid) -> Option<&str> {
        self.image
            .values()
            .find(|topic| topic.id == *id)
            .map(|topic| topic.name.as_str())
    }
}

struct Next {
    epoch: i32,
    state: MemberState,
    assigned: Assignment,
    pending: Assignment,
}

/// Request checks Kafka makes before it looks at the group
/// (`throwIfConsumerGroupHeartbeatRequestIsInvalid`).
fn validate(request: &Heartbeat) -> Result<(), HeartbeatAnswer> {
    let invalid = |message: &str| Err(HeartbeatAnswer::error(ERROR_INVALID_REQUEST, message));
    if request.group_id.trim().is_empty() {
        return invalid("GroupId can't be empty.");
    }
    if request.instance_id.as_ref().is_some_and(String::is_empty) {
        return invalid("InstanceId can't be empty.");
    }
    if request.rack_id.as_ref().is_some_and(String::is_empty) {
        return invalid("RackId can't be empty.");
    }
    match request.member_epoch {
        JOIN_EPOCH => {
            if request.version >= 1 && request.member_id.trim().is_empty() {
                return invalid("MemberId can't be empty.");
            }
            if request.rebalance_timeout_ms <= 0 {
                return invalid("RebalanceTimeoutMs must be provided in first request.");
            }
            if request.owned.as_ref().is_none_or(|owned| !owned.is_empty()) {
                return invalid("TopicPartitions must be empty when (re-)joining.");
            }
            let names = request.subscribed_topic_names.is_some();
            let regex = request.subscribed_topic_regex.is_some();
            if !names && !regex {
                return invalid(if request.version >= 1 {
                    "SubscribedTopicNames or SubscribedTopicRegex must be set in first request."
                } else {
                    "SubscribedTopicNames must be set in first request."
                });
            }
        }
        STATIC_LEAVE_EPOCH => {
            if request.member_id.trim().is_empty() {
                return invalid("MemberId can't be empty.");
            }
            if request.instance_id.is_none() {
                return invalid("InstanceId can't be null.");
            }
        }
        epoch if epoch < STATIC_LEAVE_EPOCH => return invalid("MemberEpoch is invalid."),
        _ => {
            if request.member_id.trim().is_empty() {
                return invalid("MemberId can't be empty.");
            }
        }
    }
    if let Some(name) = &request.server_assignor
        && Assignor::named(name).is_none()
    {
        return Err(HeartbeatAnswer::error(
            ERROR_UNSUPPORTED_ASSIGNOR,
            format!("ServerAssignor {name} is not supported. Supported assignors: uniform, range."),
        ));
    }
    if let Some(pattern) = &request.subscribed_topic_regex
        && !pattern.is_empty()
        && compile(pattern).is_none()
    {
        return Err(HeartbeatAnswer::error(
            ERROR_INVALID_REGULAR_EXPRESSION,
            format!("SubscribedTopicRegex `{pattern}` is not a valid regular expression."),
        ));
    }
    Ok(())
}

/// Kafka matches a subscription regex against the whole topic name (RE2J `matches`).
fn compile(pattern: &str) -> Option<Regex> {
    Regex::new(&format!("^(?:{pattern})$")).ok()
}

/// Kafka's `throwIfConsumerGroupMemberEpochIsInvalid`: a member may repeat its previous epoch
/// when it lost the answer that moved it on, as long as it owns no more than it was given.
fn check_epoch(member: &Member, request: &Heartbeat) -> Result<(), HeartbeatAnswer> {
    let received = request.member_epoch;
    let fenced = || {
        Err(HeartbeatAnswer::error(
            ERROR_FENCED_MEMBER_EPOCH,
            format!(
                "The consumer group member has a greater member epoch ({received}) than the one \
                 known by the group coordinator ({}). The member must abandon all its partitions \
                 and rejoin.",
                member.member_epoch
            ),
        ))
    };
    if received > member.member_epoch {
        return fenced();
    }
    if received < member.member_epoch {
        let lost_answer = received == member.previous_member_epoch
            && request
                .owned
                .as_ref()
                .is_some_and(|owned| is_subset(owned, &member.assigned));
        if !lost_answer {
            return Err(HeartbeatAnswer::error(
                ERROR_FENCED_MEMBER_EPOCH,
                format!(
                    "The consumer group member has a smaller member epoch ({received}) than the \
                     one known by the group coordinator ({}). The member must abandon all its \
                     partitions and rejoin.",
                    member.member_epoch
                ),
            ));
        }
    }
    Ok(())
}

/// Applies what the request changes. Returns whether the group must rebalance.
fn update_subscription(member: &mut Member, request: &Heartbeat) -> bool {
    let mut changed = false;
    if let Some(names) = &request.subscribed_topic_names {
        let names: BTreeSet<String> = names.iter().cloned().collect();
        if names != member.subscribed_names {
            member.subscribed_names = names;
            changed = true;
        }
    }
    if let Some(pattern) = &request.subscribed_topic_regex {
        let next = (!pattern.is_empty()).then(|| pattern.clone());
        if next != member.subscribed_regex {
            member.regex = next.as_deref().and_then(compile);
            member.subscribed_regex = next;
            changed = true;
        }
    }
    if let Some(name) = &request.server_assignor {
        let assignor = Assignor::named(name);
        if assignor != member.server_assignor {
            member.server_assignor = assignor;
        }
    }
    if request.rebalance_timeout_ms > 0 {
        member.rebalance_timeout =
            Duration::from_millis(u64::try_from(request.rebalance_timeout_ms).unwrap_or(0));
    }
    if request.rack_id.is_some() {
        member.rack_id.clone_from(&request.rack_id);
    }
    changed
}

fn intersect(left: &Assignment, right: &Assignment) -> Assignment {
    left.iter()
        .filter_map(|(topic, partitions)| {
            let common: BTreeSet<i32> = right
                .get(topic)
                .map(|other| partitions.intersection(other).copied().collect())
                .unwrap_or_default();
            (!common.is_empty()).then_some((*topic, common))
        })
        .collect()
}

fn subtract(left: &Assignment, right: &Assignment) -> Assignment {
    left.iter()
        .filter_map(|(topic, partitions)| {
            let rest: BTreeSet<i32> = match right.get(topic) {
                Some(other) => partitions.difference(other).copied().collect(),
                None => partitions.clone(),
            };
            (!rest.is_empty()).then_some((*topic, rest))
        })
        .collect()
}

fn is_subset(small: &Assignment, big: &Assignment) -> bool {
    small.iter().all(|(topic, partitions)| {
        partitions.is_empty()
            || big
                .get(topic)
                .is_some_and(|held| partitions.is_subset(held))
    })
}

/// Partition count as the assignors index it.
fn partitions_of(topic: &TopicMeta) -> impl Iterator<Item = i32> {
    (0..topic.partitions).map(|partition| i32::try_from(partition).unwrap_or(i32::MAX))
}

/// The `uniform` assignor: every subscribed partition to exactly one member subscribed to it,
/// counts as even as the subscriptions allow, and as much of `previous` kept as balance permits.
///
/// Keeps each member's previous partitions up to an even share, hands the rest to the least
/// loaded subscriber, then moves partitions from the most to the least loaded subscriber while
/// that narrows the spread. With one subscription for everyone, the first two steps already
/// balance and the last makes no move.
fn uniform(
    members: &[(&str, BTreeSet<&TopicMeta>)],
    previous: &BTreeMap<String, Assignment>,
) -> BTreeMap<String, Assignment> {
    let mut assignment: BTreeMap<String, Assignment> = members
        .iter()
        .map(|(member, _)| ((*member).to_string(), Assignment::new()))
        .collect();
    let mut loads: HashMap<&str, usize> = members.iter().map(|(member, _)| (*member, 0)).collect();
    let mut subscribers: BTreeMap<Uuid, Vec<&str>> = BTreeMap::new();
    let mut partitions: BTreeMap<Uuid, u32> = BTreeMap::new();
    for (member, topics) in members {
        for topic in topics {
            subscribers.entry(topic.id).or_default().push(member);
            partitions.insert(topic.id, topic.partitions);
        }
    }
    let all: Vec<(Uuid, i32)> = partitions
        .iter()
        .flat_map(|(id, count)| {
            (0..*count).map(move |partition| (*id, i32::try_from(partition).unwrap_or(i32::MAX)))
        })
        .collect();
    let ceiling = if members.is_empty() {
        0
    } else {
        all.len().div_ceil(members.len())
    };
    let mut owner: HashMap<(Uuid, i32), &str> = HashMap::new();

    // Keep what each member had, where it still subscribes, up to the ceiling.
    for (member, _) in members {
        let Some(had) = previous.get(*member) else {
            continue;
        };
        for (topic, held) in had {
            let subscribed = subscribers
                .get(topic)
                .is_some_and(|names| names.contains(member));
            let count = partitions.get(topic).copied().unwrap_or(0);
            if !subscribed {
                continue;
            }
            for partition in held {
                let key = (*topic, *partition);
                let valid = u32::try_from(*partition).is_ok_and(|index| index < count);
                if valid && !owner.contains_key(&key) && loads[member] < ceiling {
                    give(&mut assignment, &mut owner, &mut loads, *member, key);
                }
            }
        }
    }
    // Hand out the rest, each to the least loaded member that subscribes to it.
    for key in &all {
        if owner.contains_key(key) {
            continue;
        }
        let chosen = subscribers[&key.0]
            .iter()
            .copied()
            .min_by_key(|member| (loads[member], *member));
        if let Some(chosen) = chosen {
            give(&mut assignment, &mut owner, &mut loads, chosen, *key);
        }
    }
    // Narrow the spread where subscriptions differ: a partition moves to a less loaded
    // subscriber while that one has at least two fewer.
    loop {
        let mut moved = false;
        for key in &all {
            let Some(from) = owner.get(key).copied() else {
                continue;
            };
            let Some(to) = subscribers[&key.0]
                .iter()
                .copied()
                .filter(|member| *member != from)
                .min_by_key(|member| (loads[member], *member))
            else {
                continue;
            };
            if loads[from] > loads[to] + 1 {
                if let Some(topics) = assignment.get_mut(from)
                    && let Some(held) = topics.get_mut(&key.0)
                {
                    held.remove(&key.1);
                    if held.is_empty() {
                        topics.remove(&key.0);
                    }
                }
                *loads.entry(from).or_default() -= 1;
                give(&mut assignment, &mut owner, &mut loads, to, *key);
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    assignment
}

/// Gives `key` to `member` in `assignment`, and counts it.
fn give<'a>(
    assignment: &mut BTreeMap<String, Assignment>,
    owner: &mut HashMap<(Uuid, i32), &'a str>,
    loads: &mut HashMap<&'a str, usize>,
    member: &'a str,
    key: (Uuid, i32),
) {
    owner.insert(key, member);
    *loads.entry(member).or_default() += 1;
    assignment
        .entry(member.to_string())
        .or_default()
        .entry(key.0)
        .or_default()
        .insert(key.1);
}

/// The `range` assignor: per topic, its subscribers in member id order each take a contiguous
/// range, the first `partitions % members` one partition more.
fn range(members: &[(&str, BTreeSet<&TopicMeta>)]) -> BTreeMap<String, Assignment> {
    let mut assignment: BTreeMap<String, Assignment> = members
        .iter()
        .map(|(member, _)| ((*member).to_string(), Assignment::new()))
        .collect();
    let mut topics: BTreeMap<Uuid, &TopicMeta> = BTreeMap::new();
    for (_, subscribed) in members {
        for topic in subscribed {
            topics.insert(topic.id, topic);
        }
    }
    for (id, topic) in topics {
        let mut subscribers: Vec<&str> = members
            .iter()
            .filter(|(_, subscribed)| subscribed.iter().any(|meta| meta.id == id))
            .map(|(member, _)| *member)
            .collect();
        subscribers.sort_unstable();
        if subscribers.is_empty() {
            continue;
        }
        let partitions = topic.partitions as usize;
        let per_member = partitions / subscribers.len();
        let extra = partitions % subscribers.len();
        let mut next = 0usize;
        for (index, member) in subscribers.iter().enumerate() {
            let take = per_member + usize::from(index < extra);
            let range: BTreeSet<i32> = (next..next + take)
                .map(|partition| i32::try_from(partition).unwrap_or(i32::MAX))
                .collect();
            next += take;
            if !range.is_empty() {
                assignment
                    .entry((*member).to_string())
                    .or_default()
                    .insert(id, range);
            }
        }
    }
    assignment
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(name: &str, partitions: u32) -> TopicMeta {
        TopicMeta {
            name: name.to_string(),
            id: crate::topic_id::derive_topic_id("kafka", partitions, name.len() as u64),
            partitions,
        }
    }

    fn image(topics: &[&TopicMeta]) -> TopicsImage {
        topics
            .iter()
            .map(|topic| (topic.name.clone(), (*topic).clone()))
            .collect()
    }

    fn join(member: &str, topics: &[&str]) -> Heartbeat {
        Heartbeat {
            version: 1,
            group_id: "g".to_string(),
            member_id: member.to_string(),
            member_epoch: JOIN_EPOCH,
            rebalance_timeout_ms: 300_000,
            subscribed_topic_names: Some(topics.iter().map(|t| (*t).to_string()).collect()),
            owned: Some(Assignment::new()),
            ..Heartbeat::default()
        }
    }

    fn beat(member: &str, epoch: i32, owned: &Assignment) -> Heartbeat {
        Heartbeat {
            version: 1,
            group_id: "g".to_string(),
            member_id: member.to_string(),
            member_epoch: epoch,
            rebalance_timeout_ms: -1,
            owned: Some(owned.clone()),
            ..Heartbeat::default()
        }
    }

    fn size(assignment: &Assignment) -> usize {
        assignment.values().map(BTreeSet::len).sum()
    }

    /// Drives every member with heartbeats that report what it was given, until all are Stable.
    fn settle(
        group: &mut ConsumerGroup,
        image: &TopicsImage,
        now: Instant,
    ) -> BTreeMap<String, (i32, Assignment)> {
        let limits = Limits::default();
        let mut held: BTreeMap<String, (i32, Assignment)> = group
            .members
            .values()
            .map(|member| {
                (
                    member.member_id.clone(),
                    (member.member_epoch, member.assigned.clone()),
                )
            })
            .collect();
        for _ in 0..20 {
            for (member, (epoch, owned)) in &mut held {
                let answer = group.heartbeat(&beat(member, *epoch, owned), image, &limits, now);
                assert_eq!(answer.error_code, ERROR_NONE, "{answer:?}");
                *epoch = answer.member_epoch;
                if let Some(assignment) = answer.assignment {
                    *owned = assignment;
                }
            }
            if group.state() == GroupState::Stable {
                break;
            }
        }
        held
    }

    #[test]
    fn given_one_member_it_should_get_every_partition_at_epoch_one() {
        let orders = topic("orders", 3);
        let image = image(&[&orders]);
        let mut group = ConsumerGroup::default();
        let answer = group.heartbeat(
            &join("a", &["orders"]),
            &image,
            &Limits::default(),
            Instant::now(),
        );
        assert_eq!(answer.error_code, ERROR_NONE);
        assert_eq!(answer.member_epoch, 1);
        let assignment = answer.assignment.expect("an assignment");
        assert_eq!(assignment[&orders.id], BTreeSet::from([0, 1, 2]));
        assert_eq!(group.state(), GroupState::Stable);
    }

    #[test]
    fn given_a_second_member_partitions_should_move_only_after_revocation() {
        let orders = topic("orders", 4);
        let image = image(&[&orders]);
        let limits = Limits::default();
        let now = Instant::now();
        let mut group = ConsumerGroup::default();
        let first = group.heartbeat(&join("a", &["orders"]), &image, &limits, now);
        let a_owned = first.assignment.clone().expect("assigned");

        let second = group.heartbeat(&join("b", &["orders"]), &image, &limits, now);
        assert_eq!(second.member_epoch, 2);
        // a still holds everything: b may take nothing yet.
        assert_eq!(size(&second.assignment.clone().unwrap_or_default()), 0);
        assert_eq!(group.state(), GroupState::Reconciling);

        // a learns it must give up two partitions, staying at its epoch.
        let a_told = group.heartbeat(&beat("a", 1, &a_owned), &image, &limits, now);
        assert_eq!(a_told.member_epoch, 1);
        let a_kept = a_told.assignment.expect("revocation");
        assert_eq!(size(&a_kept), 2);
        assert_eq!(group.members["a"].state, MemberState::UnrevokedPartitions);

        // a reports the revocation and moves to epoch 2.
        let a_done = group.heartbeat(&beat("a", 1, &a_kept), &image, &limits, now);
        assert_eq!(a_done.member_epoch, 2);

        // b now gets the released partitions.
        let b_owned = Assignment::new();
        let b_next = group.heartbeat(&beat("b", 2, &b_owned), &image, &limits, now);
        let b_assigned = b_next.assignment.expect("released partitions");
        assert_eq!(size(&b_assigned), 2);
        assert!(intersect(&b_assigned, &a_kept).is_empty());
        assert_eq!(group.state(), GroupState::Stable);
    }

    #[test]
    fn given_any_membership_no_partition_should_have_two_owners() {
        let orders = topic("orders", 7);
        let audit = topic("audit", 2);
        let image = image(&[&orders, &audit]);
        let limits = Limits::default();
        let now = Instant::now();
        let mut group = ConsumerGroup::default();
        for member in ["a", "b", "c"] {
            let topics: &[&str] = if member == "c" {
                &["audit"]
            } else {
                &["orders", "audit"]
            };
            let answer = group.heartbeat(&join(member, topics), &image, &limits, now);
            assert_eq!(answer.error_code, ERROR_NONE);
        }
        let held = settle(&mut group, &image, now);
        let mut seen = BTreeSet::new();
        for (_, owned) in held.values() {
            for (topic, partitions) in owned {
                for partition in partitions {
                    assert!(seen.insert((*topic, *partition)), "two owners");
                }
            }
        }
        assert_eq!(seen.len(), 9);
        // c subscribes to audit only, so it holds audit partitions and nothing else.
        assert!(held["c"].1.keys().all(|topic| *topic == audit.id));
    }

    #[test]
    fn given_a_leaving_member_the_rest_should_take_its_partitions() {
        let orders = topic("orders", 4);
        let image = image(&[&orders]);
        let limits = Limits::default();
        let now = Instant::now();
        let mut group = ConsumerGroup::default();
        group.heartbeat(&join("a", &["orders"]), &image, &limits, now);
        group.heartbeat(&join("b", &["orders"]), &image, &limits, now);
        settle(&mut group, &image, now);
        let left = group.heartbeat(
            &beat("b", group.members["b"].member_epoch, &Assignment::new()).with_epoch(LEAVE_EPOCH),
            &image,
            &limits,
            now,
        );
        assert_eq!(left.member_epoch, LEAVE_EPOCH);
        let held = settle(&mut group, &image, now);
        assert_eq!(size(&held["a"].1), 4);
    }

    #[test]
    fn given_a_wrong_epoch_the_member_should_be_fenced() {
        let orders = topic("orders", 1);
        let image = image(&[&orders]);
        let limits = Limits::default();
        let now = Instant::now();
        let mut group = ConsumerGroup::default();
        group.heartbeat(&join("a", &["orders"]), &image, &limits, now);
        let ahead = group.heartbeat(&beat("a", 5, &Assignment::new()), &image, &limits, now);
        assert_eq!(ahead.error_code, ERROR_FENCED_MEMBER_EPOCH);
        let unknown = group.heartbeat(&beat("z", 1, &Assignment::new()), &image, &limits, now);
        assert_eq!(unknown.error_code, ERROR_UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn given_an_expired_session_the_member_should_leave_the_group() {
        let orders = topic("orders", 2);
        let image = image(&[&orders]);
        let limits = Limits::default();
        let now = Instant::now();
        let mut group = ConsumerGroup::default();
        group.heartbeat(&join("a", &["orders"]), &image, &limits, now);
        group.heartbeat(&join("b", &["orders"]), &image, &limits, now);
        settle(&mut group, &image, now);
        let later = now + SESSION_TIMEOUT + Duration::from_secs(1);
        let a = &group.members["a"];
        let answer = group.clone().heartbeat(
            &beat("a", a.member_epoch, &a.assigned.clone()),
            &image,
            &limits,
            later,
        );
        assert_eq!(answer.error_code, ERROR_UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn given_a_static_member_that_returns_it_should_keep_its_assignment() {
        let orders = topic("orders", 2);
        let image = image(&[&orders]);
        let limits = Limits::default();
        let now = Instant::now();
        let mut group = ConsumerGroup::default();
        let mut request = join("a", &["orders"]);
        request.instance_id = Some("i-1".to_string());
        let first = group.heartbeat(&request, &image, &limits, now);
        let mut leave = beat(
            "a",
            first.member_epoch,
            &first.assignment.clone().unwrap_or_default(),
        );
        leave.member_epoch = STATIC_LEAVE_EPOCH;
        leave.instance_id = Some("i-1".to_string());
        assert_eq!(
            group.heartbeat(&leave, &image, &limits, now).member_epoch,
            STATIC_LEAVE_EPOCH
        );

        let mut back = join("a2", &["orders"]);
        back.instance_id = Some("i-1".to_string());
        let returned = group.heartbeat(&back, &image, &limits, now);
        assert_eq!(returned.error_code, ERROR_NONE);
        assert_eq!(returned.member_epoch, first.member_epoch);
        assert_eq!(returned.assignment, first.assignment);

        let mut thief = join("a3", &["orders"]);
        thief.instance_id = Some("i-1".to_string());
        assert_eq!(
            group.heartbeat(&thief, &image, &limits, now).error_code,
            ERROR_UNRELEASED_INSTANCE_ID
        );
    }

    #[test]
    fn given_a_new_partition_the_group_epoch_should_go_up() {
        let orders = topic("orders", 2);
        let limits = Limits::default();
        let now = Instant::now();
        let mut group = ConsumerGroup::default();
        let first = group.heartbeat(&join("a", &["orders"]), &image(&[&orders]), &limits, now);
        let grown = TopicMeta {
            partitions: 3,
            ..orders.clone()
        };
        let next = group.heartbeat(
            &beat(
                "a",
                first.member_epoch,
                &first.assignment.unwrap_or_default(),
            ),
            &image(&[&grown]),
            &limits,
            now,
        );
        assert_eq!(next.member_epoch, 2);
        assert_eq!(size(&next.assignment.expect("grown")), 3);
    }

    #[test]
    fn given_a_regex_subscription_it_should_match_whole_names() {
        let orders = topic("orders", 1);
        let orders_eu = topic("orders-eu", 1);
        let audit = topic("audit", 1);
        let limits = Limits::default();
        let mut group = ConsumerGroup::default();
        let mut request = join("a", &[]);
        request.subscribed_topic_names = None;
        request.subscribed_topic_regex = Some("orders.*".to_string());
        let answer = group.heartbeat(
            &request,
            &image(&[&orders, &orders_eu, &audit]),
            &limits,
            Instant::now(),
        );
        let assignment = answer.assignment.expect("assigned");
        assert!(assignment.contains_key(&orders.id));
        assert!(assignment.contains_key(&orders_eu.id));
        assert!(!assignment.contains_key(&audit.id));

        let mut bad = join("b", &[]);
        bad.subscribed_topic_regex = Some("(".to_string());
        assert_eq!(
            group
                .heartbeat(&bad, &TopicsImage::new(), &limits, Instant::now())
                .error_code,
            ERROR_INVALID_REGULAR_EXPRESSION
        );
    }

    #[test]
    fn given_invalid_requests_they_should_be_refused_as_kafka_refuses_them() {
        let mut group = ConsumerGroup::default();
        let limits = Limits::default();
        let now = Instant::now();
        let image = TopicsImage::new();
        let mut no_timeout = join("a", &["orders"]);
        no_timeout.rebalance_timeout_ms = -1;
        assert_eq!(
            group
                .heartbeat(&no_timeout, &image, &limits, now)
                .error_code,
            ERROR_INVALID_REQUEST
        );
        let mut no_id = join("", &["orders"]);
        no_id.version = 1;
        assert_eq!(
            group.heartbeat(&no_id, &image, &limits, now).error_code,
            ERROR_INVALID_REQUEST
        );
        no_id.version = 0;
        let minted = group.heartbeat(&no_id, &image, &limits, now);
        assert_eq!(minted.error_code, ERROR_NONE);
        assert!(minted.member_id.is_some_and(|id| !id.is_empty()));
        let mut assignor = join("c", &["orders"]);
        assignor.server_assignor = Some("sticky".to_string());
        assert_eq!(
            group.heartbeat(&assignor, &image, &limits, now).error_code,
            ERROR_UNSUPPORTED_ASSIGNOR
        );
    }

    #[test]
    fn range_should_give_contiguous_ranges_in_member_order() {
        let orders = topic("orders", 5);
        let members = vec![
            ("b", BTreeSet::from([&orders])),
            ("a", BTreeSet::from([&orders])),
        ];
        let assignment = range(&members);
        assert_eq!(assignment["a"][&orders.id], BTreeSet::from([0, 1, 2]));
        assert_eq!(assignment["b"][&orders.id], BTreeSet::from([3, 4]));
    }

    #[test]
    fn uniform_should_balance_and_keep_what_it_can() {
        let orders = topic("orders", 6);
        let two = vec![
            ("a", BTreeSet::from([&orders])),
            ("b", BTreeSet::from([&orders])),
        ];
        let first = uniform(&two, &BTreeMap::new());
        assert_eq!(size(&first["a"]), 3);
        assert_eq!(size(&first["b"]), 3);
        let three = vec![
            ("a", BTreeSet::from([&orders])),
            ("b", BTreeSet::from([&orders])),
            ("c", BTreeSet::from([&orders])),
        ];
        let second = uniform(&three, &first);
        for member in ["a", "b", "c"] {
            assert_eq!(size(&second[member]), 2, "{member}");
        }
        // a and b each keep two of their three.
        assert_eq!(size(&intersect(&second["a"], &first["a"])), 2);
        assert_eq!(size(&intersect(&second["b"], &first["b"])), 2);
    }

    #[test]
    fn given_an_epoch_commit_it_should_match_the_members_epoch() {
        let orders = topic("orders", 1);
        let mut group = ConsumerGroup::default();
        let answer = group.heartbeat(
            &join("a", &["orders"]),
            &image(&[&orders]),
            &Limits::default(),
            Instant::now(),
        );
        assert_eq!(group.validate_commit("a", answer.member_epoch, 9), Ok(()));
        assert_eq!(
            group.validate_commit("a", answer.member_epoch + 1, 9),
            Err(crate::protocol::api::ERROR_STALE_MEMBER_EPOCH)
        );
        assert_eq!(
            group.validate_commit("zz", 1, 9),
            Err(ERROR_UNKNOWN_MEMBER_ID)
        );
        assert_eq!(
            group.validate_commit("", -1, 9),
            Err(ERROR_UNKNOWN_MEMBER_ID)
        );
        assert_eq!(ConsumerGroup::default().validate_commit("", -1, 9), Ok(()));
    }

    impl Heartbeat {
        fn with_epoch(mut self, epoch: i32) -> Self {
            self.member_epoch = epoch;
            self
        }
    }
}
