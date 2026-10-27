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

//! [`GroupCoordinator`]'s side of KIP-848: the consumer groups next to the classic ones.
//!
//! One group id names one kind of group at a time, as in Kafka with
//! `group.consumer.migration.policy=disabled`: a consumer heartbeat for a classic group that has
//! members answers `GROUP_ID_NOT_FOUND`, and a `JoinGroup` for a consumer group that has members
//! answers `INCONSISTENT_GROUP_PROTOCOL`. An empty group of either kind gives way, so a group
//! moves between protocols by stopping its consumers and starting them with the other protocol,
//! and keeps its committed offsets, which live in Iggy. Kafka's online migration (converting a
//! live group one member at a time) is not built.
//!
//! The lock order is the classic map, then the consumer map.

use std::collections::HashSet;

use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use super::consumer_protocol::{
    ConsumerGroup, GroupState, Heartbeat, HeartbeatAnswer, Interest, Limits, TopicsImage,
};
use super::{CommitRequest, GroupCoordinator, GroupRemoval, OffsetDeletion, Subscribed, state};
use crate::protocol::api::{
    ERROR_COORDINATOR_LOAD_IN_PROGRESS, ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_GROUP_ID_NOT_FOUND,
    ERROR_NONE,
};

/// One consumer group as `ConsumerGroupDescribe` and `ListGroups` see it.
#[derive(Debug, Clone)]
pub struct ConsumerGroupSnapshot {
    pub group_id: StrBytes,
    pub state: GroupState,
    pub group: ConsumerGroup,
}

impl GroupCoordinator {
    fn consumer_limits(&self) -> Limits {
        Limits {
            max_members: self.config.max_members_per_group,
            ..Limits::default()
        }
    }

    /// The topics a heartbeat for `request`'s group has to resolve first.
    pub async fn consumer_interest(&self, request: &Heartbeat) -> Interest {
        let consumer = self.consumer.lock().await;
        let group_id = StrBytes::from_string(request.group_id.clone());
        consumer.get(&group_id).map_or_else(
            || ConsumerGroup::default().interest(Some(request)),
            |group| group.interest(Some(request)),
        )
    }

    /// Handles one `ConsumerGroupHeartbeat`. `image` holds the subscribed topics the handler
    /// resolved; `None` (Iggy could not answer) keeps what the group saw last, so members keep
    /// their partitions through an Iggy hiccup.
    pub async fn consumer_heartbeat(
        &self,
        request: &Heartbeat,
        image: Option<TopicsImage>,
    ) -> HeartbeatAnswer {
        let now = Instant::now();
        let group_id = StrBytes::from_string(request.group_id.clone());
        let mut groups = self.groups.lock().await;
        if state::group_removal(&mut groups, &group_id, now) == GroupRemoval::Live {
            return refused(
                ERROR_GROUP_ID_NOT_FOUND,
                format!("Group {} is not a consumer group.", request.group_id),
            );
        }
        if self.offset_work.deleting(&group_id) {
            return refused(
                ERROR_COORDINATOR_LOAD_IN_PROGRESS,
                "The group's offsets are being deleted.".to_string(),
            );
        }
        let mut consumer = self.consumer.lock().await;
        if !consumer.contains_key(&group_id) {
            if consumer.len() + groups.len() >= self.config.max_groups {
                return refused(
                    ERROR_COORDINATOR_NOT_AVAILABLE,
                    "This coordinator holds as many groups as it may.".to_string(),
                );
            }
            // An empty classic group of that id gives way.
            state::remove_empty(&mut groups, &group_id, now);
        }
        drop(groups);
        let limits = self.consumer_limits();
        let group = consumer.entry(group_id.clone()).or_default();
        let image = image.unwrap_or_else(|| group.image().clone());
        let answer = group.heartbeat(request, &image, &limits, now);
        if group.is_empty() {
            consumer.remove(&group_id);
        }
        answer
    }

    /// Whether `group_id` is a consumer group with members, which a classic `JoinGroup` may not
    /// join. An empty one is removed so the classic group can take the id.
    pub(super) async fn consumer_group_blocks_join(&self, group_id: &StrBytes) -> bool {
        let mut consumer = self.consumer.lock().await;
        let now = Instant::now();
        let Some(group) = consumer.get_mut(group_id) else {
            return false;
        };
        group.expire(now);
        if group.is_empty() {
            consumer.remove(group_id);
            return false;
        }
        true
    }

    /// `Some` with the commit's answer when `group_id` is a consumer group: the member must be
    /// there at the epoch it names (KIP-848's `STALE_MEMBER_EPOCH`). `None` for any other group.
    pub(super) async fn consumer_commit(&self, request: &CommitRequest) -> Option<i16> {
        let mut consumer = self.consumer.lock().await;
        let group = consumer.get_mut(&request.group_id)?;
        group.expire(Instant::now());
        Some(
            group
                .validate_commit(
                    request.member_id.as_str(),
                    request.generation_id,
                    request.api_version,
                )
                .err()
                .unwrap_or(ERROR_NONE),
        )
    }

    /// Consumer groups for `ConsumerGroupDescribe`, one entry per id: `Err` with the code and
    /// message Kafka answers when the id is not a consumer group here.
    pub async fn describe_consumer_groups(
        &self,
        group_ids: &[StrBytes],
    ) -> Vec<Result<ConsumerGroupSnapshot, (i16, String)>> {
        let now = Instant::now();
        let mut groups = self.groups.lock().await;
        let mut consumer = self.consumer.lock().await;
        group_ids
            .iter()
            .map(|group_id| {
                if let Some(group) = consumer.get_mut(group_id) {
                    group.expire(now);
                    return Ok(ConsumerGroupSnapshot {
                        group_id: group_id.clone(),
                        state: group.state(),
                        group: group.clone(),
                    });
                }
                let message =
                    if state::group_removal(&mut groups, group_id, now) == GroupRemoval::Absent {
                        format!("Group {group_id} not found.")
                    } else {
                        format!("Group {group_id} is not a consumer group.")
                    };
                Err((ERROR_GROUP_ID_NOT_FOUND, message))
            })
            .collect()
    }

    /// Every consumer group with members, for `ListGroups`. Members whose sessions ended are
    /// fenced first, so a group whose consumers all died is not listed.
    pub async fn list_consumer_groups(&self) -> Vec<ConsumerGroupSnapshot> {
        let mut consumer = self.consumer.lock().await;
        let now = Instant::now();
        for group in consumer.values_mut() {
            group.expire(now);
        }
        consumer.retain(|_, group| !group.is_empty());
        let mut listed: Vec<ConsumerGroupSnapshot> = consumer
            .iter()
            .filter(|(_, group)| !group.is_empty())
            .map(|(group_id, group)| ConsumerGroupSnapshot {
                group_id: group_id.clone(),
                state: group.state(),
                group: group.clone(),
            })
            .collect();
        listed.sort_by(|left, right| left.group_id.cmp(&right.group_id));
        listed
    }

    /// `DeleteGroups` and `OffsetDelete` for a consumer group: `Some(Live)` when it has members.
    pub(super) async fn consumer_removal(&self, group_id: &StrBytes) -> Option<GroupRemoval> {
        let mut consumer = self.consumer.lock().await;
        let group = consumer.get_mut(group_id)?;
        group.expire(Instant::now());
        Some(if group.is_empty() {
            GroupRemoval::Empty
        } else {
            GroupRemoval::Live
        })
    }

    /// `OffsetDelete` for a consumer group: the topics its members subscribe to keep their
    /// offsets. A regex subscription keeps every topic, as `Subscribed::All`.
    pub(super) async fn consumer_offset_deletion(
        &self,
        group_id: &StrBytes,
    ) -> Option<OffsetDeletion> {
        let consumer = self.consumer.lock().await;
        let group = consumer.get(group_id)?;
        let interest = group.interest(None);
        Some(OffsetDeletion::Allowed(if interest.needs_all_topics {
            Subscribed::All
        } else {
            Subscribed::Topics(interest.names.into_iter().collect::<HashSet<_>>())
        }))
    }
}

fn refused(error_code: i16, message: String) -> HeartbeatAnswer {
    HeartbeatAnswer {
        error_code,
        error_message: Some(message),
        member_id: None,
        member_epoch: 0,
        heartbeat_interval_ms: 0,
        assignment: None,
    }
}
