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

//! The KIP-848 scenario (KW-13): two members of a consumer group reconcile one heartbeat at a
//! time, a commit is checked against the member epoch, the group is described and listed, and
//! the members leave. Only what both brokers decide alike is a fact: epochs, codes, partition
//! counts and states, never topic ids or member ids.

use std::time::Duration;

use kafka_protocol::messages::consumer_group_heartbeat_request::TopicPartitions;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    ApiKey, ConsumerGroupDescribeRequest, ConsumerGroupHeartbeatRequest,
    ConsumerGroupHeartbeatResponse, DescribeGroupsRequest, ListGroupsRequest, OffsetCommitRequest,
};
use uuid::Uuid;

use super::{
    Context, Facts, WireClient, await_topic, coordinator_loading, create_topic,
    find_group_coordinator, group, s, topic, version,
};

/// Partitions this scenario's topic has. Three, so the two members' shares differ.
const PARTITIONS: i32 = 3;

/// A member as the scenario drives it: its id, its epoch, and what it owns.
struct Member {
    id: String,
    epoch: i32,
    owned: Vec<(Uuid, Vec<i32>)>,
}

impl Member {
    fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            epoch: 0,
            owned: Vec::new(),
        }
    }

    fn partitions(&self) -> usize {
        self.owned.iter().map(|(_, partitions)| partitions.len()).sum()
    }

    /// Takes what an answer says: the epoch, and the assignment when it carries one.
    fn learn(&mut self, answer: &ConsumerGroupHeartbeatResponse) {
        if answer.error_code == 0 {
            self.epoch = answer.member_epoch;
            if let Some(assignment) = &answer.assignment {
                self.owned = assignment
                    .topic_partitions
                    .iter()
                    .map(|topic| (topic.topic_id, topic.partitions.clone()))
                    .collect();
            }
        }
    }
}

async fn beat(
    client: &mut WireClient,
    group_id: &str,
    name: &str,
    member: &Member,
    epoch: i32,
) -> Result<ConsumerGroupHeartbeatResponse, String> {
    let joining = epoch == 0;
    let request = ConsumerGroupHeartbeatRequest::default()
        .with_group_id(group(group_id))
        .with_member_id(s(&member.id))
        .with_member_epoch(epoch)
        .with_rebalance_timeout_ms(if joining { 30_000 } else { -1 })
        .with_subscribed_topic_names(joining.then(|| vec![topic(name)]))
        .with_server_assignor(joining.then(|| s("uniform")))
        .with_topic_partitions(Some(
            member
                .owned
                .iter()
                .map(|(id, partitions)| {
                    TopicPartitions::default()
                        .with_topic_id(*id)
                        .with_partitions(partitions.clone())
                })
                .collect(),
        ));
    let version = version(client, ApiKey::ConsumerGroupHeartbeat, 1)?;
    client.send(&request, version).await
}

/// Joins `member`, retrying while the coordinator loads.
async fn join(
    client: &mut WireClient,
    group_id: &str,
    name: &str,
    member: &mut Member,
) -> Result<ConsumerGroupHeartbeatResponse, String> {
    for _ in 0..60 {
        let answer = beat(client, group_id, name, member, 0).await?;
        if !coordinator_loading(answer.error_code) {
            member.learn(&answer);
            return Ok(answer);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err("the coordinator never loaded".to_string())
}

/// A heartbeat at the member's epoch, reporting what it owns.
async fn step(
    client: &mut WireClient,
    group_id: &str,
    name: &str,
    member: &mut Member,
) -> Result<ConsumerGroupHeartbeatResponse, String> {
    let answer = beat(client, group_id, name, member, member.epoch).await?;
    member.learn(&answer);
    Ok(answer)
}

async fn commit(
    client: &mut WireClient,
    group_id: &str,
    name: &str,
    member: &Member,
    epoch: i32,
) -> Result<i16, String> {
    let request = OffsetCommitRequest::default()
        .with_group_id(group(group_id))
        .with_generation_id_or_member_epoch(epoch)
        .with_member_id(s(&member.id))
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(topic(name))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(0)
                        .with_committed_leader_epoch(-1),
                ]),
        ]);
    let version = version(client, ApiKey::OffsetCommit, 9)?;
    let response = client.send(&request, version).await?;
    response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
        .map(|partition| partition.error_code)
        .ok_or_else(|| "OffsetCommit answered no partition".to_string())
}

pub async fn consumer_group(
    cx: &Context,
    client: &mut WireClient,
    facts: &mut Facts,
) -> Result<(), String> {
    let name = cx.name("consumer_group", "t");
    let group_id = cx.name("consumer_group", "g");
    create_topic(client, &name, &[]).await?;
    await_topic(client, &name).await?;
    find_group_coordinator(client, &group_id).await?;
    let partitions = usize::try_from(PARTITIONS).unwrap_or(0);

    let mut a = Member::new();
    let joined = join(client, &group_id, &name, &mut a).await?;
    facts.set("a_join.error", joined.error_code);
    facts.set("a_join.epoch", joined.member_epoch);
    facts.set("a_join.member_id_echoed", joined.member_id.as_deref() == Some(a.id.as_str()));
    facts.set("a_join.heartbeat_interval_ms", joined.heartbeat_interval_ms);
    facts.set("a_join.partitions", a.partitions());
    facts.set("a_join.all_partitions", a.partitions() == partitions);

    let mut b = Member::new();
    let joined = join(client, &group_id, &name, &mut b).await?;
    facts.set("b_join.error", joined.error_code);
    facts.set("b_join.epoch", joined.member_epoch);
    // a still owns every partition, so b gets none yet.
    facts.set("b_join.partitions", b.partitions());

    // a learns what it must give up, then reports it given up.
    let told = step(client, &group_id, &name, &mut a).await?;
    facts.set("a_revoke.error", told.error_code);
    facts.set("a_revoke.epoch", told.member_epoch);
    facts.set("a_revoke.partitions", a.partitions());
    let done = step(client, &group_id, &name, &mut a).await?;
    facts.set("a_revoked.error", done.error_code);
    facts.set("a_revoked.epoch", done.member_epoch);
    let given = step(client, &group_id, &name, &mut b).await?;
    facts.set("b_given.error", given.error_code);
    facts.set("b_given.epoch", given.member_epoch);
    facts.set("b_given.partitions", b.partitions());
    facts.set("shares_cover_the_topic", a.partitions() + b.partitions() == partitions);

    let request = ConsumerGroupDescribeRequest::default().with_group_ids(vec![group(&group_id)]);
    let describe_version = version(client, ApiKey::ConsumerGroupDescribe, 0)?;
    let described = client.send(&request, describe_version).await?;
    if let Some(described) = described.groups.first() {
        facts.set("describe.error", described.error_code);
        facts.set("describe.state", described.group_state.as_str());
        facts.set("describe.group_epoch", described.group_epoch);
        facts.set("describe.assignment_epoch", described.assignment_epoch);
        facts.set("describe.assignor", described.assignor_name.as_str());
        facts.set("describe.members", described.members.len());
        let assigned: usize = described
            .members
            .iter()
            .flat_map(|member| &member.assignment.topic_partitions)
            .map(|topic| topic.partitions.len())
            .sum();
        facts.set("describe.assigned_partitions", assigned);
        facts.set(
            "describe.topic_named",
            described
                .members
                .iter()
                .flat_map(|member| &member.assignment.topic_partitions)
                .all(|topic| topic.topic_name.0.as_str() == name),
        );
    }
    let missing = ConsumerGroupDescribeRequest::default()
        .with_group_ids(vec![group(&cx.name("consumer_group", "missing"))]);
    let described = client.send(&missing, describe_version).await?;
    facts.set(
        "describe_missing.error",
        described.groups.first().map_or(-1, |group| group.error_code),
    );

    let list_version = version(client, ApiKey::ListGroups, 5)?;
    let listed = client
        .send(
            &ListGroupsRequest::default().with_types_filter(vec![s("consumer")]),
            list_version,
        )
        .await?;
    let ours = listed
        .groups
        .iter()
        .find(|listed| listed.group_id.0.as_str() == group_id);
    facts.set("list_consumer.found", ours.is_some());
    if let Some(listed) = ours {
        facts.set("list_consumer.type", listed.group_type.as_str());
        facts.set("list_consumer.state", listed.group_state.as_str());
        facts.set("list_consumer.protocol_type", listed.protocol_type.as_str());
    }

    // The classic DescribeGroups of a live consumer group.
    let classic_version = version(client, ApiKey::DescribeGroups, 5)?;
    let classic = client
        .send(
            &DescribeGroupsRequest::default().with_groups(vec![group(&group_id)]),
            classic_version,
        )
        .await?;
    if let Some(described) = classic.groups.first() {
        facts.set("classic_describe.error", described.error_code);
        facts.set("classic_describe.state", described.group_state.as_str());
        facts.set("classic_describe.protocol_type", described.protocol_type.as_str());
        facts.set("classic_describe.members", described.members.len());
    }

    facts.set(
        "commit_current_epoch.error",
        commit(client, &group_id, &name, &a, a.epoch).await?,
    );
    facts.set(
        "commit_stale_epoch.error",
        commit(client, &group_id, &name, &a, a.epoch - 1).await?,
    );

    let fenced = beat(client, &group_id, &name, &a, a.epoch + 5).await?;
    facts.set("fenced.error", fenced.error_code);
    let stranger = Member::new();
    let unknown = beat(client, &group_id, &name, &stranger, 1).await?;
    facts.set("unknown_member.error", unknown.error_code);

    let left = beat(client, &group_id, &name, &b, -1).await?;
    facts.set("b_leave.error", left.error_code);
    facts.set("b_leave.epoch", left.member_epoch);
    // a was fenced above: it rejoins and takes the whole topic.
    a.owned.clear();
    let back = join(client, &group_id, &name, &mut a).await?;
    facts.set("a_rejoin.error", back.error_code);
    facts.set("a_rejoin.partitions", a.partitions());
    let left = beat(client, &group_id, &name, &a, -1).await?;
    facts.set("a_leave.error", left.error_code);
    Ok(())
}
