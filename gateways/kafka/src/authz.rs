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

//! Per-principal authorization (KW-10, apache/iggy#4518).
//!
//! The gateway enforces each Kafka principal's own Iggy permissions, with the bridge's Iggy user
//! as the ceiling: a request the principal may make still runs as the bridge's user, and Iggy
//! decides it against that user's grants. [`Grants`] holds the principal's permissions with the
//! stream and topic ids Iggy keys them by resolved to names, because Kafka requests name topics
//! and the topic mapping turns a Kafka name into Iggy names, not ids. The checks are exactly the
//! rules in `core/metadata/src/permissioner/permissioner_rules/`: `get_topic`, `poll_messages`,
//! `append_messages`, `create_topic` and `manage_topic`, inheritance included.
//!
//! Kafka consumer groups have no Iggy resource. Group access follows topic read access, and group
//! ids are scoped per principal ([`GroupScope`]), so a principal sees, joins and deletes only the
//! groups in its own namespace (owner ruling of 2026-10-25).

use std::collections::{BTreeMap, HashMap};

use iggy::prelude::{GlobalPermissions, Permissions, StreamPermissions, TopicPermissions};

/// What a Kafka request asks to do with a topic, as Iggy's rules see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicOp {
    /// Kafka `DESCRIBE` (Metadata, `ListOffsets`, `DescribeConfigs`). Kafka implies it from every
    /// other topic operation, so it is Iggy's `get_topic` or any of the rules below.
    Describe,
    /// Kafka `READ` (Fetch, offset commit and fetch): Iggy's `poll_messages`, which is also the
    /// rule for storing and reading consumer offsets.
    Read,
    /// Kafka `WRITE` (Produce): Iggy's `append_messages`.
    Write,
    /// Kafka `ALTER`, `DELETE` and `ALTER_CONFIGS`: Iggy's `manage_topic` (update, delete,
    /// partitions).
    Manage,
    /// Kafka `CREATE`: Iggy's `create_topic`, on the stream the topic would be created in.
    Create,
}

/// One stream's grants, with its topics' grants by topic name. The flags are Iggy's own.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamGrants {
    pub manage_stream: bool,
    pub read_stream: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
    pub topics: HashMap<String, TopicGrants>,
}

/// One topic's grants. The flags are Iggy's own.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopicGrants {
    pub manage_topic: bool,
    pub read_topic: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

impl From<&TopicPermissions> for TopicGrants {
    fn from(permissions: &TopicPermissions) -> Self {
        Self {
            manage_topic: permissions.manage_topic,
            read_topic: permissions.read_topic,
            poll_messages: permissions.poll_messages,
            send_messages: permissions.send_messages,
        }
    }
}

/// A principal's Iggy permissions, keyed by stream and topic name.
///
/// Built in two steps: [`Self::from_iggy`] keeps the global grants and the per-stream ones under
/// their ids, then [`Self::resolve`] names them. Until the names are known the per-stream grants
/// grant nothing, which only ever denies: the global grants already decide alone for every
/// principal that has no per-stream ones.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Grants {
    global: GlobalPermissions,
    /// Per-stream grants by stream name.
    streams: HashMap<String, StreamGrants>,
    /// Per-stream grants still keyed by Iggy stream id, waiting for [`Self::resolve`].
    unresolved: BTreeMap<usize, StreamPermissions>,
}

/// The names of the streams and topics a principal's grants refer to, by Iggy id.
#[derive(Debug, Clone, Default)]
pub struct IdNames {
    /// Stream name by stream id.
    pub streams: HashMap<usize, String>,
    /// Topic name by stream id and topic id.
    pub topics: HashMap<(usize, usize), String>,
}

impl Grants {
    /// The grants of `permissions`, the per-stream ones still under their ids.
    #[must_use]
    pub fn from_iggy(permissions: &Permissions) -> Self {
        Self {
            global: permissions.global.clone(),
            streams: HashMap::new(),
            unresolved: permissions.streams.clone().unwrap_or_default(),
        }
    }

    /// Only global grants, as Kafka's `TOPIC *` bindings describe them.
    #[must_use]
    pub fn global(global: GlobalPermissions) -> Self {
        Self {
            global,
            ..Self::default()
        }
    }

    /// The Iggy stream ids whose grants are not named yet, with the ids of the topics each one
    /// grants on its own.
    #[must_use]
    pub fn unresolved(&self) -> Vec<(usize, Vec<usize>)> {
        self.unresolved
            .iter()
            .map(|(&stream, permissions)| {
                let topics = permissions
                    .topics
                    .as_ref()
                    .map(|topics| topics.keys().copied().collect())
                    .unwrap_or_default();
                (stream, topics)
            })
            .collect()
    }

    /// Whether some per-stream grant still waits for its names.
    #[must_use]
    pub fn needs_names(&self) -> bool {
        !self.unresolved.is_empty()
    }

    /// Names the per-stream grants. A stream or topic `names` does not know no longer exists in
    /// Iggy, so its grant is dropped: it could only ever name a resource that is gone.
    #[must_use]
    pub fn resolve(mut self, names: &IdNames) -> Self {
        for (stream_id, permissions) in std::mem::take(&mut self.unresolved) {
            let Some(stream) = names.streams.get(&stream_id) else {
                continue;
            };
            let topics = permissions
                .topics
                .iter()
                .flatten()
                .filter_map(|(topic_id, topic)| {
                    names
                        .topics
                        .get(&(stream_id, *topic_id))
                        .map(|name| (name.clone(), TopicGrants::from(topic)))
                })
                .collect();
            self.streams.insert(
                stream.clone(),
                StreamGrants {
                    manage_stream: permissions.manage_stream,
                    read_stream: permissions.read_stream,
                    manage_topics: permissions.manage_topics,
                    read_topics: permissions.read_topics,
                    poll_messages: permissions.poll_messages,
                    send_messages: permissions.send_messages,
                    topics,
                },
            );
        }
        self
    }

    /// The global grants.
    #[must_use]
    pub const fn global_permissions(&self) -> &GlobalPermissions {
        &self.global
    }

    /// The named per-stream grants.
    #[must_use]
    pub const fn streams(&self) -> &HashMap<String, StreamGrants> {
        &self.streams
    }

    /// Whether the principal may do `op` on topic `topic` of stream `stream` (Iggy names).
    #[must_use]
    pub fn allows(&self, op: TopicOp, stream: &str, topic: &str) -> bool {
        match op {
            TopicOp::Describe => {
                self.get_topic(stream, topic)
                    || self.poll_messages(stream, topic)
                    || self.append_messages(stream, topic)
            }
            TopicOp::Read => self.poll_messages(stream, topic),
            TopicOp::Write => self.append_messages(stream, topic),
            TopicOp::Manage => self.manage_topic(stream, topic),
            TopicOp::Create => self.create_topic(stream),
        }
    }

    /// Whether the principal may read some topic anywhere: what Kafka group access follows.
    #[must_use]
    pub fn reads_any_topic(&self) -> bool {
        let global = &self.global;
        global.poll_messages
            || global.read_topics
            || global.manage_topics
            || global.read_streams
            || global.manage_streams
            || self.streams.values().any(|stream| {
                stream.manage_stream
                    || stream.read_stream
                    || stream.manage_topics
                    || stream.read_topics
                    || stream.poll_messages
                    || stream
                        .topics
                        .values()
                        .any(|topic| topic.manage_topic || topic.read_topic || topic.poll_messages)
            })
    }

    /// Whether the principal may write some topic anywhere: Kafka's `IDEMPOTENT_WRITE` on the
    /// cluster, which an `InitProducerId` without a transaction asks for, is implied by `WRITE` on
    /// any topic.
    #[must_use]
    pub fn writes_any_topic(&self) -> bool {
        let global = &self.global;
        global.send_messages
            || global.manage_topics
            || global.manage_streams
            || self.streams.values().any(|stream| {
                stream.manage_stream
                    || stream.manage_topics
                    || stream.send_messages
                    || stream
                        .topics
                        .values()
                        .any(|topic| topic.manage_topic || topic.send_messages)
            })
    }

    fn topic_grants(&self, stream: &str, topic: &str) -> Option<&TopicGrants> {
        self.streams.get(stream)?.topics.get(topic)
    }

    /// `permissioner_rules/topics.rs`, `get_topic`.
    fn get_topic(&self, stream: &str, topic: &str) -> bool {
        let global = &self.global;
        if global.read_streams
            || global.manage_streams
            || global.manage_topics
            || global.read_topics
        {
            return true;
        }
        let Some(grants) = self.streams.get(stream) else {
            return false;
        };
        grants.manage_stream
            || grants.read_stream
            || grants.manage_topics
            || grants.read_topics
            || self
                .topic_grants(stream, topic)
                .is_some_and(|topic| topic.manage_topic || topic.read_topic)
    }

    /// `permissioner_rules/messages.rs`, `poll_messages`.
    fn poll_messages(&self, stream: &str, topic: &str) -> bool {
        let global = &self.global;
        if global.poll_messages
            || global.read_topics
            || global.manage_topics
            || global.read_streams
            || global.manage_streams
        {
            return true;
        }
        let Some(grants) = self.streams.get(stream) else {
            return false;
        };
        grants.poll_messages
            || grants.manage_stream
            || grants.read_stream
            || grants.manage_topics
            || grants.read_topics
            || self
                .topic_grants(stream, topic)
                .is_some_and(|topic| topic.manage_topic || topic.read_topic || topic.poll_messages)
    }

    /// `permissioner_rules/messages.rs`, `append_messages`.
    fn append_messages(&self, stream: &str, topic: &str) -> bool {
        let global = &self.global;
        if global.send_messages || global.manage_streams || global.manage_topics {
            return true;
        }
        let Some(grants) = self.streams.get(stream) else {
            return false;
        };
        grants.send_messages
            || grants.manage_stream
            || grants.manage_topics
            || self
                .topic_grants(stream, topic)
                .is_some_and(|topic| topic.manage_topic || topic.send_messages)
    }

    /// `permissioner_rules/topics.rs`, `create_topic`.
    fn create_topic(&self, stream: &str) -> bool {
        let global = &self.global;
        global.manage_streams
            || global.manage_topics
            || self
                .streams
                .get(stream)
                .is_some_and(|grants| grants.manage_stream || grants.manage_topics)
    }

    /// `permissioner_rules/topics.rs`, `manage_topic` (update, delete, partitions).
    fn manage_topic(&self, stream: &str, topic: &str) -> bool {
        let global = &self.global;
        if global.manage_streams || global.manage_topics {
            return true;
        }
        let Some(grants) = self.streams.get(stream) else {
            return false;
        };
        grants.manage_stream
            || grants.manage_topics
            || self
                .topic_grants(stream, topic)
                .is_some_and(|topic| topic.manage_topic)
    }
}

/// The separator between a principal's name and its group in a scoped group id. A scoped id is
/// `<length of the name>:<name>/<group>`, so no name, however it is spelled, can produce the id
/// of a group in another principal's namespace.
const SCOPE_SEPARATOR: char = '/';

/// A principal's consumer group namespace.
///
/// Kafka groups have no Iggy resource to grant, so they cannot be authorized one by one. Instead
/// every principal gets a namespace of its own: the group a client names is stored, coordinated
/// and offset under `<length>:<principal>/<group>`, and a principal only ever sees the groups of its
/// own namespace. Two tenants that both use `orders-app` get two groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupScope {
    prefix: String,
}

impl GroupScope {
    /// The namespace of `principal`.
    #[must_use]
    pub fn of(principal: &str) -> Self {
        Self {
            prefix: format!("{}:{principal}{SCOPE_SEPARATOR}", principal.len()),
        }
    }

    /// The id `group` is kept under.
    #[must_use]
    pub fn scope(&self, group: &str) -> String {
        format!("{}{group}", self.prefix)
    }

    /// The group a scoped id names, when it is in this namespace.
    #[must_use]
    pub fn unscope<'a>(&self, scoped: &'a str) -> Option<&'a str> {
        scoped.strip_prefix(self.prefix.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_grants(configure: impl FnOnce(&mut StreamPermissions)) -> Permissions {
        let mut stream = StreamPermissions::default();
        configure(&mut stream);
        Permissions {
            global: GlobalPermissions::default(),
            streams: Some(BTreeMap::from([(7, stream)])),
        }
    }

    fn names() -> IdNames {
        IdNames {
            streams: HashMap::from([(7, "kafka".to_string())]),
            topics: HashMap::from([((7, 3), "orders".to_string())]),
        }
    }

    fn grants(permissions: &Permissions) -> Grants {
        Grants::from_iggy(permissions).resolve(&names())
    }

    #[test]
    fn global_flags_follow_iggys_inheritance() {
        let read = Grants::global(GlobalPermissions {
            read_topics: true,
            ..GlobalPermissions::default()
        });
        assert!(read.allows(TopicOp::Read, "any", "topic"));
        assert!(read.allows(TopicOp::Describe, "any", "topic"));
        assert!(!read.allows(TopicOp::Write, "any", "topic"));
        assert!(!read.allows(TopicOp::Manage, "any", "topic"));

        let poll = Grants::global(GlobalPermissions {
            poll_messages: true,
            ..GlobalPermissions::default()
        });
        assert!(poll.allows(TopicOp::Read, "any", "topic"));
        // Kafka's READ implies DESCRIBE, so a poller may look the topic up.
        assert!(poll.allows(TopicOp::Describe, "any", "topic"));

        let manage = Grants::global(GlobalPermissions {
            manage_topics: true,
            ..GlobalPermissions::default()
        });
        for op in [
            TopicOp::Describe,
            TopicOp::Read,
            TopicOp::Write,
            TopicOp::Manage,
            TopicOp::Create,
        ] {
            assert!(manage.allows(op, "any", "topic"), "{op:?}");
        }
        assert!(!Grants::global(GlobalPermissions::default()).allows(TopicOp::Describe, "s", "t"));
    }

    #[test]
    fn a_topic_grant_applies_to_that_topic_only() {
        let grants = grants(&stream_grants(|stream| {
            stream.topics = Some(BTreeMap::from([(
                3,
                TopicPermissions {
                    poll_messages: true,
                    ..TopicPermissions::default()
                },
            )]));
        }));
        assert!(grants.allows(TopicOp::Read, "kafka", "orders"));
        assert!(grants.allows(TopicOp::Describe, "kafka", "orders"));
        assert!(!grants.allows(TopicOp::Read, "kafka", "payments"));
        assert!(!grants.allows(TopicOp::Read, "other", "orders"));
        assert!(!grants.allows(TopicOp::Write, "kafka", "orders"));
        assert!(grants.reads_any_topic());
        assert!(!grants.writes_any_topic());
    }

    #[test]
    fn a_stream_grant_covers_every_topic_of_the_stream() {
        let grants = grants(&stream_grants(|stream| stream.send_messages = true));
        assert!(grants.allows(TopicOp::Write, "kafka", "orders"));
        assert!(grants.allows(TopicOp::Write, "kafka", "created-later"));
        assert!(!grants.allows(TopicOp::Read, "kafka", "orders"));
        assert!(!grants.allows(TopicOp::Create, "kafka", "orders"));

        let managing = grants_of(|stream| stream.manage_topics = true);
        assert!(managing.allows(TopicOp::Create, "kafka", "new"));
        assert!(managing.allows(TopicOp::Manage, "kafka", "orders"));
        assert!(!managing.allows(TopicOp::Create, "other", "new"));
    }

    fn grants_of(configure: impl FnOnce(&mut StreamPermissions)) -> Grants {
        grants(&stream_grants(configure))
    }

    #[test]
    fn a_topic_manage_grant_manages_but_does_not_create() {
        let grants = grants(&stream_grants(|stream| {
            stream.topics = Some(BTreeMap::from([(
                3,
                TopicPermissions {
                    manage_topic: true,
                    ..TopicPermissions::default()
                },
            )]));
        }));
        assert!(grants.allows(TopicOp::Manage, "kafka", "orders"));
        assert!(grants.allows(TopicOp::Write, "kafka", "orders"));
        assert!(grants.allows(TopicOp::Read, "kafka", "orders"));
        assert!(!grants.allows(TopicOp::Create, "kafka", "orders"));
    }

    #[test]
    fn unnamed_grants_grant_nothing_until_resolved_and_vanished_ids_drop() {
        let permissions = stream_grants(|stream| stream.read_topics = true);
        let pending = Grants::from_iggy(&permissions);
        assert!(pending.needs_names());
        assert_eq!(pending.unresolved(), vec![(7, vec![])]);
        assert!(!pending.allows(TopicOp::Read, "kafka", "orders"));
        let gone = pending.resolve(&IdNames::default());
        assert!(!gone.needs_names());
        assert!(gone.streams().is_empty());
    }

    #[test]
    fn group_scopes_cannot_reach_each_other() {
        let alice = GroupScope::of("alice");
        let scoped = alice.scope("orders-app");
        assert_eq!(scoped, "5:alice/orders-app");
        assert_eq!(alice.unscope(&scoped), Some("orders-app"));
        // A name that tries to spell another namespace still lands in its own.
        let crafty = GroupScope::of("alice/x");
        assert_ne!(crafty.scope("y"), alice.scope("x/y"));
        assert_eq!(alice.unscope(&crafty.scope("y")), None);
        assert_eq!(GroupScope::of("bob").unscope(&scoped), None);
    }
}
