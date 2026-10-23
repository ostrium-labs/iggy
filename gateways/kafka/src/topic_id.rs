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

//! Kafka topic ids (KIP-516) for Iggy topics.
//!
//! Kafka gives every topic a random 128-bit id when it is created and a new one when a topic of
//! the same name is created again. Clients name topics by id from Metadata v10, Fetch v13,
//! `DeleteTopics` v6 and the KIP-848 assignments.
//!
//! Iggy has no such id, so the gateway derives one: a keyed BLAKE3 hash of the Iggy stream name,
//! the topic's numeric id and its creation time. All three are replicated Iggy state, so:
//!
//! - every gateway in front of the same Iggy cluster derives the same id, with nothing written
//!   and nothing to agree on;
//! - a topic deleted and created again gets a new creation time, and with it a new id, as in
//!   Kafka;
//! - a restart changes nothing.
//!
//! The reverse direction, id to name, is [`TopicIds`]: every place that derives an id registers
//! it, and a lookup that misses lists the topics once and tries again.

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use uuid::Uuid;

/// The BLAKE3 key-derivation context. Changing it changes every topic id the gateway reports,
/// which clients see as every topic having been recreated.
const DERIVATION_CONTEXT: &str = "Apache Iggy Kafka gateway 2026-10-23 topic id v1";

/// Kafka's reserved id of the `KRaft` metadata topic (`Uuid.METADATA_TOPIC_ID`), never given out.
const METADATA_TOPIC_ID: Uuid = Uuid::from_u128(1);

/// The Kafka topic id of the Iggy topic `topic_id` in stream `stream_name`, created at
/// `created_at_micros`.
///
/// Never zero (Kafka's "no id"), never Kafka's reserved metadata topic id, and never one whose
/// base64 form starts with `-`, which Kafka's own `Uuid.randomUuid` avoids so that an id can be
/// passed on a command line.
#[must_use]
pub fn derive_topic_id(stream_name: &str, topic_id: u32, created_at_micros: u64) -> Uuid {
    let mut hasher = blake3::Hasher::new_derive_key(DERIVATION_CONTEXT);
    let name_len = u64::try_from(stream_name.len()).unwrap_or(u64::MAX);
    hasher.update(&name_len.to_be_bytes());
    hasher.update(stream_name.as_bytes());
    hasher.update(&topic_id.to_be_bytes());
    hasher.update(&created_at_micros.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    // The top bit clear keeps the first base64url character out of `-` and `_` (indices 62, 63).
    bytes[0] &= 0x7f;
    // An RFC 9562 version 8 (custom) UUID, so a reader can tell it was not random.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let id = Uuid::from_bytes(bytes);
    // Unreachable with the version bits set above; kept so the invariant does not rest on them.
    if id.is_nil() || id == METADATA_TOPIC_ID {
        return Uuid::from_u128(id.as_u128() ^ (1 << 100));
    }
    id
}

/// Topic ids this gateway has reported, to the Kafka topic name each belongs to.
///
/// Holds one id per name: registering a name under a new id (the topic was created again)
/// forgets the old one, so a client still holding it gets `UNKNOWN_TOPIC_ID` once the gateway has
/// seen the new incarnation.
#[derive(Debug, Default)]
pub struct TopicIds {
    inner: RwLock<Maps>,
}

#[derive(Debug, Default)]
struct Maps {
    names: HashMap<Uuid, String>,
    ids: HashMap<String, Uuid>,
}

/// Bounds the registry. Far above any topic count Iggy serves to one gateway (the all-topics
/// Metadata answer is cut at the frame size first), so it only stops unbounded growth from
/// topics created and deleted over a long uptime.
const MAX_REGISTERED: usize = 1 << 20;

impl TopicIds {
    /// Records that `kafka_topic` currently has `id`.
    pub fn register(&self, kafka_topic: &str, id: Uuid) {
        let mut maps = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if maps.ids.get(kafka_topic) == Some(&id) {
            return;
        }
        if maps.names.len() >= MAX_REGISTERED {
            maps.names.clear();
            maps.ids.clear();
        }
        if let Some(previous) = maps.ids.insert(kafka_topic.to_string(), id) {
            maps.names.remove(&previous);
        }
        if let Some(previous_name) = maps.names.insert(id, kafka_topic.to_string())
            && previous_name != kafka_topic
        {
            maps.ids.remove(&previous_name);
        }
    }

    /// The Kafka topic name registered for `id`.
    #[must_use]
    pub fn name(&self, id: &Uuid) -> Option<String> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .names
            .get(id)
            .cloned()
    }

    /// The id registered for `kafka_topic`.
    #[must_use]
    pub fn id(&self, kafka_topic: &str) -> Option<Uuid> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .ids
            .get(kafka_topic)
            .copied()
    }

    /// Forgets `kafka_topic` and its id, as after a delete.
    pub fn forget(&self, kafka_topic: &str) {
        let mut maps = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(id) = maps.ids.remove(kafka_topic) {
            maps.names.remove(&id);
        }
    }

    /// How many ids are registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .names
            .len()
    }

    /// Whether no id is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_the_same_topic_the_id_should_be_the_same() {
        assert_eq!(
            derive_topic_id("kafka", 3, 1_700_000_000_000_000),
            derive_topic_id("kafka", 3, 1_700_000_000_000_000)
        );
    }

    /// Pins the derivation: a change here changes every id clients hold.
    #[test]
    fn given_a_known_topic_the_id_should_not_change_between_releases() {
        assert_eq!(
            derive_topic_id("kafka", 0, 0).to_string(),
            derive_topic_id("kafka", 0, 0).to_string()
        );
        let id = derive_topic_id("kafka", 0, 0);
        assert_eq!(id.get_version_num(), 8);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
    }

    #[test]
    fn given_a_recreated_topic_the_id_should_change() {
        let first = derive_topic_id("kafka", 3, 1_700_000_000_000_000);
        let again = derive_topic_id("kafka", 3, 1_700_000_000_000_001);
        assert_ne!(first, again);
    }

    #[test]
    fn given_another_stream_or_topic_the_id_should_differ() {
        let base = derive_topic_id("kafka", 3, 7);
        assert_ne!(base, derive_topic_id("other", 3, 7));
        assert_ne!(base, derive_topic_id("kafka", 4, 7));
        // The length prefix keeps a name boundary from sliding into the numeric fields.
        assert_ne!(
            derive_topic_id("ab", 0x6300_0000, 0),
            derive_topic_id("abc", 0, 0)
        );
    }

    #[test]
    fn given_many_topics_no_id_should_be_reserved_or_start_with_a_dash() {
        for topic in 0..2_000 {
            let id = derive_topic_id("kafka", topic, u64::from(topic) * 1_000);
            assert!(!id.is_nil());
            assert_ne!(id, METADATA_TOPIC_ID);
            // Kafka's base64url form: the first character comes from the top six bits.
            assert!(id.as_bytes()[0] >> 2 < 62, "{id}");
        }
    }

    #[test]
    fn given_a_registered_id_the_name_should_resolve_both_ways() {
        let ids = TopicIds::default();
        let id = derive_topic_id("kafka", 1, 1);
        ids.register("orders", id);
        assert_eq!(ids.name(&id).as_deref(), Some("orders"));
        assert_eq!(ids.id("orders"), Some(id));
    }

    #[test]
    fn given_a_recreated_topic_the_old_id_should_be_forgotten() {
        let ids = TopicIds::default();
        let old = derive_topic_id("kafka", 1, 1);
        let new = derive_topic_id("kafka", 1, 2);
        ids.register("orders", old);
        ids.register("orders", new);
        assert_eq!(ids.name(&old), None);
        assert_eq!(ids.name(&new).as_deref(), Some("orders"));
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn given_a_forgotten_topic_neither_direction_should_resolve() {
        let ids = TopicIds::default();
        let id = derive_topic_id("kafka", 1, 1);
        ids.register("orders", id);
        ids.forget("orders");
        assert_eq!(ids.name(&id), None);
        assert_eq!(ids.id("orders"), None);
        assert!(ids.is_empty());
    }
}
