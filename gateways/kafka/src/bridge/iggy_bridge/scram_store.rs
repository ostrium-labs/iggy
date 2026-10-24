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

//! SCRAM credentials, kept by the gateway in an Iggy topic.
//!
//! Iggy stores one Argon2 hash per user, which SCRAM cannot use (`docs/AUTHENTICATION.md`,
//! "SCRAM"). The gateway therefore keeps SCRAM's salted keys itself, as a log in the one-partition
//! topic [`SCRAM_TOPIC`] of the gateway's meta stream (beside the cluster id). Each message is one
//! change, an upsert or a removal of one `(user, mechanism)` credential, and the current set is
//! the log replayed in order. Every gateway on the same Iggy and default stream reads the same
//! log, so a credential set through one is honoured by all, and a lookup catches up on the log
//! first so it sees changes made elsewhere.
//!
//! What is stored is Kafka's `UserScramCredentialRecord`: salt, iterations, `StoredKey` and
//! `ServerKey`. None of them logs in by itself, but together with a captured exchange they help an
//! attacker, so the meta stream's permissions are what protect them: grant `poll_messages` there
//! to the bridge's user only.
//!
//! The log is read and written on a client of its own, so a SASL login never queues behind a
//! slow Metadata or topic change on the bridge's shared client.
//!
//! The log is never compacted until KW-6 brings compaction; credentials change rarely, and
//! replaying a few thousand records at start is cheap.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use iggy::prelude::{
    Consumer, Durability, Identifier, IggyError, IggyExpiry, IggyMessage, MaxTopicSize,
    MessageClient, Partitioning, PollingStrategy, StreamClient, StreamCreateOptions, TopicClient,
    TopicCreateOptions,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use super::{IggyBridge, REQUEST_TIMEOUT, cluster_id_stream, with_request_timeout};
use crate::bridge::error::BridgeError;
use crate::scram::{ScramCredential, ScramMechanism};

/// The topic holding the credential log, in the stream `cluster_id_stream` names.
pub const SCRAM_TOPIC: &str = "scram";

/// Records read per poll while catching up.
const CATCH_UP_BATCH: u32 = 1000;

/// The format version every record carries, so a later format can be told apart.
const RECORD_VERSION: u8 = 1;

/// One change in the log.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Record {
    Upsert {
        v: u8,
        user: String,
        mechanism: String,
        salt: String,
        iterations: u32,
        stored_key: String,
        server_key: String,
    },
    Delete {
        v: u8,
        user: String,
        mechanism: String,
    },
}

fn mechanism_named(name: &str) -> Option<ScramMechanism> {
    [ScramMechanism::Sha256, ScramMechanism::Sha512]
        .into_iter()
        .find(|mechanism| mechanism.name() == name)
}

/// A change to apply, already validated.
#[derive(Debug, Clone)]
pub enum CredentialChange {
    Upsert {
        user: String,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    },
    Delete {
        user: String,
        mechanism: ScramMechanism,
    },
}

impl CredentialChange {
    fn record(&self) -> Record {
        match self {
            Self::Upsert {
                user,
                mechanism,
                credential,
            } => Record::Upsert {
                v: RECORD_VERSION,
                user: user.clone(),
                mechanism: mechanism.name().to_string(),
                salt: BASE64.encode(&credential.salt),
                iterations: credential.iterations,
                stored_key: BASE64.encode(&credential.stored_key),
                server_key: BASE64.encode(&credential.server_key),
            },
            Self::Delete { user, mechanism } => Record::Delete {
                v: RECORD_VERSION,
                user: user.clone(),
                mechanism: mechanism.name().to_string(),
            },
        }
    }
}

/// Bounds connecting the credential log's own client, which a first lookup does.
async fn with_connect_timeout<T>(
    connect: impl std::future::Future<Output = Result<T, BridgeError>>,
) -> Result<T, BridgeError> {
    tokio::time::timeout(REQUEST_TIMEOUT, connect)
        .await
        .map_err(|_elapsed| BridgeError::Timeout)?
}

/// The credential set as replayed so far.
#[derive(Default)]
pub struct ScramState {
    credentials: BTreeMap<(String, ScramMechanism), ScramCredential>,
    /// The next offset to read.
    next_offset: u64,
}

impl ScramState {
    fn apply(&mut self, payload: &[u8], offset: u64) {
        let record: Record = match serde_json::from_slice(payload) {
            Ok(record) => record,
            Err(error) => {
                warn!(offset, %error, "skipping a SCRAM credential record that does not parse");
                return;
            }
        };
        match record {
            Record::Upsert {
                user,
                mechanism,
                salt,
                iterations,
                stored_key,
                server_key,
                ..
            } => {
                let (Some(mechanism), Ok(salt), Ok(stored_key), Ok(server_key)) = (
                    mechanism_named(&mechanism),
                    BASE64.decode(salt),
                    BASE64.decode(stored_key),
                    BASE64.decode(server_key),
                ) else {
                    warn!(
                        offset,
                        "skipping a SCRAM credential record with invalid fields"
                    );
                    return;
                };
                self.credentials.insert(
                    (user, mechanism),
                    ScramCredential {
                        salt,
                        iterations,
                        stored_key,
                        server_key,
                    },
                );
            }
            Record::Delete {
                user, mechanism, ..
            } => {
                if let Some(mechanism) = mechanism_named(&mechanism) {
                    self.credentials.remove(&(user, mechanism));
                }
            }
        }
    }

    /// The credential for `user` under `mechanism`.
    pub fn get(&self, user: &str, mechanism: ScramMechanism) -> Option<ScramCredential> {
        self.credentials
            .get(&(user.to_string(), mechanism))
            .cloned()
    }

    /// Every user with at least one credential, with `(mechanism, iterations)` per credential.
    pub fn describe_all(&self) -> BTreeMap<String, Vec<(ScramMechanism, u32)>> {
        let mut users: BTreeMap<String, Vec<(ScramMechanism, u32)>> = BTreeMap::new();
        for ((user, mechanism), credential) in &self.credentials {
            users
                .entry(user.clone())
                .or_default()
                .push((*mechanism, credential.iterations));
        }
        users
    }

    pub fn contains(&self, user: &str, mechanism: ScramMechanism) -> bool {
        self.credentials
            .contains_key(&(user.to_string(), mechanism))
    }
}

/// The credential log's replayed state, shared by every lookup.
#[derive(Default)]
pub struct ScramStore {
    state: Mutex<ScramState>,
}

impl std::fmt::Debug for ScramStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramStore").finish_non_exhaustive()
    }
}

struct Location {
    stream_name: String,
    stream_id: Identifier,
    topic_id: Identifier,
}

impl IggyBridge {
    fn scram_location(&self) -> Result<Location, BridgeError> {
        let stream_name = cluster_id_stream(self.config.topic_mapping.default_stream());
        Ok(Location {
            stream_id: Identifier::named(&stream_name).map_err(BridgeError::Iggy)?,
            topic_id: Identifier::named(SCRAM_TOPIC).map_err(BridgeError::Iggy)?,
            stream_name,
        })
    }

    /// Reads the log from where `state` stopped to its end. A log that does not exist yet is an
    /// empty set.
    async fn scram_catch_up(&self, state: &mut ScramState) -> Result<(), BridgeError> {
        let location = self.scram_location()?;
        let client = with_connect_timeout(self.scram_client.connected(&self.config)).await?;
        loop {
            let polled = with_request_timeout(client.poll_messages(
                &location.stream_id,
                &location.topic_id,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(state.next_offset),
                CATCH_UP_BATCH,
                false,
            ))
            .await;
            let polled = match polled {
                Ok(polled) => polled,
                Err(BridgeError::Iggy(
                    IggyError::StreamIdNotFound(_)
                    | IggyError::StreamNameNotFound(_)
                    | IggyError::TopicIdNotFound(_, _)
                    | IggyError::TopicNameNotFound(_, _),
                )) => return Ok(()),
                Err(error) => return Err(error),
            };
            let Some(last) = polled.messages.last() else {
                return Ok(());
            };
            let last_offset = last.header.offset;
            for message in &polled.messages {
                if message.header.offset >= state.next_offset {
                    state.apply(&message.payload, message.header.offset);
                }
            }
            state.next_offset = last_offset + 1;
            if polled.messages.len() < CATCH_UP_BATCH as usize {
                return Ok(());
            }
        }
    }

    /// The SCRAM credential for `user` under `mechanism`, after catching up on the log.
    ///
    /// # Errors
    ///
    /// The bridge's error when the log cannot be read.
    pub async fn load_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>, BridgeError> {
        let mut state = self.scram.state.lock().await;
        self.scram_catch_up(&mut state).await?;
        Ok(state.get(user, mechanism))
    }

    /// Runs `read` over the caught-up credential set.
    ///
    /// # Errors
    ///
    /// The bridge's error when the log cannot be read.
    pub async fn with_scram_state<T>(
        &self,
        read: impl FnOnce(&ScramState) -> T,
    ) -> Result<T, BridgeError> {
        let mut state = self.scram.state.lock().await;
        self.scram_catch_up(&mut state).await?;
        Ok(read(&state))
    }

    /// Validates `plan` against the caught-up set, then appends the changes it accepts as one
    /// send. `plan` returns the changes to write and whatever per-user answer the caller builds.
    ///
    /// # Errors
    ///
    /// The bridge's error when the log cannot be read, created or written.
    pub async fn alter_scram<T>(
        &self,
        plan: impl FnOnce(&ScramState) -> (Vec<CredentialChange>, T),
    ) -> Result<T, BridgeError> {
        let mut state = self.scram.state.lock().await;
        self.scram_catch_up(&mut state).await?;
        let (changes, answer) = plan(&state);
        if changes.is_empty() {
            return Ok(answer);
        }
        let mut messages = Vec::with_capacity(changes.len());
        for change in &changes {
            let payload = serde_json::to_vec(&change.record())
                .map_err(|error| BridgeError::InvalidConfig(error.to_string()))?;
            messages.push(
                IggyMessage::builder()
                    .payload(payload.into())
                    .build()
                    .map_err(BridgeError::Iggy)?,
            );
        }
        let location = self.scram_location()?;
        match self.send_scram(&location, &mut messages).await {
            Err(BridgeError::Iggy(
                IggyError::StreamIdNotFound(_)
                | IggyError::StreamNameNotFound(_)
                | IggyError::TopicIdNotFound(_, _)
                | IggyError::TopicNameNotFound(_, _),
            )) => {
                self.create_scram_topic(&location).await?;
                self.send_scram(&location, &mut messages).await?;
            }
            other => other?,
        }
        debug!(changes = changes.len(), "SCRAM credential changes written");
        // Read back rather than applying locally, so this gateway's view is the log's order even
        // when another gateway wrote in between.
        self.scram_catch_up(&mut state).await?;
        Ok(answer)
    }

    async fn send_scram(
        &self,
        location: &Location,
        messages: &mut [IggyMessage],
    ) -> Result<(), BridgeError> {
        let client = with_connect_timeout(self.scram_client.connected(&self.config)).await?;
        with_request_timeout(client.send_messages(
            &location.stream_id,
            &location.topic_id,
            &Partitioning::partition_id(0),
            messages,
        ))
        .await
        .map(|_| ())
    }

    /// Creates the meta stream and the log topic, unless they exist. Durability `persisted`, no
    /// expiry and no size cap: losing a credential record would silently revert a password change.
    async fn create_scram_topic(&self, location: &Location) -> Result<(), BridgeError> {
        let options = StreamCreateOptions::default();
        match self
            .metadata_write(|| self.client.create_stream(&location.stream_name, &options))
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::StreamNameAlreadyExists(_))) => {}
            Err(error) => return Err(error),
        }
        let options = TopicCreateOptions {
            partitions_count: Some(1),
            message_expiry: Some(IggyExpiry::NeverExpire),
            max_topic_size: Some(MaxTopicSize::Unlimited),
            durability: Durability::Persisted,
            ..TopicCreateOptions::default()
        };
        match self
            .metadata_write(|| {
                self.client
                    .create_topic(&location.stream_id, SCRAM_TOPIC, &options)
            })
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upsert(user: &str, mechanism: ScramMechanism, iterations: u32) -> Vec<u8> {
        serde_json::to_vec(
            &CredentialChange::Upsert {
                user: user.to_string(),
                mechanism,
                credential: ScramCredential::from_password(
                    mechanism,
                    "pw",
                    vec![1, 2, 3],
                    iterations,
                ),
            }
            .record(),
        )
        .expect("encodes")
    }

    #[test]
    fn given_a_log_when_replayed_should_keep_the_last_change_per_credential() {
        let mut state = ScramState::default();
        state.apply(&upsert("alice", ScramMechanism::Sha256, 4096), 0);
        state.apply(&upsert("alice", ScramMechanism::Sha512, 8192), 1);
        state.apply(&upsert("bob", ScramMechanism::Sha256, 4096), 2);
        state.apply(&upsert("alice", ScramMechanism::Sha256, 9000), 3);
        let delete = serde_json::to_vec(
            &CredentialChange::Delete {
                user: "bob".to_string(),
                mechanism: ScramMechanism::Sha256,
            }
            .record(),
        )
        .expect("encodes");
        state.apply(&delete, 4);
        state.apply(b"not json", 5);

        let described = state.describe_all();
        assert_eq!(
            described.get("alice"),
            Some(&vec![
                (ScramMechanism::Sha256, 9000),
                (ScramMechanism::Sha512, 8192)
            ])
        );
        assert!(
            !described.contains_key("bob"),
            "bob's only credential was removed"
        );
        let alice = state
            .get("alice", ScramMechanism::Sha256)
            .expect("alice has a SHA-256 credential");
        assert_eq!(
            alice,
            ScramCredential::from_password(ScramMechanism::Sha256, "pw", vec![1, 2, 3], 9000),
            "the stored keys round-trip through the record"
        );
    }

    #[test]
    fn given_a_record_when_encoded_should_hold_no_password_material_in_clear() {
        let record =
            String::from_utf8(upsert("alice", ScramMechanism::Sha256, 4096)).expect("utf-8");
        assert!(!record.contains("pw"), "{record}");
        assert!(record.contains("\"op\":\"upsert\""), "{record}");
        assert!(record.contains("\"v\":1"), "{record}");
    }
}
