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

//! The commit metadata log in Iggy (KW-12).
//!
//! The topic [`OFFSET_METADATA_TOPIC`] of the gateway's meta stream (beside the cluster id, the
//! SCRAM credentials and the transaction log), one partition so every gateway reads the records
//! in the order they landed, durability `persisted`, no expiry. Created by the first append. Read
//! and written on a client of its own, so a commit never queues behind a slow Metadata call.

use std::sync::Arc;

use async_trait::async_trait;
use iggy::prelude::{
    Consumer, Durability, Identifier, IggyError, IggyExpiry, IggyMessage, MaxTopicSize,
    MessageClient, Partitioning, PollingStrategy, StreamClient, StreamCreateOptions, TopicClient,
    TopicCreateOptions,
};
use tokio::time::Instant;
use tracing::{info, warn};

use super::fetch::LazyClient;
use super::{IggyBridge, REQUEST_TIMEOUT, cluster_id_stream};
use crate::bridge::error::BridgeError;
use crate::commit_metadata::{MetadataLog, MetadataRecord, OFFSET_METADATA_TOPIC};

/// The log's only partition.
const PARTITION: u32 = 0;

/// Records read per poll while replaying.
const REPLAY_BATCH: u32 = 1000;

/// The commit metadata log on its own Iggy client.
pub struct IggyMetadataLog {
    bridge: Arc<IggyBridge>,
    client: LazyClient,
}

impl std::fmt::Debug for IggyMetadataLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IggyMetadataLog").finish_non_exhaustive()
    }
}

struct Location {
    stream_name: String,
    stream_id: Identifier,
    topic_id: Identifier,
}

const fn is_missing(error: &BridgeError) -> bool {
    matches!(
        error,
        BridgeError::Iggy(
            IggyError::StreamIdNotFound(_)
                | IggyError::StreamNameNotFound(_)
                | IggyError::TopicIdNotFound(_, _)
                | IggyError::TopicNameNotFound(_, _)
        )
    )
}

impl IggyMetadataLog {
    #[must_use]
    pub fn new(bridge: Arc<IggyBridge>) -> Self {
        Self {
            bridge,
            client: LazyClient::default(),
        }
    }

    fn location(&self) -> Result<Location, BridgeError> {
        let stream_name = cluster_id_stream(self.bridge.config.topic_mapping.default_stream());
        Ok(Location {
            stream_id: Identifier::named(&stream_name).map_err(BridgeError::Iggy)?,
            topic_id: Identifier::named(OFFSET_METADATA_TOPIC).map_err(BridgeError::Iggy)?,
            stream_name,
        })
    }

    async fn send(
        &self,
        location: &Location,
        records: &[MetadataRecord],
    ) -> Result<(), BridgeError> {
        let mut messages = records
            .iter()
            .map(|record| {
                IggyMessage::builder()
                    .payload(record.encode())
                    .build()
                    .map_err(BridgeError::Iggy)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let (stream_id, topic_id) = (&location.stream_id, &location.topic_id);
        self.client
            .until(&self.bridge.config, deadline, |client| async move {
                client
                    .send_messages(
                        stream_id,
                        topic_id,
                        &Partitioning::partition_id(PARTITION),
                        &mut messages,
                    )
                    .await
                    .map(|_| ())
                    .map_err(BridgeError::Iggy)
            })
            .await
    }

    /// Creates the meta stream and the log topic, unless they exist.
    async fn create(&self, location: &Location) -> Result<(), BridgeError> {
        let options = StreamCreateOptions::default();
        match self
            .bridge
            .metadata_write(|| {
                self.bridge
                    .client
                    .create_stream(&location.stream_name, &options)
            })
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
            .bridge
            .metadata_write(|| {
                self.bridge.client.create_topic(
                    &location.stream_id,
                    OFFSET_METADATA_TOPIC,
                    &options,
                )
            })
            .await
        {
            Ok(_) => {
                info!(
                    "created the Kafka commit metadata log '{}/{OFFSET_METADATA_TOPIC}'",
                    location.stream_name
                );
                Ok(())
            }
            Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Signs the log's client out.
    pub async fn close(&self) {
        if let Err(error) = self.client.close().await {
            warn!(%error, "the Kafka commit metadata log client did not close cleanly");
        }
    }
}

#[async_trait]
impl MetadataLog for IggyMetadataLog {
    async fn append(&self, records: &[MetadataRecord]) -> Result<(), BridgeError> {
        let location = self.location()?;
        match self.send(&location, records).await {
            Err(error) if is_missing(&error) => {
                self.create(&location).await?;
                self.send(&location, records).await
            }
            other => other,
        }
    }

    async fn read_from(&self, from: u64) -> Result<(Vec<MetadataRecord>, u64), BridgeError> {
        let location = self.location()?;
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let (stream_id, topic_id) = (&location.stream_id, &location.topic_id);
        let polled = self
            .client
            .until(&self.bridge.config, deadline, |client| async move {
                client
                    .poll_messages(
                        stream_id,
                        topic_id,
                        Some(PARTITION),
                        &Consumer::default(),
                        &PollingStrategy::offset(from),
                        REPLAY_BATCH,
                        false,
                    )
                    .await
                    .map_err(BridgeError::Iggy)
            })
            .await;
        let polled = match polled {
            Ok(polled) => polled,
            Err(error) if is_missing(&error) => return Ok((Vec::new(), from)),
            Err(error) => return Err(error),
        };
        let Some(last) = polled.messages.last().map(|message| message.header.offset) else {
            return Ok((Vec::new(), from));
        };
        let records = polled
            .messages
            .iter()
            .filter_map(|message| {
                let record = MetadataRecord::decode(&message.payload);
                if record.is_none() {
                    warn!(
                        offset = message.header.offset,
                        "skipping a Kafka commit metadata record that does not parse"
                    );
                }
                record
            })
            .collect::<Vec<_>>();
        // A batch of only unreadable records still moves the position on.
        Ok((records, last + 1))
    }
}
