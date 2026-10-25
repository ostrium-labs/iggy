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

//! The Kafka transaction log in Iggy, and the transaction markers on the data partitions (KW-T).
//!
//! The log is the topic [`TXN_STATE_TOPIC`] of the gateway's meta stream (beside the cluster id
//! and the SCRAM credentials), [`LOG_PARTITIONS`] partitions, durability `persisted`, no expiry.
//! It is read and written on a client of its own, so a transaction never queues behind a slow
//! Metadata call. Its permissions protect it as the meta stream's do: only the bridge's user should
//! hold `send_messages` there, or anyone could forge a commit.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iggy::prelude::{
    Consumer, Durability, Identifier, IggyError, IggyExpiry, IggyMessage, MaxTopicSize,
    MessageClient, Partitioning, PollingStrategy, StreamClient, StreamCreateOptions, TopicClient,
    TopicCreateOptions,
};
use tokio::time::Instant;
use tracing::{info, warn};

use super::fetch::LazyClient;
use super::{
    IggyBridge, Producer, REQUEST_TIMEOUT, cluster_id_stream, resolve_blocker, with_request_timeout,
};
use crate::bridge::error::BridgeError;
use crate::records::control_message;
use crate::transactions::coordinator::wall_clock_ms;
use crate::transactions::{DataPlane, LOG_PARTITIONS, Landed, LogRecord, Marker, TxnLog};

/// The topic of the transaction log, in the stream `cluster_id_stream` names.
pub const TXN_STATE_TOPIC: &str = "txn_state";

/// Records read per poll while replaying.
const REPLAY_BATCH: u32 = 1000;

/// How long one marker send may take.
const MARKER_DEADLINE: Duration = Duration::from_secs(25);

/// The transaction log on its own Iggy client.
pub struct IggyTxnLog {
    bridge: Arc<IggyBridge>,
    client: LazyClient,
}

impl std::fmt::Debug for IggyTxnLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IggyTxnLog").finish_non_exhaustive()
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

impl IggyTxnLog {
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
            topic_id: Identifier::named(TXN_STATE_TOPIC).map_err(BridgeError::Iggy)?,
            stream_name,
        })
    }

    async fn send(
        &self,
        location: &Location,
        partition: u32,
        messages: &mut [IggyMessage],
    ) -> Result<(), BridgeError> {
        let client =
            tokio::time::timeout(REQUEST_TIMEOUT, self.client.connected(&self.bridge.config))
                .await
                .map_err(|_elapsed| BridgeError::Timeout)??;
        let deadline = (Instant::now() + REQUEST_TIMEOUT).into_std();
        let partitioning = Partitioning::partition_id(partition);
        let send = async {
            loop {
                match client
                    .send_messages(
                        &location.stream_id,
                        &location.topic_id,
                        &partitioning,
                        messages,
                    )
                    .await
                {
                    // An earlier record in doubt holds the lane: settle it first, so records of
                    // one id land in order.
                    Err(IggyError::LaneBlocked(operation)) => {
                        resolve_blocker(&client, operation, deadline).await?;
                    }
                    result => return result,
                }
            }
        };
        with_request_timeout(send).await.map(|_| ())
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
            partitions_count: Some(LOG_PARTITIONS),
            message_expiry: Some(IggyExpiry::NeverExpire),
            max_topic_size: Some(MaxTopicSize::Unlimited),
            durability: Durability::Persisted,
            ..TopicCreateOptions::default()
        };
        match self
            .bridge
            .metadata_write(|| {
                self.bridge
                    .client
                    .create_topic(&location.stream_id, TXN_STATE_TOPIC, &options)
            })
            .await
        {
            Ok(_) => {
                info!(
                    "created the Kafka transaction log '{}/{TXN_STATE_TOPIC}'",
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
            warn!(%error, "the Kafka transaction log client did not close cleanly");
        }
    }
}

#[async_trait]
impl TxnLog for IggyTxnLog {
    async fn append(&self, partition: u32, record: &LogRecord) -> Result<(), BridgeError> {
        let location = self.location()?;
        let message = IggyMessage::builder()
            .payload(record.encode()?.into())
            .build()
            .map_err(BridgeError::Iggy)?;
        let mut messages = vec![message];
        match self.send(&location, partition, &mut messages).await {
            Err(error) if is_missing(&error) => {
                self.create(&location).await?;
                self.send(&location, partition, &mut messages).await
            }
            other => other,
        }
    }

    async fn read_all(&self) -> Result<Vec<LogRecord>, BridgeError> {
        let location = self.location()?;
        let client =
            tokio::time::timeout(REQUEST_TIMEOUT, self.client.connected(&self.bridge.config))
                .await
                .map_err(|_elapsed| BridgeError::Timeout)??;
        let mut records = Vec::new();
        for partition in 0..LOG_PARTITIONS {
            let mut offset = 0;
            loop {
                let polled = with_request_timeout(client.poll_messages(
                    &location.stream_id,
                    &location.topic_id,
                    Some(partition),
                    &Consumer::default(),
                    &PollingStrategy::offset(offset),
                    REPLAY_BATCH,
                    false,
                ))
                .await;
                let polled = match polled {
                    Ok(polled) => polled,
                    Err(error) if is_missing(&error) => return Ok(records),
                    Err(error) => return Err(error),
                };
                let Some(last) = polled.messages.last().map(|message| message.header.offset) else {
                    break;
                };
                for message in &polled.messages {
                    if let Some(record) = LogRecord::decode(&message.payload) {
                        records.push(record);
                    } else {
                        warn!(
                            partition,
                            offset = message.header.offset,
                            "skipping a Kafka transaction log record that does not parse"
                        );
                    }
                }
                offset = last + 1;
                if polled.messages.len() < REPLAY_BATCH as usize {
                    break;
                }
            }
        }
        Ok(records)
    }
}

#[async_trait]
impl DataPlane for IggyBridge {
    async fn high_watermark(&self, topic: &str, partition: i32) -> Result<u64, BridgeError> {
        // A negative index is a partition Iggy does not have, and answers so.
        let partition = u32::try_from(partition).unwrap_or(u32::MAX);
        let watermark = Self::high_watermark(self, topic, partition).await?;
        Ok(u64::try_from(watermark).unwrap_or(0))
    }

    async fn write_marker(&self, marker: &Marker) -> Result<Landed, BridgeError> {
        let partition = u32::try_from(marker.partition).unwrap_or(u32::MAX);
        let target = self.topic_target(&marker.topic)?;
        let deadline = Instant::now() + MARKER_DEADLINE;
        let message = control_message(
            marker.control,
            marker.producer_id,
            marker.epoch,
            marker.coordinator_epoch,
            wall_clock_ms(),
        )
        .map_err(|error| BridgeError::InvalidConfig(error.to_string()))?;
        // The producer's own session, after its turn: every send of the transaction that a
        // Produce request still holds lands first, and one in doubt is replayed ahead of this.
        let producer = Producer::Id {
            id: marker.producer_id,
            user: marker.user.clone(),
        };
        let sent = self
            .send_on_turn(producer, &target, partition, message, deadline)
            .await;
        if let Some(offset) = sent? {
            return Ok(Landed::At(offset));
        }
        let watermark = DataPlane::high_watermark(self, &marker.topic, marker.partition).await?;
        Ok(Landed::Below(watermark))
    }
}

impl IggyBridge {
    /// Sends `message` on the session of `producer`, after its turn, and gives the turn back.
    async fn send_on_turn(
        &self,
        producer: Producer,
        target: &super::TopicTarget,
        partition: u32,
        message: IggyMessage,
        deadline: Instant,
    ) -> Result<Option<u64>, BridgeError> {
        let turn = self
            .produce_turn(producer, deadline)
            .await
            .ok_or(BridgeError::Timeout)?;
        self.send_records(turn, (), target, partition, vec![message], deadline)
            .await
            .0
    }
}
