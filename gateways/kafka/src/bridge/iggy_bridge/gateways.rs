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

//! The gateways topic: where the gateways of one Iggy cluster announce themselves
//! (`crate::membership`).
//!
//! One partition, so every gateway reads the announcements in one order with one clock, Iggy's
//! append time. Announcements expire after an hour, which bounds the topic and is far longer than
//! any lease.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iggy::prelude::{
    Consumer, Identifier, IggyClient, IggyDuration, IggyError, IggyExpiry, IggyMessage,
    MaxTopicSize, MessageClient, Partitioning, PollingStrategy, StreamClient, StreamCreateOptions,
    TopicClient, TopicCreateOptions,
};
use tokio::time::Instant;
use tracing::info;

use super::{IggyBridge, cluster_id_stream};
use crate::bridge::error::BridgeError;
use crate::membership::{GATEWAYS_TOPIC, Heard, Registry};

/// How long an announcement is kept.
const ANNOUNCEMENT_EXPIRY: Duration = Duration::from_hours(1);

/// How long one announcement or one read may take. Shorter than the default lease, so a slow call
/// costs a heartbeat, not the lease.
const CALL_DEADLINE: Duration = Duration::from_secs(4);

impl IggyBridge {
    fn gateways_ids(&self) -> Result<(String, Identifier, Identifier), BridgeError> {
        let stream = cluster_id_stream(self.config.topic_mapping.default_stream());
        let stream_id = Identifier::named(&stream).map_err(BridgeError::Iggy)?;
        let topic_id = Identifier::named(GATEWAYS_TOPIC).map_err(BridgeError::Iggy)?;
        Ok((stream, stream_id, topic_id))
    }

    /// Creates the gateways topic, and its stream, unless another gateway already did.
    async fn create_gateways_topic(&self) -> Result<(), BridgeError> {
        let (stream, stream_id, _) = self.gateways_ids()?;
        let stream_options = StreamCreateOptions::default();
        match self
            .metadata_write(|| self.client.create_stream(&stream, &stream_options))
            .await
        {
            Ok(_) | Err(BridgeError::Iggy(IggyError::StreamNameAlreadyExists(_))) => {}
            Err(error) => return Err(error),
        }
        let options = TopicCreateOptions {
            partitions_count: Some(1),
            message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                ANNOUNCEMENT_EXPIRY,
            ))),
            max_topic_size: Some(MaxTopicSize::ServerDefault),
            ..TopicCreateOptions::default()
        };
        match self
            .metadata_write(|| {
                self.client
                    .create_topic(&stream_id, GATEWAYS_TOPIC, &options)
            })
            .await
        {
            Ok(_) => {
                info!(
                    "Created Iggy topic '{stream}/{GATEWAYS_TOPIC}' for Kafka gateway membership"
                );
                Ok(())
            }
            Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn on_membership_client<T, Call, Fut>(&self, call: Call) -> Result<T, BridgeError>
    where
        Call: FnOnce(Arc<IggyClient>) -> Fut,
        Fut: Future<Output = Result<T, BridgeError>>,
    {
        self.membership_client
            .until(&self.config, Instant::now() + CALL_DEADLINE, call)
            .await
    }

    async fn send_announcement(&self, payload: &[u8]) -> Result<(), BridgeError> {
        let (_, stream_id, topic_id) = self.gateways_ids()?;
        let payload = bytes::Bytes::copy_from_slice(payload);
        self.on_membership_client(|client| async move {
            let message = IggyMessage::builder()
                .payload(payload)
                .build()
                .map_err(BridgeError::Iggy)?;
            client
                .send_messages(
                    &stream_id,
                    &topic_id,
                    &Partitioning::partition_id(0),
                    &mut [message],
                )
                .await
                .map(|_sent| ())
                .map_err(BridgeError::Iggy)
        })
        .await
    }
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

#[async_trait]
impl Registry for IggyBridge {
    async fn announce(&self, payload: Vec<u8>) -> Result<(), BridgeError> {
        match self.send_announcement(&payload).await {
            Err(error) if is_missing(&error) => {
                self.create_gateways_topic().await?;
                self.send_announcement(&payload).await
            }
            sent => sent,
        }
    }

    async fn read(&self, offset: Option<u64>, count: u32) -> Result<Vec<Heard>, BridgeError> {
        let (_, stream_id, topic_id) = self.gateways_ids()?;
        let strategy = offset.map_or_else(PollingStrategy::last, PollingStrategy::offset);
        let polled = self
            .on_membership_client(|client| async move {
                client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        Some(0),
                        &Consumer::default(),
                        &strategy,
                        count,
                        false,
                    )
                    .await
                    .map_err(BridgeError::Iggy)
            })
            .await;
        let polled = match polled {
            Ok(polled) => polled,
            Err(error) if is_missing(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        Ok(polled
            .messages
            .into_iter()
            .map(|message| Heard {
                offset: message.header.offset,
                timestamp: message.header.timestamp,
                payload: message.payload,
            })
            .collect())
    }
}
