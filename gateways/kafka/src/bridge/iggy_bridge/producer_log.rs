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

//! Reading idempotent producers' batches back from the log (KW-3 part 3).
//!
//! Each idempotent batch carries a `kafka.p` marker on its first record (`records.rs`). A
//! gateway that starts, or that never wrote a partition before, rebuilds the producer state of
//! that partition from the marks in its tail, and a batch whose send ended without a known
//! outcome is looked up there.

use std::sync::Arc;

use tokio::time::Instant;

use super::IggyBridge;
use crate::bridge::error::BridgeError;
use crate::records::{ProducerMark, mark_of};

/// Messages one poll of the scan reads: 20 polls for the default window, and a bounded share of
/// memory when the records are large.
const SCAN_CHUNK: u32 = 500;

impl IggyBridge {
    /// The producer marks among the last `records` messages of `partition` of `kafka_topic`,
    /// oldest first, each with the offset of the record it is on.
    ///
    /// # Errors
    ///
    /// Those of [`Self::high_watermark`], of a poll, and [`BridgeError::Timeout`] when no Fetch
    /// slot frees or the scan does not end by `deadline`. The scan is then incomplete, and the
    /// caller must not take its silence about a batch as proof that the batch is absent.
    pub async fn producer_marks(
        self: &Arc<Self>,
        kafka_topic: &str,
        partition: u32,
        records: u32,
        deadline: Instant,
    ) -> Result<Vec<(u64, ProducerMark)>, BridgeError> {
        let end = u64::try_from(self.high_watermark(kafka_topic, partition).await?).unwrap_or(0);
        let mut offset = end.saturating_sub(u64::from(records));
        let mut marks = Vec::new();
        let mut slot = self
            .fetch_slot(deadline)
            .await
            .ok_or(BridgeError::Timeout)?;
        while offset < end {
            let count = u32::try_from(end - offset).map_or(SCAN_CHUNK, |left| left.min(SCAN_CHUNK));
            let (polled, back) = self
                .poll(slot, kafka_topic, partition, offset, count, deadline)
                .await
                .ok_or(BridgeError::Timeout)?;
            slot = back;
            let polled = polled?;
            let Some(last) = polled.messages.last().map(|message| message.header.offset) else {
                // An empty poll below the watermark proves nothing: Iggy answers one on a read
                // fault too.
                return Err(BridgeError::Timeout);
            };
            marks.extend(
                polled.messages.iter().filter_map(|message| {
                    mark_of(message).map(|mark| (message.header.offset, mark))
                }),
            );
            offset = last + 1;
        }
        drop(slot);
        Ok(marks)
    }

    /// The offset of the batch that `mark` names, if one of the last `records` messages of
    /// `partition` carries it. The newest, if a retry wrote it twice.
    ///
    /// # Errors
    ///
    /// See [`Self::producer_marks`].
    pub async fn find_batch(
        self: &Arc<Self>,
        kafka_topic: &str,
        partition: u32,
        mark: ProducerMark,
        records: u32,
        deadline: Instant,
    ) -> Result<Option<u64>, BridgeError> {
        Ok(self
            .producer_marks(kafka_topic, partition, records, deadline)
            .await?
            .into_iter()
            .rev()
            .find(|(_, found)| *found == mark)
            .map(|(offset, _)| offset))
    }
}
