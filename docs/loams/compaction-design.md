# Log compaction (KW-6): design note

Status: **draft, fork first** (owner default for the plan's open question 4:
build in the fork, keep this note ready for an upstream discussion on
apache/iggy#4521 once the owner consents). Nothing here is built yet; KW-4
already stores `cleanup_policy` per topic and gives a topic without `delete`
no expiry, so no data is lost while compaction is missing.

## Goal

A topic whose `cleanup_policy` contains `compact` keeps, for every key, at least
the last record written with that key. Older records with the same key, and
tombstones older than `delete.retention.ms`, are removed from sealed segments.
Kafka Connect's internal topics, Debezium's offsets, Schema Registry's
`_schemas` and Kafka Streams changelogs then stay bounded.

## Defaults for #4521's open questions

| Question | Default |
| --- | --- |
| What is the key? | The `kafka.key` user header the gateway already writes. No new frame field, so no wire change for any SDK. A record without it is kept (a keyless record in a compacted topic is never removed) |
| What is a tombstone? | A record carrying the gateway's `kafka.value` = `null` marker header |
| Who decides what to clean? | A replicated decision: a new internal metadata operation `CompactPartition { stream, topic, partition, clean_up_to }`, modelled on `TruncatePartition` (operation 68). Each replica then cleans locally, like retention does today, but over an offset range, not over its own segment boundaries |
| How far may it clean? | Never into the active segment; never past the lowest committed offset of any consumer group on the partition (so a consumer that has not read a record yet still reads every value it would have read); never past `now - min.compaction.lag.ms`; and, once KW-T exists, never past the last stable offset |
| Offsets | Kept. A compacted segment has gaps. Fetch and ListOffsets serve across them (the poll path already returns the next retained record) |
| Encrypted streams | Out of scope for the first version: user headers are encrypted at rest, so the cleaner would need the stream key. A compact topic on an encrypted stream keeps every record and the server logs that once |

## What the existing code already gives

From a read of the fork at the KW-4 base (paths under `core/`):

- Batches are byte-identical on every replica (`binary_protocol/src/batch.rs`;
  the primary's prepare timestamp is reused), but segment boundaries are local
  (`active_segment().is_full()`), so whole segment files are not.
- A message's offset is `base_offset + offset_delta`; the format already allows
  gaps inside a batch.
- Poll by offset (`partitions/src/poll_plan.rs`, `journal.rs`
  `select_batch_slice`) returns the next record at or after the requested
  offset, so a read inside a removed range already lands on the next retained
  record.
- The gateway's Fetch (`gateways/kafka/src/protocol/handlers/fetch.rs`) uses each
  record's real offset and does not assume consecutive offsets.
- User headers are readable zero-copy (`WireUserHeaders::from_slice`).
- Retention runs per replica in `server/src/segment_cleaner.rs` →
  `IggyPartition::clean_expired_segments`; the replicated watermark
  `deleted_up_to_offset` (`metadata/src/stm/stream.rs`) is the model for a
  replicated cleaning point.

## What has to change

1. **Recovery must accept gaps in a compacted segment.**
   `segment_recovery.rs` refuses `OffsetDiscontinuity` when a batch's
   `base_offset` differs from the previous `base_offset + message_count`, and
   `ensure_contiguous_chain` refuses gaps between segments without an anchor.
   A compacted segment gets a marker (a `compacted_through` field in its
   `SegmentAnchor`, or a `.compacted` sidecar), and the walk accepts a forward
   gap inside a marked segment. An unmarked segment keeps today's strict check,
   so a torn write is still caught.
2. **Message counts.** `settle_cleaned_segment` counts `end - start + 1` and
   `CommittedBatchStats::end_offset` is `base + count - 1`. Both become "last
   offset in the batch" (`base_offset + last offset_delta`) for compacted
   segments; partition message counts are recomputed from the rewritten
   segments.
3. **The cleaner** (`partitions/src/compaction.rs`, new):
   - Pass 1 reads the cleanable range `[log_start, clean_up_to]` across sealed
     segments and builds `key → last offset` (an offset map, bounded by a
     configurable memory budget; a range too large for the budget is cleaned in
     several passes, Kafka-style).
   - Pass 2 rewrites each sealed segment that has removable records into
     `{start}.log.cleaning` / `.index.cleaning`: retained messages keep their
     id, offset, timestamp, payload and headers; each surviving batch keeps its
     `base_offset` and gets new `offset_delta`s, `message_count`,
     `batch_length` and both checksums (`calculate_batch_checksum`). A batch
     left empty is dropped.
   - The swap is a rename of both files, then the anchor marker, fsynced in
     that order, so a crash leaves either the old or the new segment.
   - Small adjacent compacted segments are merged later (not in the first
     version).
4. **The trigger.** `segment_cleaner.rs` gains a compaction branch for topics
   whose `cleanup_policy` compacts: when the dirty ratio (bytes above the last
   cleaning point / total) passes `min.cleanable.dirty.ratio`, or
   `max.compaction.lag.ms` has passed, the primary proposes `CompactPartition`
   with `clean_up_to = min(last sealed offset, lowest committed group offset,
   now - min_compaction_lag)`. Replicas apply it through the reconciler the way
   `reconcile_segment_truncations` applies truncation.
5. **Options.** The catalog gets `delete_retention`, `min_compaction_lag`,
   `max_compaction_lag` and `min_cleanable_dirty_ratio` topic keys; the gateway
   maps `delete.retention.ms`, `min.compaction.lag.ms`, `max.compaction.lag.ms`
   and `min.cleanable.dirty.ratio` onto them, moving them from "echoed" to
   "honoured" in `CONFIGS.md`.
6. **Expiry.** With compaction built, a `compact` topic no longer needs the
   KW-4 rule "no expiry": `compact` alone still never expires (Kafka's
   semantics), `compact,delete` expires as `delete` does.
7. **Gateway Fetch.** `empty_poll` treats an empty poll below the high
   watermark as not ready. Compaction never empties the range up to the high
   watermark (the active segment is never cleaned), so this does not stall,
   but the gateway test suite gains a case for a Fetch landing inside a removed
   range.

## Tests

- Unit: the offset map and the rewrite of a batch (gaps, checksums, an emptied
  batch, a keyless record, a tombstone inside and past `delete.retention.ms`).
- Recovery: a compacted segment with gaps recovers; an unmarked segment with
  the same gap is refused; a crash at each step of the swap recovers one side.
- Simulator: a compaction operation on a replicated partition leaves every
  replica with the same retained `(offset, key, value)` set.
- Gateway: Fetch inside a removed range returns the next record, never
  `NOT_LEADER_OR_FOLLOWER` (6); Connect and Streams restart on a compacted log.
- Oracle: the same keyed writes with tombstones into a compacted topic on
  Apache Kafka (low `segment.ms`, `min.cleanable.dirty.ratio=0.01`) and on the
  gateway; after cleaning, a full read returns the same last record per key.

## Size and order

Recovery gap tolerance and the cleaner core first (one PR, no trigger: a
server test drives it), then the replicated trigger and options, then the
gateway mapping and the oracle scenario. KW-7 (log start offset, lane D)
touches the same segment code; this work rebases on it.
