# InitProducerId and idempotent producers

Status: implemented, [#3545](https://github.com/apache/iggy/issues/3545). Gates the Phase 1
end-to-end test ([#3539](https://github.com/apache/iggy/issues/3539)), which drives
`kafka-console-producer.sh`.

## The problem

A stock Java producer sets `enable.idempotence=true` without being asked. That default arrived
in Kafka 3.0 and took effect from 3.0.1, 3.1.1 and 3.2.0, where a bug that suppressed it was
fixed. `kafka-console-producer.sh` leaves it on.

An idempotent producer sends InitProducerId (key 22) before its first record. A gateway that
does not list key 22 does not advertise it either, and the producer raises
`UnsupportedVersionException` rather than dropping back to weaker semantics. It fails at
startup, before it sends a record.

The gateway's stated purpose is that a Kafka user swaps the broker and changes no application
code. A broker that the default producer cannot start against does not meet it.

## What Iggy already deduplicates

Iggy deduplicates on the partition plane, and did so before this gateway existed. The key is
`(client_id, user_id, request)`. `client_id` is a `u128` the client generates, and `request` is a
monotonic counter on the client's session. Each partition group holds a per-client watermark with
a 128-bit `committed_window` under it. A request below the watermark with its bit clear is a
reordered arrival still to execute. A request below the window is refused with `RequestTooOld`,
because its outcome is unknown.
`partition.dedup_clients_max` caps the distinct clients one partition tracks, at 4096 by default
and 65536 at the ceiling.

The shape is closer to Kafka's than it looks. A producer id with a per-partition sequence maps
onto a session id with a request id. Under that mapping a request id is `base_sequence + 1` (see
below), so it advances by each batch's record count and the window is 128 records deep, not 128
batches. Four batches of 32 records fill it, one fewer than the five a Kafka producer keeps in
flight.

What breaks the mapping is the hop each side protects. Iggy's covers gateway to Iggy. Kafka's
covers producer to gateway. A retrying producer sends a fresh Produce request, and the gateway
turns it into a fresh Iggy send carrying a fresh request id. Nothing recognises the replay.

## Closing that gap

Both halves of Iggy's key have to come from the Kafka request rather than from the SDK.

The client id is not a per-request field. `dispatch_partition_request` takes it from the
transport's bound session, and the header transmute at
`core/server/src/dispatch/partition.rs:247` overwrites whatever the SDK sent. Deriving it from a
Kafka producer id therefore needs one bound session per producer.

The request id is different. The transmute copies the header and replaces only `group`, `client`,
`session` and `user_id`, so a caller-supplied request id survives to the `ClientTable`.

One session per producer is a connection pool keyed by producer id. The gateway has that pool, for
throughput: see the README's "Concurrency ceiling". Retries are still not deduplicated, because two
SDK seams are missing:

- a caller-chosen client id at build time. `ConsensusSession::with_client_id` is public, but both
  construction sites build with `ConsensusSession::new()`
  (`core/sdk/src/tcp/tcp_client.rs:378` and `:583`)
- a caller-supplied request id. `send_raw_with_response` already takes
  `preencoded: Option<RequestHeader>` for transient replay, and it is private

None of this lands in this phase. Produce and Fetch do not need it, delivery is at-least-once
before and after, and the additions belong to whoever owns that SDK surface.

### Restart

A session registers with a fresh random client id per gateway process. A restarted gateway
therefore cannot collide with a watermark that outlived it, and a retry that spans a restart is
not deduplicated. That is still at-least-once, which the README states plainly.

The alternative is a stable client id derived from the producer id. It deduplicates across a
restart, and it is unsafe without also persisting the last request id per producer. Request ids
restarting at 1 under a live watermark land below it. Inside the window, an id whose bit is set is
absorbed as a duplicate and its fresh write is discarded, while an id whose bit is clear executes.
An id below the window is refused with `RequestTooOld`. Silent loss is worse than duplicate
delivery, so the fresh random id wins.

### Confirmed and not

The hop mismatch, one session per producer, and the restart choice are agreed in the maintainer
thread on [#3545](https://github.com/apache/iggy/issues/3545) and in Discord.

Two further readings are ours and are not confirmed yet. One session per producer is enough,
rather than one per producer and partition, because the `ClientTable` is per partition group. The
same request number on two partitions is two entries, so `request_id = base_sequence + 1` stays
monotonic inside each. And the gateway has to preserve per-partition ordering per producer, or
records land out of order. A lower sequence arriving after a higher one lands below the watermark:
inside the window, with its bit clear, it executes after its successor, and below the window it is
refused with `RequestTooOld`.

## Options

| Option | Cost | What a stock producer does |
| -------- | ------ | ---------------------------- |
| Stub with `UNSUPPORTED_VERSION` | none | fails at startup unless the user sets `enable.idempotence=false` |
| Allocate only | about a day | works untouched, at-least-once delivery |
| Allocate and pool | weeks, blocked on the SDK | works untouched, retries absorbed on both hops |

## Decision

Allocate only, and defer deduplication.

Rejecting the stock producer to avoid that work trades away the one requirement the maintainers
named. It buys a guarantee nobody is asking for yet. Allocating costs about a day, leaves
delivery where it already is, and blocks nothing deduplication later needs.

## Behavior

Key 22 is in `SUPPORTED_RANGES` (`src/protocol/api.rs`) and therefore advertised through
ApiVersions. Without both, the producer never sends the request. `kafka-protocol` 0.18 carries
the schemas, request v0 to v5 and response v0 to v6, flexible from v2; the gateway serves v0 to
v5. The handler is `src/protocol/handlers/init_producer_id.rs`.

InitProducerId with no `transactional_id`:

- allocate the next producer id, return it with epoch 0 and error code 0
- build the id from an instance number in the high 16 bits and a counter in the low 47 bits,
  leaving bit 63 clear. `producer_id` is an `i64` and `-1` means no producer id, so the value has
  to stay non-negative. That leaves room for 65536 instances holding 140 trillion ids each

The instance number comes from configuration (`IGGY_KAFKA_INSTANCE_ID`, default 0), not from a
draw at startup. A random 16-bit number collides with even odds at around 300 instances. That is
a birthday collision, not a remote one.

The id is a pool key, not a dedup identity. Under the design above, the dedup identity is the
session's own random client id, minted at register. The producer id only decides which connection
serves a producer. Kafka still requires it to be unique across the cluster, which is what the
instance number buys. It also has to stay unique across a restart once dedup lands or
Produce persists, because `producer_epoch` is always 0, so a replayed id is a replayed
`(producer_id, producer_epoch)` pair a live producer may still hold. The counter therefore starts
at the wall clock in milliseconds rather than at 0. A restarted gateway starts above every id its
previous run handed out unless that run averaged more than one allocation per millisecond of its
uptime, or the clock stepped back across the restart. A clock that reads before the Unix epoch
seeds 0 and replays from the bottom. 2^47 milliseconds leaves the counter space thousands of years
from running out.

Nothing is persisted for this, so the clock seed narrows the replay window without closing it.
Produce reads the id as a pool key, and only an id this run handed out: any other id keys by
connection. A replayed pair therefore makes two producers of one user share a session and send one
at a time, and nothing more. It stops being harmless once deduplication reads the id. That work
must first either persist a high-water mark and seed the counter above it, or bump
`producer_epoch` on every gateway start. Either one makes a replayed
`(producer_id, producer_epoch)` pair impossible rather than unlikely.

`IGGY_KAFKA_INSTANCE_ID` defaults to 0 so a single gateway needs no configuration. Two gateways
left on the default draw from the same 47-bit space and can hand out identical ids, on the first
allocation if they start in the same millisecond. A gateway started without the variable logs a
warning, and the running value is in the startup log line. Gateways that set `IGGY_KAFKA_NODE_ID`
announce their instance ids to each other, and each one warns when it hears another with its own
([`CONSUMER_GROUPS.md`](CONSUMER_GROUPS.md#several-gateways-behind-one-bootstrap-list)).

An empty `transactional_id` reads as absent. A wire null decodes to `None`, but
`kafka-protocol`'s own `Default` is `Some("")`, and a producer that is idempotent-only names no
transaction either way.

InitProducerId with a `transactional_id` (with transactions off, the default; the Ostrium fork's
`IGGY_KAFKA_TRANSACTIONS_ENABLED=true` serves it instead, see [`TRANSACTIONS.md`](TRANSACTIONS.md)):

- answer `UNSUPPORTED_VERSION` (35). Transactions stay out of scope, and so do
  AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26) and TxnOffsetCommit (28)

Not for the reason the Produce path uses. `maybeTransitionToErrorState` governs a failed Produce
batch and never sees an InitProducerId response; those reach
`InitProducerIdHandler.handleResponse`, whose trailing `else` is `fatalError(new
KafkaException("Unexpected error in InitProducerIdResponse; ..."))`. Anything it does not
recognise is fatal there, so `INVALID_REQUEST` (42) would be equally fatal and the "42 is
abortable, therefore 35" argument does not apply to this API. 35 is chosen for consistency with
the Produce guard below, and because it is the one code that also states the truth: the gateway
does not implement this version of the transactional protocol. What must be avoided is a
*retriable* code: that same handler re-enqueues `COORDINATOR_LOAD_IN_PROGRESS` (14) and
`CONCURRENT_TRANSACTIONS` (51), so the producer would never stop trying.

No single response code is terminal on both target clients. 35 is fatal for the Java producer
and an infinite retry for librdkafka, whose `rd_kafka_idemp_check_error` treats only
`__UNSUPPORTED_FEATURE`, `INVALID_TRANSACTION_TIMEOUT` (50), 53 and 31 as fatal. 35 stays the
choice, and librdkafka is stopped at FindCoordinator instead: key 10 is unadvertised today, so
`rd_kafka_init_transactions()` fails before InitProducerId is reached. Phase 3 advertises key 10
for consumer groups and has to refuse a `TXN`-type coordinator lookup explicitly.

Produce:

- accept `producer_id`, `producer_epoch` and `base_sequence` on the request. The id picks the
  producer's session when this run handed it out. Since the Ostrium fork's KW-3 (see
  [Producer state](#producer-state-ostrium-fork-kw-3) below), the epoch and the sequence are
  checked against the producer's state, and the gateway answers
  `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) and `INVALID_PRODUCER_EPOCH` (47)
- reject a non-empty `transactional_id` (v3+) with `UNSUPPORTED_VERSION` (35), at the partition
  level, keeping the connection open. Under `acks=0` there is no response to carry 35, so the
  gateway closes the connection instead, as a Kafka broker does on any `acks=0` produce error.
  The check runs before the `acks=0` branch, so no write path can see a transactional batch

The paragraph this replaced said 45 and 46 stay unsent, because Iggy's watermark cannot tell a
gap from ordinary traffic. That holds for Iggy's dedup. The producer state below keeps Kafka's
own sequence numbers in the gateway, so it can detect a gap, and answers 45 for one. 46 stays
unsent: as on Kafka, a retry of a stored batch is answered `NONE` with its first offset.

The transactional refusal is where `maybeTransitionToErrorState` is exact. `Sender.completeBatch` ->
`canRetry` false -> `failBatch` -> `handleFailedBatch` -> `maybeTransitionToErrorState`, whose
explicit fatal set holds ClusterAuthorization, TransactionalIdAuthorization, ProducerFenced,
UnsupportedVersion and InvalidPidMapping. `INVALID_TXN_STATE` (48) is explicitly rewritten to
abortable there, and `INVALID_REQUEST` (42) and `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43) fall
through to abortable, so any of those would tell the application to abort and retry something
that can never succeed.

Without this guard a transactional batch would land as ordinary records once
[#3535](https://github.com/apache/iggy/issues/3535) wires the bridge: no last stable offset, no
abort markers, `read_committed` unimplementable, and an aborted transaction's records delivered
to every consumer.

The guard reads the request-level `transactional_id` only. A record batch also carries a
transactional bit in its attributes, and the stub keeps records opaque, so a hand-built frame
that sets the bit without the request field gets the retriable stub error (6). Java and
librdkafka set both, so no stock client reaches it, and the stub does not parse batches to close
it. [#3535](https://github.com/apache/iggy/issues/3535) decodes each batch before persisting it,
and must refuse one with the transactional bit set there, the same way.

## Producer state (Ostrium fork, KW-3)

Status: part 1 built in the Ostrium Labs fork (`docs/loams/kafka-wire-plan.md`, KW-3), not
upstream. `src/producer_state.rs` keeps what Kafka's `ProducerStateManager` keeps, per producer id
and partition: the epoch and the last five batches with their first and last sequence and where
each landed. Produce checks every batch that carries a producer id and a sequence before it sends:

| Batch | Answer | Written |
| --- | --- | --- |
| Equal to a stored batch that landed | `NONE` (0), with the base offset it landed at | No |
| First sequence follows the producer's last | as the send answers | Yes |
| A gap, or older than the five stored batches | `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) | No |
| An epoch below the producer's | `INVALID_PRODUCER_EPOCH` (47) | No |
| A new epoch that does not start at 0 | 45 | No |
| A producer with no state here (new, forgotten, or from before a restart) | as the send answers | Yes |
| A retry of a batch whose send still runs | `REQUEST_TIMED_OUT` (7) | No |
| A retry of a batch whose send Iggy never answered | `NONE` (0) once a replay proves it landed, 7 while the replay gets no answer either | Only by the replay, which Iggy applies at most once |
| The batch after one whose send Iggy never answered | as the send answers, once a replay proves the earlier one landed; 45 if it was refused | Yes, after the earlier one |

The rules are Kafka's (`ProducerAppendInfo.checkSequence`, `findDuplicateBatch`), and the oracle
suite (`tests/oracle_wire_diff_tests.rs`, scenario `idempotent_producer`) checks them against
Apache Kafka. A producer's batches already reach Iggy one at a time on its own session, so a
check and its send never race another batch of the same producer. The bridge reports how each
send ended (`IggyBridge::send_records_observed`), also when it ends after its request answered 7,
so the retry that follows is checked against the truth.

### Sends in doubt (part 2)

The SDK seams this document asked for above were built upstream in another shape. The SDK keeps
one logical session per client across reconnects, and a mutation that left the client without an
answer stays *retained* in its lane: the call ends with `OperationUncertain(op)`, later calls in
that lane get `LaneBlocked(op)`, and `Client::resolve_operation(op, deadline)` replays the frozen
frame, with its client id and request id, until Iggy answers (`core/sdk/src/driver.rs`,
`core/sdk/src/session_owner/lanes.rs`). Iggy's `ClientTable` answers a replay of a request that
already committed as a duplicate, so the frame lands at most once whatever the first send did. No
SDK change is needed; the gateway had only to use it:

- A send that ends with `OperationUncertain` leaves its batch *in doubt*, holding the operation
  and the client that retains it (`bridge::InDoubt`). Before part 2 the bridge read that error as
  a refusal, forgot the batch, and the retry wrote it again after the SDK replayed the first send
  ahead of it: two copies.
- A retry of a batch in doubt replays the retained request (`IggyBridge::resolve_send`) instead of
  sending the batch. Landed: answered `NONE`; Iggy's answer to a replay names no offset, so the
  base offset is `-1` (part 3 looks it up in the log). Refused: the batch is sent again. No answer
  yet: 7, and the producer retries.
- The batch after one in doubt replays it first, so it cannot land ahead of it, and is refused with
  45 if the earlier one never landed. A send in doubt keeps its lane in the SDK, so only the newest
  batch of a producer and partition can be in doubt.
- A send of another batch that meets `LaneBlocked` and cannot resolve the blocker sent nothing,
  and answers 7.

`produce_real_bridge_tests.rs` (`given_a_send_in_doubt_that_landed_late_...`) holds the bridge's
connection to Iggy in a relay until the SDK gives up on a send, lets the send land late, and
checks that the producer's retry is answered without a second copy.

### Restart safety (part 3)

Kafka rebuilds its producer state from its log on start. The gateway does the same from Iggy:

- The first record of every idempotent batch carries a `kafka.p` header with the batch's producer
  id, epoch and first and last sequence, and says `kafka.v` 2 (`docs/BRIDGE_MAPPING.md`, "Batch-level
  fields"). Fetch shows the record unchanged.
- Before a gateway checks the first idempotent batch for a partition, it reads the marks in the
  partition's last `DEFAULT_RECOVERY_RECORDS` (10,000) records and rebuilds each producer seen
  there: its newest epoch and its last five batches with their offsets
  (`ProducerStates::restore`, `IggyBridge::producer_marks`). Once per partition and gateway run.
  If the scan fails, the batch answers as the bridge failed (retriable) and nothing is written:
  checked against no state, a retry from before the restart would be written twice.
- So after a restart or a crash, a retry of a batch that landed is answered with its original
  offset, a gap is 45 and an older epoch 47, as before it. A producer whose last batches are
  older than the window has no state and is accepted at any sequence, which is what Kafka does for
  a producer whose state expired. The plan's "answer 59" for that case is not what Kafka 4.x does
  (`ProducerAppendInfo.checkSequence` accepts it), so the gateway follows Kafka.
- The marks also settle what the replay of part 2 leaves open: a replay that proves a batch landed
  gets its offset from the log, and a batch whose outcome is unknown and whose request is gone
  (`Landing::Unknown`, `Resolution::Lost`) is looked up there before it is written again.

`producer_restart_real_bridge_tests.rs` kills a gateway process with SIGKILL after a batch landed
and before the producer retries it, and checks that the next gateway answers the retry with the
first offset, refuses a gap, and stores every record once. The oracle scenario
`idempotent_restart` (`tests/oracle_wire_diff_tests.rs`) restarts Apache Kafka and the gateway in
the middle of the same sequence and compares every answer.

Compatible with transactions later (KW-T): epochs are kept per producer id and partition and a
higher epoch replaces the state, which is what an external bump from a transaction coordinator
needs; the marker keeps a flags byte for the transactional bit. Nothing here implements
transactions, and InitProducerId keeps refusing a `transactional_id`.

What stays at-least-once:

- A batch whose outcome is unknown, whose request nothing keeps (cut off at the slot limit, its
  session replaced, or its Produce session closed after 10 idle minutes), and which is not found
  in the scanned tail: either it never landed, or it is older than the window, or it is still in
  Iggy's pipeline and lands after the gateway looked. It is written again.
- A batch that lands, from a gateway that then dies, after the next gateway already scanned the
  partition: the same late-landing window, which only a fence on the old gateway's Iggy sessions
  closes (KW-5's ownership model).
- More than 100,000 producer-partition entries, or a producer idle for a day: forgotten, and then
  accepted at any sequence, as on Kafka.
- The window is a constant today; making it configurable belongs to KW-12 (configurable limits).

## Invariants this design rests on

Both are absences. Losing either is caught: `golden_wire_fixtures_tests.rs` pins the ApiVersions
v1 and v3 bodies byte-exactly, so adding a finalized feature or any advertised key fails both
goldens.

**Never advertise `transaction.version >= 2` in the ApiVersions `finalized_features`.**
`TransactionManager.maybeUpdateTransactionV2Enabled` reads it, and under TV2 `maybeAddPartition`
adds partitions client-side, so AddPartitionsToTxn and AddOffsetsToTxn are never sent at all.
That collapses the absence gate this design depends on. `api_versions::encode_response` builds
an `ApiVersionsResponse` with no finalized features, which is correct and load-bearing.

**The transactional handshake order is fixed**: FindCoordinator(TXN) -> InitProducerId ->
AddPartitionsToTxn. While FindCoordinator (key 10) stays unadvertised, `initTransactions()`
already dies before InitProducerId is reached, and the InitProducerId branch is belt and braces.
Advertising FindCoordinator for consumer groups
([#3541](https://github.com/apache/iggy/issues/3541)) removes that shield, and is what makes the
branch load-bearing.

## What this does not give you

Upstream, as written for #3545: a producer that holds an id believes its retries are
deduplicated, and they are not. A retry after a network timeout writes the record twice, and
both copies reach the stream with their own offsets. Iggy's own deduplication does not help,
because it guards the other hop.

In the Ostrium fork, KW-3 parts 1 to 3 above close that, except for the narrow cases listed under
"What stays at-least-once".

The README states that limitation under "Delivery guarantees", so a user does not have to infer
it from the presence of key 22.

## Resolution

The open question was allocate only, as above, against stub and document
`enable.idempotence=false`. Allocate only was taken, which this document named as the default
outcome, and is what shipped.

## References

- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md), batch-level fields
- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Version firewall: `src/protocol/api.rs`, `SUPPORTED_RANGES`
- Dedup key and window: `core/consensus/src/client_table.rs`
- Session identity: `core/sdk/src/session.rs`, `core/sdk/src/tcp/tcp_client.rs`
- Header rewrite: `core/server/src/dispatch/partition.rs`
- Produce fatal path: `TransactionManager.maybeTransitionToErrorState`, apache/kafka trunk
- InitProducerId fatal path: `TransactionManager.InitProducerIdHandler.handleResponse`, same file
- librdkafka's fatal set: `rd_kafka_idemp_check_error`, `src/rdkafka_idempotence.c`
