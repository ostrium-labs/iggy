# Kafka wire compatibility: the fork plan

Status: **plan, 2026-10-23.** Nothing in this document is built yet unless a
task row says so. Transactions and exactly-once (KW-T, apache/iggy#4522) are
**planned, not built**, and stay that way until the Loams owner says otherwise.

Owner direction (2026-10-23): "work on kafka wire now, do all things except
kafka transactions but plan it fully, do these in our fork." Loams' side of the
decision is recorded in the Loams decision log as D1796–D1798, which amend D408
(Kafka clients reach Loams through Iggy's gateway, with Loams' contributions).

This plan lives on the fork's `loams` branch. Each task is one PR into `loams`,
merged when `Kafka gateway required` is green (see [README.md](README.md)).

## Contents

1. [Where the gateway stands](#1-where-the-gateway-stands)
2. [What the target clients need](#2-what-the-target-clients-need)
3. [Oracles and the test strategy](#3-oracles-and-the-test-strategy)
4. [Rules for every task](#4-rules-for-every-task)
5. [Tasks, in order](#5-tasks-in-order)
6. [KW-T: transactions and exactly-once (planned, not built)](#6-kw-t-transactions-and-exactly-once-planned-not-built)
7. [How Loams runs it](#7-how-loams-runs-it)
8. [What can run in parallel](#8-what-can-run-in-parallel)
9. [Open questions for the owner](#9-open-questions-for-the-owner)

## 1. Where the gateway stands

Surveyed on 2026-10-23 at upstream `master` 02dac7896.

`gateways/kafka/` is a mature crate: about 78k lines, 1440 tests (all green in
the fork's first CI run, 39945979616), and design notes under
`gateways/kafka/docs/` that are better than most brokers' own. It runs as its
own binary (`iggy-gateway-kafka`), speaks the Kafka wire through the
`kafka-protocol` 0.18 crate, and maps every Kafka record onto one Iggy message
(`docs/BRIDGE_MAPPING.md`).

### Served today

| Area | Keys and versions | Notes |
| --- | --- | --- |
| Data path | Produce 3–9, Fetch 4–12, ListOffsets 1–7, Metadata 0–9 | Fetch has no fetch sessions, no `isolation_level`, `log_start_offset` is unknown (-1) |
| Discovery | ApiVersions 0–3, DescribeCluster 0–2 | Stable cluster id stored in Iggy |
| Classic groups | FindCoordinator 0–4, JoinGroup 0–9, SyncGroup 0–5, Heartbeat 0–4, LeaveGroup 0–5, static membership (KIP-345) | Group state is in one gateway's memory |
| Offsets | OffsetCommit 2–9, OffsetFetch 1–9, OffsetDelete 0, DeleteGroups 0–2 | Stored as Iggy external group offsets (`kafka.cg.<group>`) |
| Group admin | DescribeGroups 0–6, ListGroups 0–5 | Offsets-only groups report `Empty` (#4559) |
| Topic admin | CreateTopics 2–5, DeleteTopics 1–5, CreatePartitions 0–3, auto topic creation | Topic configs: only `retention.ms` is stored |
| Configs | DescribeConfigs 1–4, AlterConfigs 0–2, IncrementalAlterConfigs 0–1 | Topic resources only |
| Producer ids | InitProducerId 0–5 | Allocates an id at epoch 0; a `transactional_id` gets 35 |
| Security | TLS listeners, mTLS identities, SASL PLAIN (SaslHandshake, SaslAuthenticate), DescribeAcls | Data plane runs as the bridge's Iggy user |

### Not served

Transactions (22 with a transactional id, 24, 25, 26, 28), DeleteRecords (21),
OffsetForLeaderEpoch (23), CreateAcls and DeleteAcls (30, 31), delegation tokens
(38–41), DescribeLogDirs (35), quotas (48, 49), SCRAM credentials (50, 51),
DescribeProducers (61), the transaction listings (65, 66), KIP-848 (68, 69),
telemetry (71, 72), DescribeTopicPartitions (75), share groups and Streams
groups (76–80, 88–92), and anything newer than the versions above.

### Upstream state of each parity issue

All open items of apache/iggy#3560 and the follow-ups that krishvishal opened
on 2026-10-21. No open upstream PR touches `gateways/kafka/` on 2026-10-23;
the maintainers merge about one gateway PR a day, so check again at the start
of every task.

| Issue | Title (short) | Upstream state on 2026-10-23 | Fork task |
| --- | --- | --- | --- |
| #4531 | Client compatibility matrix in CI | Unassigned | KW-1, KW-2 |
| #4513 | Idempotent producer dedup and ordering | Unassigned | KW-3 |
| #4508 | Topic configurations frameworks send | Assigned to krishvishal; part landed in #4534 (`cleanup.policy` no longer reported, `-1` accepted) | KW-4 |
| #4511 | Offsets-only groups, DeleteGroups, OffsetDelete | Assigned to krishvishal; landed in #4559 except member `client_id` and `client_host` | KW-12 |
| #4515 | Several gateways behind one bootstrap | Unassigned | KW-5 |
| #4521 | Log compaction | Unassigned; a maintainer said it needs a storage-format design discussion first | KW-6 |
| #4525 | Real log start offset, DeleteRecords | Unassigned | KW-7 |
| #4527 | Newer protocol versions, topic ids | Unassigned | KW-8 |
| #4512 | SASL SCRAM and OAUTHBEARER | Unassigned | KW-9 |
| #4518 | Per-principal authorization, ACL and quota admin | Unassigned | KW-10 |
| #4517 | Repeated header names and order | Unassigned | KW-11 |
| #4530 | Configurable limits | Unassigned | KW-12 |
| #4523 | KIP-848 consumer groups | Unassigned | KW-13 |
| #4524 | DescribeLogDirs, read-only `__consumer_offsets` | Unassigned | KW-14 |
| #4526 | Remaining admin APIs | Unassigned | KW-15 |
| #4528 | Old and pinned clients | Unassigned | KW-16 |
| #4529 | Streams groups and share groups | Unassigned | KW-17 |
| #3550 | Migration guide | Assigned to ryerraguntla | KW-18 |
| #4522 | Transactions and exactly-once | Unassigned; `SCOPE.md` says "not planned" | KW-T (plan only) |

The owner offered on #3560 (2026-10-21) to take #4513, #4521, #4531 and #4512.
No maintainer has replied. Nothing more is posted upstream without the owner's
consent.

## 2. What the target clients need

Loams' Kafka clients, from D74 and D408: Kafka Connect with Debezium, Flink,
RisingWave, Vector, Fluent Bit, librdkafka-based clients, franz-go, the Java
client and kcat. "Needs" below is what each does with default or common
settings; "blocked by" is the task that removes the failure or the silent
degradation. Rows marked (verify) come from reading client code or docs, and
KW-2's matrix replaces each with a measured result.

| Client | What it does on the wire | Works today | Blocked by |
| --- | --- | --- | --- |
| Java client 3.9 / 4.x | Idempotent producer by default (`acks=all`, 5 in flight); classic groups; Admin | Yes, at-least-once | KW-3 (duplicates and reordering on retry), KW-8 (4.x negotiates lower versions; fine), KW-13 (`group.protocol=consumer`) |
| librdkafka 2.x (confluent-kafka-*, Vector, Fluent Bit, RisingWave) | Producer not idempotent by default; classic groups via subscribe; `assign` for RisingWave; watermark queries | Yes (kcat 1.7.1 is librdkafka 1.7 in today's suite; 2.x not tested) | KW-2 (test it), KW-3 if `enable.idempotence=true`, KW-13 for `group.protocol=consumer` |
| franz-go | Idempotent by default; KIP-848 when the broker advertises it, else classic | Expected yes (not tested) | KW-2, KW-3 |
| kcat | librdkafka 1.7 | Yes (in today's suite) | None |
| Kafka Connect distributed + Debezium | Creates `config`, `offset`, `status` topics with `cleanup.policy=compact` and RF 3; reads them in full at start; Debezium schema history topic with `retention.ms`/`retention.bytes`; `connect` group protocol through JoinGroup/SyncGroup | Starts only on topics created by hand (verify) | KW-4 (configs), KW-6 (compaction, or the internal topics grow without bound), KW-3 (config topic producer is idempotent) |
| Flink Kafka connector (KafkaSource / KafkaSink) | Admin `describeTopics`, `listOffsets` (earliest, latest, timestamp); `assign` + Fetch; commits offsets on checkpoint with no group membership; sink `AT_LEAST_ONCE` uses an idempotent producer; `EXACTLY_ONCE` uses transactions | `AT_LEAST_ONCE` expected yes (verify the standalone commit) | KW-2, KW-3, KW-7 (EARLIEST and data-loss checks), KW-T for `EXACTLY_ONCE` |
| RisingWave source and sink | librdkafka; `assign` with offsets; watermark queries; sink with `upsert`/`debezium` formats, keys and tombstones | Expected yes (verify) | KW-2, KW-7 (EARLIEST) |
| Vector `kafka` source and sink | librdkafka subscribe, async commits; sink producer | Expected yes (verify) | KW-2 |
| Fluent Bit `in_kafka` / `out_kafka` | librdkafka | Expected yes (verify) | KW-2 |
| Kafka Streams (not a D74 target; Loams' users bring it) | Compacted changelogs, `message.timestamp.type`, DeleteRecords on repartition topics, large assignments | No: stops at first rebalance with a state store (#4508) | KW-4, KW-6, KW-7, KW-12 |

Two production properties matter to every client and are not client features:
more than one gateway behind one bootstrap list (KW-5), and per-principal
authorization so Loams namespaces cannot read each other (KW-10).

## 3. Oracles and the test strategy

Every task proves its behaviour against a real Kafka, not only against
hand-built frames. Two oracles, both containers, used as black boxes:

| Oracle | How it runs | Licence and rule | Used for |
| --- | --- | --- | --- |
| **Apache Kafka** | `apache/kafka` container, KRaft single node, pinned (the suite uses 3.9.0 for client tools today; KW-1 adds `apache/kafka:4.3.1` as the broker oracle) | Apache-2.0. Reading its source is fine | The default oracle for every task, in fork CI |
| **Redpanda** | `redpandadata/redpanda:v26.1.18` container (Docker Hub), single node, pinned | BSL. **Black box only**: never read or copy its source; only its container and public docs | The second transactions oracle (approved by the owner earlier); a second opinion where Kafka and the gateway disagree |

A managed Kafka (for example Confluent Cloud) is a possible later oracle, once
startup credits come through; nothing in this plan or the harness uses one
today.

### 3.1 The differential harness (KW-1)

One scenario runs against several targets and the results are compared after
normalisation:

- **Wire level**: the test sends the same request sequence with
  `kafka-protocol`'s client encoder to the gateway and to each oracle, and
  compares the decoded responses field by field. Fields that legitimately
  differ (node ids, host and port, cluster id, throttle time, timestamps under
  `LogAppendTime`, error messages, tagged fields the gateway leaves out) are
  normalised by a declared per-API rule list, so a new difference fails the test
  until someone classifies it.
- **Client level**: the same client program (in its container) runs against
  each target, and the test compares what it observed: records read back (key,
  value, headers, order), offsets, committed offsets, error classes, and how
  the client ended (exit code, fatal or retriable).
- **Recorded differences**: a difference that is intended (Iggy cannot honour
  something, or the gateway refuses on purpose) is listed in
  `gateways/kafka/tests/oracle/allowed_differences.toml` with the issue that
  owns it. The nightly report links each failure to its blocking issue (#4531's
  acceptance criterion).

### 3.2 Gates

- **Every PR into `loams`**: the existing suite plus the KW-2 smoke subset and
  the wire-level diffs against Apache Kafka, in `Kafka gateway required`.
- **Nightly** (fork workflow, schedule): the full container matrix and the
  Redpanda diffs.

## 4. Rules for every task

- One task, one PR into `loams`, merged on a green `Kafka gateway required`.
- At task start: `git fetch upstream`, check for a new upstream PR or merged
  commit on the item. If a maintainer has one, cherry-pick it with attribution
  (`git cherry-pick -x`, keep the author) instead of writing our own, and drop
  ours when upstream merges.
- Rebase-friendly: new code in new files where natural, small diffs in upstream
  files, no reformatting, upstream style (the crate's `docs/*.md` conventions,
  pedantic clippy, ASF headers on new non-Markdown files).
- Each task updates the gateway doc it changes (`SCOPE.md`,
  `kafka_api_keys_reference.md`, the topic doc) and its row in this plan.
- Wire changes in `core/binary_protocol/` types tagged `#[repr(C)]`, connector
  FFI signatures and the state-save path need the owner's OK first (upstream's
  STOP list in `AGENTS.md` applies to the fork as well).
- Local builds: the shared target, `-j 6`, never in `/tmp`; delete your own
  test artefacts. The fork CI is the full gate.
- **Upstream candidate** in a task means it is written so that it could be
  offered upstream as one PR, after the owner's consent and the issue's
  assignment (upstream's CONTRIBUTING requires both).

## 5. Tasks, in order

Order: the oracle and the matrix first (they gate everything), then what the
target clients hit with default settings, then production properties, then
breadth. Status is `Planned` unless noted.

| # | Task | Issue | Status | Depends on | Upstream candidate |
| --- | --- | --- | --- | --- | --- |
| KW-0 | Fork setup: `loams` branch, workflows, lean CI | — | **Done** (7aa0efbf4) | — | No |
| KW-1 | Differential oracle harness (Apache Kafka, Redpanda) | #4531 | **Built** (this PR) | KW-0 | Yes |
| KW-2 | Client compatibility matrix: smoke on PRs, full nightly | #4531 | Planned | KW-1 | Yes |
| KW-3 | Idempotent producer: dedup, ordering, restart safety | #4513 | **Part 1 built** (producer state); parts 2–3 planned | KW-1 | Yes |
| KW-4 | Topic configurations frameworks send | #4508 | Planned | KW-1 | Only if krishvishal has not landed it |
| KW-5 | Several gateways behind one bootstrap | #4515 | Planned | KW-3 | Yes (design first) |
| KW-6 | Log compaction | #4521 | Planned | KW-4 | Design discussion first |
| KW-7 | Real log start offset and DeleteRecords | #4525 | Planned | KW-1 | Yes |
| KW-8 | Newer protocol versions and topic ids | #4527 | Planned | KW-1 | Yes |
| KW-9 | SASL OAUTHBEARER, then SCRAM | #4512 | Planned | KW-1 | Yes |
| KW-10 | Per-principal authorization, ACL admin | #4518 | Planned | KW-9 | Yes (design first) |
| KW-11 | Repeated header names and header order | #4517 | **Built** (this PR) | KW-1 | Yes |
| KW-12 | Configurable limits; member client id and host | #4530, #4511 | **Started** (branch `kw12-limits`, not built or tested) | KW-0 | Yes |
| KW-13 | KIP-848 consumer groups | #4523 | Planned | KW-8 | Yes |
| KW-14 | DescribeLogDirs, read-only `__consumer_offsets` | #4524 | Planned | KW-7 | Yes |
| KW-15 | Remaining admin APIs | #4526 | Planned | KW-3, KW-8 | Yes |
| KW-16 | Old and pinned clients | #4528 | Planned | KW-2 | Yes |
| KW-17 | Streams groups and share groups | #4529 | Planned | KW-13 | Later |
| KW-18 | Kafka-to-Iggy migration guide | #3550 | Planned | KW-5 | Assigned upstream; fork copy only |
| KW-L1 | Fork images: `iggy-server` and `iggy-gateway-kafka` on GHCR | — | **Done** (one image, `ghcr.io/ostrium-labs/iggy`) | KW-0 | No |
| KW-L2 | Loams compose profile `kafka` | — (Loams repo) | **Done** (ostrium-labs/loams#1148) | KW-L1 | No |
| KW-T | Transactions and exactly-once | #4522 | **Planned, not built** | KW-3, KW-5, KW-6, KW-7 | Design discussion first |

### KW-1. Differential oracle harness

**Why.** Every later task needs "the same scenario against real Kafka" as its
exit check. Today's real-client suite (`kafka_client_e2e_tests.rs`) runs
clients against the gateway only.

**Files.**

- `gateways/kafka/tests/common/oracle.rs` (new): the oracle containers
  (`KafkaOracle`, `RedpandaOracle`) on free loopback ports with
  `--network host` and `--pull never` (as the existing suite does), readiness
  probes, and `WireClient`, which encodes requests with `kafka-protocol`'s
  client half and negotiates each API down to what the target advertises.
- `gateways/kafka/tests/common/facts.rs` (new): a scenario records named,
  already-normalised facts (node ids, hosts, cluster ids, throttle times and
  broker-chosen ids are never recorded); `compare` and `judge` turn them into
  unexpected, declared and stale differences.
- `gateways/kafka/tests/oracle/allowed_differences.toml` (new): every
  intended difference, with the issue that owns it.
- `gateways/kafka/tests/oracle_wire_diff_tests.rs` (new): wire-level diffs for
  ApiVersions (the gateway never advertises above the oracle), CreateTopics,
  Metadata, Produce, Fetch, ListOffsets (earliest, latest, by time, max
  timestamp), DescribeConfigs, the classic group flow (FindCoordinator →
  JoinGroup → SyncGroup → Heartbeat → OffsetCommit → OffsetFetch →
  LeaveGroup), a commit with no membership (Flink's pattern), and
  InitProducerId.
- `.config/nextest.toml`: the new binary into the `kafka_client_e2e` group
  (one container-heavy suite at a time).
- `.github/workflows/loams-kafka-gateway.yml`: pull the oracle images.

**Tests.** The diff tests themselves; plus a self-test that plants a
difference and expects the harness to report it, and one that a declared
difference which no longer occurs is reported as stale.

**Oracle comparison.** Gateway vs Apache Kafka on every PR; vs Redpanda
nightly.

**Exit.** The diff suite runs green in fork CI against Apache Kafka with every
remaining difference listed in `allowed_differences.toml` with its issue.

**As built (2026-10-23).** `oracle_wire_diff_tests.rs` passes against
`apache/kafka:4.3.1` and `redpandadata/redpanda:v26.1.18` locally, with 18
declared differences. Apache Kafka is the reference: a fact where the gateway
agrees with Kafka and only Redpanda differs is reported as "oracles disagree"
and does not fail the run (Redpanda advertises lower maxima for 11 keys,
answers `MAX_TIMESTAMP` with offset 0, and answers 6 rather than 3 for a
missing partition). Besides the gaps already owned by #4508, #4517, #4522
and #4525, the harness surfaced two documented behaviours that differ from
Kafka and that Loams' clients can notice:

- **Commit metadata is dropped** (#3542, `OFFSET_STORAGE.md`): OffsetFetch
  answers `""`. Kafka Streams stores task metadata there. Added to KW-12.
- **Lookups by time search Iggy's append time**, not the producer timestamp
  (#4516, `BRIDGE_MAPPING.md`): backfilled or replayed records with old
  `CreateTime` resolve to the wrong offset. Flink's "start from timestamp" and
  `--reset-offsets --to-datetime` see it. Added to KW-7 (a time index on the
  producer timestamp, or on `kafka.ts`).

Fetch past the high watermark answers retriable 6 for 30 s before
`OFFSET_OUT_OF_RANGE` (1), on purpose (README, Fetch error mapping); declared
against #3536.

### KW-2. Client compatibility matrix

**Why.** #4531, and D74's gate ("M5 gates on the Kafka client test suites").
What we know about RisingWave, Vector, Fluent Bit, Flink and Connect today is
from reading their code.

**Files.**

- `gateways/kafka/tests/clients/` (new): one small program or config per
  client, run in its official image pinned by digest: confluent-kafka-python
  (librdkafka 2.x) and kcat; franz-go (a Go program compiled in the pinned
  `golang` image); the Java client 3.9 and 4.x (the `apache/kafka` images'
  tools plus a probe); Vector and Fluent Bit (config files); RisingWave
  (single-node playground; `CREATE SOURCE` / `CREATE SINK`); Flink (session
  cluster plus the Kafka connector jar; one `AT_LEAST_ONCE` job); Kafka Connect
  distributed with Debezium Postgres.
- `gateways/kafka/tests/client_matrix_tests.rs` (new): each client is a
  scenario (produce N keyed records with headers, consume them back in a group,
  commit, restart, resume) run against the gateway and Apache Kafka through
  KW-1's `Target`.
- `gateways/kafka/tests/clients/MATRIX.md` (new, generated by the nightly
  job): pass or fail per client and target, with the blocking issue.
- Workflow: `smoke` (kcat, librdkafka 2.x, franz-go, Java 3.9 and 4.x) on every
  PR; the rest in the nightly job, which writes the matrix as a job summary.

**Oracle comparison.** Each client's observations against the gateway equal
those against Apache Kafka, except rows in `allowed_differences.toml`.

**Exit.** Smoke green on PRs; the nightly job reports every client in §2 with a
result and a linked blocker for each failure.

### KW-3. Idempotent producer: dedup, ordering, restart safety

**Why.** The Java producer and franz-go are idempotent by default. Today a
retried batch is written twice, and a retried batch with later ones in flight
can land after them, silently (`docs/IDEMPOTENCE.md`).

**Design.** Three parts, each a commit:

1. **Producer state in the gateway**, as Kafka's `ProducerStateManager`: per
   (producer id, partition) the epoch and the last five batches (first and last
   sequence, base offset). A retry of a stored batch gets its original base
   offset back with error 0 (Kafka answers a duplicate that way since 2.x);
   a sequence gap gets `OUT_OF_ORDER_SEQUENCE_NUMBER` (45); an older epoch gets
   `INVALID_PRODUCER_EPOCH` (47); an unknown producer id at a non-zero sequence
   gets `UNKNOWN_PRODUCER_ID` (59) on versions where clients expect it, else 45.
   Batches of one (producer id, partition) are written one at a time in
   sequence order.
2. **In-doubt sends.** A send to Iggy whose outcome is unknown (timeout) keeps
   the batch "in doubt"; its retry is resent as the same Iggy request so Iggy's
   own `ClientTable` dedups it. This needs the two SDK seams in
   `IDEMPOTENCE.md`: a caller-chosen client id at build time
   (`core/sdk/src/session.rs`, `core/sdk/src/tcp/tcp_client.rs`, today
   `ConsensusSession::new()` at both sites) and a caller-supplied request id
   (`send_raw_with_response`'s `preencoded` header, private today). Both are
   additive SDK changes, no wire change.
3. **Restart and failover safety.** The batch's producer id, epoch and base
   sequence are written as one user header on its first record (`kafka.p`, a
   new marker, so the mapping version `kafka.v` goes to 2 and Fetch reads 1 and
   2). After a restart, or when another gateway takes the partition (KW-5), the
   state of a producer is rebuilt by reading back the partition tail (bounded:
   the last `producer_state_recovery_records`, default 10,000) when that
   producer first writes. A producer older than the window gets 59, and the
   Java client and franz-go then bump their epoch (KIP-360) and continue.
   Producer ids stay unique across restarts as today (clock-seeded counter).

Default chosen here (owner may overrule): rebuild from the log (part 3) rather
than "bump the epoch and answer 59 after every restart", because it also gives
KW-5 its producer-state handover for free.

**Files.** `gateways/kafka/src/producer_state.rs` (new),
`src/protocol/handlers/produce.rs`, `src/bridge/iggy_bridge/produce.rs`,
`src/records.rs` (the `kafka.p` header, mapping v2),
`src/protocol/handlers/init_producer_id.rs` (epoch bump on re-init with an
existing id and epoch, KIP-360), `core/sdk/src/{session.rs,tcp/tcp_client.rs}`
and the client builder (the seams), `docs/IDEMPOTENCE.md`,
`docs/BRIDGE_MAPPING.md`, `README.md` "Delivery guarantees".

**Tests.** Unit: the state machine (duplicate, gap, old epoch, six batches in
flight, wrap at `i32::MAX`). Bridge: a Produce retried after a lost response
writes each record once; a retry with later batches in flight keeps key order
or answers 45; a send timed out between the gateway and Iggy (through the
existing delay relay in `offset_groups_real_bridge_tests.rs`) is retried as the
same Iggy request; a gateway restart mid-stream with recovery from the log.
E2E: the Java producer at defaults and franz-go under injected timeouts (a
TCP relay that drops responses): no duplicates, no reordering.

**Oracle comparison.** The same lost-response and in-flight scenarios against
Apache Kafka give the same records and the same per-batch error codes
(wire diff).

**Exit.** The three acceptance criteria of #4513 pass in fork CI; the README no
longer says retries are duplicated.

**As built, part 1 (2026-10-23).** `src/producer_state.rs` and the check in
`src/protocol/handlers/produce.rs`; the bridge gains `send_records_observed`, so a send that ends
after its request answered 7 still records its outcome. Tests: 12 unit tests, three bridge tests
in `produce_real_bridge_tests.rs` (retry stored once, gap refused with 45 then filled, older
epoch refused with 47), and the `idempotent_producer` oracle scenario, which matches Apache Kafka
fact for fact. Still open: part 2 (an unknown outcome is written again), part 3 (state is lost on
restart), the Java producer e2e under injected timeouts, and KIP-360's epoch bump on
InitProducerId with an existing id.

### KW-4. Topic configurations frameworks send

**Why.** Connect, Debezium, Schema Registry, Apicurio and Kafka Streams create
their own topics with configurations and read some back (#4508). Connect needs
`cleanup.policy=compact` reported on its internal topics.

**Upstream state.** krishvishal is assigned; #4534 landed the first two boxes.
At task start check for an upstream PR and cherry-pick it if there is one; this
task then only fills what Loams' clients still need.

**Design.** As #4508's scope, with these defaults for its open questions: (1)
store `cleanup.policy` per topic as an Iggy topic option in the resource-option
catalog (`core/common/src/types/options/catalog.rs`, #4615 added it, so new
keys need no wire change), and give a `compact` topic no expiry until KW-6
compacts it; (2) accept any replication factor and report the Iggy cluster's
replica count; (3) store and echo valid configs Iggy cannot honour, marked in
DescribeConfigs with source `DYNAMIC_TOPIC_CONFIG`, and list them in
`CONFIGS.md` as "accepted, not enforced". `retention.bytes` → `max_topic_size`
× partitions, `segment.bytes` → `segment_size`. BROKER resources answer fixed
values.

**Files.** `src/protocol/handlers/{create_topics.rs,topic_config.rs,describe_configs.rs,alter_configs.rs,incremental_alter_configs.rs}`,
`src/bridge/iggy_bridge/topics.rs`, the option catalog (new keys
`kafka.cleanup_policy` and a generic `kafka.config.<name>` store, or a single
`kafka.configs` map; decided by what the catalog allows), `docs/CONFIGS.md`.

**Tests.** Handler and bridge tests per key; KW-2 nightly gains Connect
distributed with Debezium (creates its own internal topics and starts),
Schema Registry and a stateful Streams app.

**Oracle comparison.** DescribeConfigs after CreateTopics with the Connect,
Streams and Debezium config sets, gateway vs Apache Kafka, with the
not-enforced keys as declared differences.

**Exit.** #4508's acceptance criteria for Connect, Schema Registry and Streams
pass in the nightly matrix.

### KW-5. Several gateways behind one bootstrap

**Why.** Every production deployment (#4515): a restart today disconnects
every client, two gateways split each group in two, one gateway is a ceiling.

**Design (default for the open question).** Group ownership by hash: each
gateway is a broker with a stable node id (`IGGY_KAFKA_NODE_ID`, unique,
required when more than one runs), the live set and their endpoints are
registered in Iggy (a `<default stream>.meta/gateways` topic of heartbeats with
a lease), FindCoordinator answers the owner of `hash(group) mod live set`
(rendezvous hashing, so a change moves few groups), and the owner keeps group
state in memory as today. On an ownership change, members get `NOT_COORDINATOR`
(16), re-find the coordinator and rejoin; committed offsets are in Iggy
already. Partition leadership: Metadata reports each partition led by the
gateway next to its Iggy primary when known, else by hash; producer state moves
with leadership through KW-3's log recovery.

**Files.** `src/membership.rs` (new), `src/protocol/handlers/{find_coordinator.rs,metadata.rs}`,
`src/group/{mod.rs,state.rs}` (refuse groups this node does not own),
`src/main.rs`, `docs/CONSUMER_GROUPS.md`.

**Tests.** Two and three gateways in one test process against one
`iggy-server`; a gateway stopped during a rebalance and during a Produce burst;
each partition consumed once; offsets kept.

**Oracle comparison.** A three-broker Apache Kafka (compose) and three gateways
run the same failover scenario with the Java consumer and librdkafka: same
records consumed exactly once per group across the failover (allowing
at-least-once redelivery from the last commit, as Kafka does).

**Exit.** #4515's acceptance criteria pass; the "one gateway per bootstrap"
rule is removed from the docs.

### KW-6. Log compaction

**Why.** Connect's internal topics, Debezium's offsets, Schema Registry and
KTables grow without bound (#4521). Correctness does not depend on it; disk
does.

**Design.** A server change, so it starts with a short design note in
`docs/loams/compaction-design.md` covering #4521's open questions, with these
defaults: the key is the existing `kafka.key` user header, not a new frame
field (no wire change for every SDK); a tombstone is the existing
`kafka.value=null` marker; cleaning runs on each replica deterministically
from replicated inputs (a replicated "clean up to offset X" trigger, so all
replicas end identical); compaction never passes the lowest committed offset
of any consumer group, nor a transaction's LSO once KW-T exists; keyless
records in a compacted topic are kept. Offsets keep their gaps; Fetch and
ListOffsets serve across gaps.

**Files.** `core/partitions/src/{segment.rs,log.rs,iggy_partition.rs}` (a
cleaner that rewrites sealed segments), `core/server/src/segment_cleaner.rs`,
the option catalog (`cleanup_policy`, `delete_retention`,
`min_compaction_lag`, `max_compaction_lag`), index and state transfer for gaps,
`core/simulator` coverage, gateway Fetch across gaps.

**Tests.** Unit and simulator tests for the cleaner; gateway tests that a
Fetch inside a removed range returns the next record, never 6; Connect and
Streams restarts read the compacted log with unchanged results.

**Oracle comparison.** The same keyed write sequence with tombstones into a
compacted topic in Apache Kafka (with low `segment.ms` and
`min.cleanable.dirty.ratio`) and in the gateway: after cleaning, a full read
returns the same last record per key.

**Exit.** #4521's acceptance criteria pass; the owner decides whether the
design note goes upstream as a discussion first.

### KW-7. Real log start offset and DeleteRecords

**Why.** EARLIEST is always 0 and Fetch reports `log_start_offset` -1 (#4525).
Flink, Spark (`failOnDataLoss`), KEDA and UIs read EARLIEST; Streams purges
repartition topics with DeleteRecords.

**Also (found by KW-1).** ListOffsets by time searches Iggy's append time
(#4516), so records with an old producer timestamp resolve to the wrong
offset; this task adds a lookup on the producer timestamp too.

**Design.** A per-partition log start offset in Iggy that retention and
segment deletion advance, readable by clients (a field on the partition in
`get_topic`, or a new read; decided by the smaller change). DeleteRecords sets
a replicated low watermark at offset precision; reads below it answer
`OFFSET_OUT_OF_RANGE`; segments wholly below it are deleted by the cleaner.

**Files.** `core/metadata/src/stm/stream.rs` (the existing
`deleted_up_to_offset` becomes the readable start offset),
`core/partitions/src/log.rs`, `core/server/src/segment_cleaner.rs`, SDK
read path, gateway `src/protocol/handlers/{list_offsets.rs,fetch.rs}`, a new
`delete_records.rs` handler, `SUPPORTED_RANGES`, `SCOPE.md`.

**Tests.** As #4525's acceptance criteria, plus a Streams app's repartition
topic shrinking in the nightly matrix.

**Oracle comparison.** DeleteRecords then ListOffsets EARLIEST and a Fetch below
it, gateway vs Apache Kafka.

**Exit.** #4525's acceptance criteria pass.

### KW-8. Newer protocol versions and topic ids

**Why.** Clients negotiate down today, but `@platformatic/kafka` mislabels
topics at Metadata v9, Sarama pinned at V2_8 gets closed at Metadata v10, and
KIP-848 assignments name topics by id (#4527).

**Design.** A stable UUID per Iggy topic, stored as a topic option at creation
(and minted for existing topics on first read, written once); then Metadata
v10–13, Fetch v13+, OffsetCommit and OffsetFetch v10, CreateTopics and
DeleteTopics by id, ListOffsets v8+, ApiVersions v4 (v5 needs KIP-1242, verify
the `kafka-protocol` crate supports it). **Never advertise Produce v13 before
Metadata returns real topic ids.** Check that `kafka-protocol` 0.18 has the
schemas; bump it if not (a lockfile change: keep it its own commit).

**Files.** `src/protocol/api.rs` (`SUPPORTED_RANGES`), every handler whose
range grows, `src/bridge/topic_map.rs` (id ↔ name), `bounds_guard.rs` shape
guards for each new version, golden fixtures, `SCOPE.md`.

**Tests.** The version-firewall matrix at each new version; Java 4.x negotiates
the new versions in the smoke matrix; `@platformatic/kafka` consumes two topics
(nightly).

**Oracle comparison.** Wire diffs at the new versions vs Apache Kafka 4.x.

**Exit.** #4527's acceptance criteria pass.

### KW-9. SASL OAUTHBEARER, then SCRAM

**Why.** Loams' identity is Zitadel (Loams design 76): Loams credentials
are OIDC tokens, which OAUTHBEARER carries directly. SCRAM is what MSK, Aiven
and Strimzi users have configured (#4512).

**Design.** OAUTHBEARER first: validate the JWT against a configured JWKS
(issuer and audience required, `exp` checked, keys cached and refreshed), map a
claim (default `sub`, configurable) to an Iggy user, send
`session_lifetime_ms` (KIP-368) so clients re-authenticate before expiry, and
re-authenticate on the open connection. SCRAM-SHA-256/512 second, with the
credential store in the gateway (default for #4512's open question, because it
needs no Iggy state-machine change): salted keys in a gateway-owned Iggy topic
`<default stream>.meta/scram`, written by AlterUserScramCredentials (51) and
read by DescribeUserScramCredentials (50). GSSAPI stays out unless asked.

**Files.** `src/protocol/sasl.rs`, `src/auth.rs`, `src/oauth.rs` and
`src/scram.rs` (new), `docs/AUTHENTICATION.md`.

**Tests.** A local JWKS server in the test; expired, wrong-issuer and
wrong-audience tokens refused; re-authentication keeps the connection;
Java, librdkafka and franz-go authenticate with OAUTHBEARER and SCRAM
(KW-2 smoke).

**Oracle comparison.** Error codes and client outcomes for good and bad
credentials vs Apache Kafka configured with the same mechanisms (OAUTHBEARER
with Kafka's unsecured-JWT validator and a JWKS validator, SCRAM through
`kafka-configs`).

**Exit.** #4512's acceptance criteria pass for OAUTHBEARER and SCRAM.

### KW-10. Per-principal authorization and ACL admin

**Why.** Loams namespaces must not read each other's topics; today every client
acts as the bridge's Iggy user (#4518).

**Design (default for the open question).** One Iggy session per active
principal, pooled and evicted like the producer pool (the PAT or user mapping
from KW-9), rather than a new "act for a user" Iggy feature, because it needs
no server change. Kafka operations map to Iggy permissions per `ACL_MAPPING.md`;
CreateAcls and DeleteAcls change Iggy permissions where the mapping is exact
and refuse with `INVALID_REQUEST` and a message where it is not (documented).
Quotas (48, 49) only on request.

**Files.** `src/auth.rs`, `src/bridge/iggy_bridge/mod.rs` (per-principal
clients), every data-plane handler's authorization check,
`src/protocol/acl.rs`, `docs/ACL_MAPPING.md`.

**Tests.** A principal without read permission gets 29 on Fetch; `kafka-acls
--add` grants a permission the next request honours.

**Oracle comparison.** The same ACL sequence vs Apache Kafka with
`StandardAuthorizer`, compared per request; declared differences where Iggy's
model is coarser.

**Exit.** #4518's acceptance criteria pass.

### KW-11. Repeated header names and header order

**Why.** KafkaJS array headers, Spring trace headers; error 87 is fatal in
several clients (#4517). CloudEvents binary mode (Loams' envelope, D270) does
not repeat headers, so this is breadth, not Loams-critical.

**Design (default).** Records with a repeated name go to the envelope fallback,
which keeps a list; every other record keeps the native path, and Fetch returns
native headers in their original order, using a `kafka.ho` header that holds
the original order only when it differs from the stored order (no cost for the
common case). Read headers from the raw record bytes.

**Files.** `src/records.rs`, `docs/BRIDGE_MAPPING.md`.

**Tests and oracle.** A record with a repeated name and an unsorted order is
byte-identical in key, value and headers after Produce and Fetch, compared
with Apache Kafka.

**Exit.** #4517's acceptance criteria pass.

**As built (2026-10-24).** Produce reads a record's full header list from the
raw bytes when a name repeats (`records::scan_records`) and stores it in the
envelope; Fetch writes the list back into the encoded batch
(`records/headers.rs`, `splice`), so the record is byte-identical to what
Kafka serves. Natively stored headers out of name order carry `kafka.ho` (one
rank byte per header); more than 255 unsorted headers take the envelope. The
mapping version stays 1: an older gateway ignores `kafka.ho` and serves the
headers sorted. A foreign Iggy message with two key kinds naming one header
now serves both. The oracle scenario `record_headers` compares the record
bytes (from the attributes on) with Apache Kafka for a repeated name with a
null, an unsorted set and a sorted set; the `produce_fetch` header-order
difference is gone from `allowed_differences.toml`.

### KW-12. Configurable limits; member client id and host

**Why.** #4530 (group caps, CreateTopics caps, frame size have no knobs), the
commit metadata string that OffsetCommit drops today (found by KW-1; Kafka
Streams stores task metadata in it), and the last box of #4511 (`client_id`, `client_host` empty in DescribeGroups,
so `kafka-consumer-groups --describe` shows no hosts).

**Files.** `src/env.rs`, `src/main.rs`, `src/group/{mod.rs,state.rs}`, handler
plumbing for the header's `client_id` and the peer address,
`docs/CONSUMER_GROUPS.md`, `README.md` limits table.

**Tests.** Each knob read from the environment and enforced; DescribeGroups
reports each member's client id and host.

**Oracle comparison.** DescribeGroups output vs Apache Kafka for the same
consumers (hosts normalised).

**Exit.** Every limit in #4530 has a variable and a documented Kafka
counterpart; `kafka-consumer-groups --describe` shows hosts.

**Status (2026-10-24).** Started and stopped on the owner's instruction. Branch
`kw12-limits` holds an unbuilt first cut: `src/limits.rs` (group caps and
session timeouts as `IGGY_KAFKA_GROUP_*`, and
`IGGY_KAFKA_MAX_ADMIN_TOPICS_PER_REQUEST` for the 100-topic cap of
CreateTopics, CreatePartitions, DeleteTopics and the config APIs), with unit
tests, and the handlers reading the cap. Left: wire `limits` into `main.rs`
(`apply_group_env`, `set_admin_topics_per_request`, `KNOWN_ENV_VARS` in the
unknown-variable check), the README limits table (the 1000-partition cap is
Iggy's `TooManyPartitions`, not a gateway setting; the 246-byte group id
follows Iggy's 255-byte names), member `client_id`/`client_host` in
DescribeGroups (the header's `client_id` is cleared in `server.rs` before
routing and the peer address is not passed to handlers), and commit metadata.
Commit metadata cannot go in Iggy's `ExternalGroup` offset (a `u64`; changing
it is the state-save path, which needs the owner's OK), so the default is a
gateway-owned Iggy topic replayed into memory at startup, like Kafka's
`__consumer_offsets`; that needs the owner's ruling before it is built.

### KW-13. KIP-848 consumer groups

**Why.** librdkafka 2.12+ and Java 4.x with `group.protocol=consumer`; the
librdkafka docs say its default will change (#4523).

**Design.** Server-side assignment (uniform and range assignors), member
epochs and the heartbeat state machine, regex subscriptions,
ConsumerGroupDescribe, and mixed classic/KIP-848 groups during an upgrade, on
the KW-5 ownership model. Assignments name topics by id (KW-8).

**Files.** `src/group/consumer_protocol.rs` (new), handlers for 68 and 69,
`docs/CONSUMER_GROUPS.md`.

**Tests and oracle.** Java and librdkafka consume with
`group.protocol=consumer`; a group moves from classic to KIP-848 one member at
a time keeping offsets; assignments and rebalance outcomes compared with Apache
Kafka 4.x.

**Exit.** #4523's acceptance criteria pass.

### KW-14. DescribeLogDirs and a read-only `__consumer_offsets`

DescribeLogDirs v1–5 from Iggy partition sizes, so kminion and UIs work; a
read-only `__consumer_offsets` in Kafka's internal record format only if a
Burrow user asks (#4524). Files: a new handler, `SUPPORTED_RANGES`,
`SCOPE.md`. Oracle: response shape vs Apache Kafka (sizes normalised). Exit:
kminion reports `kminion_exporter_up 1` in the nightly matrix.

### KW-15. Remaining admin APIs

GetTelemetrySubscriptions with an empty subscription (so Java's
`clientInstanceId()` returns at once), DescribeProducers from KW-3's state,
DescribeTopicPartitions; ListConfigResources, delegation tokens and the
transaction listings only on request (#4526; the listings wait for KW-T).
Oracle: wire diffs vs Apache Kafka. Exit: each served key passes its diff.

### KW-16. Old and pinned clients

SaslHandshake v0 with the pre-KIP-152 raw SASL tokens; message formats v0/v1
only on request (#4528). Oracle: kafka-python 2.0.2 and Sarama 1.40.1 against
the gateway and Apache Kafka 3.9 (4.x dropped these versions). Exit: both
authenticate with SASL PLAIN, produce and consume.
Status (2026-10-24): not started; lane G stopped after KW-11 on the owner's
instruction.

### KW-17. Streams groups and share groups

KIP-1071 Streams groups and KIP-932 share groups (#4529), after KW-13. Share
groups need per-record acknowledgement state, which is an Iggy feature in its
own right; this task starts with a design note. Oracle: Apache Kafka 4.x.
Lowest priority.

### KW-18. Migration guide

A fork copy of the Kafka-to-Iggy migration guide (#3550 is assigned upstream
to ryerraguntla), written from the Loams angle: dual-write, consumer cutover
with offset translation, rollback, and Loams' compose profile. After KW-5.

### KW-L1. Fork images

**Status: done.** One workflow, `.github/workflows/loams-images.yml`, builds one
image holding `iggy-server`, the `iggy` CLI and `iggy-gateway-kafka`
(`scripts/loams/Dockerfile`) and pushes it, `linux/amd64` and `linux/arm64`, to
`ghcr.io/ostrium-labs/iggy` as `loams-<shortsha>` (one per merge to `loams`)
and a moving `loams`, with build provenance and an SPDX SBOM attested to the
registry. One image rather than two, so server and gateway always move
together (Iggy's protocol is pre-1.0). It builds on pushes to `loams` and on
manual dispatch only, each architecture natively, layers cached. See
[README.md](README.md#images).

### KW-L2. Loams compose profile `kafka`

**Status: done** (ostrium-labs/loams#1148, preview). `deploy/release/compose.yaml`
has a `kafka` profile with `iggy` and `kafka-gateway` services, both from the
one KW-L1 image pinned by digest, the gateway published on loopback only, never
in the default stack. `release-compose` checks that it is off by default,
pinned and on loopback, and a smoke job starts it and has kcat produce and
consume through the gateway. Not wired to the Loams engine: the Iggy to Loams
hop is FL1's `loams_sink`. SASL is the gateway's own (`KAFKA_SASL_ENABLED`),
against Iggy users, not yet mapped to Loams credentials.

## 6. KW-T: transactions and exactly-once (planned, not built)

**Status: planned, not built.** Owner: "do all things except kafka transactions
but plan it fully." Nothing in this section is implemented, and the gateway
keeps refusing transactions as `SCOPE.md` "Transactions" describes until the
owner approves building it.

### 6.1 Who needs it

Flink `DeliveryGuarantee.EXACTLY_ONCE`, Kafka Streams `exactly_once_v2`,
Spring `transactionIdPrefix`, Connect `exactly.once.source.support`, ksqlDB
(every statement), and any consumer using `isolation.level=read_committed`
against topics transactional producers write (#4522). A Connect leader with
exactly-once sources loops every 250 ms forever.

### 6.2 What a broker must provide

Kafka's model (KIP-98, KIP-447, KIP-360, KIP-890), which is the contract:

- **Transactional id → producer id and epoch.** InitProducerId with a
  transactional id returns a stable producer id and a bumped epoch. The bump
  fences any older producer with the same transactional id
  (`PRODUCER_FENCED` (90) / `INVALID_PRODUCER_EPOCH` (47)), and aborts any
  transaction the old epoch left open.
- **A transaction coordinator** per transactional id, found with
  FindCoordinator key type `TRANSACTION`, holding the state machine
  `Empty → Ongoing → PrepareCommit | PrepareAbort → CompleteCommit |
  CompleteAbort → Empty`, plus `PrepareEpochFence` and `Dead`, in a durable
  transaction log (`__transaction_state` in Kafka: compacted, 50 partitions).
- **AddPartitionsToTxn** (v1–3 from clients) registers data partitions before
  the producer writes to them; **AddOffsetsToTxn** registers a group's offsets.
  Under transaction version 2 (KIP-890 part 2) the client stops sending these
  and the partition leader verifies membership with the coordinator itself.
- **Transactional Produce**: batches carry the transactional bit, producer id,
  epoch and sequence; idempotence rules apply (KW-3).
- **TxnOffsetCommit** to the group coordinator stores pending offsets that
  become visible only at commit.
- **EndTxn** (commit or abort): the coordinator writes `PrepareCommit` or
  `PrepareAbort` to its log, answers the client, then writes a **control
  record** (commit or abort marker) to every registered partition
  (WriteTxnMarkers, broker to broker), materialises or discards pending
  offsets, and writes `CompleteCommit` or `CompleteAbort`.
- **Last stable offset (LSO)** per partition: the first offset of the earliest
  still-open transaction. `read_committed` Fetch returns data only below the
  LSO and includes the **aborted transactions** list (producer id, first
  offset) for the range so the client drops aborted records; control records
  are never delivered to applications. ListOffsets LATEST under
  `read_committed` returns the LSO.
- **Timeouts**: `transaction.timeout.ms` (client, capped by the broker's
  `transaction.max.timeout.ms`, default 15 min; above it
  `INVALID_TRANSACTION_TIMEOUT` (50)); the coordinator aborts expired
  transactions and bumps the epoch. `transactional.id.expiration.ms` (7 days)
  forgets idle ids.

### 6.3 Mapping onto Iggy and the gateway

| Kafka concept | In the gateway and Iggy |
| --- | --- |
| Transaction log | An Iggy topic `<default stream>.meta/txn_state`, 16 partitions (`hash(transactional_id)`), durability `persisted`, holding one record per state transition (transactional id, producer id, epoch, state, partitions, group, timeout, last update). Rebuilt into memory by the owning gateway on start or takeover. Needs KW-6 (compaction) to stay bounded; until then a periodic snapshot record plus segment deletion below the oldest live snapshot |
| Coordinator placement | The KW-5 ownership model: `hash(transactional_id)` → owning gateway; FindCoordinator(TRANSACTION) answers it; ownership change answers `NOT_COORDINATOR` (16) and the new owner replays its partitions of `txn_state` |
| Producer id and epoch | KW-3's producer state, extended: the transactional id ↔ producer id mapping lives in `txn_state`; epochs are bumped there first, then enforced in Produce |
| Transactional data records | Ordinary Iggy messages plus the `kafka.p` header (KW-3) with a transactional flag. Iggy-native consumers see them as written, including aborted ones; documented, and a later Iggy-side filter is a separate item |
| Control records | One Iggy message per (partition, transaction end): payload a single byte, user headers `kafka.ctl` = commit or abort, `kafka.p` = producer id and epoch, coordinator epoch. Fetch encodes it as a Kafka control batch (attributes: control and transactional bits; key: version, type; value: version, coordinator epoch) so stock clients skip it. It consumes an offset, as in Kafka |
| LSO | Per partition, in the gateway that leads the partition (KW-5): the minimum first offset of open transactions on it, kept in memory, rebuilt on takeover by scanning from the oldest open transaction's first offset (bounded by `transaction.max.timeout.ms` worth of data) |
| Aborted transaction index | Per partition, an Iggy topic `<default stream>.meta/txn_aborted` keyed by (stream, topic, partition): one entry (producer id, first offset, marker offset) per abort, appended before the abort marker is acknowledged; loaded lazily per partition and trimmed by retention with the data |
| Pending transactional offsets | In `txn_state` with the transaction; materialised into the `kafka.cg.<group>` external offsets on commit (idempotent: the write carries the transaction's producer id and epoch, and a replay with the same pair is a no-op) |
| WriteTxnMarkers | Internal to the gateway: the coordinator writes markers to Iggy directly (it has the bridge), retrying each partition until acknowledged; markers are deduplicated by (producer id, epoch, partition) through KW-3's in-doubt mechanism |

### 6.4 Wire surface to add

InitProducerId with a transactional id (v0–5, KIP-360 epoch bump semantics);
FindCoordinator key type 1; AddPartitionsToTxn v1–3 (client form; v4+ batched
verification only with TV2); AddOffsetsToTxn v1–4; EndTxn v1–4 (v5 only with
TV2); TxnOffsetCommit v2–4; Fetch `isolation_level`, `last_stable_offset` and
`aborted_transactions` at every served version; ListOffsets `isolation_level`;
DescribeTransactions (65), ListTransactions (66), DescribeProducers (61) for
`kafka-transactions.sh`. **Transaction version 2 is not advertised** in
`finalized_features` until the TV2 verification path exists (the invariant in
`IDEMPOTENCE.md` stays load-bearing until then).

### 6.5 Interaction with idempotence (KW-3) and compaction (KW-6)

- Transactions require KW-3's per-partition sequence checks and epoch fencing;
  a transactional producer is an idempotent producer with a transactional id.
  KW-3's state must therefore store epochs per producer id (not per batch) and
  allow an external bump from the coordinator.
- KW-3's `kafka.p` header is what lets a takeover gateway rebuild both the
  producer state and the open-transaction set of a partition.
- Compaction (KW-6) must not clean above the LSO, must drop aborted records,
  and must keep a control marker until every record of its transaction below
  it is cleaned and `delete.retention.ms` has passed (Kafka's cleaner rules).
- `txn_state` itself needs compaction or snapshots (6.3).

### 6.6 Failure cases the design must handle

| Case | Expected behaviour |
| --- | --- |
| Coordinator gateway dies after `PrepareCommit` is durable, before all markers | The new owner replays `txn_state`, finds `PrepareCommit`, rewrites every marker (deduplicated), then `CompleteCommit` |
| Coordinator dies before `PrepareCommit` is durable | The client's EndTxn fails retriably; on retry the new owner sees `Ongoing` and proceeds |
| Partition-leading gateway dies with open transactions | The new leader rebuilds LSO and producer state from the log tail; `read_committed` consumers wait at the LSO |
| Zombie producer (old epoch) keeps writing | Produce answers `PRODUCER_FENCED` / `INVALID_PRODUCER_EPOCH`; its open transaction was aborted by the epoch bump |
| Transaction exceeds its timeout | The coordinator aborts it (markers, epoch bump); the producer's next call is fenced |
| Marker write to Iggy times out | Retried as the same Iggy request (KW-3 seam); the transaction stays in `Prepare*` until all are acknowledged; `CONCURRENT_TRANSACTIONS` (51) for the producer's next transaction meanwhile |
| Iggy partition unavailable during EndTxn | Markers retried with backoff; LSO on other partitions advances independently |
| Two gateways both believe they own a transactional id (lease overlap) | The coordinator epoch in each marker and `txn_state` record; the lower epoch's writes are refused by replay rules (Kafka's `TRANSACTION_COORDINATOR_FENCED` (52)) |
| Consumer at `read_uncommitted` | Sees all data records, never control records, as in Kafka |
| Iggy-native consumer | Sees data records including aborted ones, and control messages unless it filters `kafka.ctl` (documented limitation) |

### 6.7 Test plan

Conformance oracles: **Apache Kafka 4.x** (container) and **Redpanda**
(container, black box, owner-approved as the transactions oracle). A behaviour
both oracles agree on is the contract; where they differ, Apache Kafka wins and
the difference is recorded.

1. **Wire diffs** (KW-1 harness): the full handshake sequence (FindCoordinator
   TXN → InitProducerId → AddPartitionsToTxn → Produce → AddOffsetsToTxn →
   TxnOffsetCommit → EndTxn) with error codes at each step, against both.
2. **Fencing**: two producers with one transactional id; the first is fenced
   with the same exception class on the Java client and librdkafka as on each
   oracle.
3. **Timeouts and aborts**: a transaction left open past its timeout is
   aborted; a `read_committed` consumer never sees its records; the LSO
   advances.
4. **Read committed**: interleaved committed and aborted transactions on one
   partition from three producers; a `read_committed` consumer's record set
   equals the oracle's exactly; a `read_uncommitted` one equals the oracle's.
5. **Crash matrix** (gateway and Iggy): kill the coordinator
   gateway at each state transition, the partition leader mid-transaction, and
   `iggy-server` during marker writes; verify no committed record lost, no
   aborted record visible to `read_committed`, no transaction stuck past
   `transaction.max.timeout.ms`.
6. **Applications**: Kafka's `TransactionalMessageCopier` and
   `kafka-verifiable-producer --transactional-id` as in Kafka's own system
   tests; a Kafka Streams `exactly_once_v2` word count (output equals the
   oracle's under injected restarts); a Flink `EXACTLY_ONCE` sink job with a
   failing checkpoint; Connect with `exactly.once.source.support=enabled`;
   ksqlDB starts and runs one persistent query.

Exit, when built: #4522's acceptance criteria (Streams `exactly_once_v2` runs
and `read_committed` never sees an aborted record; ksqlDB starts), plus items
1–6 green against Apache Kafka and Redpanda.

### 6.8 Size and order, if approved

Roughly KW-3-sized four times over: (a) `txn_state` and the coordinator state
machine; (b) InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn,
markers; (c) LSO, aborted index, `read_committed` Fetch and ListOffsets;
(d) TxnOffsetCommit; (e) takeover and the crash matrix; (f) TV2. It depends on
KW-3, KW-5, KW-7 (start offsets bound the aborted index) and, for a bounded
`txn_state` and compacted topics, KW-6.

## 7. How Loams runs it

See Loams D1796–D1798. In short: the fork's images (KW-L1) run in an optional
`kafka` profile of the Loams compose stack (KW-L2), never in the default
stack, which stays light. The gateway and `iggy-server` move as one pinned
pair. FL1's Fabric plan (Iggy 0.9.0) aligns with the fork's pinned revision
when FL1 starts.

## 8. What can run in parallel

After KW-1 merges (every task needs its harness):

| Lane | Tasks | Touches |
| --- | --- | --- |
| A | KW-2 (matrix) | `tests/clients/`, workflow |
| B | KW-3 (idempotence), then KW-5 (multi-gateway) | Produce path, SDK seams, groups |
| C | KW-4 (configs), then KW-6 (compaction) | Topic admin, option catalog, server storage |
| D | KW-7 (start offset, DeleteRecords), then KW-14 | Server metadata, ListOffsets, Fetch |
| E | KW-8 (versions, topic ids), then KW-13 (KIP-848) | `SUPPORTED_RANGES`, every handler's range |
| F | KW-9 (SASL), then KW-10 (authz) | `sasl.rs`, `auth.rs` |
| G | KW-11, KW-12, KW-16 (small) | `records.rs`, env, header plumbing |
| H | KW-L1 (images) | Fork workflow only |

Conflicts to watch: B and D both touch Fetch; C and D both touch the server's
segment code (KW-6 after KW-7, or one agent for both); E touches every
handler's range table, so it rebases often. Keep to three building agents on
the owner's machine (disk), and let fork CI do the full runs.

## 9. Open questions for the owner

1. Upstream: may the fork's tasks be offered upstream as they finish (one PR
   per issue, after assignment), starting with KW-1/KW-2 (#4531) and KW-3
   (#4513)? Default: no posting until you say so.
2. KW-3 restart safety: rebuild producer state from the log (default), or bump
   the epoch and answer 59 after each restart (simpler, at-least-once across a
   restart)?
3. KW-4: accept-and-echo configs Iggy cannot honour (default), or refuse them?
4. KW-6: write the compaction design note for upstream discussion first, or
   build it in the fork straight away? Default: fork first, note kept ready.
5. KW-T: when to start building transactions, if at all.
