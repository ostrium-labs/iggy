# Kafka wire compatibility: the fork plan

Status: **plan, 2026-10-23.** Nothing in this document is built yet unless a
task row says so. Transactions and exactly-once (KW-T, apache/iggy#4522) are
**being built** in steps behind `IGGY_KAFKA_TRANSACTIONS_ENABLED` (owner, 2026-10-25: start now;
this overrides the 2026-10-24 ruling that they wait for KW-2, KW-3 and KW-13). Steps 1 to 3 are built;
section 6 has the state of each step.

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
9. [Owner decisions and open questions](#9-owner-decisions-and-open-questions)

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
| librdkafka 2.x (confluent-kafka-*, Vector, Fluent Bit, RisingWave) | Producer not idempotent by default; classic groups via subscribe; `assign` for RisingWave; watermark queries | Yes: confluent-kafka-python 2.16 passes the KW-2 smoke (produce, group consume, commit, resume) | KW-2 (test it), KW-3 if `enable.idempotence=true`, KW-13 for `group.protocol=consumer` |
| franz-go | Idempotent by default; KIP-848 when the broker advertises it, else classic | Yes: franz-go 1.22.1 passes the KW-2 smoke | KW-2, KW-3 |
| kcat | librdkafka 1.7 | Yes (in today's suite) | None |
| Kafka Connect distributed + Debezium | Creates `config`, `offset`, `status` topics with `cleanup.policy=compact` and RF 3; reads them in full at start; Debezium schema history topic with `retention.ms`/`retention.bytes`; `connect` group protocol through JoinGroup/SyncGroup | No: the worker does not start on the gateway (KW-2 nightly; cause not diagnosed, see KW-4) | KW-4 (configs), KW-6 (compaction, or the internal topics grow without bound), KW-3 (config topic producer is idempotent) |
| Flink Kafka connector (KafkaSource / KafkaSink) | Admin `describeTopics`, `listOffsets` (earliest, latest, timestamp); `assign` + Fetch; commits offsets on checkpoint with no group membership; sink `AT_LEAST_ONCE` uses an idempotent producer; `EXACTLY_ONCE` uses transactions | `AT_LEAST_ONCE` yes: a bounded copy job passes the KW-2 nightly; the commit on checkpoint is not covered yet | KW-2, KW-3, KW-7 (EARLIEST and data-loss checks), KW-T for `EXACTLY_ONCE` |
| RisingWave source and sink | librdkafka; `assign` with offsets; watermark queries; sink with `upsert`/`debezium` formats, keys and tombstones | Yes for a JSON source, materialized view and append-only sink (KW-2 nightly); upsert sinks and tombstones not covered | KW-2, KW-7 (EARLIEST) |
| Vector `kafka` source and sink | librdkafka subscribe, async commits; sink producer | Yes (KW-2 nightly: sink, source, commit, resume) | KW-2 |
| Fluent Bit `in_kafka` / `out_kafka` | librdkafka | Yes (KW-2 nightly: sink, source, commit, resume) | KW-2 |
| Kafka Streams (not a D74 target; Loams' users bring it) | Compacted changelogs, `message.timestamp.type`, DeleteRecords on repartition topics, large assignments | No: stops at first rebalance with a state store (#4508) | KW-4, KW-6, KW-7, KW-12 |

Two production properties matter to every client and are not client features:
more than one gateway behind one bootstrap list (KW-5), and per-principal
authorization so Loams namespaces cannot read each other (KW-10).

## 3. Oracles and the test strategy

Every task proves its behaviour against a real Kafka, not only against
hand-built frames. Three oracles, all used as black boxes: two containers and
one managed service:

| Oracle | How it runs | Licence and rule | Used for |
| --- | --- | --- | --- |
| **Apache Kafka** | `apache/kafka` container, KRaft single node, pinned (the suite uses 3.9.0 for client tools today; KW-1 adds `apache/kafka:4.3.1` as the broker oracle) | Apache-2.0. Reading its source is fine | The default oracle for every task, in fork CI |
| **Redpanda** | `redpandadata/redpanda:v26.1.18` container (Docker Hub), single node, pinned | BSL. **Black box only**: never read or copy its source; only its container and public docs | The second transactions oracle (approved by the owner earlier); a second opinion where Kafka and the gateway disagree |
| **Confluent Cloud** | Managed Kafka: one Basic cluster (`lkc-120zwvj`, AWS `ap-south-1`, single zone) on the Confluent for Startups credits, reached over `SASL_SSL` with a scoped service-account key. Multi-broker, so the harness routes partition and coordinator requests to their owners | A service, used only through the Kafka wire. **Local only**: its key never goes into CI or the repo, and CI never runs it | The managed-Kafka oracle: what a multi-broker production Kafka answers (leader routing, coordinators, transactions on a real cluster). Report-only (below) |

The Confluent Cloud oracle runs when `~/confluent-oracle.env` (or the file
`KAFKA_ORACLE_CONFLUENT_ENV` names) exists; `KAFKA_ORACLE_CONFLUENT=0` turns it
off, and it is skipped whenever `CI` or `GITHUB_ACTIONS` is set. It is
report-only: its differences, and any scenario that fails on it, are listed in
the report under `confluent (report-only)` and never fail the run; a
difference where it answers as Apache Kafka does is only counted. Each run
creates its topics under `loams-oracle-<run-id>-` and deletes them at the end,
also when the run panics; topics older than 6 hours from a run that could not
clean up are swept at the next start. Setup, cost and teardown:
[`confluent-oracle.md`](confluent-oracle.md).

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
- **Local, on demand**: the Confluent Cloud diffs, report-only, on a machine
  that has the oracle's env file. Never in CI.

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
| KW-2 | Client compatibility matrix: smoke on PRs, full nightly | #4531 | **Built** (smoke green locally; nightly first run pending in fork CI) | KW-1 | Yes |
| KW-3 | Idempotent producer: dedup, ordering, restart safety | #4513 | **Built** (parts 1–3); Java/franz-go e2e under injected timeouts open | KW-1 | Yes |
| KW-4 | Topic configurations frameworks send | #4508 | **Built, in review** (fork PR #13): unit tests and clippy pass locally; real-server, Java and oracle suites not yet run (local memlock limit); framework exit criteria wait on the KW-2 nightly matrix | KW-1 | Only if krishvishal has not landed it |
| KW-5 | Several gateways behind one bootstrap | #4515 | **Built** (fork PR #8; local CI fixes: a dropped gateway stops heartbeating, membership tests wait for a settled view; producer-state handover on a leadership move still open, see As built) | KW-3 | Yes (design first) |
| KW-6 | Log compaction | #4521 | Planned; design note drafted (`docs/loams/compaction-design.md`, PR #13), nothing built | KW-4 | Design discussion first |
| KW-7 | Real log start offset and DeleteRecords | #4525 | **Built** (gateway side; disk reclaim below the watermark planned) | KW-1 | Yes |
| KW-8 | Newer protocol versions and topic ids | #4527 | **Built** (fork PR #10; full suite and oracle green in local CI; see As built) | KW-1 | Yes |
| KW-9 | SASL OAUTHBEARER, then SCRAM | #4512 | **Built** (fork PR #12; auth oracle and real-client e2e green in local CI; see "As built") | KW-1 | Yes |
| KW-10 | Per-principal authorization, ACL admin | #4518 | **Built, in review** (fork PR for parts 1-4: enforcement, group namespace, DescribeAcls of scoped grants); ACL writes (CreateAcls/DeleteAcls) next | KW-9 | Yes (design first) |
| KW-11 | Repeated header names and header order | #4517 | **Built** (this PR) | KW-1 | Yes |
| KW-12 | Configurable limits; member client id and host | #4530, #4511 | **Started** (branch `kw12-limits`, not built or tested) | KW-0 | Yes |
| KW-13 | KIP-848 consumer groups | #4523 | **Started, not wired** (branch `kw13-kip848-draft`) | KW-8 | Yes |
| KW-14 | DescribeLogDirs, read-only `__consumer_offsets` | #4524 | Planned | KW-7 | Yes |
| KW-15 | Remaining admin APIs | #4526 | Planned | KW-3, KW-8 | Yes |
| KW-16 | Old and pinned clients | #4528 | Planned | KW-2 | Yes |
| KW-17 | Streams groups and share groups | #4529 | Planned | KW-13 | Later |
| KW-18 | Kafka-to-Iggy migration guide | #3550 | Planned | KW-5 | Assigned upstream; fork copy only |
| KW-L1 | Fork images: `iggy-server` and `iggy-gateway-kafka` on GHCR | — | **Done** (one image, `ghcr.io/ostrium-labs/iggy`) | KW-0 | No |
| KW-L2 | Loams compose profile `kafka` | — (Loams repo) | **Done** (ostrium-labs/loams#1148) | KW-L1 | No |
| KW-T | Transactions and exactly-once | #4522 | **Steps 1-3 built** (coordinator, log, handshake, markers, fencing, timeouts, recovery; `read_committed`; `AddOffsetsToTxn`, `TxnOffsetCommit`, `require_stable`; behind `IGGY_KAFKA_TRANSACTIONS_ENABLED`); steps 4-5 planned. Started 2026-10-25 on the owner's word, ahead of KW-13 | KW-3 (done); KW-5 for step 5, KW-6 for a bounded log | Design discussion first |

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

**As built (2026-10-24, PR #8, not merged).** `oracle_wire_diff_tests.rs` passes against
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

**Confluent Cloud oracle (2026-10-25).** Added as a third, report-only
oracle ([`confluent-oracle.md`](confluent-oracle.md)). On the first runs it
answered as Apache Kafka on 16 of the 17 declared differences in the
scenarios it runs, so those gaps are Kafka behaviour, not single-node
artefacts. It skips `idempotent_restart`: a managed cluster's brokers cannot
be restarted. Where it differs from both
Kafka and the gateway: ListOffsets answers the requested timestamp in the
`timestamp` field for EARLIEST, LATEST, MAX_TIMESTAMP and lookups before the
first or after the last record (Kafka answers -1 or the found record's
timestamp), and a sequence gap from an idempotent producer answers 56
(`KAFKA_STORAGE_ERROR`) rather than 45 (`OUT_OF_ORDER_SEQUENCE_NUMBER`).
Neither changes what the gateway should do: Apache Kafka stays the reference.

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

**As built (2026-10-24).** `tests/client_matrix_tests.rs`, `tests/common/matrix.rs`
and one probe or configuration per client under `tests/clients/` (contract and
pins in its README, results in `MATRIX.md`). Every image is pinned by tag and
digest; libraries come from PyPI, the Go proxy and Maven Central at pinned
versions. A client's facts on the gateway are judged against Apache Kafka's
with KW-1's judge and their own `tests/clients/allowed_differences.toml`
(empty: no client difference is declared). Local results: all five smoke
clients pass every step; Vector, Fluent Bit, RisingWave and Flink pass every
nightly step. **Kafka Connect with Debezium does not start on the gateway**
(it does on Kafka): the distributed worker stops with "Unable to initialize
REST resources" after its herder fails. The root cause is not diagnosed; the
suspect is the worker's internal-topic checks (`cleanup.policy=compact` on its
config topic, #4508). It is declared in `tests/clients/allowed_differences.toml`
against #4508 and recorded as a KW-4 item below; nothing was fixed in KW-2. CI time: the smoke test took about 2 minutes locally
(Kafka start, Go build and pip install included) and runs in its own nextest
group beside the other container suites; the nightly adds about 10 minutes
plus about 10 GB of image pulls. Still open: the first nightly run in fork CI
(it has not run yet, so its Docker handling of `tag@digest` references and the
disk step are unproven), Flink's commit on checkpoint, librdkafka with
`enable.idempotence=true`, and Kafka Streams and Schema Registry (with KW-4).

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

**Approved (owner, 2026-10-24): rebuild from the log**, as built. Default chosen here: rebuild from the log (part 3) rather
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

**As built, part 2 (2026-10-24).** The SDK seams named above were built upstream in another
shape: the SDK retains a mutation it got no answer for (`OperationUncertain(op)`, `LaneBlocked`,
`Client::resolve_operation`), and a replay carries the same client and request id, which Iggy's
`ClientTable` deduplicates. No SDK change was needed. The bridge used to read
`OperationUncertain` as a refusal, so the retry was written after the SDK replayed the first send:
two copies. Now such a batch is kept in doubt with its operation, a retry replays it
(`IggyBridge::resolve_send`), the next batch replays it first and answers 45 if it never landed,
and `LaneBlocked` answers 7. Tests: 5 unit tests, and a real-server test that holds the bridge's
connection in a relay until the SDK gives up, lets the send land late, and checks the retry is not
written again. Still open after part 2: a send whose request nothing keeps (slot-limit cut-off,
replaced session, closed session) and a retry across a restart (part 3).

**As built, part 3 (2026-10-24).** Rebuild from the log, the default above (the owner did not
overrule it). The first record of each idempotent batch carries `kafka.p` (20 bytes: layout,
producer id, epoch, first and last sequence, flags with bit 0 kept for KW-T) and `kafka.v` 2; all
other records stay at version 1, and Fetch reads both. A gateway reads the marks in a partition's
last 10,000 records before it checks that partition's first idempotent batch, and rebuilds each
producer's epoch and last five batches; the same marks give a replayed send its offset and settle
an unknown outcome before a rewrite. Ruling: a producer older than the window is accepted at any
sequence, not answered 59, because Kafka 4.x does that (`ProducerAppendInfo.checkSequence`). Tests:
unit tests for the marker and the restore; `producer_restart_real_bridge_tests.rs` SIGKILLs a
gateway process between a batch landing and the retry; the `idempotent_restart` oracle scenario
restarts Apache Kafka (and Redpanda nightly) and the gateway mid-sequence. Still at-least-once: an
unknown-outcome batch whose request is gone and that is not in the scanned tail or lands after the
scan, and a late landing from a dead gateway after the next one scanned (needs KW-5's fence).
Still open: the Java producer and franz-go e2e under injected timeouts, KIP-360's epoch bump on
InitProducerId with an existing id, and a configurable window (KW-12).

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

**Found by KW-2 (2026-10-24).** Kafka Connect distributed (Debezium 3.6.2.Final
image) never starts against the gateway; it starts against `apache/kafka:4.3.1`
with the same settings. First step of this task: run
`client_matrix_nightly_connect_debezium` with `KAFKA_CLIENT_MATRIX_FULL=1`
(it prints the worker's last 400 log lines), find the first failing request,
fix it, and remove the `client/connect-debezium` entries from
`tests/clients/allowed_differences.toml`.

**Status (built).** No newer upstream work existed at task start (#4534 and
#4572 were already on `loams`). Built on them: two topic options in the catalog,
`cleanup_policy` and `kafka_configs` (each resettable, with a default, so no
wire change); every Kafka 4.x topic configuration accepted on CreateTopics and
both alter APIs, validated and normalised as Kafka does; `AlterConfigs` now
replaces the set as Kafka does; a BROKER resource answers a fixed set; any
replication factor ≥ 1 is accepted and reported as 1. Honoured:
`cleanup.policy` (stored; without `delete` the topic never expires until KW-6),
`retention.ms`, `retention.bytes` (× partitions), `segment.bytes` (at create).
Echoed (stored, reported, not enforced): the other 29; the list is in
`gateways/kafka/docs/CONFIGS.md`. The oracle scenario `topic_configs` compares
all 33 keys' names, types and defaults, the Connect, Streams and Debezium sets,
normalisation, incremental SET/DELETE/APPEND and the broker resource against
Apache Kafka 4.3.1. Left: the nightly Connect/Schema Registry/Streams runs
(KW-2), `retention.bytes` re-applied after CreatePartitions, and the foreign
SDKs' typed surfaces for the two new option keys (their fixture copies were
regenerated; their contract tests need the new keys).

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

**As built (2026-10-24, PR #8, not merged).** `src/membership.rs` and `src/bridge/iggy_bridge/gateways.rs`. Each
gateway with `IGGY_KAFKA_NODE_ID` appends a TOML announcement (node id, incarnation, instance id,
endpoint per listener protocol) to the one-partition topic `<default stream>.meta/gateways` every
heartbeat (default 2 s) and reads the others; liveness (lease, default 10 s, or a goodbye on
shutdown) and eligibility (two heartbeats after the first announcement, so a new gateway takes
nothing until the others heard of it) are judged on Iggy's append times, never a gateway's clock,
so two gateways that read the same prefix agree. Rendezvous hashing over the eligible set picks
each group's coordinator and each partition's leader. Metadata and `DescribeCluster` list every
live gateway; FindCoordinator answers the owner per key; the others answer `NOT_COORDINATOR` (16)
to the group APIs (parked joins and syncs are released at once on a change) and
`NOT_LEADER_OR_FOLLOWER` (6) to a Produce for a partition led elsewhere; `ListGroups` lists only
owned groups; `OffsetFetch`, Fetch and ListOffsets are served anywhere. Choices made here (owner
may overrule): announcements in an Iggy topic rather than a server-side registry (no server
change); partition leadership by hash, not by the Iggy primary (the SDK does not expose it);
non-leader Produce refused so one gateway holds a partition's producer state. Tests: 10 unit
tests in `membership.rs`; `membership_real_bridge_tests.rs` (three gateways on one
`iggy-server`: brokers, coordinators, leaders and refusals agree; a goodbye moves only the
leaver's groups and partitions; a crash moves them after the lease; a gateway that comes back
takes its share again); `multi_gateway_oracle_tests.rs` runs the failover scenario (Java
verifiable consumer through a three-broker bootstrap list, the coordinator stopped between two
rounds written by kcat) against three gateways and a three-broker Apache Kafka 4.3.1 at once and
compares what the member read. Still open: producer-state handover on a leadership move (KW-3
part 3: today the new leader accepts the producer at any sequence, at-least-once); CreateTopics
and CreatePartitions still validate a replication factor of 1 and assignments naming node 1
(#4508 owns replication factor semantics); librdkafka in the failover scenario (nightly matrix,
KW-2).
Local CI fixes (2026-10-25): `KafkaGateway::run` now owns what it starts, so a dropped (aborted)
gateway stops heartbeating, without a goodbye, as a crash does; before, the spawned heartbeat task
outlived it and the "crashed" gateway's lease never expired. The membership tests wait until an
agreement holds for a join delay plus two heartbeats: the first agreement can come while the newest
gateway is still in its join delay, and the leaders captured then move once it is over.
Earlier status, superseded by the local CI run on the PR: clippy clean on the library, 866 library
tests pass (10 of them for membership). Not yet run anywhere: `membership_real_bridge_tests.rs` and
`multi_gateway_oracle_tests.rs` (the shared build target was cleared mid-run and fork Actions
are disabled); they must pass in `Kafka gateway required` before PR #8 merges.

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

**As built (2026-10-23).** Gateway-only; no Iggy wire change, so nothing on the
STOP list. The watermark is an external-group offset of the Iggy group
`kafka.logstart` on each topic (`bridge/iggy_bridge/log_start.rs`): replicated
by Iggy, read again by each gateway every 5 s, raised at once on the gateway
that wrote it. DeleteRecords v0-2 (`handlers/delete_records.rs`); EARLIEST
reads the first kept record and answers the higher of it and the watermark;
Fetch answers `OFFSET_OUT_OF_RANGE` below the watermark and below the first
kept record (it used to read from the oldest kept one), and reports
`log_start_offset`. Lookups by time are a binary search on the record
timestamp Fetch shows (the producer's on `CreateTime`), about 30 reads per
billion records, exact when timestamps do not decrease along the log; the
four #4516 declared differences and `fetch.log_start_offset` are gone from
`allowed_differences.toml`, and the `delete_records` oracle scenario diffs
DeleteRecords, EARLIEST, a Fetch below and at the new start against Apache
Kafka. Acceptance criteria: 1 and 2 pass (real-bridge tests
`delete_records_real_bridge_tests.rs`); 3 (Streams repartition topics shrink)
holds for readers, not on disk: Iggy deletes whole segments by count only, so
freeing segments wholly below the watermark needs an Iggy command that trims
to an offset (`TruncatePartition` is offset-precise inside the server). That
is an owner decision (new client command), and the nightly Streams check
waits for KW-2's matrix. Lane D's next task, KW-14 (DescribeLogDirs v1-4 from
Iggy partition sizes), is not started.

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

**As built (2026-10-24).** Topic ids are derived, not stored: `src/topic_id.rs` hashes (keyed
BLAKE3) the Iggy stream name, the topic's numeric id and its creation time into an RFC 9562 v8
UUID. Every input is replicated Iggy state, so gateways agree with no write (KW-5 gets this for
free), a restart changes nothing and a recreated topic gets a new id. This replaces the plan's
"topic option written once", which needed a catalog key in `core/common` and a race between
gateways. A registry in `GatewayState` maps ids back to names; a miss lists the topics once.
Served now: Metadata 0-13 (ids from v10, lookups by id from v12), Fetch 4-18 (by id from v13),
Produce 3-13 (by id at v13), ListOffsets 1-10 (-4 EARLIEST_LOCAL as EARLIEST, -5 LATEST_TIERED
answers -1), CreateTopics 2-7 (id in the answer), DeleteTopics 1-6 (by name or id), ApiVersions
0-4. Tests: unit tests for the derivation and registry, real-bridge tests per API at the new
versions, the version-firewall matrix moved to the new maxima, and a `topic_ids` oracle scenario
against Apache Kafka 4.3.1. Local CI (2026-10-25) found three tests still on the old maxima: the
ApiVersions v1/v3 golden bytes (updated), the above-max firewall matrix, which needs Produce v14,
Fetch v19 and ListOffsets v11 fixtures past what the generator could build (the schemas stop at 13
and 18; `kafka-protocol` encodes ListOffsets only to 10), so `kafka-message-gen generate
--above-max` now writes, per key, the version after the newest one that builds, as a newer client
sends it (the newest body under the next version number), and `ci-wire-fixtures.sh` passes it, and the SASL pre-auth probe above the firewall, which
used Fetch v13 and now uses FindCoordinator v5 (encoded by the crate, not served). Left: OffsetCommit and OffsetFetch v10 and ListOffsets v11 wait for a
`kafka-protocol` release that has their schemas (0.18 stops at 9, 9 and 10); `@platformatic/kafka`
and Sarama nightly checks belong to KW-2's matrix; FindCoordinator v5-6 not advertised.

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

**As built (2026-10-24).** Upstream had nothing on #4512 (#3549, numinnex's PLAIN, is closed and
landed; KW-9 builds on its state machine and `IggyAuthenticator` rather than replacing them).
`IGGY_KAFKA_SASL_MECHANISMS` picks any of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER.

- `src/oauth.rs`: JWT validation with `jsonwebtoken` (RustCrypto backend, as the server uses),
  JWKS from an https URL (http only on loopback or with `IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP`) or a
  file, cached, refreshed on an interval and on an unknown `kid` (at most every 10 s). Issuer and
  audience required, `exp` checked, `nbf` honoured, configurable principal claim (`sub` by
  default; `preferred_username` suits Zitadel). Algorithm-confusion defences: asymmetric
  algorithms only (`HS*` refused at startup), key type and curve must match `alg`, `oct` and
  encryption keys never loaded, token-embedded keys (`jwk`, `jku`) never consulted. A refused
  token gets Kafka's RFC 7628 `{"status":"invalid_token"}` challenge, then 58.
- `src/scram.rs` + `bridge/iggy_bridge/scram_store.rs`: SCRAM per RFC 5802/7677 with RustCrypto
  `hmac`/`sha2`/`pbkdf2`, `subtle` for the proof comparison, `getrandom` nonces. Credentials (salt,
  iterations, StoredKey, ServerKey; never the password) are a JSON log in Iggy topic
  `<default stream>.meta/scram`, replayed by every gateway, read on its own client. Stricter than
  Kafka: channel binding checked, `m=` and `tokenauth` refused, and an unknown user gets a decoy
  salt and fails at the proof rather than at the first message (no user enumeration).
- `DescribeUserScramCredentials` (50) and `AlterUserScramCredentials` (51) v0 with Kafka's
  per-user errors (91, 92, 93, 33); gated on Iggy `read_users` / `manage_users` (31 otherwise).
- KIP-368: `session_lifetime_ms` = token life capped by `IGGY_KAFKA_CONNECTIONS_MAX_REAUTH_MS`;
  re-authentication on the same connection (same mechanism, same principal, as Kafka); an expired
  session is closed on its next request.
- Every mechanism ends at an active Iggy user, looked up through the bridge (`read_users`).

Tests: unit (parsers, SCRAM RFC 7677 vector, decoys, store replay, admin validation), socket
tests with stub verifiers (`sasl_mechanisms_tests.rs`), validator tests against a minted RS256 /
ES256 / EdDSA test issuer (`oauth_validator_tests.rs`: expired, foreign issuer, wrong audience,
`alg: none`, HS256 confusion, foreign key, embedded key, rotation), real clients
(`sasl_clients_e2e_tests.rs`: Java 3.9 tools incl. `kafka-configs` SCRAM admin and OAUTHBEARER via
the token endpoint, kcat 1.7 SCRAM, confluent-kafka-python 2.16 with all mechanisms and a
re-authentication run), and the oracle (`oracle_auth_diff_tests.rs`, Apache Kafka 4.3.1 with the
same four mechanisms and Kafka's JWKS validator). Declared differences
(`tests/oracle/auth_allowed_differences.toml`): a user with no SCRAM credential fails at the proof
rather than the first message, under both mechanisms, and the gateway's `manage_users` gate on
Alter (Kafka runs without an authorizer).

Local CI fixes (2026-10-25): SCRAM and OAUTHBEARER no longer read or feed the PLAIN failed-login
throttle (it closed every login after a rejected one, which the oracle saw as SCRAM-SHA-512
failing outright and bad tokens closing instead of getting `invalid_token` and 58; Kafka does not
throttle them, and they cost Iggy nothing); a SCRAM final nonce only has to end with the agreed
one, as in Kafka (kcat 1.7 sends its nonce twice); `DescribeClientQuotas` (48) answers "no
quotas" with the SCRAM admin keys, which `kafka-configs --describe --entity-type users` needs;
the e2e sets each SCRAM mechanism in its own `kafka-configs` call (92 otherwise, in Kafka too).
Not done: franz-go (waits for the KW-2 matrix harness), Redpanda as an auth oracle (needs an OIDC
discovery document), GSSAPI and delegation tokens (on request).

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

**Status and notes for whoever picks it up (2026-10-24, lane F stopped before starting it).**
The plan's default (one Iggy session per principal) does not work for KW-9's principals: OAUTHBEARER,
SCRAM and mTLS identities carry no Iggy credential, and AUTHENTICATION.md rules out keeping PLAIN
passwords. The workable default is gateway-side enforcement of the principal's own Iggy
`Permissions` (read through the bridge with `read_users`, ids resolved to stream and topic names,
cached with a short TTL and invalidated by this gateway's own ACL writes), evaluated with exactly
the rules in `core/metadata/src/permissioner/permissioner_rules/` (poll, append, get_topic,
create_topic, manage_topic), the bridge's user as the ceiling. Kafka groups have no Iggy resource:
derive group access from topic read access, and record that one tenant can disrupt another's group
membership (not read its data) as an open question. CreateAcls/DeleteAcls map only the exact cases
(TOPIC literal READ→poll, WRITE→send, ALL→manage_topic; DESCRIBE only alongside READ/WRITE; TOPIC
`*`→default-stream permissions) and refuse the rest with a message; topics must exist (Iggy keys
grants by id). Quotas (48, 49) stay on request.

**Ruling (owner, 2026-10-25; section 9, item 7).** Approved as above, with one change to the group
rule: group ids are scoped per principal, so a principal sees, joins and deletes only the groups in
its own namespace. One tenant can no longer disrupt another's group membership, which closes the
open question above.

**As built (2026-10-26, parts 1-4).** `src/authz.rs`: `Grants` holds every Iggy grant of the
principal with stream and topic ids named by the bridge at authentication (PLAIN through the bridge
after the login), checked with Iggy's own rules (`get_topic`, `poll_messages`, `append_messages`,
`create_topic`, `manage_topic`). Enforcement per topic in Produce, Fetch, ListOffsets, Metadata
(all-topics filtered, named/identified 29), CreateTopics and auto-creation, DeleteTopics,
DeleteRecords, CreatePartitions, (Incremental)AlterConfigs, DescribeConfigs (broker: 31 without
`read_servers`), OffsetCommit/Fetch/Delete; InitProducerId needs WRITE somewhere (31).
`protocol/handlers/group_scope.rs` scopes every group API to `<len>:<principal>/<group>` and
rewrites answers back; group access = any topic read grant (else 30). DescribeAcls renders per-stream
and per-topic grants on their Kafka topics (`TopicMapping::kafka_topic_of`). Tests: `authz` unit
rules, scoped rendering, `authz_real_bridge_tests.rs` (per-topic and per-stream grants, group
isolation). Left: CreateAcls/DeleteAcls (Kafka bindings onto Iggy grants, exact cases only), grant
refresh on this gateway's own ACL writes, an ACL oracle vs Kafka's StandardAuthorizer.

With KW-T (rebased onto step 1, 2026-10-26): AddPartitionsToTxn takes in only partitions whose
topic the principal may write (else 29, the rest 55, as Kafka), since a commit writes a marker to
each; a transactional InitProducerId needs WRITE somewhere, like an idempotent one. Kafka also
authorizes the transactional id itself (`TRANSACTIONAL_ID` WRITE), and Iggy has no resource for it,
so until then any principal that may write can use, and so fence, another's transactional id.

**Decided under the KW-10 ruling (2026-10-27):** transactional ids are scoped per principal the way
groups are, with the bridge's Iggy user as the ceiling. It changes KW-T's coordinator keys, so it
lands as a follow-up PR after this one and KW-T step 3, coordinated with the KW-T work.

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
`__consumer_offsets`. **Approved (owner, 2026-10-24):** commit metadata is stored
in that gateway-owned Iggy topic and reloaded at startup, with no server change.

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

**Status (2026-10-24): started, not wired, not built.** Branch `kw13-kip848-draft` holds a first
cut that was never compiled: `src/group/consumer_protocol.rs` (the state machine: group and member
epochs, revoke-before-assign reconciliation, partition ownership, uniform and range assignors,
static members, regex subscriptions, session and revocation expiry, `STALE_MEMBER_EPOCH` commit
check, with unit tests), `src/group/consumer_coordinator.rs` (consumer groups beside classic ones,
`group.consumer.migration.policy=disabled` semantics), and handlers for 68 and 69. Still to do:
wire them in (API keys, `SUPPORTED_RANGES`, dispatch, shape guards, `ListGroups` type,
`DeleteGroups`/`OffsetDelete`/`OffsetCommit` hooks), build and fix, a Java 4
`group.protocol=consumer` e2e (two members, leave and resume; classic-to-consumer offsets carried
over), a KIP-848 oracle scenario, then online migration as part 2.

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

## 6. KW-T: transactions and exactly-once (being built)

**Status: steps 1 to 3 of 5 built (2026-10-26), behind `IGGY_KAFKA_TRANSACTIONS_ENABLED`, off by
default.** Owner, 2026-10-25: "start transactions now"; Confluent Cloud is the managed-Kafka
reference (report-only), the Apache Kafka container stays pass/fail, and the crash and
network-fault tests run on the second laptop. With the switch off the gateway keeps refusing
transactions as `SCOPE.md` "Transactions" describes. `gateways/kafka/docs/TRANSACTIONS.md` is the
as-built design.

| Step | Scope (6.8's letters) | State |
| --- | --- | --- |
| 1 | (a) log and state machine, (b) `InitProducerId`, `FindCoordinator`, `AddPartitionsToTxn`, `EndTxn`, transactional Produce, markers; timeouts, fencing, recovery of `Prepare*` | **Built** (fork PR #19) |
| 2 | (c) last stable offset, aborted transactions, `read_committed` Fetch and `ListOffsets` | **Built** |
| 3 | (d) `AddOffsetsToTxn`, `TxnOffsetCommit`, `require_stable`; Streams `exactly_once_v2`, Flink 2PC | **Built**; Streams and a Java EOS loop verified; Flink not run yet |
| 4 | `DescribeTransactions`, `ListTransactions`, `DescribeProducers`, id expiry, log snapshots | Planned |
| 5 | (e) KW-5 ownership, coordinator move, crash and fault matrix on the second laptop | Planned |
| — | (f) TV2 | Not planned yet |

**As built, step 1.** Differences from 6.3, each for a reason:

- The aborted-transaction index is not a second topic (`txn_aborted`): each `CompleteAbort`
  record of `txn_state` lists its aborted ranges, and `CompleteCommit` its committed ones (their
  markers bound the producer's next transaction). One log, written once per transaction end, in
  the order that makes it safe: the ranges are durable before the transaction stops holding the
  last stable offset.
- A transaction's first offset on a partition is not scanned for: it is the high watermark when
  the partition was added, raised to one past the producer's last marker there, and lowered to the
  first record once a send names it. Never above the first record, never at or below the previous
  marker, which is what a consumer's aborted filter needs.
- Every record of a transactional batch carries `kafka.t` (producer, epoch, its sequence), not
  only the first, so Fetch can serve a transactional batch from a read that starts mid-batch.
- Markers are written on the producer's own Iggy session after its Produce turn, so no batch the
  coordinator accepted lands after its marker (the SDK lane replays a send in doubt first).
- The coordinator epoch in markers is 0 until step 5.

**As built, step 2.** The last stable offset is the first offset of the earliest open
transaction that may have written to the partition (a send started, or recovered from the log),
taken after the poll, so a transaction that started writing meanwhile already holds it. Fetch at
`read_committed` stops there and lists the aborted transactions its records span; `ListOffsets`
`LATEST` at `read_committed` answers it, and a timestamp lookup past it answers -1, as Kafka does.
The oracle scenario `read_committed` (27 facts: a commit, an abort, an open transaction with a
plain record after it, reads from 0 and from the abort marker, `LATEST` at both levels, then the
commit) equals Apache Kafka 4.3.1 on every fact, and the Java consumer reads the same committed
records on the gateway as on Kafka.

**As built, step 3.** `AddOffsetsToTxn` and `TxnOffsetCommit` keep a transaction's group offsets
in its `txn_state` record (so a restart after `PrepareCommit` still commits them), and they become
the group's Iggy offsets after the commit's markers, before `CompleteCommit`; an abort drops them.
`TxnOffsetCommit` v3+ checks the member and generation as `OffsetCommit` does. `OffsetFetch` with
`require_stable` answers 88 for an offset an open transaction holds. Oracle scenario
`exactly_once` (12 facts) equals Apache Kafka 4.3.1. The Java consume-transform-produce loop
(`sendOffsetsToTransaction` with group metadata, every third transaction aborted) copies 60 of 60
records exactly once, and a Kafka Streams 3.9 word count at `exactly_once_v2`, restarted half way,
ends with Kafka's counts.

Tests: unit tests for the state table, the coordinator (commit, fence, timeout, recovery,
errors), the index, the log, the marks and the handlers; `transactions_real_bridge_tests.rs`
(gateway binary on `iggy-server`: markers and control batches, SIGKILL and replay, a decision
whose markers were never written, a timeout abort); `transactions_java_client_tests.rs` (Java
producer commit, abort and fencing, on the gateway and on Apache Kafka 4.3.1: same outcome);
the oracle scenario `transactions` (43 facts, all equal to Apache Kafka 4.3.1).

Confluent Cloud (report-only, 2026-10-26, Basic cluster) differs from Apache Kafka and the
gateway on the same scenario, and these are recorded, not followed: it answers `AddPartitionsToTxn`
v3 with 0 for a fenced epoch and for an unknown transactional id (Kafka: 90 and 49), lets a
transactional batch land on a partition the transaction never added (Kafka: 48), answers `EndTxn`
v3 from a fenced producer with 47 (Kafka: 90), and its `read_uncommitted` Fetch shows no control
batch: markers take no offset there (high watermark 2 after a committed transaction of two
records, where Kafka and the gateway say 3). Everything else (fencing by `InitProducerId`, the
double bump when an open transaction is fenced, 48 and 50, zombie writes refused with 47,
the records a consumer reads) matches. The plan reads these as Confluent's own engine (Kora)
verifying lazily and keeping markers out of the offset space; Apache Kafka stays the contract.

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
the difference is recorded. **Confluent Cloud** (managed Kafka, section 3) is
the third, report-only oracle for transactions: it is the one target with a
transaction coordinator, transaction log and partition leaders on separate
brokers, so items 1–4 also run against it locally before KW-T is called done,
and any difference from it is recorded with its cause. Its transactional ids
use the harness prefix, which its key is scoped to.

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
1–6 green against Apache Kafka and Redpanda, and items 1–4 run against
Confluent Cloud with every difference from it explained.

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

## 9. Owner decisions and open questions

### Approved (owner, 2026-10-24)

The owner approved these with the recommended answers ("approve all with your
recommendations").

1. **KW-3 restart safety: rebuild producer state from the log**, as built (part
   3). Not "bump the epoch and answer 59 after each restart".
2. **KW-4: accept and echo** the topic configurations Iggy cannot honour, marked
   in the topic's description as not honoured. They are not refused.
3. **KW-6: compaction is built in the fork first.** The design note for
   upstream discussion stays ready and is not posted (see the open question
   below).
4. **KW-12: offset-commit metadata** is stored in a gateway-owned Iggy topic that
   the gateway replays into memory at startup, like Kafka's `__consumer_offsets`.
   No server change, and `ExternalGroup`'s `u64` offset is untouched.
5. **KW-T: transactions start after KW-2, KW-3 and KW-13 are done.** Until then
   the gateway keeps refusing them as `SCOPE.md` "Transactions" describes.
   **Overridden 2026-10-25:** start now; classic groups suffice for `TxnOffsetCommit`; Confluent
   Cloud is the managed reference (report-only); faults run on the second laptop.
6. **KW-8: the computed topic ids are accepted**: a stable UUID per Iggy topic,
   stored as a topic option at creation and minted once for existing topics on
   first read.

### Approved (owner, 2026-10-25)

7. **KW-10: the gateway enforces each Kafka principal's own Iggy permissions.**
   SCRAM, OAUTHBEARER and PLAIN principals map to Iggy users, and the bridge's
   Iggy user is the ceiling (replacing the plan's one-Iggy-session-per-principal
   default, which cannot work for SCRAM, OAUTHBEARER and certificate principals).
   Kafka consumer groups have no Iggy equivalent, so group access follows topic
   read access, **and group ids are scoped per principal**: a principal can only
   see, join or delete groups in its own namespace, so tenants cannot disrupt
   each other's groups. The ACL admin APIs map onto that. Quota admin (48, 49)
   stays "on request". Built after KW-9, KW-8 and KW-5 land (it builds on KW-9's
   SASL).

### Still open

1. Upstream: may the fork's tasks be offered upstream as they finish (one PR
   per issue, after assignment), starting with KW-1/KW-2 (#4531) and KW-3
   (#4513)? Default: no posting until the owner says so. **Not approved on
   2026-10-24; stays open.** This includes the KW-6 design note.
