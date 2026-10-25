# Kafka transactions (Ostrium fork, KW-T)

Status: **step 1 of 5 built** in the Ostrium Labs fork, behind
`IGGY_KAFKA_TRANSACTIONS_ENABLED=true` (off by default). Not upstream. The plan is section 6 of
`docs/loams/kafka-wire-plan.md`; this document is what the code does.

With the switch off, the gateway refuses transactions exactly as [`SCOPE.md`](SCOPE.md)
"Transactions" describes. With it on:

| Step | What | State |
| --- | --- | --- |
| 1 | Coordinator, transaction log, `InitProducerId` with a transactional id, `FindCoordinator` (transaction), `AddPartitionsToTxn`, `EndTxn`, transactional Produce, markers, timeouts, fencing, recovery | **Built** |
| 2 | `read_committed`: last stable offset, aborted transactions in Fetch, `ListOffsets` at `read_committed` | Planned. Until then a `read_committed` consumer also sees aborted records, so keep the switch off where one reads |
| 3 | `AddOffsetsToTxn`, `TxnOffsetCommit`, `OffsetFetch` `require_stable`: consume-transform-produce (Kafka Streams `exactly_once_v2`, Flink 2PC) | Planned |
| 4 | `DescribeTransactions`, `ListTransactions`, `DescribeProducers`, transactional id expiry, log snapshots | Planned |
| 5 | Several gateways (KW-5 ownership), crash and network-fault matrix | Planned |

## The model

Kafka's, as Apache Kafka 4.1's `TransactionCoordinator` implements it under transaction version 1
(KIP-98, KIP-360, KIP-588, and KIP-890 part 1's verification of partitions). The gateway never
advertises `transaction.version` 2, so clients send `AddPartitionsToTxn` and the gateway answers
every API at the versions before TV2 (`AddPartitionsToTxn` 0 to 3, `EndTxn` 0 to 4).

`src/transactions/metadata.rs` holds the state machine as pure functions: `Empty`, `Ongoing`,
`PrepareCommit`, `PrepareAbort`, `CompleteCommit`, `CompleteAbort`. Kafka's pending
`PrepareEpochFence` is not kept: a fence goes straight to `PrepareAbort` at the bumped epoch, which
is the record Kafka writes for it.

## Where the state lives

| Kafka | Gateway |
| --- | --- |
| `__transaction_state`, 50 partitions | Iggy topic `<default stream>.meta/txn_state`, 16 partitions, `persisted`, no expiry. One JSON record per transition holding the id's whole metadata; partition `hash(transactional_id) mod 16` with Java's `String.hashCode`, as Kafka places it |
| Coordinator cache | Replayed from the log at start (`TransactionCoordinator::load`); `COORDINATOR_LOAD_IN_PROGRESS` (14) until done |
| Control batch (commit or abort marker) | One Iggy message per partition and transaction end: `kafka.v` 3, `kafka.ctl` the control type, `kafka.t` the producer and epoch, payload Kafka's `EndTransactionMarker` value. It takes an offset, as in Kafka. Fetch serves it as a control batch |
| Transactional batch attributes | Every record carries `kafka.t` (producer id, epoch, its own sequence); the batch's first also `kafka.p` with flag bit 0. Fetch rebuilds transactional batches from them, also when a read starts inside a batch |
| Producer state snapshot | KW-3's `kafka.p` marks, as for idempotent producers |
| Transaction index (aborted ranges) | Each `CompleteAbort` record lists, per partition, the range from the first offset the transaction may hold to its marker; `CompleteCommit` records list theirs too, which bounds where the producer's next transaction starts. Replayed into `TxnIndex` |

The first offset of a transaction on a partition is the partition's high watermark when the
transaction added it, raised to one past the producer's last marker there, and lowered to the
transaction's first record once a send names its offset. Never above its first record, and never at
or below the producer's previous marker, which is what a consumer's aborted-transaction filter
needs (`CompletedFetch.consumeAbortedTransactionsUpTo` in the Java client).

## Order of writes

- Every transition is written to the log before it becomes current, one at a time per id.
- `EndTxn` answers once its `Prepare*` record is in the log, as Kafka does. A task then writes the
  marker to every partition of the transaction and the `Complete*` record. Meanwhile the next
  `AddPartitionsToTxn` and `InitProducerId` of the producer answer `CONCURRENT_TRANSACTIONS` (51)
  and the client retries.
- A marker is sent on the producer's own Iggy session, after the producer's turn there. A Produce
  request holds that turn from its check against the coordinator until its send ends, so every
  batch the coordinator let through lands before the marker; a send Iggy never answered is
  replayed by the SDK ahead of it. A batch checked after the decision is refused.
- A gateway that stops after a `Prepare*` record finds it on start and writes the markers again. A
  second marker of the same outcome is harmless to every Kafka consumer.

## Answers

| Request | Refusal | Code |
| --- | --- | --- |
| `InitProducerId` | timeout not in `1..=transaction.max.timeout.ms` (default 15 min, `IGGY_KAFKA_TRANSACTION_MAX_TIMEOUT_MS`) | 50 |
| `InitProducerId` | markers being written, or an open transaction it just fenced and is aborting | 51, retried |
| `InitProducerId`, `AddPartitionsToTxn`, `EndTxn` | an epoch a newer producer replaced | 90; 47 before KIP-588's versions (`InitProducerId` < 4, the others < 2) |
| `AddPartitionsToTxn`, `EndTxn` | unknown id or producer id | 49 |
| `AddPartitionsToTxn` | a partition that does not exist | 3 for it, 55 for the others |
| `EndTxn` | no open transaction, or the other outcome than the one in progress | 48 |
| Produce (transactional) | partition not added to the transaction | 48 |
| Produce (transactional) | fenced epoch | 47 |
| Produce (transactional) | markers being written, coordinator loading | 19, retried |
| Produce (transactional) | a batch that is not transactional, or one that cannot carry its marks | 87 |
| Produce (not transactional) | a transactional batch | 35, as before |

The oracle scenario `transactions` (`tests/oracle/transactions.rs`) checks each of these against
Apache Kafka 4.3.1 fact for fact.

Transactions past their timeout are fenced and aborted every
`IGGY_KAFKA_TRANSACTION_ABORT_INTERVAL_MS` (Kafka's default, 10 s), as Kafka's
`abortTimedOutTransactions` does: the producer's next call is refused at its old epoch.

## Limits of step 1

- `read_committed` is not served yet (step 2): consumers see aborted records. Fetch reports the
  high watermark as the last stable offset.
- One gateway per Iggy default stream coordinates transactions. Two gateways with the switch on
  would both act as coordinator for every id (KW-5's ownership is step 5).
- The log is never compacted (KW-6); a restart replays all of it. A record is a few hundred bytes,
  three per transaction.
- Iggy-native consumers see transactional data as written, aborted records included, and the
  control messages, unless they skip `kafka.ctl`.
- A send whose outcome the gateway never learned and whose request nothing keeps (KW-3's
  at-least-once cases) can land after its transaction's marker. Kafka closed the same hole with
  transaction version 2's per-transaction epoch bump; the gateway does not serve TV2 yet.

## Tests

- Unit: `transactions::metadata` (the state table), `transactions::coordinator` (commit, fence,
  timeout, recovery from a log that stops after `Prepare*`, errors, next-transaction bound),
  `transactions::index`, `transactions::log`, `records::txn_mark`, the two handlers and their
  bounds guards.
- `tests/transactions_real_bridge_tests.rs`: through the gateway binary on a real `iggy-server`:
  markers stored and served as control batches, a SIGKILL and a replay, a decision whose markers
  were never written, a timeout abort.
- `tests/transactions_java_client_tests.rs`: the Java producer commits, aborts and is fenced, on
  the gateway and on Apache Kafka 4.3.1; both answer alike.
- Oracle scenario `transactions`: Apache Kafka (pass/fail), Redpanda (nightly), Confluent Cloud
  (report-only, local).
