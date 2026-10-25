# Confluent Cloud as a Kafka oracle

The KW-1 oracle harness (`gateways/kafka/tests/oracle_wire_diff_tests.rs`)
compares the gateway with Apache Kafka (every PR), Redpanda (nightly) and,
when run locally on a machine that has its key, Confluent Cloud: a managed,
multi-broker Kafka. Confluent Cloud is report-only and never runs in CI. Why
it exists and how it fits the test strategy: section 3 of
[`kafka-wire-plan.md`](kafka-wire-plan.md).

The cluster runs on Ostrium Labs' Confluent for Startups credits ($2,000,
valid one year from 2026-10-25).

## What exists

All in the Ostrium Labs Confluent Cloud organisation, environment
`env-3kqddw` ("default"). Created 2026-10-25.

| Resource | Id | Details |
| --- | --- | --- |
| Kafka cluster `loams-kafka-oracle` | `lkc-120zwvj` | **Basic**, single zone, AWS `ap-south-1` (Mumbai). Bootstrap `lkc-120zwvj.ap-south-1.aws.public.confluent.cloud:9092`, REST `https://lkc-120zwvj.ap-south-1.aws.public.confluent.cloud:443`. `max_ecku` capped at 1 |
| Service account `loams-kafka-oracle-sa` | `sa-3n32p6j` | No organisation, environment or cluster role. Only the Kafka ACLs below |
| Kafka API key `loams-kafka-oracle-key` | Not recorded here (`CONFLUENT_ORACLE_API_KEY` in the env file) | Owned by `sa-3n32p6j`, scoped to `lkc-120zwvj`. Key and secret are only in `~/confluent-oracle.env` on the owner's machine |

ACLs of `User:sa-3n32p6j` (all ALLOW, host `*`). Basic clusters do not
support resource-level RBAC roles, so the key is scoped with ACLs:

| Resource | Pattern | Operations |
| --- | --- | --- |
| Topic `loams-oracle-` | PREFIXED | CREATE, DELETE, READ, WRITE, DESCRIBE, DESCRIBE_CONFIGS, ALTER, ALTER_CONFIGS |
| Group `loams-oracle-` | PREFIXED | READ, DESCRIBE, DELETE |
| Transactional id `loams-oracle-` | PREFIXED | WRITE, DESCRIBE |
| Cluster `kafka-cluster` | LITERAL | IDEMPOTENT_WRITE |

The key cannot touch any topic, group or transactional id outside the
`loams-oracle-` prefix, and cannot change ACLs. To set the ACLs up, the service
account held CloudClusterAdmin on the cluster for a few minutes; that role
binding was deleted once the ACLs were in place.

Not enabled, and not to be enabled for this oracle: Schema Registry (Stream
Governance), connectors, ksqlDB, Flink, Tableflow, private networking.

## The env file

`~/confluent-oracle.env`, mode 600, never committed and never in CI:

```bash
CONFLUENT_ORACLE_BOOTSTRAP=lkc-120zwvj.ap-south-1.aws.public.confluent.cloud:9092
CONFLUENT_ORACLE_API_KEY=...
CONFLUENT_ORACLE_API_SECRET=...
CONFLUENT_ORACLE_CLUSTER_ID=lkc-120zwvj
CONFLUENT_ORACLE_ENV_ID=env-3kqddw
```

`KAFKA_ORACLE_CONFLUENT_ENV=/path/to/file` points the harness at another file.

## How to run

The oracle joins the usual diff run on its own when the env file exists:

```bash
# RLIMIT_MEMLOCK for the spawned iggy-server (io_uring), as in the local CI shim.
cargo build -p server --bin iggy-server
KAFKA_ORACLE_CONTAINER_CLI=podman KAFKA_ORACLE_REPORT=$PWD/oracle-report.txt \
  cargo nextest run -p iggy-gateway-kafka --test oracle_wire_diff_tests --no-capture
```

- Skipped when the file is absent, when `CI` or `GITHUB_ACTIONS` is set, or
  with `KAFKA_ORACLE_CONFLUENT=0`.
- The Apache Kafka container oracle still runs first and stays the reference.
- TLS with the public web PKI roots (`webpki-roots`), SASL/PLAIN with the key.
  The harness's `WireClient` sends Produce, Fetch and ListOffsets to the
  partition's leader and group and transactional requests to their
  coordinator, with one connection per broker, as a real client does.
- Report-only: lines start `confluent (report-only)`. A difference where
  Confluent Cloud answers as Apache Kafka does is only counted; a scenario that
  fails on Confluent Cloud, or the cluster being unreachable, is a report line,
  never a failed run.
- Every resource name is `loams-oracle-<run-id>-<scenario>-...`, with
  `<run-id>` = `<unix seconds>-<hex>`. At the end of the run, and on drop when
  the run panics, the harness deletes every topic with the run's prefix and
  checks a listing shows none. At start it also deletes `loams-oracle-` topics
  more than 6 hours old (a run that was killed). Consumer groups and
  transactional ids are not deleted; Kafka expires them (offsets after 7 days,
  transactional ids after `transactional.id.expiration.ms`), and they cost
  nothing.

## Cost

Confluent Cloud pricing page
(<https://www.confluent.io/confluent-cloud/pricing/>, read 2026-10-25), Basic
clusters: the first eCKU is free, each further eCKU-hour is $0.14; data in and
out $0.05/GB; storage $0.08/GB-month; up to 1,500 partitions per cluster.

- With `max_ecku` capped at 1, the cluster never bills an eCKU-hour.
- One run creates 9 topics of 3 partitions and moves a few kilobytes: well
  under $0.01 per run.
- Expected monthly spend: under $1, even with the oracle run on every local PR
  check. The credits are not at risk.

No budget or cost alert was set: the Confluent Cloud API (`billing/v1/costs`)
only reports costs; it exposes neither budgets nor credit balances. Watch spend
in the Confluent Cloud console under Billing & payment, or with
`GET https://api.confluent.cloud/billing/v1/costs?start_date=...&end_date=...`
(a Cloud API key with billing access). If spend ever passes $100 in a month,
something other than this oracle is running.

## Tear down

With the owner's Cloud API key (organisation admin), in this order:

```bash
# The key id is CONFLUENT_ORACLE_API_KEY in ~/confluent-oracle.env.
curl -u "$KEY:$SECRET" -X DELETE "https://api.confluent.cloud/iam/v2/api-keys/$CONFLUENT_ORACLE_API_KEY"
curl -u "$KEY:$SECRET" -X DELETE "https://api.confluent.cloud/cmk/v2/clusters/lkc-120zwvj?environment=env-3kqddw"
curl -u "$KEY:$SECRET" -X DELETE https://api.confluent.cloud/iam/v2/service-accounts/sa-3n32p6j
rm ~/confluent-oracle.env
```

Deleting the cluster deletes its topics and ACLs. The `default` environment
`env-3kqddw` belongs to the organisation and stays.
