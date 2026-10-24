# Topic configuration

`CreateTopics` (19), `DescribeConfigs` (32), `AlterConfigs` (33) and
`IncrementalAlterConfigs` (44) accept every Kafka 4.x topic configuration. Four
are honoured through Iggy topic options. The other 29 are validated as Kafka
validates them, stored verbatim on the Iggy topic and reported back, but Iggy
does not enforce them. Kafka Connect, Debezium, Schema Registry, Apicurio and
Kafka Streams create their own topics with configurations and read some of them
back (apache/iggy#4508); this is what lets them start.

`kafka-configs --alter` on a topic has sent API 44 since Kafka 2.5. Java
`Admin.incrementalAlterConfigs` (the only alter method left in Kafka 4.0),
MirrorMaker 2's configuration sync and kafka-ui also send API 44. API 33 comes
from Java `Admin.alterConfigs` on clients older than 4.0 and from other clients'
non-incremental alter calls, such as librdkafka's `rd_kafka_AlterConfigs`.

With the bridge off, the three config APIs answer `NOT_CONTROLLER` (41) for each
resource and do not read or write Iggy.

## Honoured and echoed

| Key | Type | Iggy | What Iggy does |
| --- | ---- | ---- | -------------- |
| `cleanup.policy` | LIST | `cleanup_policy` topic option | Stored and reported. Iggy does not compact yet (apache/iggy#4521, KW-6). A policy without `delete` (`compact`) turns expiry and the size cap off, so the topic never loses a record: `message_expiry` is never-expire and `max_topic_size` unlimited, whatever `retention.ms` and `retention.bytes` say. Those two are then kept verbatim and reported as set. `compact,delete` expires as `delete` does |
| `retention.ms` | LONG | `message_expiry` | Honoured for sealed segments; the active segment does not expire. `-1` is never expire. A value longer than Iggy can store (`u32::MAX` seconds, about 136 years), such as Debezium's `Long.MAX_VALUE`, is stored as never expire and reported as sent. `0` is refused (see "Limitations") |
| `retention.bytes` | LONG | `max_topic_size` | Kafka's value is per partition, Iggy's per topic: the gateway stores the value times the partition count, at least one segment (Iggy refuses a smaller cap). A negative value is no cap. Reported as sent |
| `segment.bytes` | INT | `segment_size` | Applied when the topic is created, rounded down to a 512-byte multiple and capped at 1 GiB (Iggy's bounds). Iggy cannot change a segment size later, so an alter is stored and reported, not applied |

Accepted and reported, not enforced (the value is checked as Kafka checks it):
`compression.gzip.level`, `compression.lz4.level`, `compression.type`,
`compression.zstd.level`, `delete.retention.ms`, `file.delete.delay.ms`,
`flush.messages`, `flush.ms`, `follower.replication.throttled.replicas`,
`index.interval.bytes`, `leader.replication.throttled.replicas`,
`local.retention.bytes`, `local.retention.ms`, `max.compaction.lag.ms`,
`max.message.bytes`, `message.timestamp.after.max.ms`,
`message.timestamp.before.max.ms`, `message.timestamp.type`,
`min.cleanable.dirty.ratio`, `min.compaction.lag.ms`, `min.insync.replicas`,
`preallocate`, `remote.log.copy.disable`, `remote.log.delete.on.disable`,
`remote.storage.enable`, `segment.index.bytes`, `segment.jitter.ms`,
`segment.ms`, `unclean.leader.election.enable`.

Notes on some of them:

- `min.insync.replicas`, `unclean.leader.election.enable` and the throttled
  replica lists have no meaning on Iggy, which replicates a partition through
  its own consensus.
- `max.message.bytes` is not the gateway's limit. The gateway refuses a record
  above its own `IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE`, whatever the topic says.
- `message.timestamp.type=LogAppendTime` is stored; Fetch still returns the
  producer's timestamp.
- `compression.type` is stored; the gateway decompresses on Produce and Fetch
  returns uncompressed batches.
- `remote.storage.enable=true` is refused with Kafka's own text for a broker
  without tiered storage.
- `segment.ms` is stored; Iggy rolls segments by size only.

The echoed configurations live in the topic's `kafka_configs` option, as sorted
`name=value` lines (`-`, the option's default, when the set is empty). An Iggy client sees them in
`GetTopic`. The server stores them and never reads them. One option value holds
at most 255 bytes, so a set that does not fit is refused with
`POLICY_VIOLATION` (44) and nothing is written.

A name Kafka 4.x does not define is `INVALID_CONFIG` (40) with
`Unknown topic config name: <name>`, as Kafka answers. That includes
`message.format.version` and `message.downconversion.enable`, which Kafka 4.0
removed, and `retention.minutes`/`retention.hours`, which exist only at broker
level.

## Values

Values are normalised as Kafka normalises them before it stores them, so a
describe returns what Kafka would: numbers in canonical decimal (`+5` and `007`
become `5` and `7`), booleans in lower case, list items trimmed and repeats
removed (`compact, compact` becomes `compact`), a ratio as Java prints a double
(`1` becomes `1.0`). A value Kafka refuses is `INVALID_CONFIG` (40) with Kafka's
own message, for example
`Invalid value 0 for configuration min.insync.replicas: Value must be at least 1`.
A null value is `INVALID_CONFIG` (40),
`Null value not supported for topic configs: <name>`.

## Creating a topic

`CreateTopics` validates every configuration before it creates anything. One
bad configuration fails that topic, and the others in the request are still
created. A topic that already exists answers `TOPIC_ALREADY_EXISTS` (36) even
when its configurations are invalid, as Kafka's controller does; Kafka Connect
relies on that when it re-sends `cleanup.policy=compact` on every start.
`validate_only` runs the same checks and creates nothing.

From v5 the result carries every configuration of the new topic, with source
`DYNAMIC_TOPIC_CONFIG` (1) for the ones the request set and `DEFAULT_CONFIG` (5)
for the rest, as a describe right after the create would report them.

A replication factor of 1 or more, or `-1`, is accepted. Iggy replicates the
topic itself, whatever the client asks for, and the gateway advertises one
broker, so the result reports a factor of 1, as Metadata does. Connect asks for
3 on its internal topics and MirrorMaker 2 for 2; a single-broker Kafka would
refuse both.

## What a describe returns

### Topics

A null or empty `configuration_keys` list returns all 33 configurations, sorted
by name. A non-empty list returns the named ones Kafka defines, in name order,
each once. A name Kafka does not define is left out, and the resource's error
code stays `NONE` (0).

A configuration the topic sets has source `DYNAMIC_TOPIC_CONFIG` (1). Any other
has source `DEFAULT_CONFIG` (5) and the value this gateway applies, which for
the honoured keys is Iggy's, not Kafka's:

| Key | Default reported | Kafka's default |
| --- | ---------------- | --------------- |
| `cleanup.policy` | `delete` | `delete` |
| `retention.ms` | `-1`: an Iggy topic never expires by default | `604800000` (7 days) |
| `retention.bytes` | `-1` | `-1` |
| `segment.bytes` | the topic's `segment_size` (1 GiB unless set) | `1073741824` |
| Every echoed key | Kafka's default | — |

When an Iggy client changes `message_expiry` or `max_topic_size` itself, the
describe follows Iggy: a kept `retention.ms` is reported only while the topic
never expires, and a kept `retention.bytes` only while `max_topic_size` still
matches it; otherwise the value is computed from Iggy's
(`max_topic_size / partitions` for `retention.bytes`).

`read_only` is false and `is_sensitive` false for every topic configuration.
From v3, `config_type` is Kafka's type for the key (`LIST`, `LONG`, `INT`,
`STRING`, `BOOLEAN`, `DOUBLE`), and `include_documentation` attaches one
sentence per key that says whether Iggy honours it. `include_synonyms` has no
effect: the synonym list is always empty.

Iggy stores `message_expiry` as `IggyExpiry`, counted in microseconds. A stored
expiry that is not a whole number of milliseconds, which only an Iggy client
can set, fails the resource with `INVALID_CONFIG` (40) and a null
`retention.ms`, source `DYNAMIC_TOPIC_CONFIG` (1); the other entries are still
returned. The gateway does not round.

### The broker

A BROKER resource (type 4) named `1`, the gateway's node id, returns a fixed
set. Each entry is read-only, source `DEFAULT_CONFIG` (5), and describes what
the gateway does:

| Key | Value |
| --- | ----- |
| `auto.create.topics.enable` | whether `IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED` is on |
| `compression.type` | `producer` |
| `default.replication.factor` | `1` |
| `delete.topic.enable` | `true` |
| `log.cleanup.policy` | `delete` |
| `log.retention.bytes` | `-1` |
| `log.retention.ms` | `-1` |
| `log.segment.bytes` | `1073741824` |
| `min.insync.replicas` | `1` |
| `num.partitions` | `1` |
| `unclean.leader.election.enable` | `false` |

`inter.broker.protocol.version` is left out on purpose: kafka-ui picks its alter
API from it, and the gateway serves neither broker alter. An empty name (the
cluster-wide dynamic defaults) answers `NONE` with no entries. Another broker id
answers `INVALID_REQUEST` (42). Other resource types answer `INVALID_REQUEST`
(42), `only topic and broker resources are supported`.

## Authorization

`AlterConfigs` and `IncrementalAlterConfigs` require the `manage_topics` (or
`manage_streams`) Iggy permission on the authenticated principal when SASL is
enabled. A principal without it gets `TOPIC_AUTHORIZATION_FAILED` (29) for every
resource in the request, and `update_topic` is never called. With SASL disabled
there is no principal to check, and the request is not gated here.
`DescribeConfigs` performs no authorization check of its own.

## What an alter stores

`AlterConfigs` (33) replaces the topic's whole configuration, as Kafka's does:
the request's set becomes the topic's set, and every configuration it leaves
out returns to its default. An empty list is a reset to defaults. One bad entry
fails that resource and writes nothing for it; other resources in the request
are still processed. A name repeated within one resource is `INVALID_CONFIG`
(40).

The gateway works out the Iggy topic options the new set needs and writes only
what differs, in one `update_topic`: `message_expiry`, `max_topic_size`,
`cleanup_policy` and `kafka_configs`, each reset to its catalog default when
the set no longer needs it. A set that changes nothing writes nothing and answers
`NONE` (0).

`validate_only` runs the same checks, including the topic lookup, and does not
call `update_topic`.

## What an incremental alter does

`IncrementalAlterConfigs` (44) carries one operation per entry. The operations
of one resource apply in request order to the topic's current set, and the
result is written as `AlterConfigs` writes it.

| Operation | Effect |
| --------- | ------ |
| `SET` (0) | Sets the key, with the checks under "Values". An unknown name is `INVALID_CONFIG` (40) |
| `DELETE` (1) | Removes the key, so it returns to its default. The value sent with it is ignored (`kafka-configs --delete-config` sends an empty string). A delete of a name Kafka does not define changes nothing |
| `APPEND` (2) | List keys only (`cleanup.policy`, the throttled replica lists): adds the items not already there, starting from the default when the topic does not set the key |
| `SUBTRACT` (3) | List keys only: removes the items. Subtracting the last item of `cleanup.policy` is `INVALID_CONFIG` (40) |

`APPEND` or `SUBTRACT` on any other key is `INVALID_CONFIG` (40),
`Can't APPEND to key retention.ms because its type is not LIST.`

Three request faults are answered `INVALID_REQUEST` (42) before the topic lookup
and without any Iggy call: a config name that appears twice in one resource
(`Error due to duplicate config keys`), a null value on anything but a `DELETE`
(`Null value not supported for : <names>`), and an operation byte outside 0-3
(`Unknown operations type <n>`). The first two texts are Kafka's own. The third is
borrowed from Kafka's broker-resource check, because Kafka's topic path has no
well-formed answer for an unknown operation. A topic named by more than one
resource gets `INVALID_REQUEST` (42) on every occurrence, with
`Each resource must appear at most once.`

The order of checks is: authorization, the topic cap, the resource type, the
topic name, the request faults, the topic lookup, then the config faults. A
missing topic is `UNKNOWN_TOPIC_OR_PARTITION` (3) for an entry that would also be
`INVALID_CONFIG`, as with `AlterConfigs`. Kafka orders these checks differently;
see "Limitations".

`validate_only` runs every check, including the topic lookup, and writes nothing.

## Limitations

- `cleanup.policy=compact` keeps every record: Iggy does not compact yet
  (KW-6). The topic grows without bound instead of losing data.
- `retention.ms=0` is refused with `INVALID_CONFIG` (40): Iggy cannot expire a
  sealed segment immediately. Use `1`.
- `cleanup.policy` is reported in Iggy's order: `delete,compact` is reported as
  `compact,delete`. Kafka keeps the order the client sent.
- `retention.bytes` is fixed at the partition count of the last write: after
  `CreatePartitions`, `max_topic_size` covers the old count until the next alter
  of the topic.
- `segment.bytes` is applied at creation only.
- Kafka validates the configuration before it checks that the topic exists, and
  checks duplicate names and null values before the topic name. This gateway
  looks the topic up first, so a missing topic with a bad entry answers
  `UNKNOWN_TOPIC_OR_PARTITION` (3) where Kafka answers `INVALID_CONFIG` (40).
- Synonyms are never returned.

## Other resource errors

| Condition | Code | Notes |
| --------- | ---- | ----- |
| Caller lacks `manage_topics` (SASL on, both alter APIs) | `TOPIC_AUTHORIZATION_FAILED` (29) | Checked before the topic lookup. Every resource in the request |
| Resource type other than topic (alter), or other than topic and broker (describe) | `INVALID_REQUEST` (42) | Other resources in the batch still run |
| Topic name fails the same rules as CreateTopics | `INVALID_TOPIC_EXCEPTION` (17) | The message is the validation reason and does not repeat the topic name |
| Topic does not exist | `UNKNOWN_TOPIC_OR_PARTITION` (3) | Checked before config keys, so a missing topic is not `INVALID_CONFIG` |
| Unknown name, bad value, null value, repeated name | `INVALID_CONFIG` (40) | Kafka's message text |
| Echoed configurations over 255 bytes | `POLICY_VIOLATION` (44) | Nothing is written |
| Iggy refuses the resulting option values | `INVALID_CONFIG` (40) | `the Iggy server refused this topic configuration` |
| Duplicate config name, null value, or unknown operation on `IncrementalAlterConfigs` | `INVALID_REQUEST` (42) | Message text under "What an incremental alter does". Checked before the topic lookup, so a missing topic gets this code too |
| More than 100 distinct topic names | `POLICY_VIOLATION` (44) | Every resource in the request, checked before the bridge-availability check |
| A topic name repeated across resources | `INVALID_REQUEST` (42) | Every occurrence of the duplicate, not just the second one. Same choice CreateTopics makes for a repeated topic name. `IncrementalAlterConfigs` adds Kafka's message, `Each resource must appear at most once.` |

Unknown tagged fields at the request, resource, or config-entry level are
ignored, not rejected: Kafka's flexible versions define them as a forward-compatible
extension point a server that does not recognize them must skip, not refuse.
