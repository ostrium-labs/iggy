# Kafka authorization, ACLs and Iggy permissions

Status: built (KW-10, [#4518](https://github.com/apache/iggy/issues/4518)). Answers the ACL half of
[#3549](https://github.com/apache/iggy/issues/3549). Depends on SASL or client certificates, because
everything here is about the principal a connection authenticated as.

## Decision

**The gateway enforces each principal's own Iggy permissions, with the bridge's Iggy user as the
ceiling** (owner ruling, 2026-10-25). SCRAM, OAUTHBEARER, PLAIN and certificate principals map to
Iggy users, and every topic a request names is checked against that user's grants, global, per
stream and per topic, with exactly Iggy's own rules (`core/metadata/src/permissioner/permissioner_rules/`),
before anything runs. What is allowed still runs as the bridge's Iggy user, so Iggy decides it
against that user's grants too: a principal can never do more than the bridge can.

**Consumer groups are scoped per principal.** Kafka groups have no Iggy resource. Group access
follows topic read access, and every principal gets a namespace of its own: the group a client
names is kept as `<length>:<principal>/<group>`, so a principal sees, joins, commits to and deletes
only its own groups, and two tenants that both name `orders-app` use two groups.

**Transactional ids are scoped per principal too** (KW-TXP, coordinator ruling of 2026-10-27 under
the KW-10 ruling). They have no Iggy resource either. Access follows topic write access, and the
transaction coordinator keeps the id a client names as `<length>:<principal>/<id>`, so a principal
can only initialize, fence, end, describe and list its own transactions, and two tenants that both
use `orders-tx` run two transactions. In Kafka terms each principal holds `WRITE` and `DESCRIBE`
on its own prefix of `TRANSACTIONAL_ID` and nothing else; a principal that may write no topic is
refused every transaction API with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53), as Kafka refuses
a principal without those ACLs.

**Enforcement needs a principal.** It applies on a gateway where a listener names principals (a
SASL listener, or `SSL` with client certificates). There every connection has one: a connection
that proved no identity, also on a `PLAINTEXT` listener beside such a listener, is `ANONYMOUS`
and holds nothing, so it is refused everything, as Kafka's authorizer refuses `User:ANONYMOUS` with
no ACLs. A gateway with no such listener enforces nothing, as before.

**The ACL view is read only for now.** `DescribeAcls` (29) renders the principal's grants. Changing
grants with `CreateAcls` (30) and `DeleteAcls` (31) is the next KW-10 step and not advertised yet;
grants are changed in Iggy (its CLI, HTTP API or SDK).

**Permissions are read once, at authentication.** The login that verifies the credentials also
reads the principal's own record (or the bridge looks it up, for SCRAM, OAUTHBEARER and
certificates), and the bridge names the streams and topics its per-stream grants refer to. The
connection keeps the result. See [Staleness](#staleness).

## Enforcement

| Request | Kafka operation | Iggy rule, on the topic the mapping gives | Refusal |
| ------- | --------------- | ----------------------------------------- | ------- |
| Produce | `WRITE` on the topic | `append_messages` | 29 on each partition of the topic; others are written |
| Fetch | `READ` on the topic | `poll_messages` | 29 on each partition; others are read |
| `ListOffsets` | `DESCRIBE` on the topic | `get_topic`, `poll_messages` or `append_messages` | 29 on each partition |
| Metadata | `DESCRIBE` on the topic | as `ListOffsets` | an all-topics answer leaves the topic out; a named or identified one answers 29, existing or not |
| Metadata auto-creation, `CreateTopics` | `CREATE` on the topic | `create_topic` on the mapped stream | 29 for the topic; others are created |
| `DeleteTopics`, `DeleteRecords`, `CreatePartitions`, `AlterConfigs`, `IncrementalAlterConfigs` | `DELETE` / `ALTER` / `ALTER_CONFIGS` on the topic | `manage_topic` | 29 for the topic or resource; others go ahead |
| `DescribeConfigs` | `DESCRIBE_CONFIGS` on the topic, or the cluster for a broker | as `ListOffsets`; a broker needs `read_servers` or `manage_servers` | 29 for the topic, 31 for the broker |
| `OffsetCommit`, `OffsetFetch`, `OffsetDelete` | `READ` on the topic | `poll_messages`, Iggy's consumer offset rule | 29 on each partition; an every-topic `OffsetFetch` leaves the topic out |
| The group APIs | `READ` / `DESCRIBE` / `DELETE` on the group | any topic read grant, anywhere | 30 (an empty `ListGroups`), in the principal's own namespace |
| `InitProducerId` | `IDEMPOTENT_WRITE` on the cluster | `append_messages` on some topic | 31 |
| `InitProducerId` with a transactional id, transactional Produce, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn` | `WRITE` on the transactional id, checked first | `append_messages` on some topic, in the principal's own namespace | 53 (every partition where the answer has partitions); then the topic and group checks above |
| `FindCoordinator` (transaction), `DescribeTransactions`, `ListTransactions` | `DESCRIBE` on the transactional id | as above | 53 per key or id; `ListTransactions` lists only the principal's own ids, none without the grant. `DescribeTransactions` leaves out topics the principal may not describe |

Kafka implies `DESCRIBE` from every other topic operation, so a principal that may read or write a
topic may also look it up. A principal whose permissions could not be read is refused everything:
nothing is authorized off a value nothing read.

## The mapping

Iggy grants permissions to a user. Kafka describes them as bindings of
`(resource, principal, host, operation, permission type)`. Three of those five are constant here.

| Kafka field | Value | Why |
| ------------- | ------- | ----- |
| principal | `User:<username>` | Kafka's own convention for a SASL principal |
| host | `*` | Iggy has no host-scoped permissions |
| permission type | `ALLOW` | Iggy has no deny rules, only grants |

The rest come from Iggy's global permissions, **after applying Iggy's own inheritance**. Its
enforcement is hierarchical, not flag for flag: polling is granted by any of the four read or manage
flags on topics or streams, appending by either manage flag, and reading server state by either
server flag. A literal copy of the flags therefore describes a principal that cannot do things Iggy
will in fact let it do, which is the same falsehood the derived group binding below exists to avoid.
The rules live in `core/metadata/src/permissioner/permissioner_rules/`.

| Iggy global permission | Kafka resource | Operation |
| ------------------------ | ---------------- | ----------- |
| `read_servers` or `manage_servers` | `CLUSTER` `kafka-cluster` | `DESCRIBE` |
| any read or manage flag on topics or streams | `TOPIC` `*` | `DESCRIBE` |
| `manage_topics` or `manage_streams` | `TOPIC` `*` | `CREATE`, `DELETE`, `ALTER` |
| `poll_messages`, or any read or manage flag | `TOPIC` `*` | `READ` |
| `send_messages`, `manage_topics` or `manage_streams` | `TOPIC` `*` | `WRITE` |
| any topic read grant, global, per stream or per topic, polling included | `GROUP` `*` | `READ` |

Three notes on that table.

**Stream permissions fold into topic ones.** Iggy puts topics inside streams, and Kafka has no
resource above a topic. The topic mapping places every Kafka topic inside one Iggy stream, so a
stream-level grant is in practice a grant over the topics a Kafka client can reach. Rendering it as
a topic binding says the true thing in Kafka's vocabulary; inventing a resource type for it would
not.

**A consumer group grant is derived, not stored.** Iggy has no group-level permission, and the
gateway's groups live in the gateway, not in Iggy's consumer groups. Under the KW-10 ruling group
access follows topic read access, so a principal that may read any topic (polling included, which is
also Iggy's consumer offset rule) holds `READ` on its groups, and `*` names the groups of its own
namespace. Before KW-10 the binding followed only the read flags, because Iggy's own group
membership rule (`get_topic`) never consults polling; the gateway does not use Iggy's membership, so
that reason no longer applies.

**`manage_servers` renders nothing of its own.** Iggy reads it in exactly one rule, as an alias for
`read_servers`, so it gates no mutation anywhere. Rendering `CLUSTER ALTER` from it would advertise
an ability with nowhere to be used, and the write ACL APIs that operation authorizes are not
advertised yet. This is the same argument that keeps `manage_users` out of the table.

**`manage_users` and `read_users` are not rendered.** Kafka's `USER` resource covers credential and
delegation-token administration, which this gateway does not expose at all. Mapping onto it would
claim an ability that has nowhere to be used.

Wildcards use `LITERAL` with the name `*`, which is how Kafka itself represents "every resource of
this type". `PREFIXED` is never emitted, because nothing in Iggy's model is prefix-scoped.

## Filters

`DescribeAcls` carries a filter. Each field is matched, with Kafka's `ANY` sentinel matching
everything:

- **resource type** filters the table above. `ANY` returns all of it.
- **resource name** matches the rendered name, so `*` matches the wildcard bindings.
- **principal** matches the authenticated principal. Naming anyone else returns empty: the view is
  the caller's own grants.
- **host** matches `*`.
- **pattern type** selects nothing when it is `PREFIXED` or unknown, because nothing rendered is
  prefix-scoped.
- **operation** and **permission type** filter the rendered rows. Only `ANY` is a wildcard here:
  Kafka's `ALL` is a concrete operation, so a filter naming it selects nothing, because nothing
  rendered is an `ALL` binding.

`MATCH` widens the name comparison rather than narrowing it. A named filter under `MATCH` also
selects a literal pattern named `*`, which is how `kafka-acls.sh --topic orders
--resource-pattern-type match` asks what affects one topic: the bindings on `orders` and the
wildcard ones both affect it.

An empty result is `error_code` 0 with no resources, not an error. Kafka draws a firm line between
"no bindings match" and "the request failed", and an admin tool prints them very differently.

### Per-stream and per-topic grants

Iggy keys them by numeric id. The bridge names them once, when the principal authenticates, and
the topic mapping is read backwards (`TopicMapping::kafka_topic_of`) to find the Kafka topic of an
Iggy topic: the override that targets it, or the same name in the default stream. A grant on one
Iggy topic renders on that Kafka topic, with the same inheritance as the global table (a topic
`manage_topic` renders `DESCRIBE`, `READ`, `WRITE`, `DELETE` and `ALTER`, but not `CREATE`, which
Iggy grants only on a stream). A grant on a whole stream renders on every Kafka topic of the
stream: `*` for the default stream when no override puts a Kafka topic in another stream, one
binding per override for any other stream. A default-stream grant next to overrides elsewhere
renders nothing, since `*` would over-report it; under-reporting is the safe direction for an
authorization view. Enforcement does not depend on this rendering: it checks the grants themselves.

## What is not mapped

**Writes.** `CreateAcls` and `DeleteAcls` would update another user's permissions, which needs
`manage_users` on the Kafka client's own Iggy user and on the bridge's. They are the next KW-10
step: Kafka bindings onto Iggy grants where the mapping is exact, a refusal with a message where it
is not.

## One divergence from a real broker

A real broker gates `DescribeAcls` on `CLUSTER DESCRIBE`. This gateway answers any authenticated
principal, including one the same response describes as lacking that permission.

That is deliberate rather than an oversight. The only thing a principal can learn here is its own
access, which it can discover anyway by attempting an operation, and enforcing the gate would mainly
hide a principal's own permissions from itself. Recorded so the difference is a decision rather than
a surprise.

## Staleness

The permissions a connection reports and enforces are the ones its principal had when it
authenticated. A permission changed afterwards, granted or revoked, is not visible until the client
reconnects. So is a topic recreated under the same name: a per-topic grant is named when the
principal authenticates.

This is deliberate. The alternative is either holding an Iggy session open per connection, which
the authentication design rejects on cost, or keeping the password to log in again, which it
rejects outright. It also matches what Iggy already does on its own data plane, where a revocation
becomes visible only once the owning shard applies it.

Say so in the README rather than leaving an operator to discover that an ACL view can lag.

## Testing

The state machine, the mapping and the rules are pure, so they unit test directly: a permission set
in, a set of bindings or a decision out (`src/authz.rs` mirrors each Iggy rule, inheritance
included), including the empty and wildcard cases. The enforcement and the group namespace run
end to end against a real Iggy server in `authz_real_bridge_tests.rs`.

End to end, `kafka-acls.sh --list` from the Kafka distribution image, against a gateway with SASL
enabled and a real Iggy server behind it. This is the part that matters, because it is the first
authorization behaviour that can be checked against a real client rather than a stub. It runs five
principals, chosen so that each one fails differently if the mapping is wrong:

| Principal | Holds | What it pins down |
| ----------- | ------- | ------------------- |
| root | everything | every row of the table renders |
| `consumer-only` | `read_topics` | the derived group binding appears |
| `poller-only` | `poll_messages` | a topic read that, since KW-10, also gives the group binding: group access follows any topic read access |
| `producer-only` | `send_messages` | a write with no read and no group |
| `no-grants` | nothing | an empty list is a success, not an error |

Each assertion is paired: the binding that must be present and the one that must be absent. A
principal asserted only on absence would also be satisfied by a client container that never
started, which is how an earlier revision of this suite passed over nothing.

`kafka_client_e2e_tests.rs` skips itself when Docker is missing, so a local run without it is green
rather than broken. CI sets `KAFKA_E2E_REQUIRED=1`, which turns that skip into a failure, because
there the missing prerequisite is a broken job rather than a developer without Docker.

## References

- Authentication: [`AUTHENTICATION.md`](AUTHENTICATION.md)
- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md)
- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Iggy permissions: `core/common/src/types/permissions/`
- Self-read exemption: `core/server/src/dispatch/authz.rs`
