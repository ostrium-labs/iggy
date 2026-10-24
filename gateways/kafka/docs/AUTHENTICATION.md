# Kafka authentication and Iggy identity

Status: implemented. Answers [#3549](https://github.com/apache/iggy/issues/3549) (PLAIN),
[#4512](https://github.com/apache/iggy/issues/4512) (SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER,
with KIP-368 re-authentication; built in the Ostrium Labs fork as KW-9), and
[#4510](https://github.com/apache/iggy/issues/4510) for the transport: a `SASL_SSL` listener
encrypts the Kafka hop and `IGGY_KAFKA_IGGY_TLS_ENABLED` the Iggy hop, so PLAIN leaves the trusted
network only inside TLS (see [Transport security](#transport-security)). A client certificate on an
`SSL` listener names a principal too (see [Client certificates](#client-certificates)). The
mechanism choice below is forced by how Iggy stores credentials, not preferred.

## Decision

Six parts.

**Mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER**, chosen per deployment with
`IGGY_KAFKA_SASL_MECHANISMS` (default `PLAIN`, the only mechanism before KW-9). PLAIN is verified by
Iggy itself; SCRAM against salted keys the gateway keeps in Iggy ([SCRAM](#scram)); OAUTHBEARER
against the identity provider's published keys ([OAUTHBEARER](#oauthbearer)). GSSAPI stays out
unless someone asks for it.

**Every mechanism ends at an Iggy user.** Whatever proved the identity, the principal is an Iggy
username, and the connection is admitted only if that user exists and is active. For SCRAM and
OAUTHBEARER the gateway looks the user up through the bridge, as it does for a client certificate, so
the bridge's Iggy user needs `read_users`. The Iggy server stays the one place accounts and their
permissions are defined.

**PLAIN credentials pass straight through.** The username and password a Kafka client sends in its PLAIN payload
are an Iggy username and password. The gateway forwards them to a login and keeps no credential store and
no mapping table of its own. An Iggy account is created for each Kafka principal, and the Iggy server stays
the only place a credential is defined or verified.

**Identity: one Iggy user per Kafka principal.** Not one shared account for everyone. Credentials are
verified on every connection, against Iggy, and that verification is deliberately not cached. See
[Verification is per connection](#verification-is-per-connection-and-cannot-be-cached).

**Authorization is meant to stay in Iggy.** The gateway will not implement its own ACL model. The intent is
to carry the authenticated Iggy identity onto the data plane and let the server's existing permission checks
decide what it can do. That is later work: today the verified client is shut down as soon as the credential
clears, and nothing carries the principal past authentication.

**A client certificate is an identity too, with the same reach.** On an `SSL` listener that verifies client
certificates, the subject maps to an Iggy username through Kafka's `ssl.principal.mapping.rules` and the
gateway confirms the user exists. Nothing is logged into on the client's behalf, since Iggy's own transports
take no certificate ([#3930](https://github.com/apache/iggy/issues/3930)); the principal gates admission and
answers `DescribeAcls`, exactly as a SASL one does, and acting as it on the data plane is the same later work.

## SCRAM

### Why the gateway keeps the credentials

Iggy stores exactly one credential per user, an Argon2id PHC string (`core/metadata/src/stm/user.rs:57`,
hashed at `core/server_common/src/crypto.rs:30`, verified at `core/server/src/dispatch/session_ops.rs:126`).

SCRAM-SHA-256 and SCRAM-SHA-512 require the server to hold `StoredKey` and `ServerKey`, both derived from
`PBKDF2(password, salt, iterations)`, and to send the salt and iteration count to the client in the
server-first message. None of that is derivable from an Argon2 hash, and the stored hash is never exposed
through any API. There is no second credential column and no per-mechanism credential table anywhere in
the tree.

Supporting SCRAM inside Iggy would mean a new replicated credential table plus derivation at
user-creation time, a change to Iggy's own wire format and state machine. #4512's open question was
whether to make that change or keep the credentials in the gateway; KW-9 keeps them in the gateway,
because that needs no server change and the store still lives in Iggy, replicated like any topic.

### The credential log

SCRAM credentials are a log in the one-partition topic `scram` of the gateway's meta stream
(`<default stream>.meta`, beside `cluster_id`), created on the first write with durability
`persisted`, no expiry and no size cap. Each message is one change, a JSON record:

| Field | Upsert | Delete |
| --- | --- | --- |
| `op` | `upsert` | `delete` |
| `v` | `1` | `1` |
| `user`, `mechanism` | the credential's key | the credential's key |
| `salt`, `iterations`, `stored_key`, `server_key` | base64, as Kafka's `UserScramCredentialRecord` | absent |

The current set is the log replayed in order, last write per `(user, mechanism)` wins. Every SCRAM
login catches up on the log first, on a client of its own so a login never queues behind Metadata,
which is how a credential set through one gateway reaches every gateway on the same Iggy and default
stream. Nothing compacts the log until KW-6 brings compaction; credentials change rarely.

What the log holds is not the password and not enough to log in with: `StoredKey` verifies a proof,
`ServerKey` signs the server's answer. Together with a captured exchange they still help an attacker,
so the meta stream's `poll_messages` should be granted to the bridge's user only.

### Administration

`AlterUserScramCredentials` (51) and `DescribeUserScramCredentials` (50), v0, are served where SCRAM
is enabled, the bridge keeps the credentials, and the listener names a principal. They are what
`kafka-configs --alter --add-config 'SCRAM-SHA-512=[password=...]' --entity-type users`, the
Strimzi User Operator and Terraform use. The client salts and hashes the password itself
(`Hi(password, salt, i)`), so only the salted password travels, and the gateway turns it into the
stored keys at once and keeps nothing else. The per-user errors are Kafka's own:
`DUPLICATE_RESOURCE` (92) for a user altered twice in one request, `RESOURCE_NOT_FOUND` (91) for
removing a credential that does not exist or describing a user with none, `UNACCEPTABLE_CREDENTIAL`
(93) for fewer than 4096 or more than 16384 iterations or an empty name, and
`UNSUPPORTED_SASL_MECHANISM` (33) for an unknown mechanism. Altering needs Iggy's `manage_users`
(Kafka's `ALTER` on the cluster) and describing `read_users` (`DESCRIBE`); without them every user is
answered `CLUSTER_AUTHORIZATION_FAILED` (31). An Iggy user is not required to exist when its SCRAM
credential is set, as in Kafka; logging in with it is refused until it does.

### The exchange

RFC 5802 and RFC 7677, as Kafka's `ScramSaslServer` runs them, over two `SaslAuthenticate` rounds.
The cryptography is RustCrypto's (`hmac`, `sha2`, `pbkdf2`), the proof comparison is constant time
(`subtle`), and nonces come from the operating system (`getrandom`). Three differences from Kafka,
each stricter and none visible to a conformant client:

- The channel binding `c=` of the client-final message must be the base64 of the client's own GS2
  header (`biws` for `n,,`), as the RFC requires. Kafka accepts any value.
- A mandatory extension (`m=`) and Kafka's delegation-token extension (`tokenauth=true`) are refused:
  there are no delegation tokens here.
- **An unknown user is not refused at the first message.** Kafka answers it with 58 at once, which
  tells anyone who asks which accounts exist. The gateway answers with a salt derived from the name and
  a per-process secret (stable across attempts) and 4096 iterations, and fails the proof like a wrong
  password (RFC 5802, section 9). A user with a credential whose Iggy account is gone fails the same
  way, after the proof.

## OAUTHBEARER

For Loams, whose identity is Zitadel (Loams design 76): a Loams credential is an OIDC access token,
which OAUTHBEARER carries as it is. Configured with `IGGY_KAFKA_OAUTH_*`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `IGGY_KAFKA_OAUTH_ISSUER` | required | The `iss` every token must carry |
| `IGGY_KAFKA_OAUTH_AUDIENCE` | required | Accepted `aud` values, comma-separated; a token must name one |
| `IGGY_KAFKA_OAUTH_JWKS_URL` | one of the two | The identity provider's `jwks_uri` (Zitadel: `https://<domain>/oauth/v2/keys`). `https`, or `http` on a loopback host |
| `IGGY_KAFKA_OAUTH_JWKS_FILE` | one of the two | A JWKS document on disk, re-read on every refresh |
| `IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP` | `false` | Allow a plain `http` JWKS URL off loopback, for an identity provider on a network you trust |
| `IGGY_KAFKA_OAUTH_JWKS_CA_FILE` | platform roots | PEM CA bundle to trust for the JWKS endpoint instead |
| `IGGY_KAFKA_OAUTH_JWKS_REFRESH_SECS` | `3600` | Kafka's `sasl.oauthbearer.jwks.endpoint.refresh.ms`, in seconds; at least 10 |
| `IGGY_KAFKA_OAUTH_PRINCIPAL_CLAIM` | `sub` | The claim whose value is the Iggy username (Kafka's `sasl.oauthbearer.sub.claim.name`); Zitadel's `sub` is a numeric id, so `preferred_username` is the usual choice |
| `IGGY_KAFKA_OAUTH_ALGORITHMS` | every asymmetric one | Accepted `alg` values; `HS*` is refused |
| `IGGY_KAFKA_OAUTH_CLOCK_SKEW_SECS` | `30` | Kafka's `sasl.oauthbearer.clock.skew.seconds` |

A token is accepted when, in order: its header names an allowed algorithm; a key from the JWKS
verifies its signature, matched on `kid`, on key type and curve against the algorithm, and on the
key's own `alg` when it states one; `iss`, `aud` and `exp` are present and right, and `nbf` when
present, each within the clock skew; and the principal claim is a non-empty string naming an active
Iggy user. JWT handling is the `jsonwebtoken` crate on its RustCrypto backend, as the Iggy server's
own HTTP API uses. The checks that keep the known JWT attacks out:

- **Algorithm confusion.** The algorithm is never trusted on its own: only allowed asymmetric ones,
  and `HS256`, `HS384` and `HS512` cannot be allowed at all, so a published RSA key can never be used
  as an HMAC secret. `none` is not an algorithm the decoder knows. Symmetric (`oct`) keys and keys
  published for encryption are never loaded from the JWKS.
- **Embedded keys.** A key in the token's own header (`jwk`, `jku`, `x5u`) is never consulted.
- **Key rotation.** Keys are fetched at startup, refreshed on the interval, and fetched again, no
  more than once every 10 seconds, when a token names a `kid` the cache lacks. A failed refresh keeps
  the keys held. The fetch follows no redirects and reads at most 1 MiB.

A refused token is answered as Kafka's `OAuthBearerSaslServer` answers it: `SaslAuthenticate` with no
error and RFC 7628's challenge `{"status":"invalid_token"}`, then, after the client's `^A`
acknowledgement, 58 with the challenge as the message. The reason (expired, wrong issuer, bad
signature) goes to the gateway's log at `debug`, never to the client, and the token itself is never
logged. No keys loaded yet (an identity provider that is down at startup) closes the connection
instead, which the client retries, the way an unreachable Iggy does for PLAIN. An authorization id in
the GS2 header that differs from the token's principal is refused, as in Kafka.

## Re-authentication (KIP-368)

`SaslAuthenticate` returns `session_lifetime_ms`, as Kafka's `SaslServerAuthenticator` computes it: an
OAUTHBEARER token's remaining life, capped by `IGGY_KAFKA_CONNECTIONS_MAX_REAUTH_MS` (Kafka's
`connections.max.reauth.ms`) when that is set; that cap alone for PLAIN and SCRAM; 0, meaning never,
when neither applies. A client re-authenticates before the lifetime passes by sending a new
`SaslHandshake` on the same connection, and once it has passed nothing else is served on it: the next
request closes the connection. As in Kafka, a re-authentication must use the session's mechanism (the
handshake is answered, the `SaslAuthenticate` after it refused with 58) and must name the same
principal ("Cannot change principals during re-authentication", 58), and a session with no lifetime
answers a second handshake with `ILLEGAL_SASL_STATE` and stays open.

The concern this section used to record no longer applies. Iggy's own re-login on a bound connection is
unsafe (`core/server/src/dispatch/session_ops.rs:232`), but no Kafka connection holds an Iggy session
in its principal's name: every verification, the first and each re-authentication, runs on a fresh
verification client or against the gateway's own state, so there is nothing to re-bind. A successful
re-authentication also refreshes the permission snapshot the connection holds.

## Why not one Iggy session per Kafka connection

The obvious reading of "one-to-one user mapping" is one authenticated Iggy connection behind every Kafka
connection. The cost rules it out.

| Per login | Cost |
| ----------- | ------ |
| Argon2id verify, run inline on the shard thread | 19 MiB, t=2, p=1 |
| Replicated VSR `Register` | one consensus commit |
| Leader settlement round trip, plus a second login if it redirects | one or two round trips |
| Session slots, cluster-wide across all transports | 8192, oldest committed evicted past it |

The Argon2 verify runs on the shard's compio runtime, which has no blocking pool. A burst of Kafka
connections therefore stalls that shard's entire request loop, including produce and fetch traffic that has
nothing to do with authentication. The 8192 ceiling (`core/consensus/src/impls.rs:178`) is cluster-wide, so
connection churn on the Kafka side turns directly into session-table pressure and eviction on the Iggy side.

## Why not a shared service account

The other option in the issue is authenticating every Kafka client against one `kafka-bridge` account. It
removes the cost above and creates a worse problem: every Kafka client would reach Iggy as the same
privileged principal, so Iggy's own permission model could no longer distinguish them. The gateway would
have to carry an ACL model of its own, and become the only thing standing between any authenticated Kafka
client and every stream the bridge account can reach.

That is a larger security surface than the one it removes.

## Verification is per connection, and cannot be cached

A Kafka principal is the SASL `authcid`. Every SASL exchange verifies the presented credentials by
logging into Iggy with them, and that verification happens on every connection.

The gateway reaches Iggy over the binary TCP transport only. Nothing uses the HTTP API, so there is no way
to check a password without establishing a full session. First contact for a connection therefore costs one
Argon2 verify, run inline on the shard thread, and one replicated `Register`.

An earlier revision of this document proposed caching the authenticated client per principal and attaching
later connections to it. That is wrong, and implementing it surfaced why. A cache keyed on the username
alone lets a second connection present *any* password for a principal already seen, which is an
authentication bypass rather than an optimisation. Keying it on the credential instead means holding
something password-equivalent in gateway memory for every principal seen. Neither is acceptable, so
verification stays on the connection path.

Two consequences worth naming rather than discovering:

- **Login rate tracks connection rate.** A client that reconnects frequently drives an Argon2 verify per
  reconnect, on a shard runtime with no blocking pool, where it competes with ordinary produce and fetch
  traffic on that shard. Measured on debug builds, a single login takes about 10 ms and 32 concurrent
  logins take about 75 ms each, with throughput flattening near 410 per second. The shape is the point,
  latency climbing while throughput plateaus, not the absolute numbers. `MANUAL_TESTING.md` has the table.
- **Nothing holds the verified session.** The connection that proved a credential drops it at once.
  Produce and Fetch act as the bridge's Iggy user for every client. A data client per principal needs
  pooling, a separate question from how credentials are verified. Pooling is gateway-side code over the
  public SDK and needs no SDK change.

If either consequence starts to hurt, the lever is below, and it is a smaller change than a credential
cache because it moves cost off the server without weakening what the gateway checks.

### Personal access tokens, if login cost becomes a problem

Not part of the baseline. The gateway logs in with the username and password the Kafka client presented,
and that is the whole credential path. This section records why a token is the lever to reach for if the
login rate turns out to hurt, and what it costs to pull.

Argon2 is the expensive half of a login and the only half that runs inline on a shard thread with no
blocking pool. A token login replaces it with a hash lookup. The replicated `Register` is unchanged either
way, so tokens buy server CPU, not consensus, and only under churn: a cached client that stays connected
pays neither.

Minting is self-scoped, so a token can only be created for a principal while authenticated as that
principal. First contact therefore always pays the password path regardless, and a token would pay off only
afterwards, on internal SDK reconnects, pool growth and cache repopulation. Credentials also cannot be
swapped on a live client, so a token would apply to the next client built for that principal, never the one
that minted it.

What makes it a real cost rather than a free optimisation: the raw token is returned exactly once and is
never retrievable, so a restarted gateway cannot recover what it minted and must delete before creating
again, handling `PersonalAccessTokenAlreadyExists` (51). Names have to be instance-scoped or two gateways
fight over one token. A user holds at most `max_tokens_per_user` tokens, 100 by default
(`core/server/config.toml:305`), and tokens leaked by instances that never cleaned up count against that.
The gateway would also be holding bearer credentials carrying the full rights of their users with no scope
narrowing, in memory, for every principal it has seen.

Measure the login rate before taking any of that on.

## Handshake

Per connection, with SASL enabled. Only ApiVersions and SaslHandshake are legal before authentication.

| State | Input | Action |
| ------- | ------- | -------- |
| `AwaitHandshake` | ApiVersions (18) | answer, stay |
| | SaslHandshake (17) v1, enabled mechanism | answer `error_code` 0, go to `AwaitToken` |
| | SaslHandshake (17) v1, unknown mechanism | answer 33 with the mechanism list, then close |
| | SaslHandshake (17) v0 | answer 35 with the mechanism list, then close |
| | anything else | answer 34 shaped for that API, then close |
| `AwaitToken` | SaslAuthenticate (36), an exchange step (SCRAM's server-first, OAUTHBEARER's error challenge) | answer 0 with the challenge in `auth_bytes`, stay |
| | SaslAuthenticate (36), credentials valid | answer 0 with the mechanism's final message (empty for PLAIN and OAUTHBEARER) and `session_lifetime_ms`, go to `Authenticated` |
| | SaslAuthenticate (36), credentials rejected | answer 58 with a generic message, then close |
| | anything else | answer 34, then close |
| `Authenticated` | any supported API, including ApiVersions again | serve, unless the session's lifetime has passed: then close |
| | SaslHandshake (17) v1, session has a lifetime | re-authentication (KIP-368): answer 0, go to `AwaitToken` keeping the principal |
| `AwaitHandshake` | ApiVersions past the allowance | answer 34, then close (two are allowed: the one a real broker allows, plus the KIP-511 downgrade retry) |
| `AwaitToken` | SaslAuthenticate above the advertised ceiling | close, no schema exists at that version |
| any | Metadata or Produce while unauthenticated | close with no body (no error field, and acks=0 forbids one) |
| | SaslHandshake (session without a lifetime) or SaslAuthenticate | answer 34, keep the connection |

Notes that decide the implementation.

- **Handshake v1 only.** The handshake version selects the token framing (KIP-152). Refusing v0 means the
  legacy bare-token mode is never entered, so the frame reader stays header-parsing-only. Only pre-1.0
  Kafka clients are excluded.
- **`session_lifetime_ms` follows the credential.** See [Re-authentication](#re-authentication-kip-368).
- **ApiVersions is answered twice.** The Java client sends it once before the handshake and again after
  authenticating, so it must stay legal in both states.
- **The pre-authentication allowance counts answers, not successes.** A refusal a client may retry at a
  lower version is one it may also repeat, and every frame resets the pre-authentication deadline, so
  spending the allowance only on a usable answer leaves the connection unbounded.
- **Produce with `acks=0` stays silent.** Answering an unauthenticated fire-and-forget produce desyncs the
  client's correlation stream, so that case closes without writing. The existing rationale in
  `protocol/api.rs` applies unchanged.
- **Pre-authentication deadline.** An unauthenticated connection holds a `max_connections` permit, so it
  gets `IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS` between frames rather than the ten-minute idle timeout.

## Error mapping

At authentication time a rejected credential becomes `SASL_AUTHENTICATION_FAILED` (58) with a generic
message. An Iggy that cannot be reached, an overloaded gateway, or a peer still inside the delay a previous
rejection earned, closes the connection without a body instead: Kafka clients treat 58 as fatal and raise it to the application, so borrowing it for a transient
condition turns a blip into a permanent failure for credentials that were always correct. A close reads as
a transport failure, which is retriable, and still says nothing about whether the account exists. Iggy's login path already runs a dummy hash for unknown users to avoid a user-enumeration oracle,
so the gateway must not reintroduce one by distinguishing unknown user from wrong password in the message
or by returning early.

### Planned: authorization errors

Not implemented. Today `bridge/error.rs` picks the Kafka code from the Iggy error, and neither 30 nor 31
is produced anywhere.

After authentication, Iggy reports exactly one permission-denied code, `IggyError::Unauthorized` (41,
`core/common/src/error/iggy_error.rs:97`), alongside `Unauthenticated` (40). Kafka distinguishes
`TOPIC_AUTHORIZATION_FAILED` (29), `GROUP_AUTHORIZATION_FAILED` (30) and `CLUSTER_AUTHORIZATION_FAILED`
(31). Once handlers act as the authenticated principal, the gateway will have to pick the Kafka code from
the operation it was performing, not from the Iggy error, because the Iggy error cannot tell them apart.

Iggy's data-plane permission checks read the local shard's view, so a permission revocation is visible on
the control plane immediately and on the data plane only after that shard applies it. Say so in the README
rather than leaving an operator to discover it.

## Transport security

SASL/PLAIN puts the password in the clear on the wire, and so does Iggy's own login. Both hops need TLS,
and the two are configured independently because they protect different hops.

**The Kafka hop** is a listener's security protocol. The gateway binds one listener per protocol, the way
Kafka's `listeners` does (`IGGY_KAFKA_LISTENERS`), and `SASL_SSL` is the one for PLAIN outside a trusted
network. TLS is rustls, one certificate for every `SSL` and `SASL_SSL` listener
(`IGGY_KAFKA_TLS_CERT_FILE`, `IGGY_KAFKA_TLS_KEY_FILE`), re-read on an interval so a rotation goes live
without a restart. Each listener advertises its own address, so a client on the TLS port is never
pointed at the plaintext one. A `SASL_PLAINTEXT` listener still exists for trusted networks, and the
gateway warns at startup that it carries the password in the clear.

**The Iggy hop** is `IGGY_KAFKA_IGGY_TLS_ENABLED` and its companions. They cover every connection the
gateway makes to Iggy: the credential check's and the bridge's, which carries `IGGY_KAFKA_IGGY_PASSWORD`.
They are what make SASL usable against a TLS-only Iggy server at all, where every verification would
otherwise fail as unreachable, and are refused while nothing dials Iggy.

Two limits worth recording. Iggy's own transports take no client certificate
([#3930](https://github.com/apache/iggy/issues/3930)), so a certificate identity on the Kafka side is
verified by the gateway and looked up through the bridge's session rather than presented to Iggy. And an
Iggy password is capped at 100 bytes with a `u8` length prefix on the wire
(`core/common/src/http/users/defaults.rs:20`), so a Kafka client with a longer PLAIN secret is rejected.

## Client certificates

`IGGY_KAFKA_TLS_CLIENT_AUTH` is Kafka's `ssl.client.auth`: `none`, `requested` or `required`. It applies
to `SSL` listeners, which verify the client certificate against `IGGY_KAFKA_TLS_CLIENT_CA_FILE` during the
handshake, and is refused when no `SSL` listener exists.

A `SASL_SSL` listener never asks for a client certificate. Kafka applies `ssl.client.auth` to a
`SASL_SSL` listener only when that listener overrides it, and leaves it at `none` otherwise; SASL names
the principal there. A deployment that wants certificate identities uses `SSL`.

On an `SSL` listener the certificate names the principal, in three steps the connection takes before its
first frame:

1. **Render the subject** the way Java's `X500Principal.getName()` does, RFC 2253 with the most specific
   RDN first and no spaces: `CN=alice,OU=eng,O=Example`. Operators write mapping rules against Java's
   rendering, so the gateway follows JDK 11 to 21's `AVA.toRFC2253String`: its keyword set, its escaping
   (NUL as `\00`), Latin-1 for `PrintableString`, `IA5String` and `GeneralString`, and hex for the string
   types Java does not decode, `UniversalString` among them. Malformed UTF-8 or BMP values are the one case
   not checked against Java.
2. **Map it** through `IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES`, Kafka's `ssl.principal.mapping.rules`
   syntax: `RULE:pattern/replacement/[LU]` applies when the pattern matches the whole subject, `DEFAULT`
   keeps it, the first match wins, and a subject no rule matches closes the connection with the subject in
   the log at `warn`. The default is `DEFAULT`. Iggy only requires a username of 3 to 50 characters, but a
   raw subject is usually longer than that and is rarely the name the operator created, so a deployment
   sets at least one rule. A replacement with a bare `$`, or a `${name}` with no such named group, is
   refused at startup, where Kafka would fail the first handshake it applies to. One that names a group
   the pattern lacks is refused too, which is stricter than Kafka: a broker writes it literally, so
   `RULE:^CN=(.*)$/$2/` maps to `$2`. The reverse case, `/$1\$2/`, makes Kafka throw while the gateway
   maps `alice` to `alice$2`, which is harmless because that Iggy user must still exist. The replacement is applied as Java's `replaceAll`, to every match in the subject, so
   `RULE:(.*)/$1@x/` maps `alice` to `alice@x@x` as it does on a broker. Patterns are the subset of
   Java's regex syntax whose meaning the gateway can reproduce: `\d \s \w` and their negations match
   ASCII only, and `.` outside `(?s)` stops at `\r`, U+0085, U+2028 and U+2029 as well as `\n`, both as
   in Java. Rust would read them over Unicode, so a CA-signed `CN=\u212Aafka` (Kelvin sign) would match
   `\w+` and lowercase to `kafka`. Case-insensitive matching (`(?i)`), the `u` and `U` flags, `\p{..}`
   and `\P{..}`, POSIX classes such as `[[:alpha:]]`, word boundaries (`\b`, `\B`), the `m`, `x` and
   `R` flags, `\v`, a quantifier directly after another (`.*+`, possessive in Java) and the `--` and
   `~~` class operations are refused at startup with the construct named: each has no faithful
   translation. Non-ASCII names still map, so
   `CN=Zoë` under `RULE:^CN=([^,]+).*$/$1/` maps to `Zoë` as on a broker.
3. **Look the name up** through the bridge's own session. The user must exist and be active, or the
   connection is closed after the handshake with the name in the log at `warn`: a certificate the CA signed
   for an account that does not exist is a provisioning mistake, not something the client can correct. The
   bridge is therefore required for certificate identities, and its Iggy user needs `read_users`. A failed
   lookup, including one refused for want of that permission, closes the connection too and never admits
   the name unchecked.

With `requested`, a client that presents no certificate is Kafka's `ANONYMOUS` principal, which holds no
permissions: `DescribeAcls` describes it as holding nothing, and `DeleteTopics`, `AlterConfigs`,
`DeleteGroups`, `OffsetDelete` and the auto topic creation a `Metadata` request asks for, which need a global
`manage_topics` or `manage_streams`, are refused. Everything else is served as the bridge's user. With
`required`, rustls fails the handshake instead.

The same holds on every listener that names nobody once any listener names principals: a `PLAINTEXT`
listener beside a `SASL_SSL` or mTLS one serves `ANONYMOUS`, so it cannot do what an authenticated user
without `manage_topics` cannot. Only a gateway whose listeners all name nobody leaves those requests
ungated.

The principal a certificate yields is the same `AuthenticatedPrincipal` a SASL login yields, with its
permissions read at admission, so everything written about the SASL identity above holds: it is a snapshot,
it is not cached across connections, and nothing acts as it on the data plane yet
([#4518](https://github.com/apache/iggy/issues/4518)).

## Out of scope

- GSSAPI, only on request.
- Delegation tokens (Kafka's `tokenauth` SCRAM extension).
- Presenting a client certificate to Iggy's own transports, which take none
  ([#3930](https://github.com/apache/iggy/issues/3930)); the gateway verifies it and looks the user up
  instead.
- Authorization. No handler asks Iggy about permissions yet, since the verified identity is not carried
  onto the data plane; SASL is an admission gate only. The `DescribeAcls` and `CreateAcls` API keys also
  stay unimplemented.

## Open questions

1. **Who creates the Iggy accounts?** Each Kafka principal needs an Iggy user, since its credentials are
   the ones the Kafka client sends. An operator can create them with the existing user API
   (`core/common/src/traits/user_client.rs:38`), or the gateway can provision on first SASL login, which
   needs the `manage_users` permission and means the gateway holds a credential that can create users.
   Auto-provisioning also cannot work here: the gateway only ever sees a password it cannot validate
   against anything until a user already exists, so provisioning would accept any credential as a new
   account. Default: an operator creates the accounts, and the gateway only consumes them.
2. **What happens when SASL is enabled on a gateway that already serves unauthenticated clients?** Enabling
   it breaks every existing client at once, since the version advertisement changes. Default: SASL is
   off unless configured, and the two SASL keys are not advertised when it is off.

## References

- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md)
- Credential storage: `core/metadata/src/stm/user.rs`, `core/server_common/src/crypto.rs`
- Login and session binding: `core/server/src/dispatch/session_ops.rs`, `core/server/src/session_manager.rs`
- Permissions: `core/common/src/types/permissions/`, `core/metadata/src/permissioner/`
- Listener and dispatch: `gateways/kafka/src/server.rs`, `gateways/kafka/src/protocol/api.rs`
