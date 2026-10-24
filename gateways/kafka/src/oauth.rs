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

//! SASL/OAUTHBEARER: validating a client's JWT against the identity provider's published keys.
//!
//! The token is checked here, in the gateway, and the principal it names is then looked up as an
//! Iggy user the way a client certificate's is (`auth::PrincipalResolver`). Nothing in Iggy
//! learns about the token. `docs/AUTHENTICATION.md`, "OAUTHBEARER", has the decisions.
//!
//! What a token must satisfy, in order:
//!
//! 1. Its header names an algorithm in the configured allow-list. The list only ever holds
//!    asymmetric algorithms: an `HS*` algorithm is refused at startup, so a JWKS public key can
//!    never be used as an HMAC secret (the algorithm-confusion attack), and `none` is not an
//!    algorithm the decoder knows at all.
//! 2. A key from the JWKS verifies its signature. Keys are matched on `kid`, on key type and
//!    curve against the algorithm, and on the key's own `alg` when it states one. Symmetric
//!    (`oct`) keys and keys published for encryption are never loaded. A key embedded in the
//!    token itself (`jwk`, `jku`, `x5u`) is never consulted.
//! 3. `iss` equals the configured issuer, `aud` contains a configured audience, `exp` is in the
//!    future and `nbf`, when present, in the past, each within the configured clock skew.
//! 4. The principal claim (`sub` by default) is a non-empty string.
//!
//! The keys are fetched at startup, refreshed on an interval, and fetched again (no more often
//! than [`MIN_ON_DEMAND_REFRESH`]) when a token names a `kid` the cached set lacks, which is how
//! an identity provider's key rotation reaches the gateway without a restart.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, Jwk, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::env::parse_bool;

/// Shortest gap between two refreshes a token with an unknown `kid` may trigger. Without it a
/// client could make the gateway fetch the JWKS once per connection by inventing key ids.
pub const MIN_ON_DEMAND_REFRESH: Duration = Duration::from_secs(10);

/// Budget for one JWKS fetch.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest JWKS document read. Real sets are a few kilobytes.
const MAX_JWKS_BYTES: usize = 1024 * 1024;

/// Keys tried for a token whose header names no `kid`. A set with more candidates than this is
/// a configuration that should be naming its keys.
const MAX_KEYS_WITHOUT_KID: usize = 8;

/// Kafka's default for `sasl.oauthbearer.clock.skew.seconds`.
pub const DEFAULT_CLOCK_SKEW: Duration = Duration::from_secs(30);

/// Kafka's default for `sasl.oauthbearer.jwks.endpoint.refresh.ms`.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_hours(1);

/// The algorithms accepted when the operator names none: every asymmetric one the decoder
/// supports. Zitadel signs with `RS256`.
pub const DEFAULT_ALGORITHMS: [Algorithm; 9] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

/// Where the JWKS comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwksSource {
    /// An HTTPS endpoint, as an identity provider publishes it (`jwks_uri`).
    Url(String),
    /// A file holding the JWKS document, re-read on every refresh. For air-gapped deployments and
    /// tests.
    File(PathBuf),
}

/// How OAUTHBEARER tokens are validated (`IGGY_KAFKA_OAUTH_*`).
#[derive(Debug, Clone)]
pub struct OAuthConfig {
    /// The `iss` every token must carry.
    pub issuer: String,
    /// Accepted `aud` values; a token must name at least one.
    pub audiences: Vec<String>,
    /// The claim that names the principal, `sub` by default. Its value is the Iggy username.
    pub principal_claim: String,
    /// Accepted signature algorithms. Never empty, never symmetric.
    pub algorithms: Vec<Algorithm>,
    pub clock_skew: Duration,
    pub jwks: JwksSource,
    pub refresh_interval: Duration,
    /// A PEM CA bundle to trust for the JWKS endpoint instead of the platform's roots.
    pub jwks_ca_file: Option<PathBuf>,
}

impl OAuthConfig {
    /// Every `IGGY_KAFKA_OAUTH_*` variable [`Self::from_env`] reads, for `main`'s unknown-variable
    /// guard.
    pub const KNOWN_ENV_VARS: &'static [&'static str] = &[
        "IGGY_KAFKA_OAUTH_ISSUER",
        "IGGY_KAFKA_OAUTH_AUDIENCE",
        "IGGY_KAFKA_OAUTH_JWKS_URL",
        "IGGY_KAFKA_OAUTH_JWKS_FILE",
        "IGGY_KAFKA_OAUTH_JWKS_CA_FILE",
        "IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP",
        "IGGY_KAFKA_OAUTH_JWKS_REFRESH_SECS",
        "IGGY_KAFKA_OAUTH_PRINCIPAL_CLAIM",
        "IGGY_KAFKA_OAUTH_ALGORITHMS",
        "IGGY_KAFKA_OAUTH_CLOCK_SKEW_SECS",
    ];

    /// Whether any `IGGY_KAFKA_OAUTH_*` variable is set, so `main` can refuse OAuth settings
    /// that no enabled mechanism would read.
    #[must_use]
    pub fn any_env_set() -> bool {
        Self::KNOWN_ENV_VARS
            .iter()
            .any(|key| std::env::var_os(key).is_some())
    }

    /// Reads the configuration from the environment.
    ///
    /// # Errors
    ///
    /// Returns a message naming the variable when a required one is missing or any is invalid.
    pub fn from_env() -> Result<Self, String> {
        let var = |key: &str| {
            std::env::var(key)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let issuer = var("IGGY_KAFKA_OAUTH_ISSUER")
            .ok_or("OAUTHBEARER needs IGGY_KAFKA_OAUTH_ISSUER, the `iss` tokens must carry")?;
        let audiences: Vec<String> = var("IGGY_KAFKA_OAUTH_AUDIENCE")
            .ok_or("OAUTHBEARER needs IGGY_KAFKA_OAUTH_AUDIENCE, the `aud` tokens must name")?
            .split(',')
            .map(str::trim)
            .filter(|audience| !audience.is_empty())
            .map(ToString::to_string)
            .collect();
        if audiences.is_empty() {
            return Err("IGGY_KAFKA_OAUTH_AUDIENCE names no audience".to_string());
        }
        let allow_http = match var("IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP") {
            Some(raw) => parse_bool("IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP", &raw)?,
            None => false,
        };
        let jwks = match (
            var("IGGY_KAFKA_OAUTH_JWKS_URL"),
            var("IGGY_KAFKA_OAUTH_JWKS_FILE"),
        ) {
            (Some(url), None) => {
                check_jwks_url(&url, allow_http)?;
                JwksSource::Url(url)
            }
            (None, Some(path)) => JwksSource::File(PathBuf::from(path)),
            (Some(_), Some(_)) => {
                return Err(
                    "set one of IGGY_KAFKA_OAUTH_JWKS_URL and IGGY_KAFKA_OAUTH_JWKS_FILE, not both"
                        .to_string(),
                );
            }
            (None, None) => {
                return Err("OAUTHBEARER needs IGGY_KAFKA_OAUTH_JWKS_URL (the identity \
                            provider's jwks_uri) or IGGY_KAFKA_OAUTH_JWKS_FILE"
                    .to_string());
            }
        };
        if allow_http && !matches!(jwks, JwksSource::Url(_)) {
            return Err(
                "IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP is set but no IGGY_KAFKA_OAUTH_JWKS_URL is"
                    .to_string(),
            );
        }
        let jwks_ca_file = var("IGGY_KAFKA_OAUTH_JWKS_CA_FILE").map(PathBuf::from);
        if jwks_ca_file.is_some() && !matches!(jwks, JwksSource::Url(_)) {
            return Err(
                "IGGY_KAFKA_OAUTH_JWKS_CA_FILE is set but no IGGY_KAFKA_OAUTH_JWKS_URL is"
                    .to_string(),
            );
        }
        let algorithms = match var("IGGY_KAFKA_OAUTH_ALGORITHMS") {
            Some(raw) => parse_algorithms(&raw)?,
            None => DEFAULT_ALGORITHMS.to_vec(),
        };
        let seconds = |key: &str, default: Duration| -> Result<Duration, String> {
            var(key).map_or(Ok(default), |raw| {
                raw.parse::<u64>()
                    .map(Duration::from_secs)
                    .map_err(|_| format!("invalid {key} `{raw}`: expected whole seconds"))
            })
        };
        let refresh_interval = seconds(
            "IGGY_KAFKA_OAUTH_JWKS_REFRESH_SECS",
            DEFAULT_REFRESH_INTERVAL,
        )?;
        if refresh_interval < MIN_ON_DEMAND_REFRESH {
            return Err(format!(
                "IGGY_KAFKA_OAUTH_JWKS_REFRESH_SECS must be at least {}",
                MIN_ON_DEMAND_REFRESH.as_secs()
            ));
        }
        Ok(Self {
            issuer,
            audiences,
            principal_claim: var("IGGY_KAFKA_OAUTH_PRINCIPAL_CLAIM")
                .unwrap_or_else(|| "sub".to_string()),
            algorithms,
            clock_skew: seconds("IGGY_KAFKA_OAUTH_CLOCK_SKEW_SECS", DEFAULT_CLOCK_SKEW)?,
            jwks,
            refresh_interval,
            jwks_ca_file,
        })
    }
}

/// Refuses a JWKS URL that would let a network attacker substitute the keys.
///
/// `https` always passes. `http` passes on a loopback host, where nothing crosses a network, or
/// with `IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP=true`, which an operator sets for an identity provider
/// on a private network it trusts (a compose network, for instance).
///
/// # Errors
///
/// Returns a message naming the URL's problem.
pub fn check_jwks_url(url: &str, allow_http: bool) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|error| format!("invalid IGGY_KAFKA_OAUTH_JWKS_URL `{url}`: {error}"))?;
    match parsed.scheme() {
        "https" => Ok(()),
        "http" if allow_http || is_loopback(&parsed) => Ok(()),
        "http" => Err(format!(
            "IGGY_KAFKA_OAUTH_JWKS_URL `{url}` is plain http, so anyone on the path could replace \
             the signing keys; use https, or set IGGY_KAFKA_OAUTH_JWKS_ALLOW_HTTP=true for a \
             network you trust"
        )),
        other => Err(format!(
            "IGGY_KAFKA_OAUTH_JWKS_URL `{url}` has scheme `{other}`; expected https"
        )),
    }
}

fn is_loopback(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|host| {
        let host = host.trim_start_matches('[').trim_end_matches(']');
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
}

/// Parses `RS256,ES256`-style lists, refusing symmetric algorithms.
///
/// # Errors
///
/// Returns a message naming the first entry that is unknown or symmetric, or saying the list is
/// empty.
pub fn parse_algorithms(raw: &str) -> Result<Vec<Algorithm>, String> {
    let mut algorithms = Vec::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let algorithm = Algorithm::from_str(entry).map_err(|_| {
            format!("unknown JWT algorithm `{entry}` in IGGY_KAFKA_OAUTH_ALGORITHMS")
        })?;
        if matches!(
            algorithm,
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
        ) {
            return Err(format!(
                "IGGY_KAFKA_OAUTH_ALGORITHMS names `{entry}`, a shared-secret algorithm; a JWKS \
                 publishes public keys, and accepting HMAC would let a public key sign tokens"
            ));
        }
        if !algorithms.contains(&algorithm) {
            algorithms.push(algorithm);
        }
    }
    if algorithms.is_empty() {
        return Err("IGGY_KAFKA_OAUTH_ALGORITHMS names no algorithm".to_string());
    }
    Ok(algorithms)
}

/// A token that passed every check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedToken {
    /// The principal claim's value: the Iggy username.
    pub principal: String,
    /// When the token stops being valid, from `exp`.
    pub expires_at: SystemTime,
}

/// Why a token was not accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    /// The token failed a check. Reaches the client as RFC 7628's `invalid_token`; the reason is
    /// for the gateway's log only.
    Invalid(&'static str),
    /// No signing keys could be loaded, so nothing could be checked. Retriable, like an Iggy
    /// that cannot be reached.
    Unavailable,
}

/// Checks OAUTHBEARER tokens. A trait so the protocol tests can drive the exchange without a
/// key server.
#[async_trait]
pub trait TokenValidator: Send + Sync + fmt::Debug {
    /// Returns what the token proves, or why it proves nothing.
    ///
    /// # Errors
    ///
    /// [`TokenError::Invalid`] for a token that fails any check, [`TokenError::Unavailable`] when
    /// there are no keys to check it against.
    async fn validate(&self, token: &str) -> Result<ValidatedToken, TokenError>;
}

/// One usable verification key.
struct VerifyingKey {
    kid: Option<String>,
    /// The `alg` the JWK pins, when it states one.
    pinned: Option<Algorithm>,
    kind: KeyKind,
    key: DecodingKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Rsa,
    P256,
    P384,
    Ed25519,
}

impl KeyKind {
    /// Whether a key of this kind may verify `algorithm`. The match is exact: an RSA key never
    /// verifies an EC algorithm, and a P-256 key never an `ES384` signature.
    const fn verifies(self, algorithm: Algorithm) -> bool {
        matches!(
            (self, algorithm),
            (
                Self::Rsa,
                Algorithm::RS256
                    | Algorithm::RS384
                    | Algorithm::RS512
                    | Algorithm::PS256
                    | Algorithm::PS384
                    | Algorithm::PS512
            ) | (Self::P256, Algorithm::ES256)
                | (Self::P384, Algorithm::ES384)
                | (Self::Ed25519, Algorithm::EdDSA)
        )
    }
}

/// The keys loaded from one JWKS document.
#[derive(Default)]
struct KeyRing {
    keys: Vec<VerifyingKey>,
    /// Whether any fetch has succeeded yet. An empty set that was fetched is a real answer (every
    /// token fails); one that was never fetched means nothing can be checked.
    loaded: bool,
}

/// Turns one JWK into a verification key, or says why it is skipped.
fn verifying_key(jwk: &Jwk) -> Result<VerifyingKey, &'static str> {
    if let Some(public_key_use) = &jwk.common.public_key_use
        && *public_key_use != PublicKeyUse::Signature
    {
        return Err("published for a use other than signatures");
    }
    let kind = match &jwk.algorithm {
        AlgorithmParameters::RSA(_) => KeyKind::Rsa,
        AlgorithmParameters::EllipticCurve(params) => match params.curve {
            EllipticCurve::P256 => KeyKind::P256,
            EllipticCurve::P384 => KeyKind::P384,
            _ => return Err("an elliptic curve the decoder does not support"),
        },
        AlgorithmParameters::OctetKeyPair(params) => match params.curve {
            EllipticCurve::Ed25519 => KeyKind::Ed25519,
            _ => return Err("an octet key pair on a curve other than Ed25519"),
        },
        // A symmetric key in a JWKS is never a verification key here: loading it would make the
        // gateway accept HMAC tokens signed with a value the identity provider published.
        AlgorithmParameters::OctetKey(_) => return Err("a symmetric key"),
        _ => return Err("an unknown key type"),
    };
    let pinned = match &jwk.common.key_algorithm {
        Some(algorithm) => Some(
            Algorithm::from_str(&algorithm.to_string())
                .map_err(|_| "an `alg` the decoder does not support")?,
        ),
        None => None,
    };
    if let Some(pinned) = pinned
        && !kind.verifies(pinned)
    {
        return Err("an `alg` that does not match its key type");
    }
    let key = DecodingKey::from_jwk(jwk).map_err(|_| "key material that does not parse")?;
    Ok(VerifyingKey {
        kid: jwk.common.key_id.clone(),
        pinned,
        kind,
        key,
    })
}

/// Parses a JWKS document, skipping keys that cannot verify signatures rather than refusing the
/// whole set, since identity providers publish encryption keys and new key types beside the ones
/// that matter here.
fn parse_jwks(document: &[u8]) -> Result<Vec<VerifyingKey>, String> {
    let value: Value =
        serde_json::from_slice(document).map_err(|error| format!("not JSON: {error}"))?;
    let keys = value
        .get("keys")
        .and_then(Value::as_array)
        .ok_or("no `keys` array")?;
    let mut usable = Vec::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        let jwk: Jwk = match serde_json::from_value(key.clone()) {
            Ok(jwk) => jwk,
            Err(error) => {
                debug!(index, %error, "skipping a JWKS entry that is not a JWK");
                continue;
            }
        };
        match verifying_key(&jwk) {
            Ok(verifying) => usable.push(verifying),
            Err(reason) => debug!(index, kid = ?jwk.common.key_id, reason, "skipping a JWKS key"),
        }
    }
    Ok(usable)
}

/// Fetches the JWKS document from its source.
struct Fetcher {
    source: JwksSource,
    client: Option<reqwest::Client>,
}

impl Fetcher {
    fn new(config: &OAuthConfig) -> Result<Self, String> {
        let client = match &config.jwks {
            JwksSource::File(_) => None,
            JwksSource::Url(_) => {
                let mut builder = reqwest::Client::builder()
                    .timeout(FETCH_TIMEOUT)
                    // A redirect could move the fetch onto plain http or another host; the
                    // configured URL is the one the operator vetted.
                    .redirect(reqwest::redirect::Policy::none());
                if let Some(path) = &config.jwks_ca_file {
                    let pem = std::fs::read(path).map_err(|error| {
                        format!(
                            "cannot read IGGY_KAFKA_OAUTH_JWKS_CA_FILE {}: {error}",
                            path.display()
                        )
                    })?;
                    let certificates =
                        reqwest::Certificate::from_pem_bundle(&pem).map_err(|error| {
                            format!(
                                "IGGY_KAFKA_OAUTH_JWKS_CA_FILE {} holds no PEM certificate: \
                                 {error}",
                                path.display()
                            )
                        })?;
                    builder = builder.tls_certs_only(certificates);
                }
                Some(
                    builder
                        .build()
                        .map_err(|error| format!("cannot build the JWKS client: {error}"))?,
                )
            }
        };
        Ok(Self {
            source: config.jwks.clone(),
            client,
        })
    }

    async fn fetch(&self) -> Result<Vec<u8>, String> {
        match (&self.source, &self.client) {
            (JwksSource::File(path), _) => tokio::fs::read(path)
                .await
                .map_err(|error| format!("read {}: {error}", path.display())),
            (JwksSource::Url(url), Some(client)) => {
                let mut response = client
                    .get(url)
                    .header(reqwest::header::ACCEPT, "application/json")
                    .send()
                    .await
                    .map_err(|error| format!("fetch {url}: {error}"))?;
                if !response.status().is_success() {
                    return Err(format!("fetch {url}: HTTP {}", response.status()));
                }
                let mut body = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|error| format!("read {url}: {error}"))?
                {
                    if body.len() + chunk.len() > MAX_JWKS_BYTES {
                        return Err(format!("{url} sent more than {MAX_JWKS_BYTES} bytes"));
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(body)
            }
            (JwksSource::Url(_), None) => Err("no HTTP client".to_string()),
        }
    }
}

/// The gateway's OAUTHBEARER validator: JWTs checked against a cached JWKS.
pub struct JwksValidator {
    config: OAuthConfig,
    keys: ArcSwap<KeyRing>,
    fetcher: Fetcher,
    /// When the last refresh started, so an on-demand refresh can be rate limited. Also
    /// serializes refreshes, so a burst of unknown `kid`s costs one fetch.
    last_refresh: Mutex<Option<Instant>>,
}

impl fmt::Debug for JwksValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JwksValidator")
            .field("issuer", &self.config.issuer)
            .field("jwks", &self.config.jwks)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for JwksValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match &self.config.jwks {
            JwksSource::Url(url) => url.clone(),
            JwksSource::File(path) => path.display().to_string(),
        };
        write!(
            f,
            "issuer {}, keys from {source}, principal claim `{}`",
            self.config.issuer, self.config.principal_claim
        )
    }
}

impl JwksValidator {
    /// A validator with no keys yet; [`Self::refresh`] loads them.
    ///
    /// # Errors
    ///
    /// Returns a message when the configuration is unusable: no algorithms, a symmetric one, or a
    /// CA file that does not load.
    pub fn new(config: OAuthConfig) -> Result<Self, String> {
        if config.algorithms.is_empty() {
            return Err("OAUTHBEARER needs at least one signature algorithm".to_string());
        }
        if config.algorithms.iter().any(|algorithm| {
            matches!(
                algorithm,
                Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
            )
        }) {
            return Err("OAUTHBEARER refuses shared-secret algorithms".to_string());
        }
        let fetcher = Fetcher::new(&config)?;
        Ok(Self {
            config,
            keys: ArcSwap::from_pointee(KeyRing::default()),
            fetcher,
            last_refresh: Mutex::new(None),
        })
    }

    /// Fetches the JWKS and replaces the cached keys. A failed fetch keeps the previous keys: an
    /// identity provider that is briefly down must not log every client out.
    ///
    /// # Errors
    ///
    /// Returns a message describing the failed fetch or parse.
    pub async fn refresh(&self) -> Result<usize, String> {
        let mut last = self.last_refresh.lock().await;
        *last = Some(Instant::now());
        let refreshed = self.refresh_locked().await;
        drop(last);
        refreshed
    }

    async fn refresh_locked(&self) -> Result<usize, String> {
        let document = self.fetcher.fetch().await?;
        let keys = parse_jwks(&document).map_err(|error| format!("JWKS {error}"))?;
        let count = keys.len();
        if count == 0 {
            warn!("the JWKS holds no key that can verify a signature; every token will fail");
        }
        self.keys.store(Arc::new(KeyRing { keys, loaded: true }));
        Ok(count)
    }

    /// Refreshes on demand for a token naming an unknown `kid`, unless the last refresh is more
    /// recent than [`MIN_ON_DEMAND_REFRESH`]. Returns whether the keys may have changed.
    async fn refresh_on_demand(&self) -> bool {
        let mut last = self.last_refresh.lock().await;
        if last.is_some_and(|at| at.elapsed() < MIN_ON_DEMAND_REFRESH) {
            return false;
        }
        *last = Some(Instant::now());
        let refreshed = self.refresh_locked().await;
        drop(last);
        match refreshed {
            Ok(count) => {
                debug!(count, "JWKS refreshed for a token with an unknown key id");
                true
            }
            Err(error) => {
                warn!(%error, "could not refresh the JWKS");
                false
            }
        }
    }

    /// Refreshes on the configured interval until `cancel` fires. Logs, never stops, on failure.
    pub async fn refresh_until_cancelled(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(self.config.refresh_interval) => {}
            }
            match self.refresh().await {
                Ok(count) => debug!(count, "JWKS refreshed"),
                Err(error) => warn!(%error, "could not refresh the JWKS; keeping the keys held"),
            }
        }
    }

    fn validation(&self, algorithm: Algorithm) -> Validation {
        let mut validation = Validation::new(algorithm);
        validation.algorithms = vec![algorithm];
        validation.leeway = self.config.clock_skew.as_secs();
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&self.config.audiences);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation
    }

    /// Verifies `token` with the first candidate key that accepts it.
    fn verify_with(
        &self,
        ring: &KeyRing,
        token: &str,
        algorithm: Algorithm,
        kid: Option<&str>,
    ) -> Option<Result<Map<String, Value>, TokenError>> {
        let candidates: Vec<&VerifyingKey> = ring
            .keys
            .iter()
            .filter(|key| kid.is_none_or(|kid| key.kid.as_deref() == Some(kid)))
            .filter(|key| key.kind.verifies(algorithm))
            .filter(|key| key.pinned.is_none_or(|pinned| pinned == algorithm))
            .take(MAX_KEYS_WITHOUT_KID)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let validation = self.validation(algorithm);
        let mut outcome = Err(TokenError::Invalid("the signature does not verify"));
        for key in candidates {
            match decode::<Map<String, Value>>(token, &key.key, &validation) {
                Ok(data) => return Some(Ok(data.claims)),
                Err(error) => {
                    outcome = Err(TokenError::Invalid(reason(error.kind())));
                    // A claim failure is the token's, not the key's: no other key will change it.
                    if !matches!(
                        error.kind(),
                        jsonwebtoken::errors::ErrorKind::InvalidSignature
                    ) {
                        break;
                    }
                }
            }
        }
        Some(outcome)
    }

    fn principal(&self, claims: &Map<String, Value>) -> Result<ValidatedToken, TokenError> {
        let principal = claims
            .get(&self.config.principal_claim)
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or(TokenError::Invalid(
                "the principal claim is missing or not a string",
            ))?;
        let exp = claims
            .get("exp")
            .and_then(Value::as_u64)
            .ok_or(TokenError::Invalid("`exp` is not a number of seconds"))?;
        Ok(ValidatedToken {
            principal: principal.to_string(),
            expires_at: UNIX_EPOCH + Duration::from_secs(exp),
        })
    }
}

/// A log-safe reason for a decoder failure. Never includes the token or a claim value.
const fn reason(kind: &jsonwebtoken::errors::ErrorKind) -> &'static str {
    use jsonwebtoken::errors::ErrorKind;
    match kind {
        ErrorKind::InvalidSignature => "the signature does not verify",
        ErrorKind::ExpiredSignature => "the token has expired",
        ErrorKind::ImmatureSignature => "the token is not valid yet (`nbf`)",
        ErrorKind::InvalidIssuer => "`iss` is not the configured issuer",
        ErrorKind::InvalidAudience => "`aud` names no configured audience",
        ErrorKind::MissingRequiredClaim(_) => "a required claim (`exp`, `iss`, `aud`) is missing",
        ErrorKind::InvalidAlgorithm => "the algorithm does not match the key",
        _ => "the token is malformed",
    }
}

#[async_trait]
impl TokenValidator for JwksValidator {
    async fn validate(&self, token: &str) -> Result<ValidatedToken, TokenError> {
        let header =
            decode_header(token).map_err(|_| TokenError::Invalid("the token is malformed"))?;
        if !self.config.algorithms.contains(&header.alg) {
            return Err(TokenError::Invalid("the algorithm is not allowed"));
        }
        let kid = header.kid.as_deref();
        let ring = self.keys.load_full();
        if !ring.loaded {
            // Never loaded: try once now rather than failing every client until the next tick.
            if !self.refresh_on_demand().await {
                return Err(TokenError::Unavailable);
            }
        }
        let ring = self.keys.load_full();
        if !ring.loaded {
            return Err(TokenError::Unavailable);
        }
        let claims = match self.verify_with(&ring, token, header.alg, kid) {
            Some(outcome) => outcome?,
            // An unknown `kid` may be a rotated key the cache has not seen yet.
            None if kid.is_some() && self.refresh_on_demand().await => {
                let ring = self.keys.load_full();
                self.verify_with(&ring, token, header.alg, kid)
                    .unwrap_or(Err(TokenError::Invalid("no key in the JWKS matches")))?
            }
            None => return Err(TokenError::Invalid("no key in the JWKS matches")),
        };
        self.principal(&claims)
    }
}

/// Loads the keys once, logging the outcome.
///
/// A failure is not fatal: the identity provider may come up after the gateway, and tokens are then refused as unavailable (a retriable close)
/// until a refresh succeeds.
pub async fn load_initial_keys(validator: &JwksValidator) {
    match validator.refresh().await {
        Ok(count) => info!("OAUTHBEARER: loaded {count} signing keys ({validator})"),
        Err(error) => warn!(
            %error,
            "OAUTHBEARER: could not load the signing keys yet; tokens are refused until a \
             refresh succeeds"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_symmetric_algorithm_when_parsed_should_refuse_it() {
        assert!(parse_algorithms("RS256,HS256").is_err());
        assert!(parse_algorithms("hs512").is_err());
        assert_eq!(
            parse_algorithms("ES256, RS256,ES256").expect("valid"),
            vec![Algorithm::ES256, Algorithm::RS256]
        );
        assert!(
            parse_algorithms("none").is_err(),
            "`none` is not an algorithm"
        );
        assert!(parse_algorithms(" , ").is_err());
    }

    #[test]
    fn given_jwks_urls_when_checked_should_allow_http_only_on_loopback_or_by_opt_in() {
        assert!(check_jwks_url("https://idp.example/oauth/v2/keys", false).is_ok());
        assert!(check_jwks_url("http://127.0.0.1:8080/keys", false).is_ok());
        assert!(check_jwks_url("http://[::1]:8080/keys", false).is_ok());
        assert!(check_jwks_url("http://localhost/keys", false).is_ok());
        assert!(check_jwks_url("http://zitadel:8080/keys", false).is_err());
        assert!(check_jwks_url("http://zitadel:8080/keys", true).is_ok());
        assert!(check_jwks_url("file:///etc/keys.json", true).is_err());
        assert!(check_jwks_url("not a url", false).is_err());
    }

    #[test]
    fn given_a_jwks_with_mixed_keys_when_parsed_should_load_only_signature_keys() {
        let document = br#"{"keys":[
            {"kty":"oct","k":"c2VjcmV0","kid":"hmac"},
            {"kty":"EC","crv":"P-256","use":"enc","kid":"enc",
             "x":"MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4","y":"4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM"},
            {"kty":"EC","crv":"P-256","kid":"ec","alg":"ES256",
             "x":"MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4","y":"4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM"},
            {"kty":"EC","crv":"P-256","kid":"mismatch","alg":"RS256",
             "x":"MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4","y":"4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM"},
            {"kty":"XYZ","kid":"unknown"},
            "not an object"
        ]}"#;
        let keys = parse_jwks(document).expect("a JWKS document");
        let kids: Vec<Option<&str>> = keys.iter().map(|key| key.kid.as_deref()).collect();
        assert_eq!(
            kids,
            vec![Some("ec")],
            "the HMAC key, the encryption key, the key whose alg contradicts its type and the \
             unknown type must all be skipped"
        );
        assert!(parse_jwks(b"{}").is_err());
        assert!(parse_jwks(b"not json").is_err());
    }

    #[test]
    fn given_key_kinds_should_verify_only_their_own_algorithms() {
        assert!(KeyKind::Rsa.verifies(Algorithm::RS256));
        assert!(KeyKind::Rsa.verifies(Algorithm::PS512));
        assert!(!KeyKind::Rsa.verifies(Algorithm::ES256));
        assert!(!KeyKind::Rsa.verifies(Algorithm::HS256));
        assert!(KeyKind::P256.verifies(Algorithm::ES256));
        assert!(!KeyKind::P256.verifies(Algorithm::ES384));
        assert!(KeyKind::Ed25519.verifies(Algorithm::EdDSA));
        assert!(!KeyKind::Ed25519.verifies(Algorithm::ES256));
    }
}
