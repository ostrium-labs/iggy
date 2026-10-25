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

//! SASL/SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802, RFC 7677), the server side.
//!
//! Pure and synchronous: the credential arrives from the caller, which looks it up in the store
//! the gateway keeps in Iggy (`bridge::iggy_bridge::scram_store`). The cryptography is
//! `RustCrypto`'s (`hmac`, `sha2`, `pbkdf2`), comparisons are constant time (`subtle`), and
//! nonces and salts come from the operating system's generator (`getrandom`).
//!
//! The message grammar follows Kafka's `ScramMessages`, with three deliberate differences, each
//! stricter and none visible to a conformant client:
//!
//! - The channel-binding attribute `c=` of the client-final message must be the base64 of the
//!   client's own GS2 header, as RFC 5802 requires. Kafka accepts any value.
//! - A client-first message with a mandatory extension (`m=`) is refused; RFC 5802 says a server
//!   that does not understand one must fail.
//! - An unknown user is not refused at the first message. It is answered with a salt and
//!   iteration count derived from the name and a per-process secret, and fails at the proof like
//!   a wrong password (RFC 5802, section 9's advice), so the exchange does not reveal which
//!   accounts exist. Kafka refuses it at once.
//!
//! One leniency of Kafka's is kept: the client-final nonce only has to end with the agreed one,
//! because librdkafka 1.x sends its own nonce twice.

use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::protocol::sasl::{SaslMechanism, decode_saslname};

/// Kafka's `ScramMechanism.minIterations()` for both mechanisms.
pub const MIN_ITERATIONS: u32 = 4096;

/// Kafka's `ScramControlManager.MAX_ITERATIONS`.
pub const MAX_ITERATIONS: u32 = 16384;

/// Iterations an unknown user's decoy credential advertises: the value `kafka-configs` uses when
/// none is given, so a decoy looks like the commonest real credential.
const DECOY_ITERATIONS: u32 = 4096;

/// Salt length for decoys and for salts this gateway generates.
const SALT_LEN: usize = 32;

/// Random bytes in a server nonce, before base64.
const NONCE_BYTES: usize = 24;

/// One of the two SCRAM mechanisms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    /// The `mechanism` code `DescribeUserScramCredentials` and `AlterUserScramCredentials` carry
    /// (`org.apache.kafka.clients.admin.ScramMechanism`): 0 is unknown.
    #[must_use]
    pub const fn from_type(code: i8) -> Option<Self> {
        match code {
            1 => Some(Self::Sha256),
            2 => Some(Self::Sha512),
            _ => None,
        }
    }

    #[must_use]
    pub const fn type_code(self) -> i8 {
        match self {
            Self::Sha256 => 1,
            Self::Sha512 => 2,
        }
    }

    #[must_use]
    pub const fn from_sasl(mechanism: SaslMechanism) -> Option<Self> {
        match mechanism {
            SaslMechanism::ScramSha256 => Some(Self::Sha256),
            SaslMechanism::ScramSha512 => Some(Self::Sha512),
            SaslMechanism::Plain | SaslMechanism::OAuthBearer => None,
        }
    }

    #[must_use]
    pub const fn sasl(self) -> SaslMechanism {
        match self {
            Self::Sha256 => SaslMechanism::ScramSha256,
            Self::Sha512 => SaslMechanism::ScramSha512,
        }
    }

    #[must_use]
    pub const fn name(self) -> &'static str {
        self.sasl().name()
    }

    const fn digest_len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    fn hmac(self, key: &[u8], message: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => {
                let mut mac =
                    Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts a key of any length");
                mac.update(message);
                mac.finalize().into_bytes().to_vec()
            }
            Self::Sha512 => {
                let mut mac =
                    Hmac::<Sha512>::new_from_slice(key).expect("HMAC accepts a key of any length");
                mac.update(message);
                mac.finalize().into_bytes().to_vec()
            }
        }
    }

    fn hash(self, message: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => Sha256::digest(message).to_vec(),
            Self::Sha512 => Sha512::digest(message).to_vec(),
        }
    }

    /// RFC 5802's `Hi`: PBKDF2 with this mechanism's HMAC, one block.
    fn salted_password(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut out = vec![0u8; self.digest_len()];
        match self {
            Self::Sha256 => pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out),
            Self::Sha512 => pbkdf2::pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut out),
        }
        out
    }
}

impl fmt::Display for ScramMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What the server keeps for one user and mechanism: never the password, nor anything that logs
/// in by itself. `StoredKey` verifies a proof; `ServerKey` signs the server's answer.
///
/// Zeroed on drop. No `Debug` beyond the iteration count.
#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct ScramCredential {
    pub salt: Vec<u8>,
    #[zeroize(skip)]
    pub iterations: u32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl fmt::Debug for ScramCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramCredential")
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

impl ScramCredential {
    /// Derives the stored keys from a salted password, as `AlterUserScramCredentials` delivers
    /// it (the client computes `Hi(password, salt, i)` so the password never leaves it).
    #[must_use]
    pub fn from_salted_password(
        mechanism: ScramMechanism,
        salt: Vec<u8>,
        iterations: u32,
        salted_password: &[u8],
    ) -> Self {
        let mut client_key = mechanism.hmac(salted_password, b"Client Key");
        let stored_key = mechanism.hash(&client_key);
        client_key.zeroize();
        Self {
            salt,
            iterations,
            stored_key,
            server_key: mechanism.hmac(salted_password, b"Server Key"),
        }
    }

    /// Derives a credential from a password, for tests and tools that provision one directly.
    #[must_use]
    pub fn from_password(
        mechanism: ScramMechanism,
        password: &str,
        salt: Vec<u8>,
        iterations: u32,
    ) -> Self {
        let mut salted = mechanism.salted_password(password.as_bytes(), &salt, iterations);
        let credential = Self::from_salted_password(mechanism, salt, iterations, &salted);
        salted.zeroize();
        credential
    }

    /// A salt from the operating system's generator.
    ///
    /// # Panics
    ///
    /// Panics if the operating system has no randomness to give, where nothing secure can run.
    #[must_use]
    pub fn random_salt() -> Vec<u8> {
        random_bytes(SALT_LEN)
    }
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    getrandom::fill(&mut bytes).expect("the operating system's random generator is available");
    bytes
}

/// Why a SCRAM exchange failed. Every variant reaches the client as the same 58.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramError {
    /// A message that does not parse, or an unsupported feature (channel binding, a mandatory
    /// extension, delegation tokens).
    Malformed(&'static str),
    /// The authorization id names someone other than the user.
    AuthorizationId,
    /// The nonce or channel binding of the final message is not the one the exchange agreed on.
    Mismatch(&'static str),
    /// The proof does not verify: a wrong password, or a user with no credential.
    InvalidProof,
}

/// A parsed client-first message.
pub struct ClientFirst {
    gs2_header: String,
    bare: String,
    /// The user, decoded from its `saslname` form.
    pub username: String,
    nonce: String,
}

/// Parses `n,[a=authzid],n=user,r=nonce[,ext=value]*`.
///
/// # Errors
///
/// [`ScramError::Malformed`] for a message outside the grammar or asking for something this
/// server does not do, [`ScramError::AuthorizationId`] for an authorization id that differs from
/// the user.
pub fn parse_client_first(message: &[u8]) -> Result<ClientFirst, ScramError> {
    let message = std::str::from_utf8(message).map_err(|_| ScramError::Malformed("not UTF-8"))?;
    let rest = message.strip_prefix("n,").ok_or(ScramError::Malformed(
        "only the `n` GS2 flag is supported; channel binding is not",
    ))?;
    let (authzid, bare) = rest
        .split_once(',')
        .ok_or(ScramError::Malformed("no GS2 header"))?;
    let gs2_header = format!("n,{authzid},");
    let mut fields = bare.split(',');
    let first = fields.next().unwrap_or_default();
    if first.starts_with("m=") {
        return Err(ScramError::Malformed(
            "mandatory extensions are not supported",
        ));
    }
    let username = first
        .strip_prefix("n=")
        .and_then(decode_saslname)
        .ok_or(ScramError::Malformed("no valid user name"))?;
    let nonce = fields
        .next()
        .and_then(|field| field.strip_prefix("r="))
        .filter(|nonce| is_printable(nonce))
        .ok_or(ScramError::Malformed("no valid client nonce"))?
        .to_string();
    for extension in fields {
        let (key, value) = extension
            .split_once('=')
            .ok_or(ScramError::Malformed("an extension without a value"))?;
        if key.is_empty() || !key.bytes().all(|byte| byte.is_ascii_alphabetic()) {
            return Err(ScramError::Malformed("an extension with an invalid name"));
        }
        // Kafka's delegation-token login. There are no delegation tokens here, and treating the
        // name as a user would authenticate a token id as an account.
        if key == "tokenauth" && value.eq_ignore_ascii_case("true") {
            return Err(ScramError::Malformed("delegation tokens are not supported"));
        }
    }
    if !authzid.is_empty() {
        let named = authzid
            .strip_prefix("a=")
            .and_then(decode_saslname)
            .ok_or(ScramError::Malformed("an invalid authorization id"))?;
        if named != username {
            return Err(ScramError::AuthorizationId);
        }
    }
    Ok(ClientFirst {
        gs2_header,
        bare: bare.to_string(),
        username,
        nonce,
    })
}

/// RFC 5802's `printable`: `%x21-2B / %x2D-7E`, everything visible but the comma.
fn is_printable(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte) && byte != b',')
}

/// A server waiting for the client-final message.
pub struct ScramServer {
    mechanism: ScramMechanism,
    first: ClientFirst,
    server_first: String,
    nonce: String,
    credential: ScramCredential,
    /// Whether `credential` is the user's. A decoy never verifies, whatever proof arrives.
    known: bool,
}

/// A per-process secret that decoy salts are derived from, so an unknown user gets the same salt
/// on every attempt against one gateway.
pub struct DecoySecret([u8; 32]);

impl Default for DecoySecret {
    fn default() -> Self {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).expect("the operating system's random generator is available");
        Self(secret)
    }
}

impl fmt::Debug for DecoySecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DecoySecret(..)")
    }
}

impl DecoySecret {
    fn decoy(&self, mechanism: ScramMechanism, username: &str) -> ScramCredential {
        let mut input = Vec::with_capacity(username.len() + 16);
        input.extend_from_slice(mechanism.name().as_bytes());
        input.push(0);
        input.extend_from_slice(username.as_bytes());
        let mut salt = ScramMechanism::Sha256.hmac(&self.0, &input);
        salt.truncate(SALT_LEN);
        ScramCredential {
            salt,
            iterations: DECOY_ITERATIONS,
            stored_key: random_bytes(mechanism.digest_len()),
            server_key: random_bytes(mechanism.digest_len()),
        }
    }
}

impl ScramServer {
    /// Answers a client-first message: `r=<client nonce><server nonce>,s=<salt>,i=<iterations>`.
    ///
    /// `credential` is the user's, or `None` for a user with none, who gets a decoy from
    /// `secret` and fails at the proof.
    #[must_use]
    pub fn start(
        mechanism: ScramMechanism,
        first: ClientFirst,
        credential: Option<ScramCredential>,
        secret: &DecoySecret,
    ) -> (Self, Vec<u8>) {
        let known = credential.is_some();
        let credential = credential.unwrap_or_else(|| secret.decoy(mechanism, &first.username));
        let server_nonce =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random_bytes(NONCE_BYTES));
        let nonce = format!("{}{server_nonce}", first.nonce);
        let server_first = format!(
            "r={nonce},s={},i={}",
            BASE64.encode(&credential.salt),
            credential.iterations
        );
        let message = server_first.clone().into_bytes();
        (
            Self {
                mechanism,
                first,
                server_first,
                nonce,
                credential,
                known,
            },
            message,
        )
    }

    /// The user the exchange is for.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.first.username
    }

    #[must_use]
    pub const fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    /// Verifies the client-final message and returns the server-final one, `v=<signature>`.
    ///
    /// # Errors
    ///
    /// [`ScramError::Malformed`] or [`ScramError::Mismatch`] for a message that is not the
    /// continuation of this exchange, [`ScramError::InvalidProof`] for a proof that does not
    /// verify.
    pub fn finish(self, message: &[u8]) -> Result<Vec<u8>, ScramError> {
        let message =
            std::str::from_utf8(message).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let (without_proof, proof) = message
            .rsplit_once(",p=")
            .ok_or(ScramError::Malformed("no proof"))?;
        let mut fields = without_proof.split(',');
        let binding = fields
            .next()
            .and_then(|field| field.strip_prefix("c="))
            .ok_or(ScramError::Malformed("no channel binding"))?;
        let nonce = fields
            .next()
            .and_then(|field| field.strip_prefix("r="))
            .ok_or(ScramError::Malformed("no nonce"))?;
        let binding = BASE64
            .decode(binding)
            .map_err(|_| ScramError::Malformed("channel binding is not base64"))?;
        if binding != self.first.gs2_header.as_bytes() {
            return Err(ScramError::Mismatch("channel binding"));
        }
        // Kafka's `ScramSaslServer` checks only that the final nonce ends with the agreed one,
        // and librdkafka 1.x needs that: it sends its own nonce again in front of the server's
        // (kcat 1.7.1 does). Matched, since the proof covers the message as sent and the
        // server's part, which is what keeps the exchange fresh, must still be there.
        let (sent, agreed) = (nonce.as_bytes(), self.nonce.as_bytes());
        let ends_with_agreed = sent.len() >= agreed.len()
            && bool::from(sent[sent.len() - agreed.len()..].ct_eq(agreed));
        if !ends_with_agreed {
            return Err(ScramError::Mismatch("nonce"));
        }
        let mut proof = BASE64
            .decode(proof)
            .map_err(|_| ScramError::Malformed("proof is not base64"))?;
        if proof.len() != self.mechanism.digest_len() {
            return Err(ScramError::InvalidProof);
        }
        let auth_message = format!("{},{},{without_proof}", self.first.bare, self.server_first);
        let client_signature = self
            .mechanism
            .hmac(&self.credential.stored_key, auth_message.as_bytes());
        for (byte, signature) in proof.iter_mut().zip(&client_signature) {
            *byte ^= signature;
        }
        let computed_stored_key = self.mechanism.hash(&proof);
        proof.zeroize();
        let matches: bool = computed_stored_key
            .ct_eq(&self.credential.stored_key)
            .into();
        if !(matches && self.known) {
            return Err(ScramError::InvalidProof);
        }
        let signature = self
            .mechanism
            .hmac(&self.credential.server_key, auth_message.as_bytes());
        Ok(format!("v={}", BASE64.encode(signature)).into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677, section 3: the SCRAM-SHA-256 example exchange, end to end.
    #[test]
    fn given_the_rfc_7677_example_should_verify_and_sign_as_the_rfc_does() {
        let mechanism = ScramMechanism::Sha256;
        let salt = BASE64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let credential = ScramCredential::from_password(mechanism, "pencil", salt, 4096);
        let first = parse_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO").expect("first");
        let (mut server, _) =
            ScramServer::start(mechanism, first, Some(credential), &DecoySecret::default());
        // The RFC fixes the server nonce, so the exchange is replayed with it.
        server.nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".to_string();
        server.server_first = format!("r={},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", server.nonce);
        let server_final = server
            .finish(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                  p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .expect("the RFC's proof verifies");
        assert_eq!(
            server_final,
            b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".to_vec()
        );
    }

    /// Plays the client side for a round trip.
    fn client_final(
        mechanism: ScramMechanism,
        password: &str,
        client_first_bare: &str,
        server_first: &str,
        gs2_header: &str,
    ) -> String {
        client_final_echoing(
            mechanism,
            password,
            client_first_bare,
            server_first,
            gs2_header,
            "",
        )
    }

    /// [`client_final`], sending `echo` in front of the server's nonce, as librdkafka 1.x
    /// sends its own nonce again.
    fn client_final_echoing(
        mechanism: ScramMechanism,
        password: &str,
        client_first_bare: &str,
        server_first: &str,
        gs2_header: &str,
        echo: &str,
    ) -> String {
        let fields: Vec<&str> = server_first.split(',').collect();
        let nonce = format!("{echo}{}", fields[0].strip_prefix("r=").expect("nonce"));
        let salt = BASE64
            .decode(fields[1].strip_prefix("s=").expect("salt"))
            .expect("salt is base64");
        let iterations: u32 = fields[2]
            .strip_prefix("i=")
            .expect("i")
            .parse()
            .expect("number");
        let salted = mechanism.salted_password(password.as_bytes(), &salt, iterations);
        let client_key = mechanism.hmac(&salted, b"Client Key");
        let stored_key = mechanism.hash(&client_key);
        let without_proof = format!("c={},r={nonce}", BASE64.encode(gs2_header));
        let auth = format!("{client_first_bare},{server_first},{without_proof}");
        let signature = mechanism.hmac(&stored_key, auth.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(signature)
            .map(|(key, sig)| key ^ sig)
            .collect();
        format!("{without_proof},p={}", BASE64.encode(proof))
    }

    fn exchange(
        mechanism: ScramMechanism,
        stored_password: Option<&str>,
        presented: &str,
    ) -> Result<Vec<u8>, ScramError> {
        let credential = stored_password.map(|password| {
            ScramCredential::from_password(
                mechanism,
                password,
                ScramCredential::random_salt(),
                4096,
            )
        });
        let first = parse_client_first(b"n,,n=alice,r=clientnonce").expect("first");
        let (server, server_first) =
            ScramServer::start(mechanism, first, credential, &DecoySecret::default());
        let server_first = String::from_utf8(server_first).expect("utf-8");
        let last = client_final(
            mechanism,
            presented,
            "n=alice,r=clientnonce",
            &server_first,
            "n,,",
        );
        server.finish(last.as_bytes())
    }

    #[test]
    fn given_the_right_password_should_authenticate_under_both_mechanisms() {
        for mechanism in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            let server_final = exchange(mechanism, Some("s3cret"), "s3cret").expect("verifies");
            assert!(server_final.starts_with(b"v="));
        }
    }

    #[test]
    fn given_a_wrong_password_should_fail_the_proof() {
        assert_eq!(
            exchange(ScramMechanism::Sha512, Some("s3cret"), "guess").err(),
            Some(ScramError::InvalidProof)
        );
    }

    #[test]
    fn given_an_unknown_user_should_fail_the_proof_not_the_first_message() {
        // A decoy answers the first message, so the exchange looks like a real one until the
        // proof, whatever password the client presents.
        assert_eq!(
            exchange(ScramMechanism::Sha256, None, "anything").err(),
            Some(ScramError::InvalidProof)
        );
    }

    #[test]
    fn given_an_unknown_user_twice_should_answer_the_same_salt() {
        let secret = DecoySecret::default();
        let start = || {
            let first = parse_client_first(b"n,,n=ghost,r=abc").expect("first");
            let (_, message) = ScramServer::start(ScramMechanism::Sha256, first, None, &secret);
            let message = String::from_utf8(message).expect("utf-8");
            message.split(',').nth(1).expect("salt").to_string()
        };
        assert_eq!(
            start(),
            start(),
            "a varying salt would mark the user as unknown"
        );
    }

    #[test]
    fn given_the_client_nonce_sent_again_before_the_agreed_one_should_authenticate_as_kafka_does() {
        let mechanism = ScramMechanism::Sha256;
        let credential =
            ScramCredential::from_password(mechanism, "pw", ScramCredential::random_salt(), 4096);
        let first = parse_client_first(b"n,,n=alice,r=abc").expect("first");
        let (server, server_first) =
            ScramServer::start(mechanism, first, Some(credential), &DecoySecret::default());
        let server_first = String::from_utf8(server_first).expect("utf-8");
        let doubled = client_final_echoing(
            mechanism,
            "pw",
            "n=alice,r=abc",
            &server_first,
            "n,,",
            "abc",
        );
        assert!(doubled.starts_with("c=biws,r=abcabc"));
        let server_final = server.finish(doubled.as_bytes()).expect("verifies");
        assert!(server_final.starts_with(b"v="));

        // The server's part must still be there: a nonce cut short of it is refused.
        let first = parse_client_first(b"n,,n=alice,r=abc").expect("first");
        let credential =
            ScramCredential::from_password(mechanism, "pw", ScramCredential::random_salt(), 4096);
        let (server, _) =
            ScramServer::start(mechanism, first, Some(credential), &DecoySecret::default());
        assert_eq!(
            server.finish(b"c=biws,r=abc,p=AAAA").err(),
            Some(ScramError::Mismatch("nonce"))
        );
    }

    #[test]
    fn given_a_tampered_nonce_or_binding_should_be_refused() {
        let mechanism = ScramMechanism::Sha256;
        let credential =
            ScramCredential::from_password(mechanism, "pw", ScramCredential::random_salt(), 4096);
        let first = parse_client_first(b"n,,n=alice,r=abc").expect("first");
        let (server, server_first) =
            ScramServer::start(mechanism, first, Some(credential), &DecoySecret::default());
        let server_first = String::from_utf8(server_first).expect("utf-8");
        let good = client_final(mechanism, "pw", "n=alice,r=abc", &server_first, "n,,");
        let other_nonce = good.replacen("r=abc", "r=abd", 1);
        assert_eq!(
            server.finish(other_nonce.as_bytes()).err(),
            Some(ScramError::Mismatch("nonce"))
        );

        let first = parse_client_first(b"n,,n=alice,r=abc").expect("first");
        let credential =
            ScramCredential::from_password(mechanism, "pw", ScramCredential::random_salt(), 4096);
        let (server, _) =
            ScramServer::start(mechanism, first, Some(credential), &DecoySecret::default());
        let binding = format!("c={},r=x,p=AAAA", BASE64.encode("y,,"));
        assert_eq!(
            server.finish(binding.as_bytes()).err(),
            Some(ScramError::Mismatch("channel binding"))
        );
    }

    #[test]
    fn given_client_first_messages_should_parse_only_the_supported_grammar() {
        let parsed = parse_client_first(b"n,,n=al=2Cice=3D,r=abc,foo=bar").expect("valid");
        assert_eq!(parsed.username, "al,ice=");
        assert!(parse_client_first(b"n,a=alice,n=alice,r=abc").is_ok());
        assert_eq!(
            parse_client_first(b"n,a=root,n=alice,r=abc").err(),
            Some(ScramError::AuthorizationId)
        );
        for refused in [
            &b"y,,n=alice,r=abc"[..],
            b"p=tls-unique,,n=alice,r=abc",
            b"n,,m=ext,n=alice,r=abc",
            b"n,,n=alice,r=abc,tokenauth=true",
            b"n,,n=al=ice,r=abc",
            b"n,,n=,r=abc",
            b"n,,n=alice",
            b"n,,n=alice,r=",
            b"n,,n=alice,r=a,bad",
            b"\xff",
        ] {
            assert!(
                parse_client_first(refused).is_err(),
                "{:?} must be refused",
                String::from_utf8_lossy(refused)
            );
        }
    }

    #[test]
    fn given_a_salted_password_should_derive_what_the_password_derives() {
        let mechanism = ScramMechanism::Sha512;
        let salt = ScramCredential::random_salt();
        let salted = mechanism.salted_password(b"pw", &salt, 4096);
        assert_eq!(
            ScramCredential::from_salted_password(mechanism, salt.clone(), 4096, &salted),
            ScramCredential::from_password(mechanism, "pw", salt, 4096)
        );
    }

    #[test]
    fn given_mechanism_codes_should_round_trip_and_refuse_unknown() {
        for mechanism in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            assert_eq!(
                ScramMechanism::from_type(mechanism.type_code()),
                Some(mechanism)
            );
        }
        assert_eq!(ScramMechanism::from_type(0), None);
        assert_eq!(ScramMechanism::from_type(3), None);
    }
}
