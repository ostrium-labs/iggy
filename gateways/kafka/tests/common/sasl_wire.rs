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

//! The client side of the SASL exchanges, over [`WireClient`]: PLAIN, SCRAM (RFC 5802) and
//! OAUTHBEARER (RFC 7628), plus `AlterUserScramCredentials` with a client-computed salted
//! password, as Kafka's own admin client sends it. Shared by the oracle and the real-bridge
//! suites.
#![allow(dead_code)]

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use kafka_protocol::messages::alter_user_scram_credentials_request::ScramCredentialUpsertion;
use kafka_protocol::messages::{
    AlterUserScramCredentialsRequest, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest,
};
use kafka_protocol::protocol::StrBytes;
use sha2::{Digest, Sha256, Sha512};

use super::oracle::WireClient;

/// `SaslAuthenticate` version the exchanges use, the one current clients negotiate.
pub const AUTHENTICATE_VERSION: i16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scram {
    Sha256,
    Sha512,
}

impl Scram {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    pub const fn type_code(self) -> i8 {
        match self {
            Self::Sha256 => 1,
            Self::Sha512 => 2,
        }
    }

    fn hmac(self, key: &[u8], message: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => {
                let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("any key");
                mac.update(message);
                mac.finalize().into_bytes().to_vec()
            }
            Self::Sha512 => {
                let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("any key");
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

    /// RFC 5802's `Hi(password, salt, i)`.
    pub fn salted_password(self, password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut out = vec![
            0u8;
            match self {
                Self::Sha256 => 32,
                Self::Sha512 => 64,
            }
        ];
        match self {
            Self::Sha256 => {
                pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut out);
            }
            Self::Sha512 => {
                pbkdf2::pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, iterations, &mut out);
            }
        }
        out
    }
}

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// Sends `SaslHandshake` v1 for `mechanism`; returns its error code.
pub async fn handshake(client: &mut WireClient, mechanism: &str) -> Result<i16, String> {
    client
        .send(
            &SaslHandshakeRequest::default().with_mechanism(s(mechanism)),
            1,
        )
        .await
        .map(|response| response.error_code)
}

pub async fn authenticate(
    client: &mut WireClient,
    token: &[u8],
) -> Result<SaslAuthenticateResponse, String> {
    client
        .send(
            &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::copy_from_slice(token)),
            AUTHENTICATE_VERSION,
        )
        .await
}

/// Handshake plus one PLAIN token; returns the authenticate error code.
pub async fn plain(client: &mut WireClient, user: &str, password: &str) -> Result<i16, String> {
    let code = handshake(client, "PLAIN").await?;
    if code != 0 {
        return Ok(code);
    }
    let token = format!("\0{user}\0{password}");
    authenticate(client, token.as_bytes())
        .await
        .map(|response| response.error_code)
}

/// What a SCRAM exchange produced, step by step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramOutcome {
    /// The error code of the first `SaslAuthenticate`, or `closed`.
    pub first: String,
    /// The error code of the second, `closed`, or `-` when the first already failed.
    pub last: String,
    /// Whether the server-final message carried the signature this password implies.
    pub server_verified: bool,
    pub session_lifetime_ms: i64,
}

/// Handshake, client-first, client-final for `user` with `password`.
pub async fn scram(
    client: &mut WireClient,
    mechanism: Scram,
    user: &str,
    password: &str,
) -> Result<ScramOutcome, String> {
    let code = handshake(client, mechanism.name()).await?;
    if code != 0 {
        return Err(format!("handshake answered {code}"));
    }
    let bare = format!("n={user},r=fyko+d2lbbFgONRv9qkxdawL");
    let Ok(first) = authenticate(client, format!("n,,{bare}").as_bytes()).await else {
        return Ok(ScramOutcome {
            first: "closed".to_string(),
            last: "-".to_string(),
            server_verified: false,
            session_lifetime_ms: 0,
        });
    };
    if first.error_code != 0 {
        return Ok(ScramOutcome {
            first: first.error_code.to_string(),
            last: "-".to_string(),
            server_verified: false,
            session_lifetime_ms: 0,
        });
    }
    let server_first = String::from_utf8(first.auth_bytes.to_vec()).map_err(|e| e.to_string())?;
    let fields: Vec<&str> = server_first.split(',').collect();
    let nonce = fields
        .first()
        .and_then(|field| field.strip_prefix("r="))
        .ok_or("no server nonce")?;
    let salt = fields
        .get(1)
        .and_then(|field| field.strip_prefix("s="))
        .and_then(|salt| BASE64.decode(salt).ok())
        .ok_or("no salt")?;
    let iterations: u32 = fields
        .get(2)
        .and_then(|field| field.strip_prefix("i="))
        .and_then(|value| value.parse().ok())
        .ok_or("no iterations")?;
    let salted = mechanism.salted_password(password, &salt, iterations);
    let client_key = mechanism.hmac(&salted, b"Client Key");
    let stored_key = mechanism.hash(&client_key);
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{bare},{server_first},{without_proof}");
    let signature = mechanism.hmac(&stored_key, auth_message.as_bytes());
    let proof: Vec<u8> = client_key
        .iter()
        .zip(&signature)
        .map(|(key, sig)| key ^ sig)
        .collect();
    let final_message = format!("{without_proof},p={}", BASE64.encode(proof));
    let Ok(last) = authenticate(client, final_message.as_bytes()).await else {
        return Ok(ScramOutcome {
            first: "0".to_string(),
            last: "closed".to_string(),
            server_verified: false,
            session_lifetime_ms: 0,
        });
    };
    let server_key = mechanism.hmac(&salted, b"Server Key");
    let expected = format!(
        "v={}",
        BASE64.encode(mechanism.hmac(&server_key, auth_message.as_bytes()))
    );
    Ok(ScramOutcome {
        first: "0".to_string(),
        last: last.error_code.to_string(),
        server_verified: last.auth_bytes.as_ref() == expected.as_bytes(),
        session_lifetime_ms: last.session_lifetime_ms,
    })
}

/// An `AlterUserScramCredentials` upsertion for `user`, the way Kafka's admin client builds one:
/// the salted password is computed here, so the password itself never travels.
pub fn upsertion(
    user: &str,
    mechanism: Scram,
    password: &str,
    iterations: i32,
) -> ScramCredentialUpsertion {
    let salt = format!("salt-{user}-{}", mechanism.name()).into_bytes();
    let salted = mechanism.salted_password(
        password,
        &salt,
        u32::try_from(iterations.max(1)).unwrap_or(4096),
    );
    ScramCredentialUpsertion::default()
        .with_name(s(user))
        .with_mechanism(mechanism.type_code())
        .with_iterations(iterations)
        .with_salt(Bytes::from(salt))
        .with_salted_password(Bytes::from(salted))
}

pub fn alter_request(
    upsertions: Vec<ScramCredentialUpsertion>,
) -> AlterUserScramCredentialsRequest {
    AlterUserScramCredentialsRequest::default().with_upsertions(upsertions)
}

/// What an OAUTHBEARER exchange produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthOutcome {
    /// The first `SaslAuthenticate` error code, or `closed`.
    pub first: String,
    /// The JSON `status` of the error challenge, or `-` when there was none.
    pub challenge_status: String,
    /// The error code after acknowledging a challenge, or `-`.
    pub after_challenge: String,
    /// Whether a successful exchange came with a positive session lifetime.
    pub lifetime_positive: bool,
}

/// Handshake plus the bearer token, acknowledging an error challenge the way clients do.
pub async fn oauthbearer(client: &mut WireClient, token: &str) -> Result<OAuthOutcome, String> {
    let code = handshake(client, "OAUTHBEARER").await?;
    if code != 0 {
        return Err(format!("handshake answered {code}"));
    }
    let message = format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}");
    let Ok(first) = authenticate(client, message.as_bytes()).await else {
        return Ok(OAuthOutcome {
            first: "closed".to_string(),
            challenge_status: "-".to_string(),
            after_challenge: "-".to_string(),
            lifetime_positive: false,
        });
    };
    if first.error_code != 0 || first.auth_bytes.is_empty() {
        return Ok(OAuthOutcome {
            first: first.error_code.to_string(),
            challenge_status: "-".to_string(),
            after_challenge: "-".to_string(),
            lifetime_positive: first.error_code == 0 && first.session_lifetime_ms > 0,
        });
    }
    let status = serde_json::from_slice::<serde_json::Value>(&first.auth_bytes)
        .ok()
        .and_then(|value| {
            value
                .get("status")
                .and_then(|s| s.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unparsed".to_string());
    let after = authenticate(client, &[0x01]).await.map_or_else(
        |_| "closed".to_string(),
        |response| response.error_code.to_string(),
    );
    Ok(OAuthOutcome {
        first: "0".to_string(),
        challenge_status: status,
        after_challenge: after,
        lifetime_positive: false,
    })
}
