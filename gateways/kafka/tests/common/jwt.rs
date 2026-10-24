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

//! A test identity provider: signing keys minted per test, a JWKS document for them, signed
//! tokens, and a loopback HTTP server that publishes the JWKS and hands out tokens the way an
//! OAuth token endpoint does (`client_credentials`), for clients that fetch their own.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rsa::KeySize;
use aws_lc_rs::signature::KeyPair as _;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The issuer every test token names unless a test says otherwise.
pub const ISSUER: &str = "https://idp.loams.test";
/// The audience the gateway is configured with.
pub const AUDIENCE: &str = "kafka-gateway";

/// One signing key and the JWK that publishes it.
pub struct SigningKey {
    pub kid: String,
    pub algorithm: Algorithm,
    pub encoding: EncodingKey,
    pub jwk: Value,
    /// The raw public key bytes, for the algorithm-confusion test that signs HMAC with them.
    pub public_bytes: Vec<u8>,
}

impl SigningKey {
    /// An RSA-2048 key for `RS256` (what Zitadel signs with).
    pub fn rsa(kid: &str) -> Self {
        let pair = aws_lc_rs::rsa::KeyPair::generate(KeySize::Rsa2048).expect("generate RSA key");
        let pkcs8 = pair.as_der().expect("PKCS#8 DER");
        let public = pair.public_key();
        let n = public.modulus().big_endian_without_leading_zero().to_vec();
        let e = public.exponent().big_endian_without_leading_zero().to_vec();
        Self {
            kid: kid.to_string(),
            algorithm: Algorithm::RS256,
            encoding: EncodingKey::from_rsa_pem(pem("PRIVATE KEY", pkcs8.as_ref()).as_bytes())
                .expect("RSA PKCS#8 PEM"),
            jwk: json!({
                "kty": "RSA", "use": "sig", "alg": "RS256", "kid": kid,
                "n": URL_SAFE_NO_PAD.encode(&n), "e": URL_SAFE_NO_PAD.encode(&e),
            }),
            public_bytes: public.as_ref().to_vec(),
        }
    }

    /// A P-256 key for `ES256`.
    pub fn ec(kid: &str) -> Self {
        let pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .expect("generate P-256 key");
        let raw = pair.public_key_raw().to_vec();
        assert_eq!(raw.len(), 65, "an uncompressed P-256 point");
        Self {
            kid: kid.to_string(),
            algorithm: Algorithm::ES256,
            encoding: EncodingKey::from_ec_pem(pair.serialize_pem().as_bytes())
                .expect("P-256 PKCS#8 PEM"),
            jwk: json!({
                "kty": "EC", "crv": "P-256", "use": "sig", "kid": kid,
                "x": URL_SAFE_NO_PAD.encode(&raw[1..33]), "y": URL_SAFE_NO_PAD.encode(&raw[33..]),
            }),
            public_bytes: raw,
        }
    }

    /// An Ed25519 key for `EdDSA`.
    pub fn ed25519(kid: &str) -> Self {
        let pair =
            rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).expect("generate Ed25519 key");
        let raw = pair.public_key_raw().to_vec();
        Self {
            kid: kid.to_string(),
            algorithm: Algorithm::EdDSA,
            encoding: EncodingKey::from_ed_pem(pair.serialize_pem().as_bytes())
                .expect("Ed25519 PKCS#8 PEM"),
            jwk: json!({
                "kty": "OKP", "crv": "Ed25519", "use": "sig", "kid": kid,
                "x": URL_SAFE_NO_PAD.encode(&raw),
            }),
            public_bytes: raw,
        }
    }

    /// Signs `claims` with this key, naming its `kid`.
    pub fn sign(&self, claims: &Value) -> String {
        let mut header = Header::new(self.algorithm);
        header.kid = Some(self.kid.clone());
        jsonwebtoken::encode(&header, claims, &self.encoding).expect("sign token")
    }
}

/// `der` as PEM with the given label.
fn pem(label: &str, der: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in encoded.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        out.push('\n');
    }
    out.push_str("-----END ");
    out.push_str(label);
    out.push_str("-----\n");
    out
}

/// A JWKS document publishing `keys`.
pub fn jwks(keys: &[&SigningKey]) -> String {
    json!({ "keys": keys.iter().map(|key| key.jwk.clone()).collect::<Vec<_>>() }).to_string()
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs()
}

/// The claims of a good token for `subject`, valid for `lifetime_secs`.
pub fn claims(subject: &str, lifetime_secs: u64) -> Value {
    let now = now();
    json!({
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": subject,
        "iat": now,
        "exp": now + lifetime_secs,
        "scope": "kafka",
    })
}

/// An unsigned (`alg: none`) token, which no validator may accept.
pub fn unsigned(claims: &Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
    let body = URL_SAFE_NO_PAD.encode(claims.to_string());
    format!("{header}.{body}.")
}

/// A token whose header says `HS256`, signed with `secret`: the algorithm-confusion attack when
/// `secret` is a published public key.
pub fn hmac_signed(kid: &str, secret: &[u8], claims: &Value) -> String {
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(kid.to_string());
    jsonwebtoken::encode(&header, claims, &EncodingKey::from_secret(secret)).expect("sign")
}

/// A loopback HTTP server: `GET` anything answers the JWKS, `POST` anything answers a token
/// response carrying the current token.
pub struct IdentityServer {
    pub addr: SocketAddr,
    jwks: Arc<Mutex<String>>,
    token: Arc<Mutex<Minter>>,
}

/// Makes the token a `POST` answers, fresh on every request.
type Minter = Box<dyn Fn() -> String + Send + Sync>;

impl IdentityServer {
    pub async fn start(jwks: String, token: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let jwks = Arc::new(Mutex::new(jwks));
        let token: Arc<Mutex<Minter>> = Arc::new(Mutex::new(Box::new(move || token.clone())));
        let (served_jwks, served_token) = (Arc::clone(&jwks), Arc::clone(&token));
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let jwks = Arc::clone(&served_jwks);
                let token = Arc::clone(&served_token);
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0u8; 4096];
                    // Read the head and any small body; the routes need neither.
                    loop {
                        let Ok(read) = stream.read(&mut buf).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..read]);
                        if let Some(end) = find(&request, b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                            let length = head
                                .lines()
                                .find_map(|line| line.strip_prefix("content-length:"))
                                .and_then(|value| value.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            if request.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    let body = if request.starts_with(b"POST") {
                        let token = (token.lock().expect("token"))();
                        json!({
                            "access_token": token,
                            "token_type": "bearer",
                            "expires_in": 3600,
                        })
                        .to_string()
                    } else {
                        jwks.lock().expect("jwks").clone()
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                         {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { addr, jwks, token }
    }

    pub fn jwks_url(&self) -> String {
        format!("http://{}/jwks", self.addr)
    }

    pub fn token_url(&self) -> String {
        format!("http://{}/token", self.addr)
    }

    pub fn set_jwks(&self, jwks: String) {
        *self.jwks.lock().expect("jwks") = jwks;
    }

    pub fn set_token(&self, token: String) {
        self.set_minter(move || token.clone());
    }

    /// Answers every token request with a token `mint` makes then.
    pub fn set_minter(&self, mint: impl Fn() -> String + Send + Sync + 'static) {
        *self.token.lock().expect("token") = Box::new(mint);
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
