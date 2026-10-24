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

//! `DescribeUserScramCredentials` (50) and `AlterUserScramCredentials` (51) over the credential
//! log the bridge keeps (`bridge::iggy_bridge::scram_store`).
//!
//! The validation and the per-user error codes are Kafka's own (`ScramControlManager`,
//! `ScramImage`): a user altered twice in one request is `DUPLICATE_RESOURCE`, removing a
//! credential that does not exist is `RESOURCE_NOT_FOUND`, fewer than 4096 or more than 16384
//! iterations or an empty name is `UNACCEPTABLE_CREDENTIAL`, and an unknown mechanism is
//! `UNSUPPORTED_SASL_MECHANISM`.
//!
//! Authorization stands in for Kafka's `CLUSTER` grants: altering needs Iggy's `manage_users`
//! (Kafka's `ALTER` on the cluster), describing `read_users` or `manage_users` (`DESCRIBE`). A
//! principal without them is answered `CLUSTER_AUTHORIZATION_FAILED` for every user. A salted
//! password is turned into the stored keys at once and never kept or logged.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;
use kafka_protocol::messages::describe_user_scram_credentials_response::{
    CredentialInfo, DescribeUserScramCredentialsResult,
};
use kafka_protocol::messages::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse,
};
use kafka_protocol::protocol::StrBytes;
use tracing::{debug, warn};
use zeroize::Zeroize;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::{CredentialChange, IggyBridge};
use crate::protocol::api::{ERROR_NONE, HandleOutcome};
use crate::protocol::handlers::{decode_exhaustive, encode_message, respond_or_close};
use crate::scram::{MAX_ITERATIONS, MIN_ITERATIONS, ScramCredential, ScramMechanism};

pub const ERROR_CLUSTER_AUTHORIZATION_FAILED: i16 =
    ResponseError::ClusterAuthorizationFailed.code();
pub const ERROR_RESOURCE_NOT_FOUND: i16 = ResponseError::ResourceNotFound.code();
pub const ERROR_DUPLICATE_RESOURCE: i16 = ResponseError::DuplicateResource.code();
pub const ERROR_UNACCEPTABLE_CREDENTIAL: i16 = ResponseError::UnacceptableCredential.code();
pub const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = ResponseError::UnsupportedSaslMechanism.code();

/// The only version of either key, as Kafka 4.x serves it.
pub const VERSION: i16 = 0;

/// Largest request body read. A credential change is under a hundred bytes.
const MAX_BODY_BYTES: usize = 256 * 1024;

/// Most users or changes in one request.
const MAX_ENTRIES: usize = 1000;

fn may_describe(principal: Option<&AuthenticatedPrincipal>) -> bool {
    principal.is_some_and(|principal| {
        principal.permissions_known
            && (principal.permissions.read_users || principal.permissions.manage_users)
    })
}

fn may_alter(principal: Option<&AuthenticatedPrincipal>) -> bool {
    principal
        .is_some_and(|principal| principal.permissions_known && principal.permissions.manage_users)
}

fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// Answers `DescribeUserScramCredentials`.
pub async fn describe(
    bridge: &IggyBridge,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if api_version != VERSION || body.len() > MAX_BODY_BYTES {
        return HandleOutcome::Close;
    }
    let Ok(request) = decode_exhaustive::<DescribeUserScramCredentialsRequest>(api_version, body)
    else {
        debug!("malformed DescribeUserScramCredentials request");
        return HandleOutcome::Close;
    };
    let requested: Option<Vec<String>> = request.users.map(|users| {
        users
            .into_iter()
            .map(|user| user.name.to_string())
            .collect()
    });
    if requested
        .as_ref()
        .is_some_and(|users| users.len() > MAX_ENTRIES)
    {
        return HandleOutcome::Close;
    }
    let response = if may_describe(principal) {
        match bridge
            .with_scram_state(|state| describe_results(&state.describe_all(), requested.as_deref()))
            .await
        {
            Ok(results) => DescribeUserScramCredentialsResponse::default().with_results(results),
            Err(error) => {
                warn!(%error, "could not read the SCRAM credential log");
                DescribeUserScramCredentialsResponse::default()
                    .with_error_code(error.to_kafka_error_code())
                    .with_error_message(Some(text("the credential store could not be read")))
            }
        }
    } else {
        DescribeUserScramCredentialsResponse::default()
            .with_error_code(ERROR_CLUSTER_AUTHORIZATION_FAILED)
            .with_error_message(Some(text(
                "describing SCRAM credentials needs the Iggy read_users permission",
            )))
    };
    respond_or_close(
        encode_message(&response, api_version, 256),
        "DescribeUserScramCredentials",
    )
}

/// Kafka's `ScramImage::describe`: every user with a credential when none is named; otherwise
/// each named user once, `DUPLICATE_RESOURCE` for a repeat and `RESOURCE_NOT_FOUND` for one
/// without credentials.
fn describe_results(
    all: &BTreeMap<String, Vec<(ScramMechanism, u32)>>,
    requested: Option<&[String]>,
) -> Vec<DescribeUserScramCredentialsResult> {
    let users: Vec<(String, bool)> = match requested {
        None | Some([]) => all.keys().map(|user| (user.clone(), false)).collect(),
        Some(requested) => {
            let mut seen: BTreeMap<String, bool> = BTreeMap::new();
            for user in requested {
                seen.entry(user.clone())
                    .and_modify(|duplicate| *duplicate = true)
                    .or_insert(false);
            }
            seen.into_iter().collect()
        }
    };
    users
        .into_iter()
        .map(|(user, duplicate)| {
            let result = DescribeUserScramCredentialsResult::default().with_user(text(&user));
            if duplicate {
                return result
                    .with_error_code(ERROR_DUPLICATE_RESOURCE)
                    .with_error_message(Some(text(&format!(
                        "Cannot describe SCRAM credentials for the same user twice in a single \
                         request: {user}"
                    ))));
            }
            match all.get(&user) {
                Some(credentials) => result.with_credential_infos(
                    credentials
                        .iter()
                        .map(|(mechanism, iterations)| {
                            CredentialInfo::default()
                                .with_mechanism(mechanism.type_code())
                                .with_iterations(i32::try_from(*iterations).unwrap_or(i32::MAX))
                        })
                        .collect(),
                ),
                None => result
                    .with_error_code(ERROR_RESOURCE_NOT_FOUND)
                    .with_error_message(Some(text(&format!(
                        "Attempt to describe a user credential that does not exist: {user}"
                    )))),
            }
        })
        .collect()
}

/// One requested change, before validation.
enum Requested {
    Delete {
        user: String,
        mechanism: i8,
    },
    Upsert {
        user: String,
        mechanism: i8,
        iterations: i32,
        salt: Vec<u8>,
        salted_password: Vec<u8>,
    },
}

impl Requested {
    fn user(&self) -> &str {
        match self {
            Self::Delete { user, .. } | Self::Upsert { user, .. } => user,
        }
    }
}

impl Drop for Requested {
    fn drop(&mut self) {
        if let Self::Upsert {
            salted_password, ..
        } = self
        {
            salted_password.zeroize();
        }
    }
}

/// Answers `AlterUserScramCredentials`.
pub async fn alter(
    bridge: &IggyBridge,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if api_version != VERSION || body.len() > MAX_BODY_BYTES {
        return HandleOutcome::Close;
    }
    let Ok(request) = decode_exhaustive::<AlterUserScramCredentialsRequest>(api_version, body)
    else {
        debug!("malformed AlterUserScramCredentials request");
        return HandleOutcome::Close;
    };
    if request.deletions.len() + request.upsertions.len() > MAX_ENTRIES {
        return HandleOutcome::Close;
    }
    let mut changes: Vec<Requested> = request
        .deletions
        .into_iter()
        .map(|deletion| Requested::Delete {
            user: deletion.name.to_string(),
            mechanism: deletion.mechanism,
        })
        .collect();
    changes.extend(
        request
            .upsertions
            .into_iter()
            .map(|upsertion| Requested::Upsert {
                user: upsertion.name.to_string(),
                mechanism: upsertion.mechanism,
                iterations: upsertion.iterations,
                salt: upsertion.salt.to_vec(),
                salted_password: upsertion.salted_password.to_vec(),
            }),
    );

    let results = if may_alter(principal) {
        match bridge
            .alter_scram(|state| plan(&changes, |user, mechanism| state.contains(user, mechanism)))
            .await
        {
            Ok(results) => results,
            Err(error) => {
                warn!(%error, "could not write the SCRAM credential log");
                let code = error.to_kafka_error_code();
                unique_users(&changes)
                    .into_iter()
                    .map(|user| {
                        result(
                            &user,
                            code,
                            Some("the credential store could not be written"),
                        )
                    })
                    .collect()
            }
        }
    } else {
        unique_users(&changes)
            .into_iter()
            .map(|user| {
                result(
                    &user,
                    ERROR_CLUSTER_AUTHORIZATION_FAILED,
                    Some("altering SCRAM credentials needs the Iggy manage_users permission"),
                )
            })
            .collect()
    };
    let response = AlterUserScramCredentialsResponse::default().with_results(results);
    respond_or_close(
        encode_message(&response, api_version, 256),
        "AlterUserScramCredentials",
    )
}

fn unique_users(changes: &[Requested]) -> Vec<String> {
    let mut seen = HashSet::new();
    changes
        .iter()
        .map(Requested::user)
        .filter(|user| seen.insert(*user))
        .map(ToString::to_string)
        .collect()
}

fn result(user: &str, code: i16, message: Option<&str>) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult::default()
        .with_user(text(user))
        .with_error_code(code)
        .with_error_message(message.map(text))
}

/// Kafka's `ScramControlManager::alterCredentials`: deletions first, then upserts, a user named
/// twice in one request refused as a whole, the rest validated one by one. Returns the changes to
/// write and the answer for every user.
fn plan(
    requested: &[Requested],
    exists: impl Fn(&str, ScramMechanism) -> bool,
) -> (Vec<CredentialChange>, Vec<AlterUserScramCredentialsResult>) {
    let mut errors: BTreeMap<String, (i16, String)> = BTreeMap::new();
    let mut accepted: BTreeMap<String, CredentialChange> = BTreeMap::new();
    for change in requested {
        let user = change.user().to_string();
        if errors.contains_key(&user) {
            continue;
        }
        if accepted.remove(&user).is_some() {
            errors.insert(
                user,
                (
                    ERROR_DUPLICATE_RESOURCE,
                    "A user credential cannot be altered twice in the same request".to_string(),
                ),
            );
            continue;
        }
        match validate(change, &exists) {
            Ok(change) => {
                accepted.insert(user, change);
            }
            Err(error) => {
                errors.insert(user, error);
            }
        }
    }
    let mut results: Vec<AlterUserScramCredentialsResult> = accepted
        .keys()
        .map(|user| result(user, ERROR_NONE, None))
        .collect();
    results.extend(
        errors
            .iter()
            .map(|(user, (code, message))| result(user, *code, Some(message))),
    );
    (accepted.into_values().collect(), results)
}

fn validate(
    change: &Requested,
    exists: impl Fn(&str, ScramMechanism) -> bool,
) -> Result<CredentialChange, (i16, String)> {
    let (user, mechanism) = match change {
        Requested::Delete { user, mechanism }
        | Requested::Upsert {
            user, mechanism, ..
        } => (user, *mechanism),
    };
    if user.is_empty() {
        return Err((
            ERROR_UNACCEPTABLE_CREDENTIAL,
            "Username must not be empty".to_string(),
        ));
    }
    let Some(mechanism) = ScramMechanism::from_type(mechanism) else {
        return Err((
            ERROR_UNSUPPORTED_SASL_MECHANISM,
            "Unknown SCRAM mechanism".to_string(),
        ));
    };
    match change {
        Requested::Delete { .. } => {
            if !exists(user, mechanism) {
                return Err((
                    ERROR_RESOURCE_NOT_FOUND,
                    "Attempt to delete a user credential that does not exist".to_string(),
                ));
            }
            Ok(CredentialChange::Delete {
                user: user.clone(),
                mechanism,
            })
        }
        Requested::Upsert {
            iterations,
            salt,
            salted_password,
            ..
        } => {
            let iterations = u32::try_from(*iterations).unwrap_or(0);
            if iterations < MIN_ITERATIONS {
                return Err((
                    ERROR_UNACCEPTABLE_CREDENTIAL,
                    "Too few iterations".to_string(),
                ));
            }
            if iterations > MAX_ITERATIONS {
                return Err((
                    ERROR_UNACCEPTABLE_CREDENTIAL,
                    "Too many iterations".to_string(),
                ));
            }
            if salt.is_empty() || salted_password.is_empty() {
                // Kafka accepts these and stores a credential nobody can log in with; refusing
                // says so instead.
                return Err((
                    ERROR_UNACCEPTABLE_CREDENTIAL,
                    "Salt and salted password must not be empty".to_string(),
                ));
            }
            Ok(CredentialChange::Upsert {
                user: user.clone(),
                mechanism,
                credential: ScramCredential::from_salted_password(
                    mechanism,
                    salt.clone(),
                    iterations,
                    salted_password,
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upsert(user: &str, mechanism: i8, iterations: i32) -> Requested {
        Requested::Upsert {
            user: user.to_string(),
            mechanism,
            iterations,
            salt: vec![1; 16],
            salted_password: vec![2; 32],
        }
    }

    fn codes(results: &[AlterUserScramCredentialsResult]) -> BTreeMap<String, i16> {
        results
            .iter()
            .map(|result| (result.user.to_string(), result.error_code))
            .collect()
    }

    #[test]
    fn given_a_mixed_request_should_answer_each_user_as_kafka_does() {
        let requested = vec![
            Requested::Delete {
                user: "gone".to_string(),
                mechanism: 1,
            },
            Requested::Delete {
                user: "kept".to_string(),
                mechanism: 1,
            },
            upsert("alice", 1, 4096),
            upsert("twice", 1, 4096),
            upsert("twice", 2, 4096),
            upsert("few", 1, 4095),
            upsert("many", 2, 16385),
            upsert("", 1, 4096),
            upsert("odd", 9, 4096),
        ];
        let (changes, results) = plan(&requested, |user, _| user == "kept");
        let codes = codes(&results);
        assert_eq!(codes["gone"], ERROR_RESOURCE_NOT_FOUND);
        assert_eq!(codes["kept"], ERROR_NONE);
        assert_eq!(codes["alice"], ERROR_NONE);
        assert_eq!(codes["twice"], ERROR_DUPLICATE_RESOURCE);
        assert_eq!(codes["few"], ERROR_UNACCEPTABLE_CREDENTIAL);
        assert_eq!(codes["many"], ERROR_UNACCEPTABLE_CREDENTIAL);
        assert_eq!(codes[""], ERROR_UNACCEPTABLE_CREDENTIAL);
        assert_eq!(codes["odd"], ERROR_UNSUPPORTED_SASL_MECHANISM);
        assert_eq!(
            changes.len(),
            2,
            "only kept's removal and alice's upsert are written"
        );
    }

    #[test]
    fn given_describe_filters_should_report_missing_and_duplicate_users() {
        let mut all = BTreeMap::new();
        all.insert(
            "alice".to_string(),
            vec![
                (ScramMechanism::Sha256, 4096),
                (ScramMechanism::Sha512, 8192),
            ],
        );
        all.insert("bob".to_string(), vec![(ScramMechanism::Sha512, 4096)]);

        let everyone = describe_results(&all, None);
        assert_eq!(everyone.len(), 2);
        assert_eq!(everyone[0].credential_infos.len(), 2);

        let named = describe_results(
            &all,
            Some(&["bob".to_string(), "ghost".to_string(), "bob".to_string()]),
        );
        let by_user: BTreeMap<String, i16> = named
            .iter()
            .map(|result| (result.user.to_string(), result.error_code))
            .collect();
        assert_eq!(by_user["bob"], ERROR_DUPLICATE_RESOURCE);
        assert_eq!(by_user["ghost"], ERROR_RESOURCE_NOT_FOUND);
    }
}
