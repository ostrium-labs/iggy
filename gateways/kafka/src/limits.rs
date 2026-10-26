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

//! The gateway limits an operator can move, and the Kafka setting each one stands for.
//!
//! Each default is lower than or equal to Kafka's, so a request Kafka accepts can be refused
//! here (#4530). The README's limits table lists every variable next to its Kafka counterpart.

use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

use crate::group::GroupCoordinatorConfig;

/// `group.min.session.timeout.ms`.
pub const GROUP_MIN_SESSION_TIMEOUT_MS: &str = "IGGY_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS";
/// `group.max.session.timeout.ms`.
pub const GROUP_MAX_SESSION_TIMEOUT_MS: &str = "IGGY_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS";
/// No Kafka counterpart: Kafka takes any `rebalance_timeout_ms`.
pub const GROUP_MAX_REBALANCE_TIMEOUT_MS: &str = "IGGY_KAFKA_GROUP_MAX_REBALANCE_TIMEOUT_MS";
/// `group.initial.rebalance.delay.ms`.
pub const GROUP_INITIAL_REBALANCE_DELAY_MS: &str = "IGGY_KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS";
/// No Kafka counterpart: Kafka has no group count cap.
pub const GROUP_MAX_GROUPS: &str = "IGGY_KAFKA_GROUP_MAX_GROUPS";
/// `group.max.size`.
pub const GROUP_MAX_SIZE: &str = "IGGY_KAFKA_GROUP_MAX_SIZE";
/// No Kafka counterpart: members across every group.
pub const GROUP_MAX_TOTAL_MEMBERS: &str = "IGGY_KAFKA_GROUP_MAX_TOTAL_MEMBERS";
/// No Kafka counterpart: one member's subscription, or one assignment, in bytes.
pub const GROUP_MAX_MEMBER_BYTES: &str = "IGGY_KAFKA_GROUP_MAX_MEMBER_BYTES";
/// No Kafka counterpart: the member list one `JoinGroup` leader response carries, in bytes.
pub const GROUP_MAX_ROSTER_BYTES: &str = "IGGY_KAFKA_GROUP_MAX_ROSTER_BYTES";
/// No Kafka counterpart: distinct topics one topic-admin or config request may name.
pub const MAX_ADMIN_TOPICS_PER_REQUEST: &str = "IGGY_KAFKA_MAX_ADMIN_TOPICS_PER_REQUEST";
/// `offset.metadata.max.bytes`.
pub const OFFSET_METADATA_MAX_BYTES: &str = "IGGY_KAFKA_OFFSET_METADATA_MAX_BYTES";
/// `producer.id.expiration.ms`.
pub const PRODUCER_ID_EXPIRATION_MS: &str = "IGGY_KAFKA_PRODUCER_ID_EXPIRATION_MS";
/// No Kafka counterpart: Kafka rebuilds producer state from its own snapshots. The records at a
/// partition's tail a gateway reads to rebuild it (`docs/IDEMPOTENCE.md`).
pub const PRODUCER_STATE_RECOVERY_RECORDS: &str = "IGGY_KAFKA_PRODUCER_STATE_RECOVERY_RECORDS";

/// Every variable this module reads.
pub const KNOWN_ENV_VARS: &[&str] = &[
    GROUP_MIN_SESSION_TIMEOUT_MS,
    GROUP_MAX_SESSION_TIMEOUT_MS,
    GROUP_MAX_REBALANCE_TIMEOUT_MS,
    GROUP_INITIAL_REBALANCE_DELAY_MS,
    GROUP_MAX_GROUPS,
    GROUP_MAX_SIZE,
    GROUP_MAX_TOTAL_MEMBERS,
    GROUP_MAX_MEMBER_BYTES,
    GROUP_MAX_ROSTER_BYTES,
    MAX_ADMIN_TOPICS_PER_REQUEST,
    OFFSET_METADATA_MAX_BYTES,
    PRODUCER_ID_EXPIRATION_MS,
    PRODUCER_STATE_RECOVERY_RECORDS,
];

/// Default for [`MAX_ADMIN_TOPICS_PER_REQUEST`].
///
/// `bounds_guard`'s element ceiling is a pre-decode limit. `CreateTopics` can spend several
/// lockstep Iggy calls per name on the shared client (`ensure_stream`, `create_topic`, and a
/// possible race-retry read), so a request is held to this many distinct names, shared with the
/// config APIs. Duplicate names count once toward the cap and are rejected on their own. A larger
/// batch is `POLICY_VIOLATION` on every resource. `DeleteTopics` counts every name, duplicates
/// included, since each costs its own round trip.
pub const DEFAULT_ADMIN_TOPICS_PER_REQUEST: usize = 100;

/// Default for [`OFFSET_METADATA_MAX_BYTES`]: Kafka's 4096.
pub const DEFAULT_OFFSET_METADATA_MAX_BYTES: usize = 4096;

/// The per-request limits the handlers read from [`crate::protocol::api::GatewayState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLimits {
    /// Distinct topics one `CreateTopics`, `CreatePartitions`, `DescribeConfigs`, `AlterConfigs`
    /// or `IncrementalAlterConfigs` request may name, and names one `DeleteTopics` may list.
    pub admin_topics_per_request: usize,
    /// Kafka's `offset.metadata.max.bytes`: the longest commit metadata string `OffsetCommit`
    /// keeps. A longer one is `OFFSET_METADATA_TOO_LARGE` (12) on its partition. Counted as Kafka
    /// counts it, in UTF-16 code units.
    pub offset_metadata_max_bytes: usize,
    /// Kafka's `producer.id.expiration.ms`: how long an idle producer's state is kept.
    pub producer_id_expiration: Duration,
    /// Records at a partition's tail read to rebuild its producers' state after a restart. A
    /// producer older than that is accepted at any sequence, as Kafka 4 accepts one it forgot.
    pub producer_state_recovery_records: u32,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            admin_topics_per_request: DEFAULT_ADMIN_TOPICS_PER_REQUEST,
            offset_metadata_max_bytes: DEFAULT_OFFSET_METADATA_MAX_BYTES,
            producer_id_expiration: crate::producer_state::DEFAULT_EXPIRY,
            producer_state_recovery_records: crate::producer_state::DEFAULT_RECOVERY_RECORDS,
        }
    }
}

/// Reads the group variables into `config`, leaving the default for each one unset.
///
/// `get` looks a variable up, so a test can feed values without touching the process
/// environment.
///
/// # Errors
///
/// Returns a message naming the variable when a value does not parse, is 0 where 0 would turn the
/// feature off, or contradicts another (a minimum session timeout above the maximum).
pub fn apply_group_env(
    config: &mut GroupCoordinatorConfig,
    get: impl Fn(&str) -> Option<String>,
) -> Result<(), String> {
    let millis = |key: &str, zero_allowed: bool| -> Result<Option<Duration>, String> {
        get(key)
            .map(|raw| parse::<u64>(key, &raw, zero_allowed).map(Duration::from_millis))
            .transpose()
    };
    let count = |key: &str| -> Result<Option<usize>, String> {
        get(key)
            .map(|raw| parse::<usize>(key, &raw, false))
            .transpose()
    };

    if let Some(value) = millis(GROUP_MIN_SESSION_TIMEOUT_MS, false)? {
        config.min_session_timeout = value;
    }
    if let Some(value) = millis(GROUP_MAX_SESSION_TIMEOUT_MS, false)? {
        config.max_session_timeout = value;
    }
    if config.min_session_timeout > config.max_session_timeout {
        return Err(format!(
            "{GROUP_MIN_SESSION_TIMEOUT_MS} ({} ms) is above {GROUP_MAX_SESSION_TIMEOUT_MS} ({} ms): \
             no session timeout could join",
            config.min_session_timeout.as_millis(),
            config.max_session_timeout.as_millis()
        ));
    }
    if let Some(value) = millis(GROUP_MAX_REBALANCE_TIMEOUT_MS, false)? {
        config.max_rebalance_timeout = value;
    }
    // 0 is Kafka's own advice for tests: complete the first join at once.
    if let Some(value) = millis(GROUP_INITIAL_REBALANCE_DELAY_MS, true)? {
        config.initial_rebalance_delay = value;
    }
    if let Some(value) = count(GROUP_MAX_GROUPS)? {
        config.max_groups = value;
    }
    if let Some(value) = count(GROUP_MAX_SIZE)? {
        config.max_members_per_group = value;
    }
    if let Some(value) = count(GROUP_MAX_TOTAL_MEMBERS)? {
        config.max_total_members = value;
    }
    if let Some(value) = count(GROUP_MAX_MEMBER_BYTES)? {
        config.max_member_blob_bytes = value;
    }
    if let Some(value) = count(GROUP_MAX_ROSTER_BYTES)? {
        config.max_group_roster_bytes = value;
    }
    Ok(())
}

/// Reads the per-request and producer-state variables, keeping the default of each one unset.
///
/// # Errors
///
/// Returns a message naming the variable when a value does not parse or is 0. A metadata cap of
/// 0 is refused too: Kafka would then refuse every commit that carries metadata, and no client
/// expects that.
pub fn request_limits_env(get: impl Fn(&str) -> Option<String>) -> Result<RequestLimits, String> {
    let mut limits = RequestLimits::default();
    if let Some(raw) = get(MAX_ADMIN_TOPICS_PER_REQUEST) {
        limits.admin_topics_per_request = parse(MAX_ADMIN_TOPICS_PER_REQUEST, &raw, false)?;
    }
    if let Some(raw) = get(OFFSET_METADATA_MAX_BYTES) {
        limits.offset_metadata_max_bytes = parse(OFFSET_METADATA_MAX_BYTES, &raw, false)?;
    }
    if let Some(raw) = get(PRODUCER_ID_EXPIRATION_MS) {
        limits.producer_id_expiration =
            Duration::from_millis(parse(PRODUCER_ID_EXPIRATION_MS, &raw, false)?);
    }
    if let Some(raw) = get(PRODUCER_STATE_RECOVERY_RECORDS) {
        limits.producer_state_recovery_records =
            parse(PRODUCER_STATE_RECOVERY_RECORDS, &raw, false)?;
    }
    Ok(limits)
}

fn parse<T>(key: &str, raw: &str, zero_allowed: bool) -> Result<T, String>
where
    T: FromStr + Default + PartialEq,
    T::Err: Display,
{
    let value: T = raw
        .parse()
        .map_err(|error| format!("invalid {key} `{raw}`: {error}"))?;
    if !zero_allowed && value == T::default() {
        return Err(format!("{key} must be greater than 0"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn given_no_variables_should_keep_every_default() {
        let mut config = GroupCoordinatorConfig::default();
        apply_group_env(&mut config, env(&[])).unwrap();
        let default = GroupCoordinatorConfig::default();
        assert_eq!(config.max_groups, default.max_groups);
        assert_eq!(config.max_member_blob_bytes, default.max_member_blob_bytes);
        assert_eq!(config.min_session_timeout, default.min_session_timeout);
        assert_eq!(request_limits_env(env(&[])), Ok(RequestLimits::default()));
    }

    #[test]
    fn given_every_group_variable_should_read_each_one() {
        let mut config = GroupCoordinatorConfig::default();
        apply_group_env(
            &mut config,
            env(&[
                (GROUP_MIN_SESSION_TIMEOUT_MS, "1000"),
                (GROUP_MAX_SESSION_TIMEOUT_MS, "60000"),
                (GROUP_MAX_REBALANCE_TIMEOUT_MS, "120000"),
                (GROUP_INITIAL_REBALANCE_DELAY_MS, "0"),
                (GROUP_MAX_GROUPS, "5000"),
                (GROUP_MAX_SIZE, "2000"),
                (GROUP_MAX_TOTAL_MEMBERS, "50000"),
                (GROUP_MAX_MEMBER_BYTES, "1048576"),
                (GROUP_MAX_ROSTER_BYTES, "16777216"),
            ]),
        )
        .unwrap();
        assert_eq!(config.min_session_timeout, Duration::from_secs(1));
        assert_eq!(config.max_session_timeout, Duration::from_mins(1));
        assert_eq!(config.max_rebalance_timeout, Duration::from_mins(2));
        assert_eq!(config.initial_rebalance_delay, Duration::ZERO);
        assert_eq!(config.max_groups, 5000);
        assert_eq!(config.max_members_per_group, 2000);
        assert_eq!(config.max_total_members, 50_000);
        assert_eq!(config.max_member_blob_bytes, 1 << 20);
        assert_eq!(config.max_group_roster_bytes, 16 << 20);
    }

    #[test]
    fn given_a_zero_or_a_typo_should_name_the_variable() {
        for (key, raw) in [
            (GROUP_MAX_GROUPS, "0"),
            (GROUP_MAX_SIZE, "many"),
            (GROUP_MIN_SESSION_TIMEOUT_MS, "0"),
            (GROUP_MAX_MEMBER_BYTES, "-1"),
        ] {
            let error = apply_group_env(&mut GroupCoordinatorConfig::default(), env(&[(key, raw)]))
                .unwrap_err();
            assert!(error.contains(key), "{error}");
        }
        for key in [
            MAX_ADMIN_TOPICS_PER_REQUEST,
            OFFSET_METADATA_MAX_BYTES,
            PRODUCER_ID_EXPIRATION_MS,
            PRODUCER_STATE_RECOVERY_RECORDS,
        ] {
            assert!(
                request_limits_env(env(&[(key, "0")]))
                    .unwrap_err()
                    .contains(key)
            );
        }
    }

    #[test]
    fn given_a_minimum_session_above_the_maximum_should_refuse() {
        let error = apply_group_env(
            &mut GroupCoordinatorConfig::default(),
            env(&[
                (GROUP_MIN_SESSION_TIMEOUT_MS, "60000"),
                (GROUP_MAX_SESSION_TIMEOUT_MS, "30000"),
            ]),
        )
        .unwrap_err();
        assert!(error.contains(GROUP_MIN_SESSION_TIMEOUT_MS), "{error}");
    }

    #[test]
    fn given_request_limits_should_read_each_one() {
        assert_eq!(
            request_limits_env(env(&[
                (MAX_ADMIN_TOPICS_PER_REQUEST, "500"),
                (OFFSET_METADATA_MAX_BYTES, "65536"),
                (PRODUCER_ID_EXPIRATION_MS, "3600000"),
                (PRODUCER_STATE_RECOVERY_RECORDS, "50000"),
            ])),
            Ok(RequestLimits {
                admin_topics_per_request: 500,
                offset_metadata_max_bytes: 65_536,
                producer_id_expiration: Duration::from_hours(1),
                producer_state_recovery_records: 50_000,
            })
        );
    }

    /// The gateway hands the producer-state limits to the producer states it builds.
    #[test]
    fn given_producer_state_limits_the_gateway_state_should_use_them() {
        let state = crate::protocol::api::GatewayState::stub(1024).with_limits(RequestLimits {
            producer_state_recovery_records: 7,
            ..RequestLimits::default()
        });
        assert_eq!(state.producer_states.recovery_records(), 7);
        assert_eq!(state.limits.producer_state_recovery_records, 7);
    }
}
