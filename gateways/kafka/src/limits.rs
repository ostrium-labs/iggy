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
use std::sync::atomic::{AtomicUsize, Ordering};
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
];

/// Default for [`MAX_ADMIN_TOPICS_PER_REQUEST`].
pub const DEFAULT_ADMIN_TOPICS_PER_REQUEST: usize = 100;

/// Distinct topics one `CreateTopics`, `CreatePartitions`, `DescribeConfigs`, `AlterConfigs` or
/// `IncrementalAlterConfigs` request may name. Set once at startup, read by those handlers.
static ADMIN_TOPICS_PER_REQUEST: AtomicUsize = AtomicUsize::new(DEFAULT_ADMIN_TOPICS_PER_REQUEST);

/// The current cap on distinct topics per topic-admin or config request.
#[must_use]
pub fn admin_topics_per_request() -> usize {
    ADMIN_TOPICS_PER_REQUEST.load(Ordering::Relaxed)
}

/// Sets the cap [`admin_topics_per_request`] answers. Called once, before any listener starts.
pub fn set_admin_topics_per_request(topics: usize) {
    ADMIN_TOPICS_PER_REQUEST.store(topics, Ordering::Relaxed);
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
        get(key).map(|raw| parse::<usize>(key, &raw, false)).transpose()
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

/// Reads [`MAX_ADMIN_TOPICS_PER_REQUEST`], or the default when it is unset.
///
/// # Errors
///
/// Returns a message naming the variable when the value does not parse or is 0.
pub fn admin_topics_env(get: impl Fn(&str) -> Option<String>) -> Result<usize, String> {
    get(MAX_ADMIN_TOPICS_PER_REQUEST).map_or(Ok(DEFAULT_ADMIN_TOPICS_PER_REQUEST), |raw| {
        parse(MAX_ADMIN_TOPICS_PER_REQUEST, &raw, false)
    })
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
        assert_eq!(
            admin_topics_env(env(&[])),
            Ok(DEFAULT_ADMIN_TOPICS_PER_REQUEST)
        );
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
        assert!(
            admin_topics_env(env(&[(MAX_ADMIN_TOPICS_PER_REQUEST, "0")]))
                .unwrap_err()
                .contains(MAX_ADMIN_TOPICS_PER_REQUEST)
        );
    }

    #[test]
    fn given_a_minimum_session_above_the_maximum_should_refuse() {
        let error = apply_group_env(
            &mut GroupCoordinatorConfig::default(),
            env(&[(GROUP_MIN_SESSION_TIMEOUT_MS, "60000"), (GROUP_MAX_SESSION_TIMEOUT_MS, "30000")]),
        )
        .unwrap_err();
        assert!(error.contains(GROUP_MIN_SESSION_TIMEOUT_MS), "{error}");
    }

    #[test]
    fn given_an_admin_topic_cap_should_read_it() {
        assert_eq!(
            admin_topics_env(env(&[(MAX_ADMIN_TOPICS_PER_REQUEST, "500")])),
            Ok(500)
        );
    }
}
