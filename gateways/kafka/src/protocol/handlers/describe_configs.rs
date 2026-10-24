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

//! `DescribeConfigs` (API key 32).

use std::collections::HashSet;

use bytes::Bytes;
use kafka_protocol::messages::DescribeConfigsRequest;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
};
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{DEFAULT_TOPIC_PARTITIONS, IggyBridge, StreamTopicCache, TopicLoad};
use crate::protocol::api::{
    API_KEY_DESCRIBE_CONFIGS, ApiVersionRange, ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION,
    ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_describe_configs_shape;
use crate::protocol::handlers::create_topics::SINGLE_BROKER_ID;
use crate::protocol::handlers::kafka_topic_configs::{
    ConfigType, TOPIC_CONFIGS, TopicConfigSnapshot,
};
use crate::protocol::handlers::topic_config::{
    CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_TOPIC, RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC,
    bridge_failure, name_reason, topic_cap_and_duplicates, topic_cap_message,
};
use crate::protocol::handlers::{
    decode_guarded, encode_message, is_supported_version, respond_or_close,
    unsupported_version_response,
};

const ONLY_TOPIC_AND_BROKER_RESOURCES: &str = "only topic and broker resources are supported";

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DESCRIBE_CONFIGS,
    min_version: 1,
    max_version: 4,
};

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_DESCRIBE_CONFIGS, api_version) {
        return unsupported_version_response(API_KEY_DESCRIBE_CONFIGS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<DescribeConfigsRequest>(api_version, body, |version, body| {
        validate_describe_configs_shape(version, body, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            tracing::debug!(%error, "Failed to decode DescribeConfigs request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "DescribeConfigs",
            );
        }
    };

    // Checked before the bridge-availability stub, not inside `describe_all`: the cap is a
    // request-shape policy, not a controller-availability one, and a check that only runs once
    // the bridge is up is never exercised by a serverless/stub test.
    let (exceeds_cap, duplicate_names) = topic_cap_and_duplicates(&req.resources);
    if exceeds_cap {
        let message = StrBytes::from(topic_cap_message("DescribeConfigs"));
        tracing::warn!("DescribeConfigs request addresses too many distinct topics; rejecting");
        let results = req
            .resources
            .iter()
            .map(|resource| {
                resource_error(
                    resource,
                    ERROR_POLICY_VIOLATION,
                    Some(message.clone()),
                    Vec::new(),
                )
            })
            .collect();
        return respond_or_close(
            encode_message(&response(results), api_version, 256),
            "DescribeConfigs",
        );
    }

    let Some(bridge) = &state.bridge else {
        let results = req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_NOT_CONTROLLER, None, Vec::new()))
            .collect();
        return respond_or_close(
            encode_message(&response(results), api_version, 256),
            "DescribeConfigs",
        );
    };

    let deadline = Instant::now() + REQUEST_DEADLINE;
    let results = describe_all(state, bridge, &req, &duplicate_names, deadline).await;
    respond_or_close(
        encode_message(&response(results), api_version, 256),
        "DescribeConfigs",
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> crate::error::Result<Bytes> {
    let results = vec![
        DescribeConfigsResult::default()
            .with_error_code(error_code)
            .with_error_message(None)
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_static_str(""))
            .with_configs(Vec::new()),
    ];
    encode_message(&response(results), version, 128)
}

fn response(results: Vec<DescribeConfigsResult>) -> DescribeConfigsResponse {
    DescribeConfigsResponse::default()
        .with_throttle_time_ms(0)
        .with_results(results)
}

async fn describe_all(
    state: &GatewayState,
    bridge: &IggyBridge,
    req: &DescribeConfigsRequest,
    duplicate_names: &HashSet<&str>,
    deadline: Instant,
) -> Vec<DescribeConfigsResult> {
    let mut cache = StreamTopicCache::default();
    let mut results = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    for resource in &req.resources {
        if resource.resource_type == RESOURCE_TYPE_TOPIC
            && duplicate_names.contains(resource.resource_name.as_str())
        {
            results.push(resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                None,
                Vec::new(),
            ));
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "DescribeConfigs deadline passed; answering remaining resources retriable \
                     instead of starting new Iggy calls"
                );
            }
            results.push(resource_error(
                resource,
                ERROR_REQUEST_TIMED_OUT,
                None,
                Vec::new(),
            ));
            continue;
        }

        let result = describe_one(
            state,
            bridge,
            &mut cache,
            resource,
            req.include_documentation,
            deadline,
        )
        .await;
        if Instant::now() >= deadline {
            deadline_exceeded = true;
        }
        results.push(result);
    }
    results
}

async fn describe_one(
    state: &GatewayState,
    bridge: &IggyBridge,
    cache: &mut StreamTopicCache,
    resource: &DescribeConfigsResource,
    include_documentation: bool,
    deadline: Instant,
) -> DescribeConfigsResult {
    if resource.resource_type == RESOURCE_TYPE_BROKER {
        return describe_broker(state, resource, include_documentation);
    }
    if resource.resource_type != RESOURCE_TYPE_TOPIC {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from_static_str(ONLY_TOPIC_AND_BROKER_RESOURCES)),
            Vec::new(),
        );
    }
    let name = resource.resource_name.as_str();
    if let Err(error) = validate_kafka_topic_name("kafka_topic", name) {
        return resource_error(
            resource,
            ERROR_INVALID_TOPIC_EXCEPTION,
            Some(StrBytes::from(name_reason(&error))),
            Vec::new(),
        );
    }

    let snapshot = match cache
        .lookup(bridge, name, deadline, "DescribeConfigs")
        .await
    {
        TopicLoad::Found(snapshot) => snapshot,
        TopicLoad::Missing => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None, Vec::new());
        }
        TopicLoad::Failed(error) => {
            let (code, message) = bridge_failure(error, "DescribeConfigs", "reading");
            return resource_error(resource, code, message, Vec::new());
        }
        TopicLoad::NotStarted | TopicLoad::TimedOut => {
            return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None, Vec::new());
        }
    };

    let (configs, unrepresentable) = described_entries(
        snapshot,
        resource.configuration_keys.as_deref(),
        include_documentation,
    );

    if unrepresentable {
        return resource_error(
            resource,
            ERROR_INVALID_CONFIG,
            Some(StrBytes::from_static_str(
                "stored message expiry is not a whole number of milliseconds",
            )),
            configs,
        );
    }
    resource_error(resource, ERROR_NONE, None, configs)
}

/// Whether `name` is selected by `configuration_keys`: a null or empty list selects every
/// name.
fn selected(configuration_keys: Option<&[StrBytes]>, name: &str) -> bool {
    configuration_keys
        .filter(|keys| !keys.is_empty())
        .is_none_or(|keys| keys.iter().any(|key| key.as_str() == name))
}

/// Every Kafka topic configuration `configuration_keys` selects, sorted by name, and whether the
/// stored expiry could not be shown as `retention.ms`.
///
/// A name Kafka does not define is omitted, as Kafka omits it, and a name repeated in the
/// request is returned once. A configuration the topic sets is `DYNAMIC_TOPIC_CONFIG`, any other
/// `DEFAULT_CONFIG` with the value this gateway applies.
#[must_use]
fn described_entries(
    snapshot: &TopicConfigSnapshot,
    configuration_keys: Option<&[StrBytes]>,
    include_documentation: bool,
) -> (Vec<DescribeConfigsResourceResult>, bool) {
    let mut unrepresentable = false;
    let mut configs = Vec::new();
    for key in TOPIC_CONFIGS
        .iter()
        .filter(|key| selected(configuration_keys, key.name))
    {
        let (value, source) = match snapshot.describe(key) {
            Ok((value, explicit)) => (
                Some(value),
                if explicit {
                    CONFIG_SOURCE_TOPIC
                } else {
                    CONFIG_SOURCE_DEFAULT
                },
            ),
            // A stored duration that cannot be shown as retention.ms is still the topic's
            // expiry, not the never-expire default.
            Err(()) => {
                unrepresentable = true;
                (None, CONFIG_SOURCE_TOPIC)
            }
        };
        configs.push(config_entry(
            key.name,
            value.as_deref(),
            false,
            source,
            key.config_type.code(),
            include_documentation.then_some(key.documentation),
        ));
    }
    (configs, unrepresentable)
}

/// The fixed configuration this gateway reports for its one broker, node 1.
///
/// Strimzi's Topic Operator and UIs read these at startup. They describe what the gateway does;
/// none can be altered. `inter.broker.protocol.version` is left out on purpose: kafka-ui picks
/// its alter API from it.
fn broker_configs(state: &GatewayState) -> [(&'static str, String, ConfigType); 11] {
    [
        (
            "auto.create.topics.enable",
            state.auto_topic_creation.is_some().to_string(),
            ConfigType::Boolean,
        ),
        (
            "compression.type",
            "producer".to_string(),
            ConfigType::String,
        ),
        (
            "default.replication.factor",
            "1".to_string(),
            ConfigType::Int,
        ),
        (
            "delete.topic.enable",
            "true".to_string(),
            ConfigType::Boolean,
        ),
        ("log.cleanup.policy", "delete".to_string(), ConfigType::List),
        ("log.retention.bytes", "-1".to_string(), ConfigType::Long),
        ("log.retention.ms", "-1".to_string(), ConfigType::Long),
        (
            "log.segment.bytes",
            iggy::prelude::DEFAULT_SEGMENT_SIZE.to_string(),
            ConfigType::Int,
        ),
        ("min.insync.replicas", "1".to_string(), ConfigType::Int),
        (
            "num.partitions",
            DEFAULT_TOPIC_PARTITIONS.to_string(),
            ConfigType::Int,
        ),
        (
            "unclean.leader.election.enable",
            "false".to_string(),
            ConfigType::Boolean,
        ),
    ]
}

fn describe_broker(
    state: &GatewayState,
    resource: &DescribeConfigsResource,
    include_documentation: bool,
) -> DescribeConfigsResult {
    let name = resource.resource_name.as_str();
    // An empty name is the cluster-wide dynamic default set, and this gateway has none.
    if name.is_empty() {
        return resource_error(resource, ERROR_NONE, None, Vec::new());
    }
    if name != SINGLE_BROKER_ID.to_string() {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from(format!(
                "this gateway is broker {SINGLE_BROKER_ID}, not {name}"
            ))),
            Vec::new(),
        );
    }
    let configs = broker_configs(state)
        .into_iter()
        .filter(|(key, _, _)| selected(resource.configuration_keys.as_deref(), key))
        .map(|(key, value, config_type)| {
            config_entry(
                key,
                Some(&value),
                true,
                CONFIG_SOURCE_DEFAULT,
                config_type.code(),
                include_documentation.then_some(BROKER_DOC),
            )
        })
        .collect();
    resource_error(resource, ERROR_NONE, None, configs)
}

const BROKER_DOC: &str =
    "Fixed by the Kafka gateway; it describes what the gateway does and cannot be altered.";

fn config_entry(
    name: &str,
    value: Option<&str>,
    read_only: bool,
    source: i8,
    config_type: i8,
    documentation: Option<&str>,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult::default()
        .with_name(StrBytes::from(name.to_string()))
        .with_value(value.map(|value| StrBytes::from(value.to_string())))
        .with_read_only(read_only)
        .with_config_source(source)
        .with_is_sensitive(false)
        .with_synonyms(Vec::new())
        .with_config_type(config_type)
        .with_documentation(documentation.map(|text| StrBytes::from(text.to_string())))
}

fn resource_error(
    resource: &DescribeConfigsResource,
    error_code: i16,
    error_message: Option<StrBytes>,
    configs: Vec<DescribeConfigsResourceResult>,
) -> DescribeConfigsResult {
    DescribeConfigsResult::default()
        .with_error_code(error_code)
        .with_error_message(error_message)
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
        .with_configs(configs)
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
    use kafka_protocol::protocol::StrBytes;

    use iggy::prelude::{CleanupPolicy, IggyDuration, IggyExpiry};

    use super::described_entries;
    use crate::protocol::handlers::kafka_topic_configs::{
        CLEANUP_POLICY, RETENTION_MS, TOPIC_CONFIGS, TopicConfigSnapshot,
    };
    use crate::protocol::handlers::topic_config::{CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_TOPIC};

    fn expiring(millis: u64) -> TopicConfigSnapshot {
        TopicConfigSnapshot {
            message_expiry: IggyExpiry::ExpireDuration(IggyDuration::from(millis * 1_000)),
            message_expiry_explicit: true,
            ..TopicConfigSnapshot::default()
        }
    }

    fn entry<'a>(
        configs: &'a [DescribeConfigsResourceResult],
        name: &str,
    ) -> Option<&'a DescribeConfigsResourceResult> {
        configs.iter().find(|config| config.name.as_str() == name)
    }

    fn value_of<'a>(configs: &'a [DescribeConfigsResourceResult], name: &str) -> Option<&'a str> {
        entry(configs, name).and_then(|config| config.value.as_ref().map(StrBytes::as_str))
    }

    #[test]
    fn default_describe_returns_every_kafka_topic_config_sorted() {
        let (configs, bad) = described_entries(&expiring(120_000), None, false);
        assert!(!bad);
        let names: Vec<&str> = configs.iter().map(|config| config.name.as_str()).collect();
        let expected: Vec<&str> = TOPIC_CONFIGS.iter().map(|key| key.name).collect();
        assert_eq!(names, expected);
        assert_eq!(value_of(&configs, RETENTION_MS), Some("120000"));
        assert_eq!(
            entry(&configs, RETENTION_MS).map(|config| config.config_source),
            Some(CONFIG_SOURCE_TOPIC)
        );
        assert_eq!(value_of(&configs, CLEANUP_POLICY), Some("delete"));
        assert_eq!(
            entry(&configs, CLEANUP_POLICY).map(|config| config.config_source),
            Some(CONFIG_SOURCE_DEFAULT)
        );
        assert!(configs.iter().all(|config| !config.read_only));
    }

    #[test]
    fn a_stored_policy_is_reported_as_set_on_the_topic() {
        let snapshot = TopicConfigSnapshot {
            cleanup_policy: Some(CleanupPolicy::Compact),
            ..TopicConfigSnapshot::default()
        };
        let asked = [StrBytes::from_static_str(CLEANUP_POLICY)];
        let (configs, _) = described_entries(&snapshot, Some(&asked), false);
        assert_eq!(configs.len(), 1);
        assert_eq!(value_of(&configs, CLEANUP_POLICY), Some("compact"));
        assert_eq!(configs[0].config_source, CONFIG_SOURCE_TOPIC);
    }

    #[test]
    fn synonyms_are_always_empty() {
        let (configs, _) = described_entries(&expiring(120_000), None, false);
        assert!(configs.iter().all(|config| config.synonyms.is_empty()));
    }

    #[test]
    fn requested_unknown_names_are_omitted_and_known_keys_remain() {
        let asked = [
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str("no.such"),
            StrBytes::from_static_str("retention.minutes"),
            StrBytes::from_static_str("retention.hours"),
        ];
        let (configs, bad) = described_entries(&expiring(1_500), Some(&asked), false);
        assert!(!bad);
        assert_eq!(configs.len(), 1);
        assert_eq!(value_of(&configs, RETENTION_MS), Some("1500"));
    }

    #[test]
    fn a_name_repeated_in_the_request_is_returned_once() {
        let asked = [
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str(RETENTION_MS),
        ];
        let (configs, _) = described_entries(&expiring(1_500), Some(&asked), false);
        assert_eq!(configs.len(), 1);
    }

    #[test]
    fn never_expire_reports_negative_one() {
        let (configs, bad) = described_entries(&TopicConfigSnapshot::default(), None, false);
        assert!(!bad);
        assert_eq!(value_of(&configs, RETENTION_MS), Some("-1"));
    }

    #[test]
    fn a_sub_millisecond_expiry_fails_the_resource_but_keeps_the_entry() {
        let snapshot = TopicConfigSnapshot {
            message_expiry: IggyExpiry::ExpireDuration(IggyDuration::from(1_500_u64)),
            message_expiry_explicit: true,
            ..TopicConfigSnapshot::default()
        };
        let (configs, bad) = described_entries(&snapshot, None, false);
        assert!(bad);
        assert_eq!(
            entry(&configs, RETENTION_MS).map(|config| config.value.is_none()),
            Some(true)
        );
    }

    #[test]
    fn documentation_is_attached_only_when_asked() {
        let (configs, _) = described_entries(&expiring(1_500), None, true);
        assert!(configs.iter().all(|config| config.documentation.is_some()));
        let (configs, _) = described_entries(&expiring(1_500), None, false);
        assert!(configs.iter().all(|config| config.documentation.is_none()));
    }
}
