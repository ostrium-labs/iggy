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

//! Request handling shared by `DescribeConfigs`, `AlterConfigs` and
//! `IncrementalAlterConfigs`: resource caps, duplicate names, authorization and the shape of
//! incremental operations. The configurations themselves, and how each maps onto Iggy, are in
//! [`super::kafka_topic_configs`].

use std::collections::HashSet;

use iggy::prelude::IggyError;
use kafka_protocol::messages::alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource as IncrementalAlterConfigsResource;
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::auth::AuthenticatedPrincipal;
use crate::authz::TopicOp;
use crate::bridge::{BridgeError, IggyBridge};
use crate::protocol::api::{
    ERROR_INVALID_CONFIG, ERROR_NONE, ERROR_POLICY_VIOLATION, ERROR_REQUEST_TIMED_OUT,
};
use crate::protocol::handlers::kafka_topic_configs::TopicWrite;
use crate::protocol::handlers::may_topic_on;

/// Kafka `ConfigResource.Type.TOPIC`.
pub const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Kafka `ConfigSource.DYNAMIC_TOPIC_CONFIG`: the value was stored on the topic.
pub const CONFIG_SOURCE_TOPIC: i8 = 1;

/// Kafka `ConfigSource.DEFAULT_CONFIG`: the value is the gateway default.
pub const CONFIG_SOURCE_DEFAULT: i8 = 5;

/// Kafka `ConfigResource.Type.BROKER`.
pub const RESOURCE_TYPE_BROKER: i8 = 4;

pub const ONLY_TOPIC_RESOURCES: &str = "only topic resources are supported";

const NOT_AUTHORIZED_TO_ALTER: &str =
    "the authenticated principal is not authorized to alter topic configuration";

pub use super::kafka_topic_configs::ConfigFault;

/// Kafka `AlterConfigOp.OpType`, one per `IncrementalAlterConfigs` entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOperation {
    Set,
    Delete,
    Append,
    Subtract,
}

impl ConfigOperation {
    /// The spelling Kafka's own error text uses.
    #[must_use]
    pub const fn kafka_name(self) -> &'static str {
        match self {
            Self::Set => "SET",
            Self::Delete => "DELETE",
            Self::Append => "APPEND",
            Self::Subtract => "SUBTRACT",
        }
    }
}

impl TryFrom<i8> for ConfigOperation {
    /// The byte that matched nothing, for the client-facing text.
    type Error = i8;

    fn try_from(operation: i8) -> Result<Self, Self::Error> {
        match operation {
            0 => Ok(Self::Set),
            1 => Ok(Self::Delete),
            2 => Ok(Self::Append),
            3 => Ok(Self::Subtract),
            other => Err(other),
        }
    }
}

/// One `IncrementalAlterConfigs` entry that passed [`incremental_ops`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOp<'a> {
    pub name: &'a str,
    pub operation: ConfigOperation,
    pub value: Option<&'a str>,
}

/// Why one resource's incremental alter is refused before any Iggy call. Every variant is
/// `INVALID_REQUEST` (42). The duplicate-key and null-value texts are Kafka's own; the
/// unknown-operation text is borrowed from Kafka's broker-resource check, because Kafka's topic
/// path has no well-formed answer for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestFault {
    DuplicateKeys,
    NullValues(Vec<String>),
    UnknownOperation(i8),
}

impl RequestFault {
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::DuplicateKeys => "Error due to duplicate config keys".to_string(),
            Self::NullValues(names) => {
                format!("Null value not supported for : {}", names.join(", "))
            }
            Self::UnknownOperation(operation) => format!("Unknown operations type {operation}"),
        }
    }
}

/// Whether `names` contains more than `max` distinct strings.
#[must_use]
pub fn exceeds_topic_cap<'a>(names: impl IntoIterator<Item = &'a str>, max: usize) -> bool {
    names.into_iter().collect::<HashSet<_>>().len() > max
}

/// Client-facing text for a request over a cap of `max` topics.
#[must_use]
pub fn topic_cap_message(api_name: &str, max: usize) -> String {
    format!("this gateway addresses at most {max} distinct topics per {api_name} request")
}

/// Names in `names` that occur more than once.
///
/// Real Kafka refuses a duplicate resource name with `INVALID_REQUEST` (42) rather than
/// silently picking a winner. Shared by `CreateTopics`, `CreatePartitions`, `DescribeConfigs`
/// and `AlterConfigs`. `CreatePartitions` answers once per repeated name, as Kafka's
/// `ControllerApis.createPartitions` does, the others once per occurrence.
#[must_use]
pub fn find_duplicate_names<'a>(names: impl IntoIterator<Item = &'a str>) -> HashSet<&'a str> {
    let mut seen = HashSet::new();
    let mut duplicates = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            duplicates.insert(name);
        }
    }
    duplicates
}

/// A resource `DescribeConfigs` or `AlterConfigs` can filter down to the ones naming a topic,
/// and check for the cap and for a duplicate name. The two APIs' generated request resource
/// types have no shared trait of their own, so this is the smallest common surface letting
/// [`topic_cap_and_duplicates`] serve both without either handler re-walking its resource list.
pub trait TopicResource {
    fn kafka_resource_type(&self) -> i8;
    fn kafka_resource_name(&self) -> &str;
}

impl TopicResource for AlterConfigsResource {
    fn kafka_resource_type(&self) -> i8 {
        self.resource_type
    }

    fn kafka_resource_name(&self) -> &str {
        self.resource_name.as_str()
    }
}

impl TopicResource for DescribeConfigsResource {
    fn kafka_resource_type(&self) -> i8 {
        self.resource_type
    }

    fn kafka_resource_name(&self) -> &str {
        self.resource_name.as_str()
    }
}

impl TopicResource for IncrementalAlterConfigsResource {
    fn kafka_resource_type(&self) -> i8 {
        self.resource_type
    }

    fn kafka_resource_name(&self) -> &str {
        self.resource_name.as_str()
    }
}

/// The cap/duplicate verdict shared by `DescribeConfigs` and `AlterConfigs`: both filter
/// resources down to [`RESOURCE_TYPE_TOPIC`] before deciding whether the request exceeds
/// `max` distinct topics or repeats a name, and the business decision is identical even
/// though the two APIs render it into different response types.
#[must_use]
pub fn topic_cap_and_duplicates<R: TopicResource>(
    resources: &[R],
    max: usize,
) -> (bool, HashSet<&str>) {
    let names: Vec<&str> = resources
        .iter()
        .filter(|resource| resource.kafka_resource_type() == RESOURCE_TYPE_TOPIC)
        .map(TopicResource::kafka_resource_name)
        .collect();
    (
        exceeds_topic_cap(names.iter().copied(), max),
        find_duplicate_names(names.iter().copied()),
    )
}

/// Whether `principal` may alter the configuration of one resource.
///
/// A topic needs Iggy's `manage_topic` on it (Kafka's `ALTER_CONFIGS` on the topic,
/// `crate::authz`); any other resource the global `manage_topics`, as before per-topic grants
/// were enforced. No principal means no listener of this gateway names principals, so there is
/// nothing to enforce against. Once one does, a connection that proved no identity carries
/// [`AuthenticatedPrincipal::anonymous`] instead and is denied here. Permissions that were never
/// read deny: granting would authorize a write off a value nothing read.
#[must_use]
pub fn may_alter(
    bridge: Option<&IggyBridge>,
    principal: Option<&AuthenticatedPrincipal>,
    resource: &impl TopicResource,
    api_name: &str,
) -> bool {
    let Some(known) = principal else {
        return true;
    };
    if !known.permissions_known {
        tracing::warn!(
            principal = %known.username,
            "{api_name} asked on a connection whose permissions were never read; denying"
        );
        return false;
    }
    if resource.kafka_resource_type() == RESOURCE_TYPE_TOPIC {
        may_topic_on(
            bridge,
            principal,
            resource.kafka_resource_name(),
            TopicOp::Manage,
        )
    } else {
        known.permissions.manage_topics
    }
}

/// The message a resource denied by [`may_alter`] carries.
#[must_use]
pub const fn not_authorized_to_alter() -> StrBytes {
    StrBytes::from_static_str(NOT_AUTHORIZED_TO_ALTER)
}

/// The reason a topic name failed Kafka's own naming rules, or a fixed fallback.
#[must_use]
pub fn name_reason(error: &BridgeError) -> String {
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => reason.clone(),
        _ => "invalid topic name".to_string(),
    }
}

/// Maps a bridge failure to a Kafka error code and client-facing message, logging the real
/// cause server-side.
///
/// `handler` names the API in the log line (`"DescribeConfigs"`/`"AlterConfigs"`); `action` is
/// the present participle of what the bridge call was doing (`"reading"`/`"altering"`), reused
/// in both the timeout log and the generic internal-error message.
///
/// Only [`BridgeError::Timeout`] gets its own arm. `InvalidKafkaTopicName` and `SendLost` are
/// unreachable here: the handler already validated the Kafka name before calling the bridge (so
/// the bridge's own, redundant validation inside `get_kafka_topic`/`update_kafka_topic_message_expiry`
/// cannot fail), and `SendLost` is only ever produced by the Produce path (`iggy_bridge/produce.rs`),
/// never by a config read or write. Both fall into the generic arm below rather than keeping a
/// branch no call site here can reach.
#[must_use]
pub fn bridge_failure(error: &BridgeError, handler: &str, action: &str) -> (i16, Option<StrBytes>) {
    let code = error.to_kafka_error_code();
    match error {
        BridgeError::Timeout => {
            tracing::warn!(%error, "{handler} {action} exceeded the bridge deadline");
            (code, None)
        }
        other => {
            tracing::error!(%other, "{handler} failed while {action} topic configuration");
            (
                code,
                Some(StrBytes::from(format!(
                    "internal error {action} topic configuration"
                ))),
            )
        }
    }
}

/// The Kafka error code and message for a configuration `fault`.
#[must_use]
pub fn fault_response(fault: &ConfigFault) -> (i16, Option<StrBytes>) {
    let code = if fault.is_policy() {
        ERROR_POLICY_VIOLATION
    } else {
        ERROR_INVALID_CONFIG
    };
    (code, Some(StrBytes::from(fault.message())))
}

/// Writes `write` onto the topic behind `kafka_topic`, answering the resource's error code and
/// message. A write that changes nothing calls nothing and answers `NONE`.
///
/// Not started past `deadline` and not wrapped in `timeout_at`: dropping the future does not
/// abort the SDK call, so `update_topic` could commit after this handler answered
/// `REQUEST_TIMED_OUT`, and a short retention would then delete sealed segments. The bridge
/// bounds its own round trip with `with_request_timeout`.
pub async fn write_configs(
    bridge: &IggyBridge,
    kafka_topic: &str,
    write: &TopicWrite,
    deadline: Instant,
    api_name: &str,
) -> (i16, Option<StrBytes>) {
    if write.is_empty() {
        return (ERROR_NONE, None);
    }
    if Instant::now() >= deadline {
        tracing::warn!(
            resource = kafka_topic,
            "{api_name} deadline passed before update_topic; not starting the write"
        );
        return (ERROR_REQUEST_TIMED_OUT, None);
    }
    match bridge.update_kafka_topic_configs(kafka_topic, write).await {
        Ok(()) | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => (ERROR_NONE, None),
        Err(error) => {
            refused_option(&error).unwrap_or_else(|| bridge_failure(&error, api_name, "altering"))
        }
    }
}

/// `INVALID_CONFIG` for an option value the Iggy server refused, `None` for any other failure.
#[must_use]
pub fn refused_option(error: &BridgeError) -> Option<(i16, Option<StrBytes>)> {
    match error {
        BridgeError::Iggy(
            IggyError::InvalidOptionValue(_)
            | IggyError::UnsupportedOptionKey(_)
            | IggyError::OptionNotUpdatable(_)
            | IggyError::OptionsBlockTooLarge(_),
        ) => {
            tracing::debug!(%error, "Iggy refused a topic configuration");
            Some((
                ERROR_INVALID_CONFIG,
                Some(StrBytes::from_static_str(
                    "the Iggy server refused this topic configuration",
                )),
            ))
        }
        _ => None,
    }
}

/// The entries of one `IncrementalAlterConfigs` resource, checked in the order Kafka's broker
/// checks them before it forwards the request: a repeated name, a null value on anything but a
/// delete, then an operation byte it does not know.
///
/// # Errors
///
/// Returns the first [`RequestFault`] in that order.
pub fn incremental_ops<'a>(
    configs: impl IntoIterator<Item = (&'a str, i8, Option<&'a str>)>,
) -> Result<Vec<ConfigOp<'a>>, RequestFault> {
    let configs: Vec<(&str, i8, Option<&str>)> = configs.into_iter().collect();
    let mut seen = HashSet::new();
    if configs.iter().any(|(name, _, _)| !seen.insert(*name)) {
        return Err(RequestFault::DuplicateKeys);
    }
    let null_values: Vec<String> = configs
        .iter()
        .filter(|(_, operation, value)| {
            value.is_none() && ConfigOperation::try_from(*operation) != Ok(ConfigOperation::Delete)
        })
        .map(|(name, _, _)| (*name).to_string())
        .collect();
    if !null_values.is_empty() {
        return Err(RequestFault::NullValues(null_values));
    }
    configs
        .into_iter()
        .map(|(name, operation, value)| {
            let operation =
                ConfigOperation::try_from(operation).map_err(RequestFault::UnknownOperation)?;
            Ok(ConfigOp {
                name,
                operation,
                value,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::handlers::kafka_topic_configs::{CLEANUP_POLICY, RETENTION_MS};

    #[test]
    fn find_duplicate_names_finds_a_name_repeated_across_two_resources() {
        let duplicates = find_duplicate_names(["orders", "payments", "orders"]);
        assert_eq!(duplicates, HashSet::from(["orders"]));
    }

    #[test]
    fn find_duplicate_names_is_empty_when_every_name_is_unique() {
        assert!(find_duplicate_names(["orders", "payments"]).is_empty());
    }

    #[test]
    fn incremental_ops_rejects_a_repeated_name_before_a_null_value() {
        let err = incremental_ops([(RETENTION_MS, 0, Some("1")), (RETENTION_MS, 0, None)])
            .expect_err("repeated");
        assert_eq!(err, RequestFault::DuplicateKeys);
        assert_eq!(err.message(), "Error due to duplicate config keys");
    }

    #[test]
    fn incremental_ops_names_every_null_value_except_a_delete() {
        let err = incremental_ops([
            (RETENTION_MS, 0, None),
            (CLEANUP_POLICY, 1, None),
            ("no.such", 9, None),
        ])
        .expect_err("null values");
        assert_eq!(
            err,
            RequestFault::NullValues(vec![RETENTION_MS.to_string(), "no.such".to_string()])
        );
        assert_eq!(
            err.message(),
            "Null value not supported for : retention.ms, no.such"
        );
    }

    #[test]
    fn incremental_ops_rejects_an_unknown_operation_byte() {
        let err = incremental_ops([(RETENTION_MS, 4, Some("1"))]).expect_err("unknown op");
        assert_eq!(err, RequestFault::UnknownOperation(4));
        assert_eq!(err.message(), "Unknown operations type 4");
        let err = incremental_ops([(RETENTION_MS, -1, Some("1"))]).expect_err("negative op");
        assert_eq!(err, RequestFault::UnknownOperation(-1));
        assert_eq!(err.message(), "Unknown operations type -1");
    }

    #[test]
    fn incremental_ops_keeps_request_order_and_a_delete_value() {
        let ops = incremental_ops([(RETENTION_MS, 1, Some("")), ("no.such", 0, Some("1"))])
            .expect("well formed");
        assert_eq!(
            ops,
            vec![
                ConfigOp {
                    name: RETENTION_MS,
                    operation: ConfigOperation::Delete,
                    value: Some(""),
                },
                ConfigOp {
                    name: "no.such",
                    operation: ConfigOperation::Set,
                    value: Some("1"),
                },
            ]
        );
    }

    #[tokio::test]
    async fn given_a_passed_deadline_when_writing_should_answer_timed_out_without_calling_iggy() {
        use iggy::prelude::IggyExpiry;
        let bridge = IggyBridge::offline();
        let write = TopicWrite {
            message_expiry: Some(IggyExpiry::NeverExpire),
            ..TopicWrite::default()
        };
        let (code, message) =
            write_configs(&bridge, "orders", &write, Instant::now(), "AlterConfigs").await;
        // A write started on the offline bridge fails fast as NOT_LEADER_OR_FOLLOWER instead.
        assert_eq!(code, ERROR_REQUEST_TIMED_OUT);
        assert!(message.is_none());
    }

    #[tokio::test]
    async fn given_an_empty_write_should_answer_none_without_calling_iggy() {
        let bridge = IggyBridge::offline();
        let (code, _) = write_configs(
            &bridge,
            "orders",
            &TopicWrite::default(),
            Instant::now(),
            "AlterConfigs",
        )
        .await;
        assert_eq!(code, ERROR_NONE);
    }
}
