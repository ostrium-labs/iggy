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

//! Every Kafka topic configuration, and how each maps onto an Iggy topic.
//!
//! Kafka 4.x defines 33 topic configurations. A client may set any of them on `CreateTopics`,
//! `AlterConfigs` and `IncrementalAlterConfigs`, and `DescribeConfigs` reports all of them. Four
//! are honoured through Iggy topic options:
//!
//! - `cleanup.policy` is the `cleanup_policy` option. Iggy does not compact yet, so a policy
//!   without `delete` turns expiry and the size cap off: the topic never loses a record.
//! - `retention.ms` is `message_expiry`.
//! - `retention.bytes` is `max_topic_size`, times the partition count: Kafka's value is per
//!   partition, Iggy's is per topic.
//! - `segment.bytes` is `segment_size`, at creation only (Iggy cannot change it later).
//!
//! Every other configuration is validated as Kafka validates it, stored verbatim in the
//! `kafka_configs` option and reported back, but not enforced. `docs/CONFIGS.md` lists which.
//!
//! Values are normalised the way Kafka normalises them before it stores them: numbers in
//! canonical decimal, booleans lower case, list items trimmed and repeats removed.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use super::topic_config::ConfigOperation;
use iggy::prelude::{
    CleanupPolicy, HeaderKey, IggyByteSize, IggyDuration, IggyExpiry, KafkaTopicConfigs,
    MAX_TOPIC_SEGMENT_SIZE, MIN_TOPIC_SEGMENT_SIZE, MaxTopicSize, OptionKey, ResourceOptions,
    topic_option_keys,
};

/// Kafka `ConfigType`, as `DescribeConfigs` v3+ reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    Boolean = 1,
    String = 2,
    Int = 3,
    Long = 5,
    Double = 6,
    List = 7,
}

impl ConfigType {
    #[must_use]
    pub const fn code(self) -> i8 {
        self as i8
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::String => "STRING",
            Self::Int => "INT",
            Self::Long => "LONG",
            Self::Double => "DOUBLE",
            Self::List => "LIST",
        }
    }
}

/// The values one configuration accepts, as Kafka's own validator for it does.
#[derive(Debug, Clone, Copy)]
enum Check {
    /// A whole number within the bounds (inclusive).
    Range(i64, i64),
    /// `-1`, or a whole number within the bounds (Kafka's gzip level).
    MinusOneOrRange(i64, i64),
    /// A number within the bounds (inclusive).
    Ratio(f64, f64),
    Bool,
    /// One of these strings, case-sensitive.
    OneOf(&'static [&'static str]),
    /// A non-empty list of these strings.
    ListOf(&'static [&'static str]),
    /// `*`, or a list of `partition:broker` pairs.
    ThrottledReplicas,
}

/// How Iggy treats one configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Honour {
    /// Mapped onto an Iggy topic option.
    Honoured,
    /// Validated, stored and reported, not enforced.
    Echoed,
}

/// One Kafka topic configuration.
#[derive(Debug)]
pub struct TopicConfigKey {
    pub name: &'static str,
    pub config_type: ConfigType,
    /// What `DescribeConfigs` reports when the topic does not set it.
    pub default: &'static str,
    check: Check,
    pub honour: Honour,
    pub documentation: &'static str,
}

pub const CLEANUP_POLICY: &str = "cleanup.policy";
pub const RETENTION_MS: &str = "retention.ms";
pub const RETENTION_BYTES: &str = "retention.bytes";
pub const SEGMENT_BYTES: &str = "segment.bytes";

const NOT_ENFORCED: &str = "Accepted and reported; Iggy does not enforce it.";

/// Every Kafka 4.x topic configuration, sorted by name.
pub static TOPIC_CONFIGS: [TopicConfigKey; 33] = [
    TopicConfigKey {
        name: CLEANUP_POLICY,
        config_type: ConfigType::List,
        default: "delete",
        check: Check::ListOf(&["compact", "delete"]),
        honour: Honour::Honoured,
        documentation: "Stored as the Iggy topic's cleanup_policy. Iggy does not compact yet: a policy without delete keeps every record (no expiry, no size cap).",
    },
    echoed(
        "compression.gzip.level",
        ConfigType::Int,
        "-1",
        Check::MinusOneOrRange(1, 9),
    ),
    echoed(
        "compression.lz4.level",
        ConfigType::Int,
        "9",
        Check::Range(1, 17),
    ),
    echoed(
        "compression.type",
        ConfigType::String,
        "producer",
        Check::OneOf(&["uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"]),
    ),
    echoed(
        "compression.zstd.level",
        ConfigType::Int,
        "3",
        Check::Range(-131_072, 22),
    ),
    echoed(
        "delete.retention.ms",
        ConfigType::Long,
        "86400000",
        Check::Range(0, i64::MAX),
    ),
    echoed(
        "file.delete.delay.ms",
        ConfigType::Long,
        "60000",
        Check::Range(0, i64::MAX),
    ),
    echoed(
        "flush.messages",
        ConfigType::Long,
        "9223372036854775807",
        Check::Range(1, i64::MAX),
    ),
    echoed(
        "flush.ms",
        ConfigType::Long,
        "9223372036854775807",
        Check::Range(0, i64::MAX),
    ),
    echoed(
        "follower.replication.throttled.replicas",
        ConfigType::List,
        "",
        Check::ThrottledReplicas,
    ),
    echoed(
        "index.interval.bytes",
        ConfigType::Int,
        "4096",
        Check::Range(0, i32::MAX as i64),
    ),
    echoed(
        "leader.replication.throttled.replicas",
        ConfigType::List,
        "",
        Check::ThrottledReplicas,
    ),
    echoed(
        "local.retention.bytes",
        ConfigType::Long,
        "-2",
        Check::Range(-2, i64::MAX),
    ),
    echoed(
        "local.retention.ms",
        ConfigType::Long,
        "-2",
        Check::Range(-2, i64::MAX),
    ),
    echoed(
        "max.compaction.lag.ms",
        ConfigType::Long,
        "9223372036854775807",
        Check::Range(1, i64::MAX),
    ),
    echoed(
        "max.message.bytes",
        ConfigType::Int,
        "1048588",
        Check::Range(0, i32::MAX as i64),
    ),
    echoed(
        "message.timestamp.after.max.ms",
        ConfigType::Long,
        "3600000",
        Check::Range(0, i64::MAX),
    ),
    echoed(
        "message.timestamp.before.max.ms",
        ConfigType::Long,
        "9223372036854775807",
        Check::Range(0, i64::MAX),
    ),
    echoed(
        "message.timestamp.type",
        ConfigType::String,
        "CreateTime",
        Check::OneOf(&["CreateTime", "LogAppendTime"]),
    ),
    echoed(
        "min.cleanable.dirty.ratio",
        ConfigType::Double,
        "0.5",
        Check::Ratio(0.0, 1.0),
    ),
    echoed(
        "min.compaction.lag.ms",
        ConfigType::Long,
        "0",
        Check::Range(0, i64::MAX),
    ),
    echoed(
        "min.insync.replicas",
        ConfigType::Int,
        "1",
        Check::Range(1, i32::MAX as i64),
    ),
    echoed("preallocate", ConfigType::Boolean, "false", Check::Bool),
    echoed(
        "remote.log.copy.disable",
        ConfigType::Boolean,
        "false",
        Check::Bool,
    ),
    echoed(
        "remote.log.delete.on.disable",
        ConfigType::Boolean,
        "false",
        Check::Bool,
    ),
    echoed(
        "remote.storage.enable",
        ConfigType::Boolean,
        "false",
        Check::Bool,
    ),
    TopicConfigKey {
        name: RETENTION_BYTES,
        config_type: ConfigType::Long,
        default: "-1",
        check: Check::Range(i64::MIN, i64::MAX),
        honour: Honour::Honoured,
        documentation: "Per-partition cap on sealed segments, stored as the Iggy topic's max_topic_size times the partition count; -1 means no cap.",
    },
    TopicConfigKey {
        name: RETENTION_MS,
        config_type: ConfigType::Long,
        default: "-1",
        check: Check::Range(-1, i64::MAX),
        honour: Honour::Honoured,
        documentation: "How long a sealed segment is kept, in milliseconds, stored as the Iggy topic's message_expiry. The active segment does not expire. -1 means sealed segments never expire.",
    },
    TopicConfigKey {
        name: SEGMENT_BYTES,
        config_type: ConfigType::Int,
        default: "1073741824",
        check: Check::Range(1_048_576, i32::MAX as i64),
        honour: Honour::Honoured,
        documentation: "Segment size, applied as the Iggy topic's segment_size when the topic is created (rounded down to 512 bytes, at most 1 GiB); later changes are reported, not applied.",
    },
    echoed(
        "segment.index.bytes",
        ConfigType::Int,
        "10485760",
        Check::Range(4, i32::MAX as i64),
    ),
    echoed(
        "segment.jitter.ms",
        ConfigType::Long,
        "0",
        Check::Range(0, i64::MAX),
    ),
    echoed(
        "segment.ms",
        ConfigType::Long,
        "604800000",
        Check::Range(1, i64::MAX),
    ),
    echoed(
        "unclean.leader.election.enable",
        ConfigType::Boolean,
        "false",
        Check::Bool,
    ),
];

const fn echoed(
    name: &'static str,
    config_type: ConfigType,
    default: &'static str,
    check: Check,
) -> TopicConfigKey {
    TopicConfigKey {
        name,
        config_type,
        default,
        check,
        honour: Honour::Echoed,
        documentation: NOT_ENFORCED,
    }
}

/// The configuration named `name`, if Kafka defines one.
#[must_use]
pub fn lookup(name: &str) -> Option<&'static TopicConfigKey> {
    TOPIC_CONFIGS
        .binary_search_by(|key| key.name.cmp(name))
        .ok()
        .map(|index| &TOPIC_CONFIGS[index])
}

/// Why a configuration change cannot be stored. Every variant is `INVALID_CONFIG` (40) except
/// [`ConfigFault::TooLarge`], which is `POLICY_VIOLATION` (44).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFault {
    /// Kafka defines no topic configuration of this name.
    UnknownKey(String),
    /// The same name twice in one resource.
    Repeated(String),
    /// A value Kafka itself refuses, with Kafka's reason.
    InvalidValue {
        name: String,
        value: String,
        reason: String,
    },
    /// A value Kafka accepts and Iggy cannot store, with the reason.
    Unsupported(String),
    /// An `APPEND` or `SUBTRACT` on a key that is not a list, in Kafka's own words.
    NotAList {
        operation: &'static str,
        key: String,
    },
    /// A null value where one is required.
    NullValue(String),
    /// The configurations Iggy keeps verbatim do not fit one option value.
    TooLarge,
}

impl ConfigFault {
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::UnknownKey(name) => format!("Unknown topic config name: {name}"),
            Self::Repeated(name) => format!("config key '{name}' is repeated"),
            Self::InvalidValue {
                name,
                value,
                reason,
            } => format!("Invalid value {value} for configuration {name}: {reason}"),
            Self::Unsupported(reason) => reason.clone(),
            Self::NotAList { operation, key } => {
                format!("Can't {operation} to key {key} because its type is not LIST.")
            }
            Self::NullValue(name) => format!("Null value not supported for topic configs: {name}"),
            Self::TooLarge => format!(
                "the topic configurations Iggy stores verbatim exceed {} bytes",
                iggy::prelude::KAFKA_CONFIGS_MAX_LENGTH
            ),
        }
    }

    /// Whether the fault is a gateway limit rather than a bad configuration.
    #[must_use]
    pub const fn is_policy(&self) -> bool {
        matches!(self, Self::TooLarge)
    }
}

/// The canonical form of `value` for `key`, as Kafka would store and describe it.
///
/// # Errors
///
/// [`ConfigFault::InvalidValue`] when Kafka would refuse the value, and
/// [`ConfigFault::Unsupported`] for `remote.storage.enable=true`, which Kafka refuses on a broker
/// without tiered storage, as this one is.
pub fn normalize(key: &TopicConfigKey, value: &str) -> Result<String, ConfigFault> {
    let invalid = |reason: String| ConfigFault::InvalidValue {
        name: key.name.to_string(),
        value: value.to_string(),
        reason,
    };
    let trimmed = value.trim();
    match key.check {
        Check::Range(min, max) | Check::MinusOneOrRange(min, max) => {
            let number = parse_whole(key.config_type, trimmed).ok_or_else(|| {
                invalid(format!("Not a number of type {}", key.config_type.name()))
            })?;
            let in_range = number >= min && number <= max;
            if matches!(key.check, Check::MinusOneOrRange(..)) {
                if number != -1 && !in_range {
                    return Err(invalid(format!(
                        "Value must be between {min} and {max} or equal to -1"
                    )));
                }
            } else if number < min {
                return Err(invalid(format!("Value must be at least {min}")));
            } else if number > max {
                return Err(invalid(format!("Value must be no more than {max}")));
            }
            Ok(number.to_string())
        }
        Check::Ratio(min, max) => {
            let number: f64 = trimmed
                .parse()
                .ok()
                .filter(|number: &f64| number.is_finite())
                .ok_or_else(|| invalid("Not a number of type DOUBLE".to_string()))?;
            if number < min {
                return Err(invalid(format!(
                    "Value must be at least {}",
                    java_double(min)
                )));
            }
            if number > max {
                return Err(invalid(format!(
                    "Value must be no more than {}",
                    java_double(max)
                )));
            }
            Ok(java_double(number))
        }
        Check::Bool => {
            let canonical = if trimmed.eq_ignore_ascii_case("true") {
                "true"
            } else if trimmed.eq_ignore_ascii_case("false") {
                "false"
            } else {
                return Err(invalid(
                    "Expected value to be either true or false".to_string(),
                ));
            };
            if key.name == "remote.storage.enable" && canonical == "true" {
                return Err(ConfigFault::Unsupported(
                    "Tiered Storage functionality is disabled in the broker. Topic cannot be \
                     configured with remote log storage."
                        .to_string(),
                ));
            }
            Ok(canonical.to_string())
        }
        Check::OneOf(allowed) => {
            if allowed.contains(&trimmed) {
                Ok(trimmed.to_string())
            } else {
                Err(invalid(format!(
                    "String must be one of: {}",
                    allowed.join(", ")
                )))
            }
        }
        Check::ListOf(allowed) => {
            let items = list_items(trimmed);
            if items.is_empty() {
                return Err(invalid(format!(
                    "String must be one of: {}",
                    allowed.join(", ")
                )));
            }
            if let Some(bad) = items.iter().find(|item| !allowed.contains(item)) {
                return Err(ConfigFault::InvalidValue {
                    name: key.name.to_string(),
                    value: (*bad).to_string(),
                    reason: format!("String must be one of: {}", allowed.join(", ")),
                });
            }
            Ok(items.join(","))
        }
        Check::ThrottledReplicas => {
            let items = list_items(trimmed);
            let valid = items == ["*"]
                || items.iter().all(|item| {
                    item.split_once(':').is_some_and(|(partition, broker)| {
                        partition.trim().parse::<i32>().is_ok()
                            && broker.trim().parse::<i32>().is_ok()
                    })
                });
            if valid {
                Ok(items.join(","))
            } else {
                Err(ConfigFault::InvalidValue {
                    name: key.name.to_string(),
                    value: format!("[{}]", items.join(", ")),
                    reason: format!(
                        "{} must be the literal '*' or a list of replicas in the following \
                         format: [partitionId]:[brokerId],[partitionId]:[brokerId],...",
                        key.name
                    ),
                })
            }
        }
    }
}

/// A trimmed, comma-separated list with empty items and repeats removed, first occurrence kept.
fn list_items(text: &str) -> Vec<&str> {
    let mut seen = BTreeSet::new();
    text.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty() && seen.insert(*item))
        .collect()
}

fn parse_whole(config_type: ConfigType, text: &str) -> Option<i64> {
    match config_type {
        ConfigType::Int => text.parse::<i32>().ok().map(i64::from),
        _ => text.parse::<i64>().ok(),
    }
}

/// `number` as Java's `Double.toString` spells it, for the values a ratio takes.
fn java_double(number: f64) -> String {
    let magnitude = number.abs();
    if number == 0.0 || (1e-3..1e7).contains(&magnitude) {
        let text = format!("{number}");
        if text.contains('.') {
            text
        } else {
            format!("{text}.0")
        }
    } else {
        let text = format!("{number:E}");
        match text.split_once('E') {
            Some((mantissa, exponent)) if !mantissa.contains('.') => {
                format!("{mantissa}.0E{exponent}")
            }
            _ => text,
        }
    }
}

/// The Kafka-visible configuration of one Iggy topic, read from its options.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicConfigSnapshot {
    pub partitions: u32,
    pub message_expiry: IggyExpiry,
    pub message_expiry_explicit: bool,
    pub max_topic_size: MaxTopicSize,
    pub max_topic_size_explicit: bool,
    pub segment_size: u64,
    pub segment_size_explicit: bool,
    pub cleanup_policy: Option<CleanupPolicy>,
    /// Whether the topic carries a `kafka_configs` option at all.
    pub kafka_configs_stored: bool,
    pub kafka_configs: KafkaTopicConfigs,
}

impl Default for TopicConfigSnapshot {
    fn default() -> Self {
        Self {
            partitions: 1,
            message_expiry: IggyExpiry::NeverExpire,
            message_expiry_explicit: false,
            max_topic_size: MaxTopicSize::Unlimited,
            max_topic_size_explicit: false,
            segment_size: iggy::prelude::DEFAULT_SEGMENT_SIZE,
            segment_size_explicit: false,
            cleanup_policy: None,
            kafka_configs_stored: false,
            kafka_configs: KafkaTopicConfigs::new(),
        }
    }
}

fn option<'a>(options: &'a ResourceOptions, key: &str) -> Option<&'a iggy::prelude::OptionValue> {
    HeaderKey::from_str(key)
        .ok()
        .and_then(|key| options.get(&key))
}

fn is_explicit(options: &ResourceOptions, key: &str) -> bool {
    option(options, key).is_some_and(|entry| entry.explicit)
}

impl TopicConfigSnapshot {
    /// The snapshot of a topic with `partitions` partitions, its stored `options`, and the
    /// effective values Iggy resolved from them.
    #[must_use]
    pub fn from_options(
        partitions: u32,
        options: &ResourceOptions,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
        segment_size: u64,
    ) -> Self {
        // A derived entry is the catalog default, which the topic does not set.
        let explicit_text = |key: &str| {
            option(options, key)
                .filter(|entry| entry.explicit)
                .and_then(|entry| entry.value.as_str().ok())
                .map(str::to_owned)
        };
        let cleanup_policy = explicit_text(topic_option_keys::CLEANUP_POLICY).and_then(|policy| {
            policy
                .parse()
                .inspect_err(|error| tracing::warn!(%error, "ignoring a stored cleanup_policy"))
                .ok()
        });
        let stored_configs = explicit_text(topic_option_keys::KAFKA_CONFIGS);
        let kafka_configs = stored_configs
            .as_deref()
            .and_then(|configs| {
                configs
                    .parse()
                    .inspect_err(|error| tracing::warn!(%error, "ignoring stored kafka_configs"))
                    .ok()
            })
            .unwrap_or_default();
        Self {
            partitions,
            message_expiry,
            message_expiry_explicit: is_explicit(options, topic_option_keys::MESSAGE_EXPIRY),
            max_topic_size,
            max_topic_size_explicit: is_explicit(options, topic_option_keys::MAX_TOPIC_SIZE),
            segment_size,
            segment_size_explicit: is_explicit(options, topic_option_keys::SEGMENT_SIZE),
            cleanup_policy,
            kafka_configs_stored: stored_configs.is_some(),
            kafka_configs,
        }
    }

    /// The snapshot of a topic just created with `partitions` partitions and `write`, before
    /// any read: what Iggy stores for that create.
    #[must_use]
    pub fn after_create(partitions: u32, write: &TopicWrite) -> Self {
        Self {
            partitions,
            message_expiry: write.message_expiry.unwrap_or(IggyExpiry::NeverExpire),
            message_expiry_explicit: write.message_expiry.is_some(),
            max_topic_size: write.max_topic_size.unwrap_or(MaxTopicSize::Unlimited),
            max_topic_size_explicit: write.max_topic_size.is_some(),
            segment_size: write
                .segment_size
                .unwrap_or(iggy::prelude::DEFAULT_SEGMENT_SIZE),
            segment_size_explicit: write.segment_size.is_some(),
            cleanup_policy: write.cleanup_policy,
            kafka_configs_stored: write.kafka_configs.is_some(),
            kafka_configs: write.kafka_configs.clone().unwrap_or_default(),
        }
    }

    fn policy(&self) -> CleanupPolicy {
        self.cleanup_policy.unwrap_or_default()
    }

    /// `retention.ms` as stored, `Err` when the stored expiry is no whole number of
    /// milliseconds, `Ok(None)` when the topic does not set it.
    fn explicit_retention_ms(&self) -> Result<Option<String>, ()> {
        if self.message_expiry == IggyExpiry::NeverExpire
            && let Some(echoed) = self.kafka_configs.get(RETENTION_MS)
        {
            return Ok(Some(echoed.to_string()));
        }
        if !self.message_expiry_explicit {
            return Ok(None);
        }
        retention_ms_text(self.message_expiry).map(Some)
    }

    fn explicit_retention_bytes(&self) -> Option<String> {
        if let Some(echoed) = self.kafka_configs.get(RETENTION_BYTES)
            && let Ok(bytes) = echoed.parse::<i64>()
            && max_topic_size_for(
                Some(bytes),
                self.policy(),
                self.partitions,
                self.segment_size,
            ) == self.max_topic_size
        {
            return Some(echoed.to_string());
        }
        if !self.max_topic_size_explicit {
            return None;
        }
        Some(match self.max_topic_size {
            MaxTopicSize::Custom(size) => {
                (size.as_bytes_u64() / u64::from(self.partitions.max(1))).to_string()
            }
            MaxTopicSize::Unlimited | MaxTopicSize::ServerDefault => "-1".to_string(),
        })
    }

    fn explicit_segment_bytes(&self) -> Option<String> {
        if let Some(echoed) = self.kafka_configs.get(SEGMENT_BYTES) {
            return Some(echoed.to_string());
        }
        self.segment_size_explicit
            .then(|| self.segment_size.to_string())
    }

    /// Every configuration the topic sets, by name.
    ///
    /// # Errors
    ///
    /// `Err` when the stored expiry cannot be shown as `retention.ms`.
    pub fn explicit(&self) -> Result<BTreeMap<&'static str, String>, ()> {
        let mut set = BTreeMap::new();
        for key in &TOPIC_CONFIGS {
            let value = match key.name {
                CLEANUP_POLICY => self
                    .cleanup_policy
                    .map(|policy| policy.as_str().to_string()),
                RETENTION_MS => self.explicit_retention_ms()?,
                RETENTION_BYTES => self.explicit_retention_bytes(),
                SEGMENT_BYTES => self.explicit_segment_bytes(),
                name => self.kafka_configs.get(name).map(str::to_string),
            };
            if let Some(value) = value {
                set.insert(key.name, value);
            }
        }
        Ok(set)
    }

    /// What `DescribeConfigs` reports for `key`: the value, and whether the topic sets it.
    ///
    /// # Errors
    ///
    /// `Err` for `retention.ms` when the stored expiry is no whole number of milliseconds.
    pub fn describe(&self, key: &TopicConfigKey) -> Result<(String, bool), ()> {
        let explicit = match key.name {
            CLEANUP_POLICY => self
                .cleanup_policy
                .map(|policy| policy.as_str().to_string()),
            RETENTION_MS => self.explicit_retention_ms()?,
            RETENTION_BYTES => self.explicit_retention_bytes(),
            SEGMENT_BYTES => {
                return Ok(self.explicit_segment_bytes().map_or_else(
                    || (self.segment_size.to_string(), false),
                    |value| (value, true),
                ));
            }
            name => self.kafka_configs.get(name).map(str::to_string),
        };
        Ok(explicit.map_or_else(|| (key.default.to_string(), false), |value| (value, true)))
    }
}

/// `expiry` as `retention.ms` text, `Err` when it is no whole positive number of milliseconds.
fn retention_ms_text(expiry: IggyExpiry) -> Result<String, ()> {
    match expiry {
        IggyExpiry::NeverExpire | IggyExpiry::ServerDefault => Ok("-1".to_string()),
        IggyExpiry::ExpireDuration(duration) => {
            let micros = duration.as_micros();
            if micros == 0 || !micros.is_multiple_of(1_000) {
                return Err(());
            }
            Ok((micros / 1_000).to_string())
        }
    }
}

/// The Iggy cap for a per-partition `retention.bytes` of `bytes`: none without a value, for a
/// negative one, or for a policy without `delete`. Iggy refuses a finite cap below one segment,
/// so a smaller one is raised to `segment_size`, which keeps at least one sealed segment.
fn max_topic_size_for(
    bytes: Option<i64>,
    policy: CleanupPolicy,
    partitions: u32,
    segment_size: u64,
) -> MaxTopicSize {
    match bytes.and_then(|bytes| u64::try_from(bytes).ok()) {
        Some(bytes) if policy.deletes() => {
            let total = bytes
                .saturating_mul(u64::from(partitions.max(1)))
                .max(segment_size);
            if total == u64::MAX {
                MaxTopicSize::Unlimited
            } else {
                MaxTopicSize::Custom(IggyByteSize::from(total))
            }
        }
        _ => MaxTopicSize::Unlimited,
    }
}

/// The longest expiry Iggy's own parser accepts: `u32::MAX` seconds.
const MAX_EXPIRY_MILLIS: u64 = u32::MAX as u64 * 1_000;

/// The Iggy expiry for `retention.ms`, and whether `retention.ms` must also be kept verbatim
/// because the expiry does not show it.
fn message_expiry_for(
    retention_ms: Option<i64>,
    policy: CleanupPolicy,
) -> Result<(IggyExpiry, bool), ConfigFault> {
    let Some(millis) = retention_ms else {
        return Ok((IggyExpiry::NeverExpire, false));
    };
    if millis == -1 {
        return Ok((IggyExpiry::NeverExpire, false));
    }
    if !policy.deletes() {
        return Ok((IggyExpiry::NeverExpire, true));
    }
    let millis = u64::try_from(millis).unwrap_or(0);
    if millis == 0 {
        return Err(ConfigFault::Unsupported(
            "retention.ms=0 is not supported: Iggy cannot expire a sealed segment immediately; \
             use 1 or more"
                .to_string(),
        ));
    }
    if millis > MAX_EXPIRY_MILLIS {
        // Longer than Iggy can store: never expiring is what the client asked for in practice
        // (Debezium sends Long.MAX_VALUE).
        return Ok((IggyExpiry::NeverExpire, true));
    }
    Ok((
        IggyExpiry::ExpireDuration(IggyDuration::from(millis * 1_000)),
        false,
    ))
}

/// The Iggy segment size for `segment.bytes`: rounded down to 512 bytes, within Iggy's bounds.
fn segment_size_for(bytes: i64) -> u64 {
    let bytes = u64::try_from(bytes).unwrap_or(MIN_TOPIC_SEGMENT_SIZE);
    (bytes - bytes % 512).clamp(MIN_TOPIC_SEGMENT_SIZE, MAX_TOPIC_SEGMENT_SIZE)
}

/// What one create or alter writes to the Iggy topic. A `None` field is left unchanged.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicWrite {
    pub message_expiry: Option<IggyExpiry>,
    pub max_topic_size: Option<MaxTopicSize>,
    /// Keys to return to their catalog defaults.
    pub reset: BTreeSet<OptionKey>,
    pub cleanup_policy: Option<CleanupPolicy>,
    pub kafka_configs: Option<KafkaTopicConfigs>,
    /// Create only.
    pub segment_size: Option<u64>,
}

impl TopicWrite {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    /// The string-valued options this write sets.
    #[must_use]
    pub fn raw_options(&self) -> BTreeMap<String, String> {
        let mut raw = BTreeMap::new();
        if let Some(policy) = self.cleanup_policy {
            raw.insert(
                topic_option_keys::CLEANUP_POLICY.to_string(),
                policy.as_str().to_string(),
            );
        }
        if let Some(configs) = &self.kafka_configs {
            raw.insert(
                topic_option_keys::KAFKA_CONFIGS.to_string(),
                configs.to_text(),
            );
        }
        raw
    }
}

fn number(set: &BTreeMap<&'static str, String>, name: &str) -> Option<i64> {
    set.get(name).and_then(|value| value.parse().ok())
}

/// The Iggy state a topic with exactly the Kafka configurations `set` has, for `partitions`
/// partitions of `segment_size` bytes.
struct Desired {
    policy: Option<CleanupPolicy>,
    message_expiry: IggyExpiry,
    message_expiry_explicit: bool,
    max_topic_size: MaxTopicSize,
    max_topic_size_explicit: bool,
    kafka_configs: KafkaTopicConfigs,
}

fn desired(
    set: &BTreeMap<&'static str, String>,
    partitions: u32,
    segment_size: u64,
) -> Result<Desired, ConfigFault> {
    let policy = set
        .get(CLEANUP_POLICY)
        .map(|policy| {
            CleanupPolicy::from_str(policy).map_err(|reason| ConfigFault::InvalidValue {
                name: CLEANUP_POLICY.to_string(),
                value: policy.clone(),
                reason,
            })
        })
        .transpose()?;
    let effective = policy.unwrap_or_default();
    let retention_ms = number(set, RETENTION_MS);
    let (message_expiry, keep_retention_ms) = message_expiry_for(retention_ms, effective)?;
    let retention_bytes = number(set, RETENTION_BYTES);
    let max_topic_size = max_topic_size_for(retention_bytes, effective, partitions, segment_size);
    let mut kafka_configs = KafkaTopicConfigs::new();
    for (name, value) in set {
        let keep = match *name {
            CLEANUP_POLICY => false,
            RETENTION_MS => keep_retention_ms,
            _ => true,
        };
        if keep {
            kafka_configs
                .insert(name, value)
                .map_err(ConfigFault::Unsupported)?;
        }
    }
    if !kafka_configs.encoded_len_fits() {
        return Err(ConfigFault::TooLarge);
    }
    Ok(Desired {
        policy,
        message_expiry,
        message_expiry_explicit: retention_ms.is_some(),
        max_topic_size,
        max_topic_size_explicit: retention_bytes.is_some(),
        kafka_configs,
    })
}

/// What a new topic with the Kafka configurations `set` stores, beyond its partition count.
///
/// # Errors
///
/// A [`ConfigFault`] when Iggy cannot store the set.
pub fn plan_create(
    set: &BTreeMap<&'static str, String>,
    partitions: u32,
) -> Result<TopicWrite, ConfigFault> {
    let segment_size =
        number(set, SEGMENT_BYTES).map_or(iggy::prelude::DEFAULT_SEGMENT_SIZE, segment_size_for);
    let desired = desired(set, partitions, segment_size)?;
    Ok(TopicWrite {
        message_expiry: desired
            .message_expiry_explicit
            .then_some(desired.message_expiry),
        max_topic_size: desired
            .max_topic_size_explicit
            .then_some(desired.max_topic_size),
        reset: BTreeSet::new(),
        cleanup_policy: desired.policy,
        kafka_configs: (!desired.kafka_configs.is_empty()).then_some(desired.kafka_configs),
        segment_size: set.contains_key(SEGMENT_BYTES).then_some(segment_size),
    })
}

/// What turns `current` into a topic with exactly the Kafka configurations `set`. Only what
/// differs is written; an empty result means nothing changes.
///
/// # Errors
///
/// A [`ConfigFault`] when Iggy cannot store the set.
pub fn plan_alter(
    current: &TopicConfigSnapshot,
    set: &BTreeMap<&'static str, String>,
) -> Result<TopicWrite, ConfigFault> {
    let desired = desired(set, current.partitions, current.segment_size)?;
    let mut write = TopicWrite::default();

    if desired.message_expiry_explicit {
        if !current.message_expiry_explicit || current.message_expiry != desired.message_expiry {
            write.message_expiry = Some(desired.message_expiry);
        }
    } else if current.message_expiry_explicit {
        write.reset.insert(OptionKey::MessageExpiry);
    }

    if desired.max_topic_size_explicit {
        if !current.max_topic_size_explicit || current.max_topic_size != desired.max_topic_size {
            write.max_topic_size = Some(desired.max_topic_size);
        }
    } else if current.max_topic_size_explicit {
        write.reset.insert(OptionKey::MaxTopicSize);
    }

    match desired.policy {
        Some(policy) if current.cleanup_policy != Some(policy) => {
            write.cleanup_policy = Some(policy);
        }
        None if current.cleanup_policy.is_some() => {
            write.reset.insert(OptionKey::CleanupPolicy);
        }
        _ => {}
    }

    if desired.kafka_configs != current.kafka_configs {
        if desired.kafka_configs.is_empty() {
            write.reset.insert(OptionKey::KafkaConfigs);
        } else {
            write.kafka_configs = Some(desired.kafka_configs);
        }
    }
    Ok(write)
}

/// The Kafka configurations of one request entry, normalised, by name.
///
/// # Errors
///
/// The first [`ConfigFault`] in request order: an unknown name, a repeated one, a null value or
/// a value Kafka refuses.
pub fn requested_set<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<BTreeMap<&'static str, String>, ConfigFault> {
    let mut set = BTreeMap::new();
    for (name, value) in configs {
        let key = lookup(name).ok_or_else(|| ConfigFault::UnknownKey(name.to_string()))?;
        let value = value.ok_or_else(|| ConfigFault::NullValue(name.to_string()))?;
        let value = normalize(key, value)?;
        if set.insert(key.name, value).is_some() {
            return Err(ConfigFault::Repeated(name.to_string()));
        }
    }
    Ok(set)
}

/// `set` after one `IncrementalAlterConfigs` operation, as Kafka's broker applies it.
///
/// `DELETE` removes an override and ignores its value; a delete of a name Kafka does not define
/// changes nothing. `APPEND` and `SUBTRACT` work on list configurations only, starting from the
/// default when the topic does not set the key.
///
/// # Errors
///
/// A [`ConfigFault`] for an unknown name (other than on `DELETE`), a value Kafka refuses, or a
/// list operation on a key that is not a list.
pub fn apply_operation(
    set: &mut BTreeMap<&'static str, String>,
    name: &str,
    operation: ConfigOperation,
    value: Option<&str>,
) -> Result<(), ConfigFault> {
    let Some(key) = lookup(name) else {
        return match operation {
            ConfigOperation::Delete => Ok(()),
            ConfigOperation::Set => Err(ConfigFault::UnknownKey(name.to_string())),
            ConfigOperation::Append | ConfigOperation::Subtract => Err(ConfigFault::NotAList {
                operation: operation.kafka_name(),
                key: name.to_string(),
            }),
        };
    };
    match operation {
        ConfigOperation::Delete => {
            set.remove(key.name);
        }
        ConfigOperation::Set => {
            let value = value.ok_or_else(|| ConfigFault::NullValue(name.to_string()))?;
            set.insert(key.name, normalize(key, value)?);
        }
        ConfigOperation::Append | ConfigOperation::Subtract => {
            if key.config_type != ConfigType::List {
                return Err(ConfigFault::NotAList {
                    operation: operation.kafka_name(),
                    key: name.to_string(),
                });
            }
            let value = value.ok_or_else(|| ConfigFault::NullValue(name.to_string()))?;
            let current = set.get(key.name).map_or(key.default, String::as_str);
            let mut items: Vec<String> = list_items(current)
                .into_iter()
                .map(str::to_string)
                .collect();
            let given = list_items(value);
            if operation == ConfigOperation::Append {
                for item in given {
                    if !items.iter().any(|existing| existing == item) {
                        items.push(item.to_string());
                    }
                }
            } else {
                items.retain(|existing| !given.contains(&existing.as_str()));
            }
            set.insert(key.name, normalize(key, &items.join(","))?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(entries: &[(&'static str, &str)]) -> BTreeMap<&'static str, String> {
        entries
            .iter()
            .map(|(name, value)| (*name, (*value).to_string()))
            .collect()
    }

    #[test]
    fn the_table_is_sorted_and_names_every_kafka_topic_config_once() {
        assert!(
            TOPIC_CONFIGS
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name),
            "binary search needs the table sorted"
        );
        for key in &TOPIC_CONFIGS {
            assert_eq!(lookup(key.name).map(|found| found.name), Some(key.name));
            assert_eq!(
                normalize(key, key.default).as_deref(),
                Ok(key.default),
                "{} default must pass its own check",
                key.name
            );
        }
        assert!(lookup("no.such").is_none());
        assert!(lookup("message.format.version").is_none());
    }

    #[test]
    fn values_are_normalised_as_kafka_stores_them() {
        let cases = [
            (RETENTION_MS, " +5 ", "5"),
            (RETENTION_MS, "007", "7"),
            ("preallocate", "TRUE", "true"),
            (CLEANUP_POLICY, "compact, delete", "compact,delete"),
            (CLEANUP_POLICY, "compact,compact", "compact"),
            (CLEANUP_POLICY, "delete,compact", "delete,compact"),
            ("min.cleanable.dirty.ratio", "1", "1.0"),
            ("min.cleanable.dirty.ratio", "0.25", "0.25"),
            ("min.cleanable.dirty.ratio", "0.0001", "1.0E-4"),
            (
                "leader.replication.throttled.replicas",
                " 0:1 , 1:1",
                "0:1,1:1",
            ),
            ("follower.replication.throttled.replicas", "*", "*"),
            ("compression.gzip.level", "-1", "-1"),
        ];
        for (name, given, stored) in cases {
            let key = lookup(name).expect("known key");
            assert_eq!(
                normalize(key, given).as_deref(),
                Ok(stored),
                "{name}={given}"
            );
        }
    }

    #[test]
    fn values_kafka_refuses_are_refused_with_its_reason() {
        let cases = [
            (
                CLEANUP_POLICY,
                "foo",
                "Invalid value foo for configuration cleanup.policy: String must be one of: compact, delete",
            ),
            (
                "min.insync.replicas",
                "0",
                "Invalid value 0 for configuration min.insync.replicas: Value must be at least 1",
            ),
            (
                RETENTION_MS,
                "-2",
                "Invalid value -2 for configuration retention.ms: Value must be at least -1",
            ),
            (
                RETENTION_MS,
                "abc",
                "Invalid value abc for configuration retention.ms: Not a number of type LONG",
            ),
            (
                SEGMENT_BYTES,
                "100",
                "Invalid value 100 for configuration segment.bytes: Value must be at least 1048576",
            ),
            (
                "max.message.bytes",
                "2147483648",
                "Invalid value 2147483648 for configuration max.message.bytes: Not a number of type INT",
            ),
            (
                "message.timestamp.type",
                "Foo",
                "Invalid value Foo for configuration message.timestamp.type: String must be one of: CreateTime, LogAppendTime",
            ),
            (
                "min.cleanable.dirty.ratio",
                "2",
                "Invalid value 2 for configuration min.cleanable.dirty.ratio: Value must be no more than 1.0",
            ),
            (
                "unclean.leader.election.enable",
                "yes",
                "Invalid value yes for configuration unclean.leader.election.enable: Expected value to be either true or false",
            ),
            (
                "compression.gzip.level",
                "10",
                "Invalid value 10 for configuration compression.gzip.level: Value must be between 1 and 9 or equal to -1",
            ),
            (
                "compression.zstd.level",
                "23",
                "Invalid value 23 for configuration compression.zstd.level: Value must be no more than 22",
            ),
        ];
        for (name, given, message) in cases {
            let key = lookup(name).expect("known key");
            let fault = normalize(key, given).expect_err(given);
            assert_eq!(fault.message(), message);
        }
        let key = lookup("remote.storage.enable").expect("known key");
        assert!(matches!(
            normalize(key, "true"),
            Err(ConfigFault::Unsupported(_))
        ));
    }

    #[test]
    fn a_request_with_an_unknown_repeated_or_null_entry_is_refused() {
        assert_eq!(
            requested_set([("foo.bar", Some("1"))]),
            Err(ConfigFault::UnknownKey("foo.bar".to_string()))
        );
        assert_eq!(
            requested_set([(RETENTION_MS, Some("1")), (RETENTION_MS, Some("2"))]),
            Err(ConfigFault::Repeated(RETENTION_MS.to_string()))
        );
        assert_eq!(
            requested_set([(RETENTION_MS, None)]),
            Err(ConfigFault::NullValue(RETENTION_MS.to_string()))
        );
    }

    #[test]
    fn connect_internal_topics_store_compact_without_expiry() {
        let configs = set(&[(CLEANUP_POLICY, "compact")]);
        let write = plan_create(&configs, 25).expect("storable");
        assert_eq!(write.cleanup_policy, Some(CleanupPolicy::Compact));
        assert_eq!(write.message_expiry, None);
        assert_eq!(write.max_topic_size, None);
        assert_eq!(write.kafka_configs, None);
    }

    #[test]
    fn a_compacted_topic_keeps_retention_verbatim_and_never_expires() {
        let configs = set(&[
            (CLEANUP_POLICY, "compact"),
            (RETENTION_MS, "3600000"),
            (RETENTION_BYTES, "1048576"),
        ]);
        let write = plan_create(&configs, 3).expect("storable");
        assert_eq!(write.message_expiry, Some(IggyExpiry::NeverExpire));
        assert_eq!(write.max_topic_size, Some(MaxTopicSize::Unlimited));
        let kept = write.kafka_configs.expect("echoed");
        assert_eq!(kept.get(RETENTION_MS), Some("3600000"));
        assert_eq!(kept.get(RETENTION_BYTES), Some("1048576"));
    }

    #[test]
    fn delete_topics_map_retention_onto_iggy() {
        let configs = set(&[
            (RETENTION_MS, "3600000"),
            (RETENTION_BYTES, "1073741824"),
            (SEGMENT_BYTES, "52428800"),
            ("message.timestamp.type", "CreateTime"),
        ]);
        let write = plan_create(&configs, 4).expect("storable");
        assert_eq!(
            write.message_expiry,
            Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                3_600_000_000_u64
            )))
        );
        assert_eq!(
            write.max_topic_size,
            Some(MaxTopicSize::Custom(IggyByteSize::from(
                4 * 1_073_741_824_u64
            )))
        );
        assert_eq!(write.segment_size, Some(52_428_800));
        let kept = write.kafka_configs.expect("echoed");
        assert_eq!(kept.get(RETENTION_MS), None, "honoured, not echoed");
        assert_eq!(kept.get("message.timestamp.type"), Some("CreateTime"));
    }

    #[test]
    fn debezium_retention_beyond_iggy_never_expires_and_is_reported_as_sent() {
        let configs = set(&[
            (RETENTION_MS, "9223372036854775807"),
            (RETENTION_BYTES, "-1"),
        ]);
        let write = plan_create(&configs, 1).expect("storable");
        assert_eq!(write.message_expiry, Some(IggyExpiry::NeverExpire));
        assert_eq!(write.max_topic_size, Some(MaxTopicSize::Unlimited));
        let kept = write.kafka_configs.clone().expect("echoed");
        let snapshot = TopicConfigSnapshot {
            message_expiry: IggyExpiry::NeverExpire,
            message_expiry_explicit: true,
            max_topic_size: MaxTopicSize::Unlimited,
            max_topic_size_explicit: true,
            kafka_configs_stored: true,
            kafka_configs: kept,
            ..TopicConfigSnapshot::default()
        };
        let retention = lookup(RETENTION_MS).expect("known");
        assert_eq!(
            snapshot.describe(retention),
            Ok(("9223372036854775807".to_string(), true))
        );
        let bytes = lookup(RETENTION_BYTES).expect("known");
        assert_eq!(snapshot.describe(bytes), Ok(("-1".to_string(), true)));
    }

    #[test]
    fn retention_ms_zero_is_refused_as_unsupported() {
        let fault = plan_create(&set(&[(RETENTION_MS, "0")]), 1).expect_err("zero");
        assert!(matches!(fault, ConfigFault::Unsupported(_)));
    }

    #[test]
    fn segment_bytes_is_rounded_down_and_capped() {
        assert_eq!(segment_size_for(52_428_800), 52_428_800);
        assert_eq!(segment_size_for(1_048_577), 1_048_576);
        assert_eq!(
            segment_size_for(i64::from(i32::MAX)),
            MAX_TOPIC_SEGMENT_SIZE
        );
    }

    #[test]
    fn a_small_retention_bytes_is_raised_to_one_segment() {
        assert_eq!(
            max_topic_size_for(Some(0), CleanupPolicy::Delete, 3, 1_048_576),
            MaxTopicSize::Custom(IggyByteSize::from(1_048_576_u64))
        );
        assert_eq!(
            max_topic_size_for(Some(-1), CleanupPolicy::Delete, 3, 1_048_576),
            MaxTopicSize::Unlimited
        );
    }

    #[test]
    fn an_untouched_topic_describes_every_default() {
        let snapshot = TopicConfigSnapshot::default();
        assert_eq!(snapshot.explicit(), Ok(BTreeMap::new()));
        for key in &TOPIC_CONFIGS {
            let (value, explicit) = snapshot.describe(key).expect("representable");
            assert!(!explicit, "{}", key.name);
            if key.name == SEGMENT_BYTES {
                assert_eq!(value, "1073741824");
            } else {
                assert_eq!(value, key.default, "{}", key.name);
            }
        }
    }

    #[test]
    fn altering_writes_only_what_differs() {
        let current = TopicConfigSnapshot::default();
        assert!(
            plan_alter(&current, &BTreeMap::new())
                .expect("no-op")
                .is_empty()
        );

        let write = plan_alter(&current, &set(&[("min.insync.replicas", "2")])).expect("echo");
        assert_eq!(write.message_expiry, None);
        assert!(write.reset.is_empty());
        assert_eq!(
            write
                .kafka_configs
                .expect("echoed")
                .get("min.insync.replicas"),
            Some("2")
        );

        let explicit = TopicConfigSnapshot {
            message_expiry: IggyExpiry::ExpireDuration(IggyDuration::from(1_000_000_u64)),
            message_expiry_explicit: true,
            ..TopicConfigSnapshot::default()
        };
        let write = plan_alter(&explicit, &BTreeMap::new()).expect("reset");
        assert_eq!(write.reset, BTreeSet::from([OptionKey::MessageExpiry]));
    }

    #[test]
    fn switching_to_compact_turns_expiry_off_and_back() {
        let expiring = TopicConfigSnapshot {
            message_expiry: IggyExpiry::ExpireDuration(IggyDuration::from(1_000_000_u64)),
            message_expiry_explicit: true,
            ..TopicConfigSnapshot::default()
        };
        let mut configs = expiring.explicit().expect("representable");
        assert_eq!(configs.get(RETENTION_MS).map(String::as_str), Some("1000"));
        apply_operation(
            &mut configs,
            CLEANUP_POLICY,
            ConfigOperation::Set,
            Some("compact"),
        )
        .expect("set");
        let write = plan_alter(&expiring, &configs).expect("compact");
        assert_eq!(write.message_expiry, Some(IggyExpiry::NeverExpire));
        assert_eq!(write.cleanup_policy, Some(CleanupPolicy::Compact));
        assert_eq!(
            write.kafka_configs.expect("echoed").get(RETENTION_MS),
            Some("1000")
        );
    }

    #[test]
    fn deleting_a_policy_resets_it_to_the_default() {
        let compacted = TopicConfigSnapshot {
            cleanup_policy: Some(CleanupPolicy::Compact),
            ..TopicConfigSnapshot::default()
        };
        let mut configs = compacted.explicit().expect("representable");
        apply_operation(&mut configs, CLEANUP_POLICY, ConfigOperation::Delete, None)
            .expect("delete");
        let write = plan_alter(&compacted, &configs).expect("delete");
        assert_eq!(write.cleanup_policy, None);
        assert_eq!(write.reset, BTreeSet::from([OptionKey::CleanupPolicy]));
    }

    #[test]
    fn append_and_subtract_work_on_lists_from_their_default() {
        let mut configs = BTreeMap::new();
        apply_operation(
            &mut configs,
            CLEANUP_POLICY,
            ConfigOperation::Append,
            Some("compact"),
        )
        .expect("append");
        assert_eq!(
            configs.get(CLEANUP_POLICY).map(String::as_str),
            Some("delete,compact")
        );
        apply_operation(
            &mut configs,
            CLEANUP_POLICY,
            ConfigOperation::Subtract,
            Some("delete"),
        )
        .expect("subtract");
        assert_eq!(
            configs.get(CLEANUP_POLICY).map(String::as_str),
            Some("compact")
        );
        let emptied = apply_operation(
            &mut configs,
            CLEANUP_POLICY,
            ConfigOperation::Subtract,
            Some("compact"),
        );
        assert!(matches!(emptied, Err(ConfigFault::InvalidValue { .. })));
        let fault = apply_operation(
            &mut configs,
            RETENTION_MS,
            ConfigOperation::Append,
            Some("1"),
        )
        .expect_err("not a list");
        assert_eq!(
            fault.message(),
            "Can't APPEND to key retention.ms because its type is not LIST."
        );
    }

    #[test]
    fn deleting_an_unknown_name_changes_nothing() {
        let mut configs = BTreeMap::new();
        apply_operation(&mut configs, "no.such", ConfigOperation::Delete, None).expect("no-op");
        assert!(configs.is_empty());
        assert_eq!(
            apply_operation(&mut configs, "no.such", ConfigOperation::Set, Some("1")),
            Err(ConfigFault::UnknownKey("no.such".to_string()))
        );
    }

    #[test]
    fn too_many_echoed_configs_are_a_policy_violation() {
        let configs = set(&[
            (
                "follower.replication.throttled.replicas",
                &"0:1,".repeat(60)[..239],
            ),
            ("leader.replication.throttled.replicas", "*"),
        ]);
        let fault = plan_create(&configs, 1).expect_err("too large");
        assert!(fault.is_policy());
    }

    #[test]
    fn a_native_max_topic_size_is_reported_per_partition() {
        let snapshot = TopicConfigSnapshot {
            partitions: 4,
            max_topic_size: MaxTopicSize::Custom(IggyByteSize::from(4_000_000_000_u64)),
            max_topic_size_explicit: true,
            ..TopicConfigSnapshot::default()
        };
        let bytes = lookup(RETENTION_BYTES).expect("known");
        assert_eq!(
            snapshot.describe(bytes),
            Ok(("1000000000".to_string(), true))
        );
    }
}
