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

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// The stored form of an empty set: a header value is never empty.
pub const KAFKA_CONFIGS_EMPTY: &str = "-";

/// Longest stored form: one string header value.
pub const KAFKA_CONFIGS_MAX_LENGTH: usize = 255;

/// Kafka topic configurations a topic carries verbatim, for the Kafka gateway.
///
/// The server stores and returns them and never interprets them. The gateway
/// keeps here the configurations Iggy does not apply, so a later
/// `DescribeConfigs` reports what the client set. Canonical form: one
/// `name=value` line per entry, sorted by name, or [`KAFKA_CONFIGS_EMPTY`].
/// A name is lowercase ASCII letters, digits, `.`, `_` and `-`; a value is
/// any text without a line break.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KafkaTopicConfigs(BTreeMap<String, String>);

impl KafkaTopicConfigs {
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Sets `name`, refusing a name or value the stored form cannot carry.
    ///
    /// # Errors
    ///
    /// The reason, when `name` or `value` breaks the rules above.
    pub fn insert(&mut self, name: &str, value: &str) -> Result<(), String> {
        check_name(name)?;
        check_value(value)?;
        self.0.insert(name.to_owned(), value.to_owned());
        Ok(())
    }

    pub fn remove(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }

    /// The canonical stored form. It may be longer than one header value;
    /// [`Self::encoded_len_fits`] says whether it can be stored.
    #[must_use]
    pub fn to_text(&self) -> String {
        if self.0.is_empty() {
            return KAFKA_CONFIGS_EMPTY.to_owned();
        }
        let mut text = String::new();
        for (index, (name, value)) in self.0.iter().enumerate() {
            if index > 0 {
                text.push('\n');
            }
            text.push_str(name);
            text.push('=');
            text.push_str(value);
        }
        text
    }

    /// Whether the stored form fits one header value.
    #[must_use]
    pub fn encoded_len_fits(&self) -> bool {
        self.to_text().len() <= KAFKA_CONFIGS_MAX_LENGTH
    }
}

fn check_name(name: &str) -> Result<(), String> {
    let valid = !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid Kafka config name '{}'",
            name.escape_debug()
        ))
    }
}

fn check_value(value: &str) -> Result<(), String> {
    if value.contains(['\n', '\r']) {
        Err("a Kafka config value cannot hold a line break".to_owned())
    } else {
        Ok(())
    }
}

impl Display for KafkaTopicConfigs {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_text())
    }
}

/// Parses the canonical form only: sorted, no repeated name, no empty line,
/// at most one header value long, so one set has one stored form.
impl FromStr for KafkaTopicConfigs {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() > KAFKA_CONFIGS_MAX_LENGTH {
            return Err(format!(
                "Kafka configs longer than {KAFKA_CONFIGS_MAX_LENGTH} bytes"
            ));
        }
        if text == KAFKA_CONFIGS_EMPTY {
            return Ok(Self::new());
        }
        let mut configs = Self::new();
        let mut previous: Option<&str> = None;
        for line in text.split('\n') {
            let (name, value) = line.split_once('=').ok_or_else(|| {
                format!("Kafka config line without '=': '{}'", line.escape_debug())
            })?;
            if previous.is_some_and(|previous| previous >= name) {
                return Err("Kafka configs are not sorted by name, or repeat one".to_owned());
            }
            configs.insert(name, value)?;
            previous = Some(name);
        }
        Ok(configs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_configs_when_encoding_should_sort_and_round_trip() {
        let mut configs = KafkaTopicConfigs::new();
        assert_eq!(configs.to_text(), KAFKA_CONFIGS_EMPTY);
        configs.insert("min.insync.replicas", "2").unwrap();
        configs
            .insert("leader.replication.throttled.replicas", "")
            .unwrap();
        configs
            .insert("message.timestamp.type", "CreateTime")
            .unwrap();
        let text = configs.to_text();
        assert_eq!(
            text,
            "leader.replication.throttled.replicas=\nmessage.timestamp.type=CreateTime\nmin.insync.replicas=2"
        );
        assert_eq!(text.parse::<KafkaTopicConfigs>(), Ok(configs));
        assert_eq!(
            KAFKA_CONFIGS_EMPTY.parse::<KafkaTopicConfigs>(),
            Ok(KafkaTopicConfigs::new())
        );
    }

    #[test]
    fn given_a_non_canonical_form_when_parsing_should_refuse() {
        for text in [
            "",
            "b=1\na=2",
            "a=1\na=2",
            "a=1\n",
            "A=1",
            "a",
            "a=1\r",
            &format!("a={}", "x".repeat(KAFKA_CONFIGS_MAX_LENGTH)),
        ] {
            assert!(text.parse::<KafkaTopicConfigs>().is_err(), "{text:?}");
        }
    }

    #[test]
    fn given_a_line_break_when_inserting_should_refuse() {
        let mut configs = KafkaTopicConfigs::new();
        assert!(configs.insert("a", "x\ny").is_err());
        assert!(configs.insert("", "x").is_err());
        assert!(configs.is_empty());
    }
}
