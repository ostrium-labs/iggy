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

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// What a topic's log does with old records, in Kafka's terms.
///
/// Stored and reported. The server does not compact yet (apache/iggy#4521),
/// so `Compact` keeps every record; the Kafka gateway gives a topic whose
/// policy has no `delete` no expiry and no size cap, so it never loses data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub enum CleanupPolicy {
    /// Old segments expire by `message_expiry` and `max_topic_size`.
    #[default]
    Delete,
    /// Only the last record per key is kept, once compaction exists.
    Compact,
    /// Both: compacted, and old segments still expire.
    CompactDelete,
}

impl CleanupPolicy {
    /// The canonical spelling, Kafka's own list form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Compact => "compact",
            Self::CompactDelete => "compact,delete",
        }
    }

    /// Whether old segments may expire under this policy.
    #[must_use]
    pub const fn deletes(self) -> bool {
        matches!(self, Self::Delete | Self::CompactDelete)
    }

    /// Whether the log is compacted under this policy.
    #[must_use]
    pub const fn compacts(self) -> bool {
        matches!(self, Self::Compact | Self::CompactDelete)
    }

    /// The policy with exactly these parts, or `None` for neither.
    #[must_use]
    pub const fn from_parts(compact: bool, delete: bool) -> Option<Self> {
        match (compact, delete) {
            (true, true) => Some(Self::CompactDelete),
            (true, false) => Some(Self::Compact),
            (false, true) => Some(Self::Delete),
            (false, false) => None,
        }
    }
}

impl AsRef<str> for CleanupPolicy {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for CleanupPolicy {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A comma-separated list of `compact` and `delete`, in any order, with
/// spaces around items and repeats allowed, as Kafka accepts it.
impl FromStr for CleanupPolicy {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut compact = false;
        let mut delete = false;
        for part in text.split(',') {
            match part.trim() {
                "compact" => compact = true,
                "delete" => delete = true,
                other => return Err(format!("unknown cleanup policy '{other}'")),
            }
        }
        Self::from_parts(compact, delete).ok_or_else(|| "empty cleanup policy".to_owned())
    }
}

impl From<CleanupPolicy> for String {
    fn from(policy: CleanupPolicy) -> Self {
        policy.as_str().to_owned()
    }
}

impl TryFrom<String> for CleanupPolicy {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_kafka_spellings_when_parsing_should_reach_the_canonical_policy() {
        for (text, policy) in [
            ("delete", CleanupPolicy::Delete),
            ("compact", CleanupPolicy::Compact),
            ("compact,delete", CleanupPolicy::CompactDelete),
            ("delete,compact", CleanupPolicy::CompactDelete),
            ("compact, delete", CleanupPolicy::CompactDelete),
            ("compact,compact", CleanupPolicy::Compact),
        ] {
            assert_eq!(text.parse::<CleanupPolicy>(), Ok(policy), "{text}");
        }
        assert_eq!(CleanupPolicy::CompactDelete.to_string(), "compact,delete");
    }

    #[test]
    fn given_an_unknown_or_empty_part_when_parsing_should_refuse() {
        for text in ["", "Compact", "compact,", "foo", "compact,foo"] {
            assert!(text.parse::<CleanupPolicy>().is_err(), "{text}");
        }
    }
}
