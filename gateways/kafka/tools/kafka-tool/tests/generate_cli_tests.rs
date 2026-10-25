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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Removes its directory on drop (including on test panic/failure), so a failed assertion here
/// cannot leak a `kafka-gen-test-*` directory into the system temp dir.
struct TempOutputDir(PathBuf);

impl Drop for TempOutputDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn has_file_with_prefix(dir: &Path, prefix: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(prefix))
    })
}

#[test]
fn generate_accepts_repeated_api_key_flags() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let output = TempOutputDir(std::env::temp_dir().join(format!("kafka-gen-test-{nanos}")));

    let status = Command::new(env!("CARGO_BIN_EXE_kafka-message-gen"))
        .arg("generate")
        .arg("--output")
        .arg(&output.0)
        .arg("--api-key")
        .arg("0")
        .arg("--api-key")
        .arg("1")
        .status()
        .expect("run kafka-message-gen generate");

    assert!(
        status.success(),
        "generate should accept multiple --api-key flags (PR description / Verify parity); \
         got exit status {status:?}"
    );

    // A successful exit status alone doesn't prove either requested key actually produced
    // fixtures - `cmd_generate` used to swallow a requested key generating zero files as a
    // silent no-op. Check the files landed, not just that the process didn't error.
    assert!(
        has_file_with_prefix(&output.0, "000_Produce_v"),
        "expected at least one 000_Produce_v*.bin fixture in {}",
        output.0.display()
    );
    assert!(
        has_file_with_prefix(&output.0, "001_Fetch_v"),
        "expected at least one 001_Fetch_v*.bin fixture in {}",
        output.0.display()
    );
}

#[test]
fn generate_above_max_writes_the_next_version_with_the_newest_body() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let output = TempOutputDir(std::env::temp_dir().join(format!("kafka-gen-above-{nanos}")));

    let status = Command::new(env!("CARGO_BIN_EXE_kafka-message-gen"))
        .arg("generate")
        .arg("--output")
        .arg(&output.0)
        .arg("--api-key")
        .arg("0")
        .arg("--above-max")
        .status()
        .expect("run kafka-message-gen generate --above-max");
    assert!(status.success(), "generate --above-max: {status:?}");

    // Produce's newest schema version is 13: v14 is the next one, and it carries v13's body.
    let newest = std::fs::read(output.0.join("000_Produce_v13.bin")).expect("v13 written");
    let above = std::fs::read(output.0.join("000_Produce_v14.bin")).expect("v14 written");
    assert_eq!(
        i16::from_be_bytes([above[6], above[7]]),
        14,
        "header version"
    );
    // Same length and bytes, apart from the header version and the correlation id.
    assert_eq!(newest.len(), above.len());
    assert_eq!(newest[..6], above[..6]);
    assert_eq!(newest[12..], above[12..]);
    assert!(!has_file_with_prefix(&output.0, "000_Produce_v15"));
}

#[test]
fn generate_above_max_follows_the_newest_version_that_builds() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let output = TempOutputDir(std::env::temp_dir().join(format!("kafka-gen-lo-{nanos}")));
    let status = Command::new(env!("CARGO_BIN_EXE_kafka-message-gen"))
        .arg("generate")
        .arg("--output")
        .arg(&output.0)
        .arg("--api-key")
        .arg("2")
        .arg("--above-max")
        .status()
        .expect("run kafka-message-gen generate --above-max");
    assert!(status.success(), "generate --above-max: {status:?}");
    // The schema reaches ListOffsets v11, but `kafka-protocol` 0.18 encodes up to v10, so the
    // version after the newest that builds is v11, carrying v10's body.
    assert!(output.0.join("002_ListOffsets_v10.bin").is_file());
    let above = std::fs::read(output.0.join("002_ListOffsets_v11.bin")).expect("v11 written");
    assert_eq!(i16::from_be_bytes([above[6], above[7]]), 11);
    assert!(!has_file_with_prefix(&output.0, "002_ListOffsets_v12"));
}
