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

//! Java client probes on the host JDK: the Kafka 3.9 client jars from Maven Central (cached
//! under the workspace `target/`), `javac`, and a bounded `java` run. The same pattern as
//! `configs_java_client_tests.rs`, shared by the KW-T suites.

#![allow(dead_code)]

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// How long one `javac`, `java` or `curl` may run.
pub const CHILD_LIMIT: Duration = Duration::from_secs(240);

const JARS: &[(&str, &str)] = &[
    (
        "kafka-clients-3.9.0.jar",
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar",
    ),
    (
        "kafka-streams-3.9.0.jar",
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-streams/3.9.0/kafka-streams-3.9.0.jar",
    ),
    (
        "slf4j-api-1.7.36.jar",
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar",
    ),
    (
        "slf4j-simple-1.7.36.jar",
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar",
    ),
    (
        "lz4-java-1.8.0.jar",
        "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar",
    ),
    (
        "snappy-java-1.1.10.5.jar",
        "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
    ),
    (
        "zstd-jni-1.5.6-4.jar",
        "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.6-4/zstd-jni-1.5.6-4.jar",
    ),
];

/// Why the suite cannot run. Panics when `JAVA_E2E_REQUIRED=1`, so a run that means to cover
/// this fails instead of passing over nothing.
pub fn skip(reason: &str) {
    assert!(
        std::env::var("JAVA_E2E_REQUIRED").as_deref() != Ok("1"),
        "JAVA_E2E_REQUIRED=1 but the suite cannot run: {reason}"
    );
    eprintln!("skipping a Java client test: {reason}");
}

pub fn java_available() -> bool {
    ["java", "javac"].iter().all(|tool| {
        Command::new(tool)
            .arg("-version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    })
}

pub fn output_bounded(command: &mut Command, what: &str) -> Result<Output, String> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn {what}: {error}"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("{what} stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("{what} stderr"))?;
    let out = std::thread::spawn(move || {
        let mut buf = Vec::new();
        stdout.read_to_end(&mut buf).map(|_| buf)
    });
    let err = std::thread::spawn(move || {
        let mut buf = Vec::new();
        stderr.read_to_end(&mut buf).map(|_| buf)
    });
    let deadline = Instant::now() + CHILD_LIMIT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{what} exceeded {CHILD_LIMIT:?}"));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(error) => return Err(format!("poll {what}: {error}")),
        }
    };
    let stdout = out
        .join()
        .map_err(|_| format!("{what} stdout thread"))?
        .map_err(|error| format!("read {what} stdout: {error}"))?;
    let stderr = err
        .join()
        .map_err(|_| format!("{what} stderr thread"))?
        .map_err(|error| format!("read {what} stderr: {error}"))?;
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

pub fn jar_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-3.9-admin-jars")
}

pub fn ensure_jars(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|error| format!("create jar cache: {error}"))?;
    for (name, url) in JARS {
        let path = dir.join(name);
        if path.exists() {
            continue;
        }
        let output = output_bounded(
            Command::new("curl")
                .args([
                    "--fail",
                    "--silent",
                    "--show-error",
                    "--location",
                    "--max-time",
                    "60",
                    "-o",
                ])
                .arg(&path)
                .arg(url),
            name,
        );
        match output {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                let _ = std::fs::remove_file(&path);
                return Err(format!(
                    "download {url} failed: {}{}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                return Err(error);
            }
        }
    }
    Ok(())
}

pub fn classpath(jars: &Path, classes: &Path) -> String {
    let mut parts = vec![classes.display().to_string()];
    for (name, _) in JARS {
        parts.push(jars.join(name).display().to_string());
    }
    parts.join(":")
}

/// Compiles `tests/java/<source>` into `classes`.
pub fn compile(jars: &Path, classes: &Path, source: &str) -> Result<(), String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/java")
        .join(source);
    let output = output_bounded(
        Command::new("javac")
            .arg("--release")
            .arg("17")
            .arg("-cp")
            .arg(classpath(jars, classes))
            .arg("-d")
            .arg(classes)
            .arg(&path),
        "javac",
    )?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "javac {source} failed\n{}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// Runs `class` with `args`; its stdout and stderr together, and whether it exited 0.
pub fn run(
    jars: &Path,
    classes: &Path,
    class: &str,
    args: &[&str],
) -> Result<(bool, String), String> {
    let output = output_bounded(
        Command::new("java")
            .arg("-cp")
            .arg(classpath(jars, classes))
            .arg(class)
            .args(args),
        class,
    )?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok((output.status.success(), text))
}

/// The value of the `key=` line `text` prints.
pub fn line<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
}
