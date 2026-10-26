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

//! Toxiproxy in its container, on the host network, driven over its HTTP API with `curl`: TCP
//! proxies whose upstream can move (a restarted gateway on a new port) and that can be cut,
//! slowed or reset, for network faults without root.

#![allow(dead_code)]

use std::process::Command;
use std::time::Duration;

/// Pinned. Pulled by the fault suite only, never by pull requests.
pub const TOXIPROXY: &str = "ghcr.io/shopify/toxiproxy:2.12.0";

pub struct Toxiproxy {
    name: String,
    api: String,
    cli: String,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("a free port")
}

impl Toxiproxy {
    /// Starts the container. `Err` when the container CLI or the image is missing.
    pub fn start() -> Result<Self, String> {
        let cli = std::env::var("KAFKA_ORACLE_CONTAINER_CLI").unwrap_or_else(|_| "podman".into());
        let port = free_port();
        let name = format!("kwt-toxiproxy-{}-{port}", std::process::id());
        let output = Command::new(&cli)
            .args(["run", "-d", "--rm", "--network", "host", "--name", &name, TOXIPROXY])
            .args(["-host", "127.0.0.1", "-port", &port.to_string()])
            .output()
            .map_err(|error| format!("{cli} run: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "{cli} run {TOXIPROXY}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let proxy = Self {
            name,
            api: format!("http://127.0.0.1:{port}"),
            cli,
        };
        for _ in 0..100 {
            if proxy.call("GET", "/version", None).is_ok() {
                return Ok(proxy);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err("toxiproxy did not answer".into())
    }

    fn call(&self, method: &str, path: &str, body: Option<&str>) -> Result<String, String> {
        let mut command = Command::new("curl");
        command
            .args(["--fail", "--silent", "--show-error", "--max-time", "10", "-X", method])
            .arg(format!("{}{path}", self.api));
        if let Some(body) = body {
            command.args(["-H", "Content-Type: application/json", "-d", body]);
        }
        let output = command.output().map_err(|error| format!("curl: {error}"))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(format!(
                "{method} {path}: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    /// A proxy `name` on a free local port to `upstream`. Its listen address.
    pub fn proxy(&self, name: &str, upstream: &str) -> String {
        let listen = format!("127.0.0.1:{}", free_port());
        self.call(
            "POST",
            "/proxies",
            Some(&format!(
                r#"{{"name":"{name}","listen":"{listen}","upstream":"{upstream}","enabled":true}}"#
            )),
        )
        .expect("create a proxy");
        listen
    }

    /// Points proxy `name` at `upstream`.
    pub fn retarget(&self, name: &str, upstream: &str) {
        self.call(
            "POST",
            &format!("/proxies/{name}"),
            Some(&format!(r#"{{"upstream":"{upstream}"}}"#)),
        )
        .expect("retarget a proxy");
    }

    /// Cuts (`false`) or restores (`true`) proxy `name`: every connection through it closes and
    /// new ones are refused while it is off.
    pub fn enable(&self, name: &str, enabled: bool) {
        self.call(
            "POST",
            &format!("/proxies/{name}"),
            Some(&format!(r#"{{"enabled":{enabled}}}"#)),
        )
        .expect("toggle a proxy");
    }

    /// Adds toxic `toxic` of `kind` to proxy `name`, with `attributes` as a JSON object.
    pub fn toxic(&self, name: &str, toxic: &str, kind: &str, attributes: &str) {
        self.call(
            "POST",
            &format!("/proxies/{name}/toxics"),
            Some(&format!(
                r#"{{"name":"{toxic}","type":"{kind}","stream":"downstream","toxicity":1.0,"attributes":{attributes}}}"#
            )),
        )
        .expect("add a toxic");
    }

    pub fn remove_toxic(&self, name: &str, toxic: &str) {
        self.call("DELETE", &format!("/proxies/{name}/toxics/{toxic}"), None)
            .expect("remove a toxic");
    }
}

impl Drop for Toxiproxy {
    fn drop(&mut self) {
        let _ = Command::new(&self.cli).args(["rm", "-f", &self.name]).output();
    }
}
