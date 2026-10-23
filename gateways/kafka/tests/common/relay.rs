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

//! A TCP relay between the bridge and `iggy-server` that can hold what the bridge sends, so a
//! call waits unanswered and the SDK gives up on it in doubt.
//!
//! The same relay as `offset_groups_real_bridge_tests.rs`, shared here for the Produce suites.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// A relay to `upstream` that numbers its connections in the order they open.
pub struct PausingRelay {
    pub address: String,
    holds: Arc<Mutex<Holds>>,
}

#[derive(Default)]
struct Holds {
    /// Per connection, in the order they opened: `true` holds what its client sends.
    switches: Vec<watch::Sender<bool>>,
    /// Whether a connection that opens now starts held.
    new: bool,
}

impl PausingRelay {
    pub async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the relay");
        let address = listener.local_addr().expect("relay address").to_string();
        let holds = Arc::new(Mutex::new(Holds::default()));
        let connections = Arc::clone(&holds);
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let paused = {
                    let mut holds = connections.lock().expect("relay connections");
                    let (switch, paused) = watch::channel(holds.new);
                    holds.switches.push(switch);
                    paused
                };
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    if let Ok(outbound) = TcpStream::connect(&upstream).await {
                        relay(inbound, outbound, paused).await;
                    }
                });
            }
        });
        Self { address, holds }
    }

    /// The connections opened so far.
    pub fn opened(&self) -> usize {
        self.holds.lock().expect("relay connections").switches.len()
    }

    /// Holds what the client of every connection sends from now on, those that open later too,
    /// so a client that reconnects cannot get past the hold.
    pub fn hold_all(&self) {
        let mut holds = self.holds.lock().expect("relay connections");
        holds.new = true;
        for switch in &holds.switches {
            switch.send_replace(true);
        }
    }

    /// Sends on what every connection held, and holds no connection that opens from now on.
    pub fn release_all(&self) {
        let mut holds = self.holds.lock().expect("relay connections");
        holds.new = false;
        for switch in &holds.switches {
            switch.send_replace(false);
        }
    }
}

/// Copies both ways between a client and the server. What the client sends waits while `paused`
/// is `true`.
async fn relay(inbound: TcpStream, outbound: TcpStream, mut paused: watch::Receiver<bool>) {
    let (mut from_client, mut to_client) = inbound.into_split();
    let (mut from_server, mut to_server) = outbound.into_split();
    let requests = async move {
        let mut buf = vec![0; 64 * 1024];
        loop {
            let read = match from_client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            if paused.wait_for(|paused| !*paused).await.is_err()
                || to_server.write_all(&buf[..read]).await.is_err()
            {
                break;
            }
        }
    };
    let replies = async move {
        let _ = tokio::io::copy(&mut from_server, &mut to_client).await;
    };
    tokio::join!(requests, replies);
}
