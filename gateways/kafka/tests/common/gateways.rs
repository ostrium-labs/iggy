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

//! Gateways that share one bootstrap list (`IGGY_KAFKA_NODE_ID`), in the test process, each with
//! a bridge of its own to one `iggy-server`.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::membership::MembershipConfig;
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

use crate::iggy_server::TestServer;

/// Short timings, so a test sees joins and expiries in seconds.
pub const HEARTBEAT: Duration = Duration::from_millis(200);
pub const LEASE: Duration = Duration::from_millis(1500);
/// How long a gateway announces itself before it owns groups and leads partitions.
pub const JOIN_DELAY: Duration = MembershipConfig {
    node_id: 0,
    heartbeat_interval: HEARTBEAT,
    lease: LEASE,
}
.join_delay();

pub struct Gateway {
    pub node_id: i32,
    pub addr: SocketAddr,
    shutdown: Option<broadcast::Sender<()>>,
    task: JoinHandle<()>,
}

impl Gateway {
    pub async fn start(server: &TestServer, node_id: i32) -> Self {
        let bridge = IggyBridge::connect(server.test_config())
            .await
            .expect("the bridge connects to a ready server");
        let socket = bind_listener("127.0.0.1:0").expect("bind an ephemeral port");
        let addr = socket.local_addr().expect("local addr");
        let listener = BoundListener::new(
            ListenerConfig::new(SecurityProtocol::Plaintext, addr.to_string()),
            socket,
        );
        let config = GatewayConfig {
            membership: Some(MembershipConfig {
                node_id,
                heartbeat_interval: HEARTBEAT,
                lease: LEASE,
            }),
            instance_id: u16::try_from(node_id).expect("a small node id"),
            shutdown_drain_timeout: Duration::from_secs(2),
            ..GatewayConfig::default()
        };
        let gateway = KafkaGateway::new(config).with_bridge(Some(Arc::new(bridge)));
        let (shutdown, receiver) = broadcast::channel(1);
        let task = tokio::spawn(async move {
            gateway
                .run(vec![listener], receiver)
                .await
                .expect("the gateway runs");
        });
        Self {
            node_id,
            addr,
            shutdown: Some(shutdown),
            task,
        }
    }

    /// Shuts down as an operator's SIGTERM does: the gateway says goodbye.
    pub async fn stop(mut self) {
        drop(self.shutdown.take());
        tokio::time::timeout(Duration::from_secs(15), &mut self.task)
            .await
            .expect("the gateway stops")
            .expect("the gateway task does not panic");
    }

    /// Stops without a goodbye, as a crash does. The others notice when its lease runs out.
    pub fn crash(self) {
        self.task.abort();
        // Its shutdown sender drops with it, which nothing reads any more.
    }
}
