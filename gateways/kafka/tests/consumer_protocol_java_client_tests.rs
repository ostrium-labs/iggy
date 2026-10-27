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

//! The Java 4.x consumer with `group.protocol=consumer` (KIP-848, KW-13) against the gateway and
//! against Apache Kafka 4.3.1.
//!
//! `tests/java/ConsumerProtocolProbe.java` runs two members and lets one leave, resumes the group
//! with a third, describes it, and resumes with a KIP-848 consumer a group a classic one committed
//! in. The gateway must print what Apache Kafka prints.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/java.rs"]
mod java;
#[path = "common/oracle.rs"]
mod oracle;

use iggy_server::TestServer;
use oracle::{KafkaOracle, Unavailable};

/// Every line the probe prints that both brokers must agree on.
const COMPARED: &[&str] = &[
    "two_members_consumed",
    "two_members_a_partitions",
    "two_members_b_partitions",
    "two_members_disjoint",
    "after_leave_a_partitions",
    "resumed",
    "resumed_first_offset",
    "describe_type",
    "describe_state",
    "describe_members",
    "describe_client_id",
    "describe_partitions",
    "classic_read",
    "classic_then_consumer",
];

/// The gateway bridged to `server`, as Kafka's oracle runs: no initial rebalance delay.
async fn gateway(server: &TestServer) -> (String, broadcast::Sender<()>) {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("the bridge connects");
    let listener = bind_listener("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("address").to_string();
    let listener = BoundListener::new(
        ListenerConfig::new(SecurityProtocol::Plaintext, address.clone()),
        listener,
    );
    let mut config = GatewayConfig::default();
    config.group.initial_rebalance_delay = Duration::ZERO;
    let gateway = KafkaGateway::new(config).with_bridge(Some(Arc::new(bridge)));
    let (shutdown, receiver) = broadcast::channel(1);
    tokio::spawn(async move {
        let _ = gateway.run(vec![listener], receiver).await;
    });
    (address, shutdown)
}

fn probe(jars: &std::path::Path, classes: &std::path::Path, bootstrap: &str, run: &str) -> String {
    let topic = format!("kw13-{run}");
    let (ok, text) = java::run_with(
        java::KAFKA_4,
        jars,
        classes,
        "ConsumerProtocolProbe",
        &[bootstrap, &topic, &topic],
    )
    .unwrap_or_else(|error| panic!("{error}"));
    println!("ConsumerProtocolProbe against {bootstrap}:\n{text}");
    assert!(
        ok && text.contains("CONSUMER_PROBE_OK"),
        "the probe failed\n{text}"
    );
    text
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_the_java_4_consumer_with_the_consumer_protocol_should_consume_as_on_kafka() {
    if !java::java_available() {
        java::skip("java or javac is unavailable");
        return;
    }
    let jars = java::KAFKA_4.dir();
    if let Err(reason) = java::ensure_jars_of(java::KAFKA_4, &jars) {
        java::skip(&reason);
        return;
    }
    let classes = tempfile::tempdir().expect("class dir");
    java::compile_with(java::KAFKA_4, &jars, classes.path(), "ConsumerProtocolProbe.java")
        .unwrap_or_else(|error| panic!("{error}"));
    let run = oracle::run_id();

    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (address, shutdown) = gateway(&server).await;
    let ours = tokio::task::spawn_blocking({
        let (jars, classes, run) = (jars.clone(), classes.path().to_path_buf(), run.clone());
        move || probe(&jars, &classes, &address, &run)
    })
    .await
    .expect("the probe ran");
    assert_eq!(java::line(&ours, "two_members_consumed"), Some("20"));
    assert_eq!(java::line(&ours, "two_members_disjoint"), Some("true"));
    assert_eq!(java::line(&ours, "after_leave_a_partitions"), Some("2"));
    assert_eq!(java::line(&ours, "resumed"), Some("6"));
    assert_eq!(java::line(&ours, "classic_then_consumer"), Some("4"));

    match KafkaOracle::start().await {
        Ok(kafka) => {
            let bootstrap = kafka.target.address.clone();
            let theirs = tokio::task::spawn_blocking({
                let (jars, classes) = (jars.clone(), classes.path().to_path_buf());
                move || probe(&jars, &classes, &bootstrap, &run)
            })
            .await
            .expect("the probe ran");
            for key in COMPARED {
                assert_eq!(
                    java::line(&ours, key),
                    java::line(&theirs, key),
                    "{key}: the gateway and Apache Kafka differ"
                );
            }
            drop(kafka);
        }
        Err(Unavailable::Skipped(reason)) => java::skip(&reason),
        Err(Unavailable::Failed(reason)) => panic!("Apache Kafka oracle: {reason}"),
    }
    let _ = shutdown.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}
