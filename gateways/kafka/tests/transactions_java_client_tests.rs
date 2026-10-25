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

//! The Java producer's transactions against the gateway and against Apache Kafka (KW-T).
//!
//! `tests/java/TransactionsProbe.java` commits, aborts, and fences a producer with a second one of
//! the same transactional id, then reads at both isolation levels. The gateway must give the
//! answers Apache Kafka 4.3.1 gives, record for record.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::transactions::TransactionConfig;
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/java.rs"]
mod java;
#[path = "common/oracle.rs"]
mod oracle;

use iggy_server::TestServer;
use oracle::{KafkaOracle, Unavailable};

/// The gateway with transactions on, bridged to `server`. Its address, and the sender that stops
/// it.
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
    let config = GatewayConfig {
        transactions: Some(TransactionConfig::default()),
        ..GatewayConfig::default()
    };
    let gateway = KafkaGateway::new(config).with_bridge(Some(Arc::new(bridge)));
    let (shutdown, receiver) = broadcast::channel(1);
    tokio::spawn(async move {
        let _ = gateway.run(vec![listener], receiver).await;
    });
    (address, shutdown)
}

fn probe(jars: &std::path::Path, classes: &std::path::Path, bootstrap: &str) -> String {
    let (ok, text) = java::run(
        jars,
        classes,
        "TransactionsProbe",
        &[bootstrap, "txn-probe", "txn-probe-id"],
    )
    .unwrap_or_else(|error| panic!("{error}"));
    println!("TransactionsProbe against {bootstrap}:\n{text}");
    assert!(
        ok && text.contains("TXN_PROBE_OK"),
        "the probe failed\n{text}"
    );
    text
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_the_java_producer_when_it_commits_aborts_and_is_fenced_should_see_what_kafka_shows()
{
    if !java::java_available() {
        java::skip("java or javac is unavailable");
        return;
    }
    let jars = java::jar_dir();
    if let Err(reason) = java::ensure_jars(&jars) {
        java::skip(&reason);
        return;
    }
    let classes = tempfile::tempdir().expect("class dir");
    java::compile(&jars, classes.path(), "TransactionsProbe.java")
        .unwrap_or_else(|error| panic!("{error}"));

    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (address, shutdown) = gateway(&server).await;
    let ours = tokio::task::spawn_blocking({
        let (jars, classes) = (jars.clone(), classes.path().to_path_buf());
        move || probe(&jars, &classes, &address)
    })
    .await
    .expect("the probe ran");

    let committed = "[c1, c2, c3, c4, c5]";
    let uncommitted = "[a1, a2, c1, c2, c3, c4, c5]";
    assert_eq!(java::line(&ours, "fenced"), Some("ProducerFencedException"));
    assert_eq!(java::line(&ours, "read_uncommitted"), Some(uncommitted));
    assert_eq!(java::line(&ours, "read_committed"), Some(committed));

    match KafkaOracle::start().await {
        Ok(kafka) => {
            let bootstrap = kafka.target.address.clone();
            let theirs = tokio::task::spawn_blocking({
                let (jars, classes) = (jars.clone(), classes.path().to_path_buf());
                move || probe(&jars, &classes, &bootstrap)
            })
            .await
            .expect("the probe ran");
            for key in ["fenced", "read_uncommitted", "read_committed"] {
                assert_eq!(
                    java::line(&ours, key),
                    java::line(&theirs, key),
                    "{key}: the gateway and Apache Kafka differ"
                );
            }
            assert_eq!(java::line(&theirs, "read_committed"), Some(committed));
            drop(kafka);
        }
        Err(Unavailable::Skipped(reason)) => java::skip(&reason),
        Err(Unavailable::Failed(reason)) => panic!("Apache Kafka oracle: {reason}"),
    }
    let _ = shutdown.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_consume_transform_produce_loop_with_aborts_should_copy_each_record_once() {
    if !java::java_available() {
        java::skip("java or javac is unavailable");
        return;
    }
    let jars = java::jar_dir();
    if let Err(reason) = java::ensure_jars(&jars) {
        java::skip(&reason);
        return;
    }
    let classes = tempfile::tempdir().expect("class dir");
    java::compile(&jars, classes.path(), "EosCopyProbe.java")
        .unwrap_or_else(|error| panic!("{error}"));
    let run = |bootstrap: String| {
        let (jars, classes) = (jars.clone(), classes.path().to_path_buf());
        move || {
            let (ok, text) = java::run(
                &jars,
                &classes,
                "EosCopyProbe",
                &[&bootstrap, "eos-in", "eos-out", "eos-group", "eos-copier", "60"],
            )
            .unwrap_or_else(|error| panic!("{error}"));
            println!("EosCopyProbe against {bootstrap}:\n{text}");
            assert!(ok && text.contains("EOS_PROBE_OK"), "the probe failed\n{text}");
            text
        }
    };

    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (address, shutdown) = gateway(&server).await;
    let ours = tokio::task::spawn_blocking(run(address))
        .await
        .expect("the probe ran");
    assert_eq!(java::line(&ours, "exactly_once"), Some("true"), "{ours}");
    assert_eq!(java::line(&ours, "output_records"), Some("60"));
    assert_ne!(java::line(&ours, "aborted"), Some("0"), "the loop aborted some");

    match KafkaOracle::start().await {
        Ok(kafka) => {
            let theirs = tokio::task::spawn_blocking(run(kafka.target.address.clone()))
                .await
                .expect("the probe ran");
            assert_eq!(java::line(&theirs, "exactly_once"), Some("true"), "{theirs}");
            drop(kafka);
        }
        Err(Unavailable::Skipped(reason)) => java::skip(&reason),
        Err(Unavailable::Failed(reason)) => panic!("Apache Kafka oracle: {reason}"),
    }
    let _ = shutdown.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_a_kafka_streams_word_count_at_exactly_once_v2_should_count_as_on_kafka() {
    if !java::java_available() {
        java::skip("java or javac is unavailable");
        return;
    }
    let jars = java::jar_dir();
    if let Err(reason) = java::ensure_jars(&jars) {
        java::skip(&reason);
        return;
    }
    let classes = tempfile::tempdir().expect("class dir");
    java::compile(&jars, classes.path(), "StreamsEosProbe.java")
        .unwrap_or_else(|error| panic!("{error}"));
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let run = |bootstrap: String, application: String| {
        let (jars, classes) = (jars.clone(), classes.path().to_path_buf());
        move || {
            let (ok, text) = java::run(
                &jars,
                &classes,
                "StreamsEosProbe",
                &[&bootstrap, &application, "wc-in", "wc-out"],
            )
            .unwrap_or_else(|error| panic!("{error}"));
            println!("StreamsEosProbe against {bootstrap}:\n{text}");
            assert!(ok && text.contains("STREAMS_PROBE_OK"), "the probe failed\n{text}");
            text
        }
    };

    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let (address, shutdown) = gateway(&server).await;
    let ours = tokio::task::spawn_blocking(run(address, format!("wc-gateway-{stamp}")))
        .await
        .expect("the probe ran");
    assert_eq!(java::line(&ours, "exactly_once"), Some("true"), "{ours}");

    match KafkaOracle::start().await {
        Ok(kafka) => {
            let theirs = tokio::task::spawn_blocking(run(
                kafka.target.address.clone(),
                format!("wc-kafka-{stamp}"),
            ))
            .await
            .expect("the probe ran");
            assert_eq!(java::line(&theirs, "exactly_once"), Some("true"), "{theirs}");
            assert_eq!(java::line(&ours, "counts"), java::line(&theirs, "counts"));
            drop(kafka);
        }
        Err(Unavailable::Skipped(reason)) => java::skip(&reason),
        Err(Unavailable::Failed(reason)) => panic!("Apache Kafka oracle: {reason}"),
    }
    let _ = shutdown.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}
