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

//! KW-T's crash and network-fault matrix (plan section 6.7, item 5): the gateway process killed
//! mid-transaction and between marker writes, Iggy cut off or paused while a transaction ends,
//! and an exactly-once loop run through repeated gateway kills.
//!
//! Clients reach the gateway, and the gateway reaches Iggy, through Toxiproxy (a pinned
//! container, no root), so a restarted gateway keeps its advertised address and the network
//! between the gateway and Iggy can be cut or slowed. Heavy and slow: runs only with
//! `KAFKA_FAULT_TESTS=1`, on a machine set aside for it (the plan's second laptop).

use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;

use iggy_gateway_kafka::bridge::{IggyBridge, IggyTxnLog};
use iggy_gateway_kafka::records::{ControlType, control_of};
use iggy_gateway_kafka::transactions::{TxnLog, TxnState};

#[path = "common/gateway_process.rs"]
mod gateway_process;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/java.rs"]
mod java;
#[path = "common/oracle.rs"]
mod oracle;
#[path = "common/toxiproxy.rs"]
mod toxiproxy;
#[path = "common/txn_wire.rs"]
mod txn_wire;

use gateway_process::TestGateway;
use iggy::prelude::{Consumer, Identifier, MessageClient, PollingStrategy};
use iggy_server::{TestServer, raw_client};
use oracle::WireClient;
use toxiproxy::Toxiproxy;
use txn_wire::{add, batch, end, init, produce, read_until, reconnect};

const STREAM: &str = "kafka";

fn enabled() -> bool {
    if std::env::var("KAFKA_FAULT_TESTS").as_deref() == Ok("1") {
        return true;
    }
    eprintln!("skipping the KW-T fault matrix: set KAFKA_FAULT_TESTS=1 to run it");
    false
}

/// An `iggy-server`, and a gateway with transactions on that clients reach through the `client`
/// proxy and that reaches Iggy through the `iggy` proxy.
struct Rig {
    // Dropped last: the gateway and Iggy go first.
    proxy: Toxiproxy,
    server: TestServer,
    gateway: Option<TestGateway>,
    /// What clients dial and what the gateway advertises.
    address: String,
    iggy_address: String,
    _data_dir: tempfile::TempDir,
}

impl Rig {
    async fn start(topics: &[(&str, u32)]) -> Self {
        let proxy = Toxiproxy::start().unwrap_or_else(|error| panic!("toxiproxy: {error}"));
        let data_dir = tempfile::tempdir().expect("tempdir");
        let server = TestServer::spawn(data_dir.path()).await;
        let bridge = IggyBridge::connect(server.test_config())
            .await
            .expect("seed bridge");
        for (topic, partitions) in topics {
            bridge
                .ensure_stream_and_topic(topic, *partitions)
                .await
                .expect("seed a topic");
        }
        let iggy_address = proxy.proxy("iggy", &server.test_config().address);
        // A placeholder upstream until the gateway listens.
        let address = proxy.proxy("client", "127.0.0.1:9");
        let mut rig = Self {
            proxy,
            server,
            gateway: None,
            address,
            iggy_address,
            _data_dir: data_dir,
        };
        rig.restart().await;
        rig
    }

    /// Starts a gateway, a new process on a new port, and points the client proxy at it.
    async fn restart(&mut self) {
        let port = self
            .address
            .rsplit_once(':')
            .map(|(_, port)| port.to_string())
            .expect("a port");
        let gateway = TestGateway::spawn_with(
            &self.server,
            &[
                ("IGGY_KAFKA_TRANSACTIONS_ENABLED", "true"),
                ("IGGY_KAFKA_TRANSACTION_ABORT_INTERVAL_MS", "500"),
                ("IGGY_KAFKA_IGGY_ADDR", &self.iggy_address),
                ("IGGY_KAFKA_ADVERTISED_HOST", "127.0.0.1"),
                ("IGGY_KAFKA_ADVERTISED_PORT", &port),
            ],
        )
        .await;
        self.proxy.retarget("client", &gateway.address);
        self.gateway = Some(gateway);
    }

    /// SIGKILL, as a crash.
    fn crash(&mut self) {
        if let Some(gateway) = self.gateway.take() {
            gateway.crash();
        }
    }

    async fn client(&self) -> WireClient {
        let mut client = WireClient::connect(&self.address)
            .await
            .unwrap_or_else(|_| panic!("connect to {}", self.address));
        reconnect(&mut client, &self.address).await;
        client
    }

    /// The control records stored on `partition` of `topic`, straight from Iggy, waiting until
    /// `count` are there.
    async fn controls(&self, topic: &str, partition: u32, count: usize) -> Vec<ControlType> {
        let mut found = Vec::new();
        for _ in 0..300 {
            let polled = raw_client(&self.server)
                .await
                .poll_messages(
                    &Identifier::named(STREAM).expect("stream"),
                    &Identifier::named(topic).expect("topic"),
                    Some(partition),
                    &Consumer::new(Identifier::named("fault-reader").expect("consumer")),
                    &PollingStrategy::offset(0),
                    1000,
                    false,
                )
                .await
                .expect("poll the partition");
            found = polled
                .messages
                .iter()
                .filter_map(control_of)
                .map(|(control, _)| control)
                .collect();
            if found.len() >= count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        found
    }

    /// The last state of `transactional_id` in the transaction log, once it leaves the
    /// `Prepare*` and open states or 30 s pass.
    async fn settled_state(&self, transactional_id: &str) -> TxnState {
        let bridge = Arc::new(
            IggyBridge::connect(self.server.test_config())
                .await
                .expect("bridge"),
        );
        let log = IggyTxnLog::new(bridge);
        let mut state = TxnState::Ongoing;
        for _ in 0..300 {
            let records = log.read_all().await.expect("read the transaction log");
            if let Some(record) = records
                .iter()
                .rev()
                .find(|record| record.transactional_id() == transactional_id)
            {
                state = record.metadata().state;
                if !state.is_open() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        log.close().await;
        state
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_gateway_killed_mid_transaction_when_the_producer_returns_should_abort_and_hide_it()
{
    if !enabled() {
        return;
    }
    let mut rig = Rig::start(&[("f1", 1)]).await;
    let mut client = rig.client().await;
    let producer = init(&mut client, &rig.address, "f1-tx", 60_000).await;
    assert_eq!(add(&mut client, "f1-tx", producer, "f1", &[0]).await, 0);
    let records = batch(producer.0, producer.1, 0, &["open1", "open2"]);
    assert_eq!(produce(&mut client, "f1-tx", "f1", 0, records).await.0, 0);

    rig.crash();
    rig.restart().await;
    let mut client = rig.client().await;
    let (values, stable, _) =
        read_until(&mut client, &rig.address, "f1", 0, |_, stable, _| stable >= 0).await;
    assert_eq!(
        (values.len(), stable),
        (0, 0),
        "the recovered open transaction holds the last stable offset"
    );

    let (same, epoch) = init(&mut client, &rig.address, "f1-tx", 60_000).await;
    assert_eq!(same, producer.0);
    assert_eq!(epoch, producer.1 + 2, "fenced, aborted, then bumped again");
    let (values, stable, high_watermark) =
        read_until(&mut client, &rig.address, "f1", 0, |_, stable, high| stable == high).await;
    assert_eq!((stable, high_watermark), (3, 3), "two records and an abort marker");
    assert!(values.is_empty(), "read_committed never shows them: {values:?}");
    assert_eq!(rig.controls("f1", 0, 1).await, [ControlType::Abort]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_gateway_killed_mid_transaction_when_the_producer_never_returns_should_abort_at_the_timeout()
 {
    if !enabled() {
        return;
    }
    let mut rig = Rig::start(&[("f2", 1)]).await;
    let mut client = rig.client().await;
    let producer = init(&mut client, &rig.address, "f2-tx", 2_000).await;
    assert_eq!(add(&mut client, "f2-tx", producer, "f2", &[0]).await, 0);
    let records = batch(producer.0, producer.1, 0, &["lost"]);
    assert_eq!(produce(&mut client, "f2-tx", "f2", 0, records).await.0, 0);

    rig.crash();
    rig.restart().await;
    let mut client = rig.client().await;
    let (values, stable, high_watermark) =
        read_until(&mut client, &rig.address, "f2", 0, |_, stable, high| {
            stable == high && high > 1
        })
        .await;
    assert_eq!((stable, high_watermark), (2, 2));
    assert!(values.is_empty(), "{values:?}");
    assert_eq!(rig.settled_state("f2-tx").await, TxnState::CompleteAbort);
    assert_eq!(
        end(&mut client, "f2-tx", producer, true).await,
        Ok(90),
        "the producer that never came back is fenced"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_gateway_killed_between_marker_writes_should_finish_the_commit_on_restart() {
    if !enabled() {
        return;
    }
    let mut rig = Rig::start(&[("f3", 3)]).await;
    let mut client = rig.client().await;
    let producer = init(&mut client, &rig.address, "f3-tx", 60_000).await;
    assert_eq!(add(&mut client, "f3-tx", producer, "f3", &[0, 1, 2]).await, 0);
    for partition in 0..3 {
        let records = batch(producer.0, producer.1, 0, &[&format!("p{partition}")]);
        assert_eq!(produce(&mut client, "f3-tx", "f3", partition, records).await.0, 0);
    }
    // Every Iggy call now takes 400 ms each way, so the markers go out one by one, slowly.
    rig.proxy
        .toxic("iggy", "slow", "latency", r#"{"latency":400,"jitter":0}"#);
    assert_eq!(end(&mut client, "f3-tx", producer, true).await, Ok(0));
    tokio::time::sleep(Duration::from_millis(900)).await;
    rig.crash();
    rig.proxy.remove_toxic("iggy", "slow");
    let before: Vec<usize> = futures::future::join_all(
        (0..3).map(|partition| rig.controls("f3", partition, 0)),
    )
    .await
    .iter()
    .map(Vec::len)
    .collect();
    eprintln!("commit markers per partition when the gateway died: {before:?}");

    rig.restart().await;
    let mut client = rig.client().await;
    for partition in 0..3 {
        let expected = vec![format!("p{partition}")];
        let (values, stable, high) = read_until(
            &mut client,
            &rig.address,
            "f3",
            partition,
            |values, stable, high| stable == high && values.len() == 1,
        )
        .await;
        assert_eq!(values, expected, "partition {partition}: committed exactly once");
        assert_eq!(stable, high);
        let controls = rig.controls("f3", u32::try_from(partition).unwrap_or(0), 1).await;
        assert!(
            !controls.is_empty() && controls.iter().all(|control| *control == ControlType::Commit),
            "partition {partition}: {controls:?}"
        );
    }
    assert_eq!(rig.settled_state("f3-tx").await, TxnState::CompleteCommit);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_iggy_cut_off_while_a_transaction_ends_should_finish_it_once_iggy_is_back() {
    if !enabled() {
        return;
    }
    let rig = Rig::start(&[("f4", 1)]).await;
    let mut client = rig.client().await;
    let producer = init(&mut client, &rig.address, "f4-tx", 60_000).await;

    // Cut before the decision: EndTxn cannot write it, says so retriably, and a retry later
    // succeeds.
    assert_eq!(add(&mut client, "f4-tx", producer, "f4", &[0]).await, 0);
    let records = batch(producer.0, producer.1, 0, &["first"]);
    assert_eq!(produce(&mut client, "f4-tx", "f4", 0, records).await.0, 0);
    rig.proxy.enable("iggy", false);
    let refused = end(&mut client, "f4-tx", producer, true).await;
    eprintln!("EndTxn with Iggy cut off answered {refused:?}");
    rig.proxy.enable("iggy", true);
    let mut code = refused.unwrap_or(-1);
    for _ in 0..100 {
        if code == 0 {
            break;
        }
        reconnect(&mut client, &rig.address).await;
        code = end(&mut client, "f4-tx", producer, true).await.unwrap_or(-1);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(code, 0, "the retried decision lands");

    // Cut after the decision: the markers wait for Iggy, then land.
    assert_eq!(add(&mut client, "f4-tx", producer, "f4", &[0]).await, 0);
    let records = batch(producer.0, producer.1, 1, &["second"]);
    assert_eq!(produce(&mut client, "f4-tx", "f4", 0, records).await.0, 0);
    assert_eq!(end(&mut client, "f4-tx", producer, true).await, Ok(0));
    rig.proxy.enable("iggy", false);
    tokio::time::sleep(Duration::from_secs(3)).await;
    rig.proxy.enable("iggy", true);

    let (values, stable, high) = read_until(&mut client, &rig.address, "f4", 0, |values, stable, high| {
        stable == high && values.len() == 2
    })
    .await;
    assert_eq!(values, ["first", "second"], "both committed, once each");
    assert_eq!(stable, high);
    assert_eq!(rig.settled_state("f4-tx").await, TxnState::CompleteCommit);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_iggy_paused_while_markers_are_written_should_finish_the_abort() {
    if !enabled() {
        return;
    }
    let rig = Rig::start(&[("f5", 2)]).await;
    let mut client = rig.client().await;
    let producer = init(&mut client, &rig.address, "f5-tx", 60_000).await;
    assert_eq!(add(&mut client, "f5-tx", producer, "f5", &[0, 1]).await, 0);
    for partition in 0..2 {
        let records = batch(producer.0, producer.1, 0, &["doomed"]);
        assert_eq!(produce(&mut client, "f5-tx", "f5", partition, records).await.0, 0);
    }
    assert_eq!(end(&mut client, "f5-tx", producer, false).await, Ok(0));
    let pid = rig.server.pid();
    // SIGSTOP and SIGCONT: Iggy freezes, with every connection open, as a stalled VM would.
    let signal = |name: &str| {
        std::process::Command::new("kill")
            .args([name, &pid.to_string()])
            .status()
            .expect("signal iggy-server");
    };
    signal("-STOP");
    tokio::time::sleep(Duration::from_secs(4)).await;
    signal("-CONT");
    for partition in 0..2 {
        let (values, stable, high) =
            read_until(&mut client, &rig.address, "f5", partition, |_, stable, high| {
                stable == high && high >= 2
            })
            .await;
        assert!(values.is_empty(), "partition {partition}: {values:?}");
        assert_eq!((stable, high), (2, 2), "partition {partition}");
    }
    assert_eq!(rig.settled_state("f5-tx").await, TxnState::CompleteAbort);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_the_gateway_killed_repeatedly_an_exactly_once_loop_should_copy_each_record_once() {
    if !enabled() {
        return;
    }
    if !java::java_available() {
        java::skip("java or javac is unavailable");
        return;
    }
    let jars = java::jar_dir();
    java::ensure_jars(&jars).unwrap_or_else(|error| panic!("{error}"));
    let classes = tempfile::tempdir().expect("class dir");
    java::compile(&jars, classes.path(), "EosChaosProbe.java")
        .unwrap_or_else(|error| panic!("{error}"));

    let mut rig = Rig::start(&[]).await;
    let bootstrap = rig.address.clone();
    let probe = tokio::task::spawn_blocking({
        let (jars, classes) = (jars.clone(), classes.path().to_path_buf());
        move || {
            java::run(
                &jars,
                &classes,
                "EosChaosProbe",
                &[&bootstrap, "chaos-in", "chaos-out", "chaos-g", "chaos-tx", "400", "150"],
            )
        }
    });
    for round in 0..3 {
        tokio::time::sleep(Duration::from_secs(12)).await;
        eprintln!("killing the gateway, round {round}");
        rig.crash();
        tokio::time::sleep(Duration::from_secs(2)).await;
        rig.restart().await;
    }
    let (ok, text) = probe
        .await
        .expect("the probe ran")
        .unwrap_or_else(|error| panic!("{error}"));
    println!("EosChaosProbe:\n{text}");
    assert!(ok && text.contains("CHAOS_PROBE_OK"), "{text}");
    assert_eq!(java::line(&text, "output_records"), Some("400"), "{text}");
    assert_eq!(java::line(&text, "exactly_once"), Some("true"), "{text}");
}
