# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""SASL probe for librdkafka 2.x through confluent-kafka-python (KW-9).

Usage: rdkafka_auth_probe.py BOOTSTRAP MECHANISM TOPIC SECONDS

Credentials come from the environment: PROBE_USER and PROBE_PASSWORD for PLAIN and SCRAM,
PROBE_TOKEN_URL for OAUTHBEARER (an OAuth token endpoint the probe POSTs to from librdkafka's
oauth_cb, so a refreshed token is fetched whenever librdkafka asks for one).

The probe produces one record, reads it back, and keeps doing so every second for SECONDS, so a
session that must re-authenticate (KIP-368) is exercised on its live connections. Prints
`ROUND n` per round trip and `DONE n` at the end, or `ERROR <code> <text>` and exits 1.
"""

import json
import os
import sys
import time
import urllib.request

import base64

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition


def oauth_cb(_config):
    request = urllib.request.Request(
        os.environ["PROBE_TOKEN_URL"],
        data=b"grant_type=client_credentials",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read())
    token = body["access_token"]
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload))
    print(f"TOKEN exp={claims['exp']}", flush=True)
    return token, float(claims["exp"])


def config(bootstrap, mechanism):
    common = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": mechanism,
        "socket.timeout.ms": 10000,
        "error_cb": on_error,
    }
    if mechanism == "OAUTHBEARER":
        common["oauth_cb"] = oauth_cb
    else:
        common["sasl.username"] = os.environ["PROBE_USER"]
        common["sasl.password"] = os.environ["PROBE_PASSWORD"]
    return common


def on_error(error):
    name = error.name()
    if error.code() == KafkaError._AUTHENTICATION or error.fatal():
        print(f"ERROR {name} {error.str()}", flush=True)
        os._exit(1)
    print(f"WARN {name} {error.str()}", flush=True)


def main():
    bootstrap, mechanism, topic, seconds = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    producer = Producer(config(bootstrap, mechanism))
    consumer_config = config(bootstrap, mechanism)
    consumer_config.update({"group.id": f"kw9-probe-{os.getpid()}", "enable.auto.commit": False})
    consumer = Consumer(consumer_config)
    consumer.assign([TopicPartition(topic, 0, 0)])
    deadline = time.time() + seconds
    rounds = 0
    seen = 0
    while True:
        delivered = []
        producer.produce(topic, key=b"k", value=f"round-{rounds}".encode(), partition=0,
                         on_delivery=lambda err, msg: delivered.append(err))
        producer.flush(15)
        if not delivered or delivered[0] is not None:
            print(f"ERROR PRODUCE {delivered}", flush=True)
            sys.exit(1)
        want = rounds + 1
        until = time.time() + 15
        while seen < want and time.time() < until:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"ERROR FETCH {message.error()}", flush=True)
                sys.exit(1)
            seen += 1
        if seen < want:
            print(f"ERROR FETCH read {seen} of {want}", flush=True)
            sys.exit(1)
        rounds += 1
        print(f"ROUND {rounds}", flush=True)
        if time.time() >= deadline:
            break
        time.sleep(1)
    consumer.close()
    print(f"DONE {rounds}", flush=True)


if __name__ == "__main__":
    main()
