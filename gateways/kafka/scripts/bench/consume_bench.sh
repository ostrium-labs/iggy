#!/usr/bin/env bash
# Kafka-wire consume throughput of the Iggy Kafka gateway, with its CPU, against native Iggy.
#
#   consume_bench.sh <label> [runs]
#
# Env: SERVER_BIN (iggy-server), GW_BIN (iggy-gateway-kafka), BENCH_BIN (iggy-bench, for the
# native baseline; unset skips it), PY (a python with confluent-kafka), DATA (a scratch dir on
# disk, not tmpfs), MESSAGES (3000000), SIZE (100), PARTITIONS (4), PROCS (4), SHARDS (4),
# CONSUME_ARGS (extra kafka_consume.py flags, e.g. --fetch-wait-ms 50), KAFKA_IMAGE (an
# apache/kafka image: also runs its Java kafka-consumer-perf-test, one consumer reading every
# partition in one Fetch, on the same data).
# Layout (14 cores): iggy-server 0-3, gateway 4-7, load 8-13. Run it alone on the machine (on
# the Loams laptops: under .he0-shared/benchslot.sh with the quiet file set).
# One JSON line per run on stdout.
set -euo pipefail
LABEL=${1:?label}; RUNS=${2:-2}
HERE="$(cd "$(dirname "$0")" && pwd)"
: "${SERVER_BIN:?}" "${GW_BIN:?}" "${PY:?}" "${DATA:?}"
MESSAGES=${MESSAGES:-3000000}; SIZE=${SIZE:-100}; PARTITIONS=${PARTITIONS:-4}; PROCS=${PROCS:-4}; SHARDS=${SHARDS:-4}
SERVER_CPUS=${SERVER_CPUS:-0-3}; GW_CPUS=${GW_CPUS:-4-7}; LOAD_CPUS=${LOAD_CPUS:-8-13}
LOAD="taskset -c $LOAD_CPUS"
SPID=; GPID=
down() {
  for p in $GPID $SPID; do kill "$p" 2>/dev/null || true; done
  for p in $GPID $SPID; do while kill -0 "$p" 2>/dev/null; do sleep 0.2; done; done
  SPID=; GPID=; rm -rf "$DATA/iggy"
}
trap down EXIT
wait_port() { for _ in $(seq 120); do (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0; sleep 0.5; done; echo "port $1 never opened" >&2; return 1; }
up() {
  mkdir -p "$DATA/logs"; rm -rf "$DATA/iggy"
  for _ in $(seq 90); do [ -z "$(ss -Htan "( sport = :48090 or sport = :49093 )")" ] && break; sleep 1; done
  IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=cbench IGGY_PATH="$DATA/iggy" \
  IGGY_TCP_ADDRESS=127.0.0.1:48090 IGGY_HTTP_ADDRESS=127.0.0.1:43000 \
  IGGY_QUIC_ADDRESS=127.0.0.1:48080 IGGY_WEBSOCKET_ADDRESS=127.0.0.1:48092 \
  IGGY_SHARDING_CPU_ALLOCATION="$SHARDS" \
    taskset -c "$SERVER_CPUS" "$SERVER_BIN" >"$DATA/logs/server.log" 2>&1 </dev/null & SPID=$!
  wait_port 48090
  if [ "${1:-}" = gw ]; then
    IGGY_KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:49093 IGGY_KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:49093 \
    IGGY_KAFKA_BRIDGE_ENABLED=true IGGY_KAFKA_IGGY_ADDR=127.0.0.1:48090 \
    IGGY_KAFKA_IGGY_USERNAME=iggy IGGY_KAFKA_IGGY_PASSWORD=cbench IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED=true \
      taskset -c "$GW_CPUS" "$GW_BIN" >"$DATA/logs/gateway.log" 2>&1 </dev/null & GPID=$!
    wait_port 49093
  fi
}
for run in $(seq "$RUNS"); do
  up gw
  $LOAD "$PY" "$HERE/kafka_consume.py" preload --topic cb --partitions "$PARTITIONS" --messages "$MESSAGES" --size "$SIZE" >&2
  # Let the write path settle so the window measures reads only.
  sleep 3
  $LOAD "$PY" "$HERE/kafka_consume.py" consume --topic cb --partitions "$PARTITIONS" --procs "$PROCS" \
      --messages "$MESSAGES" --server-pid "$SPID" --gateway-pid "$GPID" \
      --server-cores "$SERVER_CPUS" --gateway-cores "$GW_CPUS" --load-cores "$LOAD_CPUS" ${CONSUME_ARGS:-} \
    | sed "s/^{/{\"label\":\"$LABEL\",\"path\":\"kafka\",\"run\":$run,/"
  if [ -n "${KAFKA_IMAGE:-}" ]; then
    # start, end, MB, MB/s, records, records/s, rebalance ms, fetch ms, fetch MB/s, fetch records/s
    podman run --rm --network host --cpuset-cpus "$LOAD_CPUS" "$KAFKA_IMAGE" \
        /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server 127.0.0.1:49093 --topic cb \
        --messages "$MESSAGES" --group "java-$run-$$" --timeout 30000 2>/dev/null | tail -1 \
      | awk -F', ' -v l="$LABEL" -v r="$run" '{printf "{\"label\":\"%s\",\"path\":\"kafka-java\",\"run\":%s,\"records\":%s,\"msgs_per_s\":%.0f,\"fetch_msgs_per_s\":%.0f}\n", l, r, $5, $6, $10}'
  fi
  down
  if [ -n "${BENCH_BIN:-}" ]; then
    up
    $LOAD "$BENCH_BIN" -u iggy -p cbench -m "$SIZE" -P 1000 -T "$((MESSAGES * SIZE / 1000000))MB" \
        balanced-producer -s 1 -a "$PARTITIONS" -p "$PROCS" tcp --server-address 127.0.0.1:48090 >"$DATA/logs/native-produce.log" 2>&1
    sleep 3
    python3 "$HERE/cpu_measure.py" "$LABEL-native" "$SPID" "$SPID" -- \
      $LOAD "$BENCH_BIN" -u iggy -p cbench -m "$SIZE" -P 1000 -T "$((MESSAGES * SIZE / 1000000))MB" \
        balanced-consumer-group -s 1 -c "$PROCS" -g 1 tcp --server-address 127.0.0.1:48090 \
      > "$DATA/logs/native-consume.log" 2>&1
    grep -o 'Consumers Results: Total throughput: [^,]*, [0-9.]* messages/s' "$DATA/logs/native-consume.log" | sed "s/^/{\"label\":\"$LABEL\",\"path\":\"native\",\"run\":$run} /"
    grep '^{' "$DATA/logs/native-consume.log" || true
    down
  fi
done
