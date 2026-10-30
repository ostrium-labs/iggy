#!/usr/bin/env python3
"""Kafka-wire consume load for the Iggy Kafka gateway (librdkafka via confluent-kafka).

  kafka_consume.py preload --topic T --partitions P --messages N --size B
      creates T (P partitions) through the gateway and produces N messages of B bytes
  kafka_consume.py consume --topic T --partitions P --procs C [--messages N]
      C consumer processes, each assigned P/C partitions from the beginning (no group
      rebalance), read until N messages or 3 s idle. One JSON line: msgs/s, MB/s, seconds.

The measured window starts when every consumer is connected and assigned, and ends at the
last message, so it holds no setup or idle tail.

CPU (with --server-pid/--gateway-pid): sampled every 100 ms over the window, three ways, since
they disagree on loopback: the process's threads (/proc/PID/task/*/stat, utime+stime), and the
busy time of the cores the process is pinned to (/proc/stat, everything but idle and iowait),
which also holds the softirq time of the loopback traffic those cores handle and anything else
that ran there. Run on a quiet machine, the pinned figure is the upper bound and the thread
figure the lower one.
"""
import argparse, json, multiprocessing as mp, os, sys, time

def admin_topic(bs, topic, parts):
    from confluent_kafka.admin import AdminClient, NewTopic
    a = AdminClient({"bootstrap.servers": bs})
    for _, f in a.create_topics([NewTopic(topic, num_partitions=parts, replication_factor=1)]).items():
        try: f.result(60)
        except Exception as e:
            if "EXISTS" not in str(e).upper(): raise
    for _ in range(120):
        md = a.list_topics(topic, timeout=10).topics.get(topic)
        if md and not md.error and len(md.partitions) == parts: return
        time.sleep(0.5)
    raise SystemExit(f"topic {topic} not ready")

def preload(a):
    from confluent_kafka import Producer
    admin_topic(a.bootstrap, a.topic, a.partitions)
    p = Producer({"bootstrap.servers": a.bootstrap, "acks": "1", "linger.ms": 20, "batch.size": 1048576,
                  "queue.buffering.max.messages": 1000000, "queue.buffering.max.kbytes": 1048576,
                  "enable.idempotence": False})
    payload = os.urandom(a.size); errs = [0]
    def cb(err, _m):
        if err: errs[0] += 1
    t0 = time.time()
    for i in range(a.messages):
        while True:
            try: p.produce(a.topic, payload, partition=i % a.partitions, on_delivery=cb); break
            except BufferError: p.poll(0.01)
        if i % 10000 == 0: p.poll(0)
    left = p.flush(120)
    print(json.dumps({"preloaded": a.messages - errs[0] - left, "errors": errs[0], "seconds": round(time.time() - t0, 2)}))
    if errs[0] or left: sys.exit(1)

HZ = os.sysconf("SC_CLK_TCK")

def cores_ids(spec):
    out = []
    for part in spec.split(","):
        if "-" in part:
            lo, hi = part.split("-"); out += range(int(lo), int(hi) + 1)
        elif part:
            out.append(int(part))
    return out

def snapshot(a):
    """(time, {core: (total, idle+iowait)}, {pid: ticks of its threads})"""
    cores = {}
    for line in open("/proc/stat"):
        if line.startswith("cpu") and line[3].isdigit():
            f = line.split(); v = list(map(int, f[1:])); cores[int(f[0][3:])] = (sum(v), v[3] + v[4])
    procs = {}
    for name, pid in (("server", a.server_pid), ("gateway", a.gateway_pid)):
        if not pid: continue
        t = 0
        for task in os.listdir(f"/proc/{pid}/task"):
            try:
                f = open(f"/proc/{pid}/task/{task}/stat").read().rsplit(")", 1)[1].split(); t += int(f[11]) + int(f[12])
            except FileNotFoundError:
                pass
        procs[name] = t
    return time.time(), cores, procs

def cpu_between(a, s0, s1):
    d = s1[0] - s0[0]; out = {}
    for name, spec in (("server", a.server_cores), ("gateway", a.gateway_cores), ("load", a.load_cores)):
        ids = cores_ids(spec)
        busy = sum((s1[1][i][0] - s0[1][i][0]) - (s1[1][i][1] - s0[1][i][1]) for i in ids)
        out[f"{name}_cores_pinned"] = round(busy / HZ / d, 3)
        if name in s1[2]:
            out[f"{name}_cores_threads"] = round((s1[2][name] - s0[2][name]) / HZ / d, 3)
    return out

def worker(a, idx, q, ready, go):
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": f"cbench-{os.getpid()}", "enable.auto.commit": False,
                  "fetch.min.bytes": a.fetch_min_bytes, "fetch.wait.max.ms": a.fetch_wait_ms,
                  "fetch.max.bytes": a.fetch_max_bytes, "max.partition.fetch.bytes": a.partition_max_bytes,
                  "queued.max.messages.kbytes": 1048576, "queued.min.messages": 10000000,
                  # librdkafka postpones a partition's next Fetch by this much once its local queue
                  # passes the thresholds above (1000 ms by default): it measured the consumer.
                  "fetch.queue.backoff.ms": 0, "client.id": f"cbench-{idx}"})
    parts = [TopicPartition(a.topic, p, -2) for p in range(idx, a.partitions, a.procs)]
    c.assign(parts)
    c.list_topics(a.topic, timeout=10)
    ready.put(idx); go.wait()
    got = nbytes = 0; t0 = time.time(); last = t0; want = a.messages_per_proc[idx]
    while True:
        ms = c.consume(num_messages=100000, timeout=0.2)
        # Per-message Python work would make the consumer the bottleneck: count the batch, and
        # check one record of it for an error (librdkafka delivers errors as their own events).
        if ms:
            if ms[-1].error() is not None:
                ms = ms[:-1]
            got += len(ms); nbytes += sum(len(m.value()) for m in ms[:1]) * len(ms)
        now = time.time()
        if ms: last = now
        if got >= want or now - last > 3: break
    c.close(); q.put({"got": got, "bytes": nbytes, "t0": t0, "last": last})

def consume(a):
    per = [len(range(i, a.partitions, a.procs)) for i in range(a.procs)]
    a.messages_per_proc = [a.messages * n // a.partitions if a.messages else 1 << 62 for n in per]
    q, ready, go = mp.Queue(), mp.Queue(), mp.Event()
    ps = [mp.Process(target=worker, args=(a, i, q, ready, go)) for i in range(a.procs)]
    for p in ps: p.start()
    for _ in ps: ready.get(timeout=60)
    if a.start_file:  # lets a CPU sampler start at the same moment
        open(a.start_file, "w").close()
    samples = [snapshot(a)]
    go.set()
    r = []
    while len(r) < len(ps):
        try:
            r.append(q.get(timeout=0.1))
        except Exception:
            pass
        samples.append(snapshot(a))
    for p in ps: p.join()
    t0 = min(x["t0"] for x in r); t1 = max(x["last"] for x in r); got = sum(x["got"] for x in r)
    s0 = max((s for s in samples if s[0] <= t0), key=lambda s: s[0], default=samples[0])
    s1 = min((s for s in samples if s[0] >= t1), key=lambda s: s[0], default=samples[-1])
    cpu = cpu_between(a, s0, s1)
    per_core = {}
    if got and cpu.get("gateway_cores_threads"):
        per_core["msgs_per_gateway_core_s"] = round(got / (s1[0] - s0[0]) / cpu["gateway_cores_threads"])
    print(json.dumps({**cpu, **per_core, "consumed": got, "seconds": round(t1 - t0, 3), "msgs_per_s": round(got / (t1 - t0)),
                      "mb_per_s": round(sum(x["bytes"] for x in r) / (t1 - t0) / 1e6, 1), "procs": a.procs,
                      "partitions": a.partitions, "fetch_wait_ms": a.fetch_wait_ms, "fetch_min_bytes": a.fetch_min_bytes}))

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["preload", "consume"])
    ap.add_argument("--bootstrap", default="127.0.0.1:49093"); ap.add_argument("--topic", default="cbench")
    ap.add_argument("--partitions", type=int, default=4); ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--messages", type=int, default=0); ap.add_argument("--size", type=int, default=100)
    ap.add_argument("--fetch-min-bytes", type=int, default=1); ap.add_argument("--fetch-wait-ms", type=int, default=500)
    ap.add_argument("--fetch-max-bytes", type=int, default=52428800)
    ap.add_argument("--partition-max-bytes", type=int, default=1048576)
    ap.add_argument("--start-file", default="")
    ap.add_argument("--server-pid", type=int, default=0); ap.add_argument("--gateway-pid", type=int, default=0)
    ap.add_argument("--server-cores", default="0-3"); ap.add_argument("--gateway-cores", default="4-7")
    ap.add_argument("--load-cores", default="8-13")
    a = ap.parse_args()
    preload(a) if a.mode == "preload" else consume(a)

if __name__ == "__main__":
    main()
