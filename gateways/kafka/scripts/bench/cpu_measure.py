#!/usr/bin/env python3
"""cpu_measure.py LABEL SERVER_PID GATEWAY_PID -- cmd...

Runs cmd and prints, as one JSON line, the CPU the server and the gateway used while it ran,
three ways: busy time of the cores each is pinned to (/proc/stat less idle and iowait; also
holds softirq time and anything else that ran there), /proc/PID/stat utime+stime, and the sum
over /proc/PID/task/*/stat. Cores from SERVER_CORES, GW_CORES, LOAD_CORES (comma lists).

The thread figures are the process's own CPU. The pinned figures are an upper bound and are
only meaningful on a quiet machine. BQ1-iggy's earlier `server_cores` read the process's
utime+stime once before and once after a whole iggy-bench run, warm-up included, so a run held
up elsewhere showed a nearly idle server (0.04 cores at 344k msg/s).
"""
import json, os, subprocess, sys, time
HZ = os.sysconf("SC_CLK_TCK")
def cores():
    out = {}
    for l in open("/proc/stat"):
        if l.startswith("cpu") and l[3].isdigit():
            f = l.split(); v = list(map(int, f[1:]))
            out[int(f[0][3:])] = (sum(v), v[3] + v[4], v[0] + v[1], v[2], v[5] + v[6])
    return out
def proc(pid):
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])
def tasks(pid):
    s = 0
    for t in os.listdir(f"/proc/{pid}/task"):
        try:
            f = open(f"/proc/{pid}/task/{t}/stat").read().rsplit(")", 1)[1].split(); s += int(f[11]) + int(f[12])
        except FileNotFoundError: pass
    return s
def busy(c0, c1, ids):
    return sum((c1[i][0]-c0[i][0]) - (c1[i][1]-c0[i][1]) for i in ids) / HZ
label, spid, gpid = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]); cmd = sys.argv[5:]
srv = [int(x) for x in os.environ.get("SERVER_CORES", "0,1,2,3").split(",")]
gw = [int(x) for x in os.environ.get("GW_CORES", "4,5,6,7").split(",")]
ld = [int(x) for x in os.environ.get("LOAD_CORES", "8,9,10,11,12,13").split(",")]
c0, p0, t0, g0, gt0, w0 = cores(), proc(spid), tasks(spid), proc(gpid), tasks(gpid), time.time()
rc = subprocess.call(cmd)
c1, p1, t1, g1, gt1, w1 = cores(), proc(spid), tasks(spid), proc(gpid), tasks(gpid), time.time()
d = w1 - w0
print(json.dumps({"label": f"cpu-{label}", "wall_s": round(d, 2), "rc": rc,
  "server_cores_pinned": round(busy(c0, c1, srv)/d, 3), "server_cores_procstat": round((p1-p0)/HZ/d, 3), "server_cores_tasks": round((t1-t0)/HZ/d, 3),
  "gateway_cores_pinned": round(busy(c0, c1, gw)/d, 3), "gateway_cores_procstat": round((g1-g0)/HZ/d, 3), "gateway_cores_tasks": round((gt1-gt0)/HZ/d, 3),
  "load_cores_pinned": round(busy(c0, c1, ld)/d, 3),
  "server_split_user_sys_irq": [round(sum(c1[i][k]-c0[i][k] for i in srv)/HZ/d, 3) for k in (2, 3, 4)],
  "gateway_split_user_sys_irq": [round(sum(c1[i][k]-c0[i][k] for i in gw)/HZ/d, 3) for k in (2, 3, 4)]}), flush=True)
