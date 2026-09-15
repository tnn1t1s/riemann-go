#!/usr/bin/env python3
"""Probe 4 load: post N events in batches of B as fast as possible, stdlib only.
Usage: burst.py [total] [batch] [threads]"""
import json, sys, time, threading, urllib.request, urllib.error

URL = "http://127.0.0.1:18085/events"
total = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
batch = int(sys.argv[2]) if len(sys.argv) > 2 else 100
threads = int(sys.argv[3]) if len(sys.argv) > 3 else 1

lock = threading.Lock()
stats = {"202": 0, "429": 0, "other": 0, "accepted": 0, "rejected": 0, "max_inbox": 0}
batches = [list(range(i, i + batch)) for i in range(0, total, batch)]
stop = False

def poll_metrics():
    while not stop:
        try:
            m = json.load(urllib.request.urlopen("http://127.0.0.1:18085/metrics"))
            with lock:
                stats["max_inbox"] = max(stats["max_inbox"], m["inbox_depth"])
        except Exception:
            pass
        time.sleep(0.005)

def post(events):
    req = urllib.request.Request(URL, data=json.dumps(events).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        r = urllib.request.urlopen(req)
        return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)

def worker(idx):
    for bi in range(idx, len(batches), threads):
        evs = [{"host": "h%d" % (i % 10), "service": "probe.burst", "state": "ok",
                "metric": float(i), "ttl": 60, "tags": ["probe4"],
                "attributes": {"batch": str(bi)}} for i in batches[bi]]
        code, body = post(evs)
        with lock:
            stats[str(code) if code in (202, 429) else "other"] += 1
            stats["accepted"] += body.get("accepted", 0)
            stats["rejected"] += body.get("rejected", 0)

t0 = time.time()
pm = threading.Thread(target=poll_metrics, daemon=True); pm.start()
ws = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
for w in ws: w.start()
for w in ws: w.join()
stop = True
wall = time.time() - t0
m = json.load(urllib.request.urlopen("http://127.0.0.1:18085/metrics"))
print(json.dumps({"client": stats, "wall_s": round(wall, 3), "server": m}, indent=1))
ok1 = stats["accepted"] + stats["rejected"] == total
still_queued = m["inbox_depth"] + m["shard_inflight"] + m["sink_depth"] + m["sink_inflight"]
ok2 = m["accepted_events"] == m["sink_processed"] + m["sink_dropped"] + still_queued
print("check accepted+rejected==total:", ok1)
print("check accepted==sink_processed+sink_dropped+still_queued(inbox+shard_inflight+sink+sink_inflight):", ok2,
      "(%d == %d + %d + %d)" % (m["accepted_events"], m["sink_processed"], m["sink_dropped"], still_queued))
