#!/bin/sh
# Probe 5b: shard loop fully stuck (shard-work 30s), so a one-event offer sees 429.
cd "$(dirname "$0")"
./probe4 -sink-sleep 0 -shard-work 30s > server-p5-stuck.log 2>&1 & pid=$!; sleep 0.3
python3 -c 'import json,urllib.request,urllib.error
try: urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:18085/events", json.dumps([{"host":"h","service":"fill","metric":0}]*4200).encode(), {"Content-Type":"application/json"}))
except urllib.error.HTTPError as e: print("prefill:", e.code, e.read().decode().strip())'
echo "--- curl -si, one event, loop stuck:"
curl -si -d '{"host":"ghost","service":"agent.tokens.out","metric":1234,"ttl":90}' http://127.0.0.1:18085/events
echo "--- curl one-liner that honours Retry-After (exit status shown):"
curl -sf --retry 1 --retry-delay 1 -d '{"host":"ghost","service":"x","metric":1}' http://127.0.0.1:18085/events; echo "curl exit=$?"
echo "--- emit.py retry-aware, loop stuck (expect sleep 1 s, second 429 raises):"
python3 -c 'import time; from emit import emit
t=time.time()
try: print("status", emit({"host":"ghost","service":"agent.tokens.out","metric":1,"ttl":90}))
except Exception as e: print("raised", e, "elapsed %.2fs" % (time.time()-t))'
curl -s http://127.0.0.1:18085/metrics
kill $pid; wait $pid 2>/dev/null
