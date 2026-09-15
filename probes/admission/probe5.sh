#!/bin/sh
# Probe 5: emit ergonomics and what a 429 looks like on the wire.
cd "$(dirname "$0")"
echo "--- idle server: curl one-liner (202 path)"
./probe4 -sink-sleep 0 > server-p5-idle.log 2>&1 & pid=$!; sleep 0.3
curl -si -d '{"host":"ghost","service":"agent.tokens.out","metric":1234,"ttl":90}' http://127.0.0.1:18085/events
echo "--- python minimal (no retry), lines: $(wc -l < emit_min.py)"
python3 emit_min.py && echo "emit_min ok"
echo "--- python retry-aware emit.py, lines: $(wc -l < emit.py)"
python3 -c 'from emit import emit; print("status", emit({"host":"ghost","service":"agent.tokens.out","metric":1,"ttl":90}))'
kill $pid; wait $pid 2>/dev/null
echo "--- stalled server (shard-work 5ms), prefilled with 4200 events"
./probe4 -sink-sleep 50ms -shard-work 5ms > server-p5-stall.log 2>&1 & pid=$!; sleep 0.3
python3 -c 'import json,urllib.request;urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:18085/events", json.dumps([{"host":"h","service":"fill","metric":0}]*4200).encode(), {"Content-Type":"application/json"}))' 2>&1 | tail -1
echo "--- what the emitter sees on 429 (curl -si, one event):"
curl -si -d '{"host":"ghost","service":"agent.tokens.out","metric":1234,"ttl":90}' http://127.0.0.1:18085/events
echo "--- what the emitter sees on 429 (batch of 100, partial admit):"
python3 -c 'import json,urllib.request,urllib.error
try: urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:18085/events", json.dumps([{"host":"h","service":"x","metric":0}]*100).encode(), {"Content-Type":"application/json"}))
except urllib.error.HTTPError as e: print(e.code, dict(e.headers), e.read().decode().strip())'
echo "--- retry-aware emit.py against the stalled server (Retry-After: 1 -> sleeps 1 s, retries once):"
python3 -c 'import time; from emit import emit; t=time.time(); print("status", emit({"host":"ghost","service":"agent.tokens.out","metric":1,"ttl":90}), "elapsed %.2fs" % (time.time()-t))'
kill $pid; wait $pid 2>/dev/null
