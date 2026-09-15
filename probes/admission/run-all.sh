#!/bin/sh
# Runs the probe 4 matrix; each case starts a fresh server on 127.0.0.1:18085.
cd "$(dirname "$0")"
run() { name=$1; shift; sflags=$1; shift; cargs=$*
  ./probe4 $sflags > server-$name.log 2>&1 & pid=$!
  sleep 0.3
  echo "=== $name  server: [$sflags]  client: [$cargs]"
  python3 burst.py $cargs | tee run-$name.txt | grep -E '"(202|429|accepted|rejected|max_inbox|inbox_max_depth|stalled_offers|sink_dropped|sink_processed|shard_processed|inbox_depth|sink_depth|inflight)"|wall_s|check'
  kill $pid; wait $pid 2>/dev/null
}
#run sink50-seq   "-sink-sleep 50ms"                  10000 100 1
#run sink0-seq    "-sink-sleep 0"                     10000 100 1
run work5ms-seq  "-sink-sleep 50ms -shard-work 5ms"  10000 100 1
run work1ms-t8   "-sink-sleep 50ms -shard-work 1ms"  10000 100 8
