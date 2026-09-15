# riemann-go

riemann-go is a single-node stream-processing monitor for a home cluster whose emitters are mostly agents. It keeps the Riemann event model and index: the last event per (host, service), expiry when `time + ttl` passes, and an expired event delivered to rules like any other. Rules are JSON submitted over HTTP rather than a Clojure config, and backpressure is a design requirement: every queue is bounded, every drop is counted, and the emitter is told in the reply.

The wire is HTTP/1.1 with JSON bodies. `POST /events` takes a batch and answers 202 with the sink queue state or 429 after the admission deadline. Reads are `GET /index?q=`, `GET /index/{host}/{service}`, `GET /events?q=&since=&limit=` from a bounded ring, and `GET /subscribe?q=&snapshot=true` as server-sent events where the snapshot and the subscription are taken in one step inside the shard loop. Queries use expr-lang syntax over the event fields plus `tagged(name)`, `now` and `expired`. The scope, invariants, parameters and milestones are in [SCOPE.md](SCOPE.md).

## Run

```
go build ./cmd/riemannd
./riemannd -listen 127.0.0.1:5557 -shards 4
```

Post an event and read it back:

```
curl -s -X POST localhost:5557/events -d '{"host":"ghost","service":"cpu","state":"ok","metric":0.5,"tags":["health-loop"]}'
curl -s 'localhost:5557/index?q=tagged("health-loop")'
curl -s localhost:5557/index/ghost/cpu
curl -sN 'localhost:5557/subscribe?snapshot=true&q=service%20==%20"cpu"'
curl -s localhost:5557/metrics
```

On restart the index is empty, rules start fresh, and expiry events for entries live before the restart are never produced.

## Test

```
go test -race ./...
```

`probes/` holds the standalone design probes; nothing under it is imported by the server.
