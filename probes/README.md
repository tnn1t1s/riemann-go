# Probes

Standalone programs that test one design question each. Every probe is its own Go module and runs in under three minutes. Results and the environment they were measured in are on the probes page under `medios/riemann/` on leafwiki.

| Directory | Question | Run |
| --- | --- | --- |
| `throughput/` | Events per second through one shard loop with index, expiry heap and a four-combinator pipeline | `go run .` |
| `expr/` | Whether expr-lang expresses every predicate and transform in the fleet's rules, and whether read sets can be extracted from the AST | `go run . > run.log` |
| `timers/` | Whether `stable` and `throttle` as loop-owned heap timers pass the upstream tests without a lock | `go test -race -count=3 ./...` |
| `admission/` | What the HTTP ingest returns under burst with a slow sink, and how many lines an emitter needs | `./run-all.sh`, `./probe5.sh` |

Probes are evidence, not product code. Nothing under this directory is imported by the server.
