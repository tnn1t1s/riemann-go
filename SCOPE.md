# riemann-go: scope

Status: scope, 2026-09-15. Repo `github.com/tnn1t1s/riemann-go`, module path the same, binaries `riemannd` and `riemann-mcp`. Nothing is built beyond the five probes in `medios/riemann/riemann-go-probes-2026-09-15`, which are the repo's first commit.

Claims below carry one of the categories from `writing-researchable-software.md`: invariant, observation, hypothesis, heuristic, parameter, default, research question. Every default is provisional unless an observation is cited beside it.

## What riemann-go is

riemann-go is a single-node stream-processing monitor for a home cluster whose emitters are mostly agents. It keeps the Riemann event model and index, replaces the Clojure config with rules submitted as JSON over HTTP, and treats backpressure as a design requirement: every queue is bounded, every drop is counted, and the emitter is told. It is built in Go rather than adopted from Vector, Benthos or Prometheus because the parts that matter here (an index keyed by identity with expiry-as-event, per-key state transitions, rules as resources with dry run and explain) are the parts none of them have (hypothesis; the milestones test it; evidence in `medios/riemann/riemann-go-product-assessment`, "Building instead of adopting").

There are no external users and no compatibility obligations to anyone but the owner.

What it is intentionally not:

- no configuration language; rules are data, submitted over HTTP
- no reload; there is no file to reload
- no UI in the core
- no clustering, no multi-node, no exactly-once
- no protobuf, no TCP listener, no bridge for old clients
- no sinks beyond ntfy and InfluxDB v2
- no persistence of stream state across restart
- no compatibility with upstream Riemann clients, wire, or query grammar

## Users

Roles, not people. From `medios/riemann/riemann-go-product-assessment`, condensed.

| Role | Who it is | Interaction it gets |
| --- | --- | --- |
| Emitter | health-agent tick, pyntfy-listen, the atlas SDK emitter, `riemann-emit` for synth and sims; any agent with `curl` | One HTTP POST, fire-and-forget. The reply says how many events were admitted and how the sinks are doing. A 429 is counted locally and the event is gone; nothing retries. |
| Rule author | One human today; agents once the API exists | Create, replace, disable, delete a rule by id. Dry-run it against the ring before it can page. List rules and their owners. |
| State reader | Nobody today; the medios-services skill queries InfluxDB over ssh instead | "Is any session stalled?" as one HTTP GET returning JSON with a server timestamp, or one MCP call once `riemann-mcp` exists. |
| Alert consumer | ntfy topic readers, human and agent | Every alert names the rule id, rule version, triggering event and prior state, so the reader can act without opening the config. |
| Owner at a dashboard | riemann-surface at `ghost:4567` | Same queries as today, in expr syntax, over SSE, with rule churn and shed counts visible as ordinary events. |

The atlas vocabulary (`riemann-atlas/VOCABULARY.md`) fixes the emitter contract: cumulative counters, no content, fire-and-forget. Rule 3 there, "emission never affects the agent", is what backpressure means here. Emitters neither block nor retry; the server sheds by declared policy, counts what it shed, and reports it.

## Invariants

Each is enforced by validation, a test, or an explicit failure; a scope page that lists them is not enforcement.

1. Event model: host, service, state, metric, ttl, tags, attributes, time, description, plus an optional source identity. Nothing else is indexed or matched on.
2. The index holds the last event per (host, service). An entry expires when `time + ttl` passes, and expiry produces an event with host and service kept and state `expired`, delivered to rules like any other event. Enforced by a test that indexes, advances the clock, and expects the expired event at a sink. Behaviour kept from `src/riemann/core.clj:274-308`.
3. Every queue has a capacity in events, a policy from {block-with-deadline, shed-newest}, and a dropped counter. No unbounded queue exists anywhere in the process; a test in `engine` fails on any channel or slice used as a queue without a declared capacity.
4. Every drop is counted and the count is emitted as an event into the system's own index, tagged `riemann`, so a rule can alert on it. Silent loss is the failure mode designed out.
5. Timers due at or before time T fire before an event stamped T is dispatched. Observed necessary by the timer probe: two ported `stable` cases fail without it.
6. In-flight gauges exist at the loop and at each sink worker, so `accepted = processed + dropped + queued + in_flight` holds exactly. The admission probe found the identity off by two without them.
7. Dry run is deterministic: a rule instance is a pure function of (rule, clock, ordered events, sink stubs). Enforced by a test that runs every dry run twice and diffs.
8. The core packages carry no deployment or monitoring dependency: stdlib plus `expr-lang/expr`, nothing else. Enforced by an import-graph test.
9. Emission never blocks the emitter past the admission deadline. After the deadline the reply is 429, not a stall.
10. A read is consistent with one shard's loop at one instant. Nothing is promised across shards or across hosts.

## Event schema

The JSON body of a single event, from `medios/riemann/riemann-go-architecture`, "Wire and ingest". A batch is a JSON array; a single object is a batch of one.

```json
{"host":"ghost","service":"agent.tokens.out","state":"ok","metric":1234,
 "time":1757900000.5,"ttl":90,"tags":["agent-obs"],
 "attributes":{"run_id":"r-42"},"description":""}
```

| Field | Type | Required | Default when absent | Owner of the default |
| --- | --- | --- | --- | --- |
| `host` | string | yes | none; a missing host is a 400 | ingest |
| `service` | string | yes | none; 400 | ingest |
| `state` | string | no | `""` | event |
| `metric` | float64 | no | absent, distinct from 0 | event |
| `time` | float seconds since epoch | no | receive time, as upstream (`src/riemann/common.clj:83-86`) | ingest |
| `ttl` | float seconds | no | `event.default_ttl`, 60 s, carried from the fleet's config | engine |
| `tags` | array of string | no | empty | event |
| `attributes` | map string to string | no | empty | event |
| `description` | string | no | `""` | event |
| `source` | string | no | stamped from the bearer token if one is present, else `""` | transport/http |

One `metric` replaces the three typed protobuf fields. `attributes` is a map, not a repeated pair. `time` is float seconds. A missing key on `attributes` reads as `""` in expressions, so authors test presence with `"key" in attributes`.

## Wire and backpressure

HTTP/1.1 with JSON bodies is the only wire. `POST /events` takes a batch. The six existing emitters get rewritten to it; there is no protobuf listener and no bridge.

```
emitter --POST /events--> ingest handler --offer(deadline)--> shard inbox (bounded)
              ^                  |                                  |
              |                  | inbox full past deadline          v
          429 + Retry-After <----+                            shard loop (one goroutine)
              |                                                     |
          202 {accepted, sinks}                                     v
                                                        sink queues (bounded, shed-newest)
                                                                    |
                                                                    v
                                                          ntfy / influx / subscribers
```

Ingest never processes. The handler decodes the batch, hashes each event's host to a shard, and offers each event to that shard's inbox with a deadline. While the offer waits the handler goroutine is parked, which is TCP backpressure to the client at no cost. If every event is admitted the reply is 202; if the deadline passes on any inbox the reply is 429 with `Retry-After` and a body that says how many were admitted before the stall. Admitted events are not rolled back.

The 202 body:

```json
{"accepted": 100,
 "sinks": {"ntfy": {"depth": 3, "capacity": 1000, "dropped": 0},
           "influx": {"depth": 812, "capacity": 10000, "dropped": 0}}}
```

`sinks` is advisory: a snapshot of each sink's bounded queue at reply time, so a curl reply distinguishes "accepted" from "accepted and being shed downstream". The server makes no promise that an accepted event reached any sink.

The 429 body is `{"accepted": n, "rejected": m}` with `Retry-After` in whole seconds. Emitters do not honour it; they count the 429 locally and drop the batch. This is the fire-and-forget contract from the atlas vocabulary, and it keeps the minimal Python emitter at two lines (observation, one-line-emit probe).

What 429 signals, and what it does not (observation, admission probe, single shard on Apple M3): a 429 reports loop saturation only. A slow sink alone never produces one, because the loop never blocks on a sink; with a sink sleeping 50 ms per event and no loop work, 10,000 events got 100 replies of 202 and the sink dropped 8,998 of them. Only loop cost fills the inbox. Sink saturation is visible through the dropped counter and the `sinks` field of the reply, nowhere else.

Parameters on the ingest path. All defaults are provisional; the flood in milestone 3 is the observation that would revise them.

| Parameter | Owner | Units | Default | Reason |
| --- | --- | --- | --- | --- |
| `ingest.max_inflight_requests` | transport/http | requests | 256 | bound on parked handlers; beyond it new connections wait in the kernel accept queue |
| `ingest.max_batch_events` | transport/http | events | 1000 | one atlas fan-out burst; larger bodies get 413 |
| `ingest.max_batch_bytes` | transport/http | bytes | 1 MiB | body cap independent of event count |
| `ingest.admission_deadline` | transport/http | milliseconds | 200 | shorter than any HTTP client's default timeout; longer than an estimated 50 ms rule burst so a busy loop does not surface as 429 |
| `shard.inbox_capacity` | shard | events | 4096 | tens of events per tool-call flurry; the deadline, not the inbox, is what handles a thousand saturating sessions. Probe 4 filled it to 4,096 only when the loop was given 1 to 5 ms of work per event |
| `sink.<name>.queue_capacity` | sink | events | ntfy 1000, influx 10,000 | carried from today's config (`probes/rules-json/rules.json` sinks block) |
| `subscribe.queue_capacity` | transport/http | events per subscriber | 1000 | same order as the ntfy sink; a lagging subscriber gets a `lagged` frame |

Per-source quotas are a research question, not a parameter in the first release; the trigger is one emitter observed starving others at the inbox.

## Runtime

One goroutine per shard. A shard owns an inbox, a timer heap, its slice of the index, its ring, and a live instance of every rule's state for the keys it holds. Combinators are what they are in Clojure: a function that takes an event and calls its children, closing over state. No lock protects that state because only the shard goroutine touches it, and timers are entries in the shard's own heap fired by the same goroutine between events (observation, timer probe: `go test -race -count=3` passes with no lock; firing timers from another goroutine trips the detector on the first run).

Partitioning. Events hash by host to `engine.shards` shards (parameter, engine, default `runtime.GOMAXPROCS(0)`, restart to change). Each rule declares a `partition`: `host` (default), `host,service`, or `global`. A host-partitioned rule runs on every host shard and each shard sees only its hosts' events. A global rule runs on one extra shard that receives a copy of every event; the fleet's `fleet.burn` coalesce goes there, as would `top` or `clock-skew`. The compiler rejects a host-partitioned rule whose tree contains a global combinator, so the mistake is a validation error.

Throughput (observation, throughput probe, Apple M3, 8 cores, Go 1.26.3, one goroutine, 1,000,000 events from 1,000 hosts through `where -> by host -> changed-state -> counting sink` with expiry running): 3.4 million events per second, 0.035 allocations per event, 6 MB heap growth. Loop plus index costs about 250 ns per event; the four combinators add about 45 ns. The global shard therefore has three orders of magnitude of margin over the fleet's estimated 1,000 events/s bursts. Not observed: GC and index growth over minutes, and the race detector was off. Both are milestone 1 acceptance.

Hot-host skew is a known limitation of hashing by host (heuristic). The metric that reveals it is per-shard inbox depth; the retirement condition is one shard observed above 80 percent depth while others idle.

Timers. The heap is keyed by (time, seq) so ties are stable. Due timers fire before same-timestamp events (invariant 5). `stable` uses one generation-numbered heap entry per value change rather than upstream's N racing tasks (`src/riemann/streams.clj:2020-2027`); the two are equivalent for monotone event times and differ only when event times go backwards, which is recorded in the source comment (observation, timer probe, six `stable-test` and one `throttle-test` case ported and passing).

`by` fork lifecycle. `by` creates a child on first sight of a key, as today, and frees it once the key's expired event has passed through and no timer references the fork. Upstream never frees (`src/riemann/streams.clj:1577`). This is the one behavioural addition in the runtime, classified as a heuristic with two metrics, forks live and forks freed, so its effect can be observed.

Cross-goroutine surfaces, in full: inbox channels, sink queues, subscriber queues, an atomic pointer to the immutable rule set, and a read path that sends a closure into a shard's inbox and waits on a reply channel.

## Rules

A rule is a JSON document: metadata, a match expression, and a combinator tree whose leaves are expressions compiled by `expr-lang/expr` (v1.17.8 in the probe). There is no separate query grammar; `GET /index?q=` and `GET /subscribe?q=` take the same expression syntax as a rule's `match`.

The listener-connected rule (kept behaviour defined in `ansible/roles/medios_riemann/templates/riemann.config.j2` on medios-teams, mapped in `probes/rules-json/rules.json`):

```json
{"id": "listener-connected",
 "owner": "ops",
 "partition": "host",
 "match": "service == \"ntfy.listen.connected\"",
 "stream":
   {"op": "changed-state", "initial": "ok", "children": [
     {"op": "by", "fields": ["host", "service"], "children": [
       {"op": "throttle", "limit": 5, "window_seconds": 300,
        "children": [{"sink": "ntfy"}]}]}]}}
```

The atlas cost rule, with `set` replacing the four invented `smap` kinds:

```json
{"id": "atlas-cost",
 "owner": "atlas",
 "partition": "host",
 "match": "tagged(\"agent-obs\") && service == \"agent.cost\"",
 "stream":
   {"op": "splitp", "test": "{} < metric", "branches": [
     {"threshold": 20.0, "child": {"op": "set", "fields": {"state": "\"critical\""},
        "children": [{"sink": "index"}, {"ref": "transition"}]}},
     {"threshold": 5.0,  "child": {"op": "set", "fields": {"state": "\"warning\""},
        "children": [{"sink": "index"}, {"ref": "transition"}]}}],
    "otherwise": {"op": "set", "fields": {"state": "\"ok\""},
        "children": [{"sink": "index"}, {"ref": "transition"}]}},
 "bindings":
   {"transition": {"op": "changed-state", "initial": "ok", "children": [{"sink": "ntfy"}]}}}
```

The thresholds 20.0 and 5.0 USD are parameters of the rule, owned by the rule author, carried from `atlas_thresholds` in the probe file. The burn transform from the same rule family is `{"op": "set", "fields": {"service": "\"agent.burn\"", "metric": "metric * 60.0"}}`, and the fleet sum after `coalesce` is `{"op": "set", "fields": {"host": "\"fleet\"", "service": "\"fleet.burn\"", "metric": "sum(map(events, .metric))"}}` on the global shard.

Expression syntax reference (observation, expr parity probe: 31 predicates and four transform kinds match the Clojure semantics with no custom Go function):

| Retired grammar | expr | Note |
| --- | --- | --- |
| `service = "x"` | `service == "x"` | |
| `tagged "x"` | `tagged("x")` | engine-supplied function |
| `not host = "x"` | `not (host == "x")` | `not` binds tighter than comparison |
| `service =~ "agent.%"` | `service matches "^agent\\."` | RE2, unanchored |
| `attributes.k = nil` | `not ("k" in attributes)` | missing key reads as `""` |
| `metric > 5 and state = "ok"` | `metric > 5 && state == "ok"` | `and`/`or` also accepted |
| unknown name | compile error | closed world; `now`, `expired`, `events` are the engine-supplied names |

Compile cost is 5 to 12 µs per expression and evaluation 80 to 350 ns (observation, same probe, Apple M3).

Lifecycle. A rule has `id`, `owner`, `version`, `enabled`, `expires_at`. `PUT /rules/{id}` is idempotent by content hash of the canonical JSON: an unchanged hash is a no-op, a new hash bumps `version`, creates a fresh instance, and the old state does not carry across (default, stated in the API as "a replaced rule forgets"). `expires_at` is the cheap control on rule sprawl once agents author rules. Every rule change emits `riemann.rule.changed` into the index with the id, version and owner as attributes.

Dry run. `POST /rules/{id}/dryrun` with a body and a window compiles the rule, replays the ring filtered by the rule's partition under a clock that follows event times, and returns what each sink stub received. Nothing touches live sinks.

Explain. Every node's id is its path in the tree. A firing carries `rule`, `version`, the node path traversed, and at each stateful node the state it read: prior value for `changed-state`, count for `throttle`, buffer age for `stable`, fork key for `by`. On by default for alert sinks, off for `index` and `influx` (default; provenance in InfluxDB is a research question, trigger below).

Static read and write sets. The compiler walks each expression's AST (about 25 lines, observation from the probe) and records the fields a rule reads and the `service` values it can write. A `PUT` whose write set overlaps another enabled rule's is refused unless `force` is set (heuristic).

Identity. `owner` is a string. How it resolves to a person or team is pluggable and lives outside the core; the core never reads `teams/*.yaml`.

## Index and reads

Each shard holds `map[key]*entry` keyed by (host, service) plus a min-heap keyed by expiry time. Insert updates the entry and pushes a heap item carrying the entry's generation; the loop pops due items and discards stale generations. Expiry costs O(log n) per entry instead of a full scan every reaper tick, so an entry expires within the loop's timer granularity rather than up to 60 s late as on the fleet today (`ansible` sets the reaper interval to 60 s; `src/riemann/core.clj:290` defaults to 10). That is a behaviour change and gets one observation in milestone 1 before any rule depends on it.

Cardinality cap: `index.max_entries_per_shard`, parameter, owner index, units entries, default 1,000,000. Reason: at an estimated 300 bytes per entry that is 300 MB per shard, right for a hot shard and wrong for a small host; the milestone 1 mirror is the observation that would revise it. On overflow the insert is refused and counted as `riemann.index.rejected`, never silently evicted.

Ring: `shard.ring_events` (default 100,000 events) and `shard.ring_bytes` (default 64 MiB), whichever fills first, both owned by the shard. It serves dry run and `GET /events`, nothing else, and is lost on restart.

| Endpoint | Ordering and consistency |
| --- | --- |
| `GET /index?q=<expr>` | Scatter-gather. Each shard's slice is a snapshot between two events of its loop; slices are not simultaneous. Reply carries `as_of` as (min, max) across shards. |
| `GET /index/{host}/{service}` | One shard, O(1), snapshot at one instant, with last transition time. |
| `GET /events?q=&since=&limit=` | From the ring. Processing order within a shard; interleaving across shards by time is best-effort. |
| `GET /subscribe?q=&snapshot=true` (SSE) | Snapshot and subscription taken inside the shard loop in one step, so no event falls between them. At-most-once delivery; a slow subscriber gets `event: lagged` with a count. |
| `GET /rules` | All rules with id, owner, version, enabled, expires_at. |
| `GET /rules/{id}` | Body, version, owner, read set, write set, per-node counters. |
| `GET /rules/{id}/firings?limit=` | Last N firings with their trace. |
| `PUT /rules/{id}`, `DELETE /rules/{id}` | As in Lifecycle. |
| `POST /rules/{id}/dryrun` | As in Dry run. |
| `POST /events` | Ingest. |

The atomic snapshot-plus-subscribe fixes the gap in upstream's `ws-index-handler` (`src/riemann/transport/websockets.clj:60-76`), where a search, a send, and a subscribe happen in sequence and events indexed in between reach neither.

## Sinks

```go
type Sink interface {
    Name() string
    Send(ctx context.Context, e Event) error
}
```

Each sink sits behind a bounded queue owned by `sink/`, policy shed-newest with a dropped counter; the shard loop never blocks on a sink. Two sinks exist: ntfy and InfluxDB v2. The ntfy sink coalesces identical (rule, key, state) alerts while its queue is above half full (heuristic, bounded by the queue, observed through the coalesced count). Each sink's depth, capacity and dropped count appear in the 202 reply and in the self-observation events.

Alert provenance fields on every ntfy post: `rule`, `version`, `owner`, the triggering event's host, service, state, metric and time, the prior state a `changed-state` left, and the node path. The existing ntfy alert format ("host service is state (metric)") carries none of these.

## Self-observation

Every bounded queue exposes depth, capacity and dropped; every rule node exposes events in, events out, last fired; every shard exposes loop lag (admission to processing), timer heap size, index entries, forks live and freed. One `Metrics() Snapshot` method on the engine supplies all of it and imports nothing.

Two destinations. `adapter/selfmetrics` in `cmd/riemannd` samples the snapshot every `self.interval` (parameter, default 10 s, ttl twice the interval, the rule at `src/riemann/core.clj:44-46`) and posts it through ordinary ingest with tag `riemann`, so `riemann.sink.ntfy.dropped > 0` is a rule like any other and riemann-surface shows it with no new tooling. `adapter/prom` exposes a Prometheus text endpoint, off by default.

## Package layout and dependency policy

```
event/      Event type, JSON codec, canonical key            (stdlib)
expr/       compile predicates and transforms                (expr-lang/expr)
stream/     combinators as functions over a Clock interface  (stdlib)
rule/       JSON schema, compiler to stream trees, versions,
            trace, read/write sets                            (event, expr, stream)
index/      per-shard map + expiry heap                       (event)
shard/      loop, inbox, timer heap, ring, snapshot           (index, rule, stream)
engine/     shard set, admission, rule registry, dry run,
            Metrics() Snapshot                                (shard, rule)
sink/       Sink interface, bounded queue with policy         (event)
---- adapters, each its own package with its own deps ----
sink/ntfy, sink/influx
transport/http   ingest, index, events, subscribe (SSE), rules
adapter/prom
adapter/selfmetrics
cmd/riemannd, cmd/riemann-mcp
```

Core packages import stdlib plus `expr-lang/expr` and nothing else. Adapters import core, never each other. `cmd/*` wires adapters to the engine and is the only place a port, a token or a URL appears. A test in `engine` walks the import graph and fails on a forbidden edge. Rule persistence goes through a `rule.Store` interface whose file implementation lives in `cmd/riemannd`. `cmd/riemann-mcp` imports the HTTP client and `event` only.

## Testing strategy

Port the `streams_test.clj` cases for kept combinators as table-driven Go tests under `testing/synctest`: timed events in, what each sink stub received out. First, because their semantics are least obvious: `changed-state`, `stable`, `coalesce`, `throttle`, `rollup`, `batch`, both `ddt` forms, `rate`, `ewma`, the three time-window tests, `by-single`, `by-multiple`, `splitp`, `top`, `project`. Not ported: `exception-stream`, `execute-on`, `pipe`, `by-builder`, `sdo`; those are Clojure surface.

Written fresh, in order of value:

- a race-detector test that timers fire between events and never concurrently with them (exists in the timer probe; becomes a first commit)
- backpressure properties under `testing.F` or `rapid`: for any burst schedule and sink speed, an event that received 202 reaches the shard, inbox depth never exceeds capacity, every drop increments exactly one counter, in-flight gauges close the accounting identity, and 429 appears only after the deadline
- dry-run determinism, run twice and diff
- rule round-trip through canonical JSON with a stable hash
- expression parity against the query strings in `test/riemann/query_test.clj` (exists in the expr probe)
- index expiry under a controlled clock

## Milestones

Each is an increment someone can use. Sizes are estimates. Every probe is under three minutes of wall-clock. Milestone 1 is the gate: if the Go index does not match Clojure on the health-loop set, stop and read the wire code before anything else.

| # | Increment | Usable by | Acceptance | Smallest probe |
| --- | --- | --- | --- | --- |
| 1 | Ingest, index, expr query, SSE subscribe with atomic snapshot, fed from a mirror of the live stream. The five probe sources are the first commits. | Owner, via riemann-surface pointed at the new port | For `tagged("health-loop")`, the Go index returns the same host and service set as Clojure over ten minutes; GC and index growth flat over that window under `-race` | One SSE query diffed against the Clojure websocket, 30 s |
| 2 | Rules as resources with lifecycle; the ten combinators the fleet uses; ntfy and InfluxDB sinks; provenance on alerts; seed with today's five rules | Owner (same alerts, now with rule ids); first agent author | `riemann-synth` through both servers produces the same ntfy posts, with rule id and prior state on the Go side | One synth ramp across the 75 and 90 thresholds, under two minutes |
| 3 | Backpressure flood: bounded stages end to end, 202 with sink state, 429 on loop saturation, self-observation events | Emitters (they see loss); owner (queue depth on the dashboard) | A 60 s flood at 1,000 events/s from `riemann-emit` yields counted drops, accounting identity exact, no unbounded memory, dashboard stays live | The flood itself, 60 s, memory reading before and after |
| 4 | Ring reads, dry run, replay, explain | Rule authors | Dry-running the atlas cost rule over the last 15 min returns the alerts that fired; explain on one shows the `splitp` branch taken | Dry run against 15 min of ring, seconds |
| 5 | `riemann-mcp` and structured reads (timing open, see below) | Agents | A Claude Code session answers "is any session stalled?" through MCP without knowing expr | One MCP call |
| 6 | Combinators on demand: windows, rate, percentiles, apdex, predict-linear, each with its ported test | Rule authors who need them | Ported test passes under `synctest` | `go test` on the combinator, seconds |
| 7 | The six emitters rewritten to HTTP JSON; riemann-surface cutover or retirement; Clojure server stopped | Everyone | Zero connections on the old TCP port for a week | Port counter at zero |

## Research questions

Each is recorded, not pursued, until its trigger fires.

- On-disk segment log behind the ring. Trigger: a restart observed to cause a missed alert that mattered, or a dry run over more than the ring's window requested twice.
- Stream-state persistence across restart. Trigger: a missed alert attributable to a restart. Until then the README states: on restart the index is empty, rules start fresh, and expiry events for entries live before the restart are never produced.
- Seeding `changed-state` from the index when a rule is replaced, so an edit does not re-fire every current state. Trigger: a rule edit observed to page for states that had not changed.
- Per-source ingest quotas. Trigger: one emitter observed starving others at the inbox.
- Peak index cardinality at fan-out scale, and whether 1,000,000 entries per shard is the right cap. Trigger: `riemann.index.rejected > 0` or the milestone 1 mirror showing growth.
- Provenance fields in InfluxDB. Trigger: a historical question about which rule produced a stored point.
- Expiry latency change from reaper-scan to heap. Trigger: any rule whose behaviour depends on expiry timing within 60 s; observe before writing it.
- Rule count and sprawl once agents author. Trigger: more than 50 enabled rules, or an `expires_at` observed to have been extended three times.
- Whether one shard suffices for the fleet, which would simplify the runtime to a single loop. Trigger: milestone 3 flood showing per-shard depth flat with `engine.shards` = 1.

## Open decisions

The owner's, listed without a recommendation.

- Whether `riemann-mcp` ships in milestone 1 alongside the read API, or at milestone 5 after backpressure.
- Whether riemann-surface moves to the SSE read API or is retired in favour of an agent-generated view.
- Which identity source resolves rule `owner` strings; pluggable, outside the core, not `teams/*.yaml`.
