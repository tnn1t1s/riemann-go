# riemann-go — specification

riemann-go is a single-node stream-processing monitor. It accepts events over HTTP, keeps an index of the last event per identity, evaluates rules submitted as JSON, and drives two sinks: an ntfy topic and an InfluxDB v2 bucket. Correctness is judged by what a harness-owned sink receiver observed arrive, not by anything riemann-go says about itself.

This document defines required behavior. Sections marked **(normative)** pin the wire; a generation that renames a flag, moves a field, or changes a status code does not conform regardless of which properties it satisfies. Everything else is the generator's choice: the internal state model, the package layout, the concurrency model, and how rules are stored.

Read `INVARIANTS.md` for the principles that bind every generation, `SCALE.md` for the backpressure floors, and `HARNESS.md` for how scenarios bind to the trace.

## Scope

In scope for v0:

- Event ingest over HTTP, batched, with bounded admission and a typed reply.
- An index of the last event per `(host, service)`, with expiry delivered as an event.
- Rules as JSON documents with a lifecycle: create, replace, disable, delete, dry run.
- Nine stream combinators: `where`, `by`, `changed-state`, `throttle`, `splitp`, `set`, `coalesce`, `ddt`, `stable`.
- Expressions in expr-lang syntax for predicates and field transforms.
- Three sink leaves: `ntfy`, `influx`, `index`.
- Alert provenance sufficient to identify the rule, its version, its owner, and the event that fired it.
- Read surface: index query, index lookup by identity, recent events, SSE subscription, health, metrics.

Out of scope for v0:

- Protobuf, a TCP listener, and any bridge for upstream Riemann clients.
- The upstream query grammar. Expressions are expr-lang; there is no second language.
- Sinks other than ntfy and InfluxDB v2.
- Persistence of stream state across restart. On restart the index is empty, rules start fresh, and entries live before the restart never produce their expiry event.
- Clustering, multi-node, exactly-once delivery.
- Authentication beyond an optional source identity string.
- A configuration language or a reload signal. Rules are data over HTTP.
- A UI in the core.
- Combinators beyond the nine above. Windows, `rate`, `percentiles`, `top`, `apdex` and `predict-linear` land later, each with its ported test.

## Vocabulary

- An **event** is one observation about one identity at one time. Its identity is the pair `(host, service)`.
- The **index** holds the last event per identity. An entry **expires** when `time + ttl` passes.
- A **rule** is a JSON document naming a match expression and a stream tree.
- A **combinator** is a node in that tree. A **sink leaf** is a terminal node naming a destination.
- A **firing** is one event reaching one sink leaf through one rule.
- The **sink receiver** is the harness-owned HTTP server standing in for ntfy and InfluxDB. It is the oracle.
- `shard` names a partition of the event stream. How many exist, and whether more than one exists, is the implementation's choice bounded by `--shards`.

## Event model (normative)

The body of `POST /events` is a JSON array of event objects, or a single event object, which is a batch of one.

```json
{"host":"ghost","service":"agent.tokens.out","state":"ok","metric":1234,
 "time":1757900000.5,"ttl":90,"tags":["agent-obs"],
 "attributes":{"run_id":"r-42"},"description":""}
```

| Field | Type | Required | Default when absent |
| --- | --- | --- | --- |
| `host` | string | yes | none; absence is a 400 |
| `service` | string | yes | none; absence is a 400 |
| `state` | string | no | `""` |
| `metric` | number | no | absent, which is distinct from `0` |
| `time` | float seconds since epoch | no | receive time |
| `ttl` | float seconds | no | `60` |
| `tags` | array of string | no | empty |
| `attributes` | object, string to string | no | empty |
| `description` | string | no | `""` |
| `source` | string | no | `""` |

One `metric` replaces upstream's three typed protobuf fields. `attributes` is a map, not a repeated pair list. A key absent from `attributes` reads as `""` in an expression, so an author tests presence with `"key" in attributes` rather than a nil comparison.

`ttl`'s default of `60` seconds is a default, not an invariant: it is carried from the fleet's current Clojure configuration, and the value that would revise it is a rule whose behavior depends on expiry timing.

## Behavioral properties (v0)

Each property is a MUST that a scenario asserts against the trace. A property without a scenario is not enforced; see `INVARIANTS.md` I10.

1. **Routed events reach their sink.** An event that matches an enabled rule and traverses the tree to `{"sink":"ntfy"}` produces exactly one `ntfy_post` at the sink receiver. The same holds for `{"sink":"influx"}` and `influx_write`.

2. **Ingest answers truthfully.** `POST /events` returns `202` with `accepted` equal to the number of events admitted, or `429` with `accepted` plus `rejected` summing to the batch size. Across a scenario, the events observed at the sinks are drawn only from those the trace's `ingest_response` events counted as accepted.

3. **Validation fails fast.** A batch containing an event with no `host`, or no `service`, is rejected with `400`. A batch over `ingest.max_batch_events` or `ingest.max_batch_bytes` is rejected with `413`. Neither status admits any event from that batch.

4. **Expiry is an event, not a deletion.** When `time + ttl` passes for an indexed entry, riemann-go produces an event carrying that entry's `host` and `service` with `state` equal to `expired`, and delivers it to rules exactly as an ingested event. A rule matching `state == "expired"` therefore fires at a sink with no further ingest.

5. **The index holds the last event per identity.** `GET /index/{host}/{service}` returns the most recently indexed event for that pair, or `404` when no entry exists or the entry has expired.

6. **Index query selects by expression.** `GET /index?q=<expr>` returns exactly the entries for which the compiled expression evaluates true, and carries `as_of` bounding the instants the slices were taken.

7. **`by` partitions state.** Two identities passing through one `by` node hold independent state. A `changed-state` beneath `by ["host","service"]` fires for each identity's first transition, not once for the first identity only.

8. **`changed-state` suppresses repeats.** Consecutive events carrying the same state, for one fork key, produce one firing. The state left by the previous event appears in the alert's provenance as `prior_state`.

9. **`throttle` bounds firings.** At most `limit` events pass a `throttle` node per fork key in any `window_seconds` interval. Events beyond the limit are discarded, not deferred.

10. **`splitp` selects one branch.** An event takes the first branch whose `test` holds with that branch's `threshold` substituted, and takes `otherwise` when none holds. Exactly one branch receives the event, and the node path recorded in the provenance names it.

11. **`set` rewrites before the sink sees it.** A `set` node evaluates each field expression against the incoming event and passes a new event downstream. The value observed at the sink is the rewritten one.

12. **Timers are ordered against events.** A timer due at or before time T fires before an event stamped T is dispatched. This is what makes `stable` and `throttle` reproducible under a scenario's timeline rather than dependent on scheduling.

13. **Every alert carries its provenance.** Every `ntfy_post` carries the rule id, the rule version, the owner, the triggering event's host, service, state and metric, the prior state a traversed `changed-state` left, and the node path. The placement of these fields is pinned in `## Alert shape (normative)`.

14. **Rules are idempotent by content.** `PUT /rules/{id}` with a body whose canonical form hashes to the stored rule's hash is a no-op and returns the existing `version`. A body with a different hash increments `version`, creates a fresh instance, and does not carry the previous instance's state across.

15. **Dry run touches nothing live.** `POST /rules/{id}/dryrun` returns what each sink stub received and produces no `ntfy_post` and no `influx_write` at the sink receiver. Running the same dry run twice over the same ring content returns the same result.

16. **A drop is counted, never silent.** Every event discarded at a bounded queue increments that queue's `dropped` counter, which is readable in the `sinks` object of a `202` reply and at `GET /metrics`. See `SCALE.md` for the accounting identity these counters close.

17. **Missing configuration is fatal.** `riemannd` started without a required flag exits non-zero, before binding a port, with a message naming the missing flag. It never substitutes a default for a URL, a topic, a bucket, or a listen address.

## CLI (normative)

The binary is `riemannd`. It takes no subcommand.

| Flag | Required | Type | Meaning |
| --- | --- | --- | --- |
| `--listen` | yes | string | Address the HTTP server binds. Accepts `host:port` or `:port`. |
| `--ntfy-url` | yes | string | Base URL of the ntfy server, for example `http://127.0.0.1:19090`. |
| `--ntfy-topic` | yes | string | Topic every alert publishes to. |
| `--influx-url` | yes | string | Base URL of the InfluxDB v2 server. |
| `--influx-org` | yes | string | InfluxDB organization. |
| `--influx-bucket` | yes | string | InfluxDB bucket. |
| `--rules` | yes | string | Path to a JSON file holding an array of rule documents, loaded at startup. |
| `--shards` | no | integer | Number of event partitions. Default `runtime.GOMAXPROCS(0)`. |
| `--set` | no | `name=value`, repeatable | Sets one parameter named in `SCALE.md`. An unrecognised name is fatal at startup, named in the error. |

`--set` exists because `SCALE.md` calls those values parameters. A parameter with no way to set it is a constant, and a document that calls it otherwise is wrong. One repeatable flag keeps the surface flat: a new parameter in `SCALE.md` adds no new flag, and a name the binary does not know fails at startup rather than being ignored, which is property 17 applied to configuration rather than to a missing flag.

Flag names are double-dash long form. Short aliases are not part of the contract. A missing required flag is property 17: exit non-zero naming the flag, bind nothing, start no goroutine that talks to a sink.

Two things are deliberately open. Whether rules submitted over HTTP are written back to `--rules` is the implementation's choice, since no scenario asserts across a restart. Sink credentials are not in the contract for v0; the sink receiver requires none, and a generation that adds a token flag has added surface the spec did not ask for.

## HTTP surface (normative)

Renaming a path or changing a verb is a wire break. Capabilities are added as new paths, never by repurposing one below.

**Ingest.**

- `POST /events` — body is a JSON array of events or one event object.
  - `202` with `{"accepted":n,"sinks":{"<name>":{"depth":d,"capacity":c,"dropped":k}}}`. The `sinks` object is advisory: a snapshot of each bounded sink queue at reply time, so a caller can distinguish "accepted" from "accepted and being shed downstream". Admission is not a delivery promise.
  - `429` with `{"accepted":n,"rejected":m}` and `Retry-After: 1`. Events already admitted are not rolled back, and the body says how many those were.
  - `400` when an event in the batch has no `host` or no `service`.
  - `413` when the batch exceeds `ingest.max_batch_events` or `ingest.max_batch_bytes`.

**Reads.**

- `GET /index?q=<expr>` — `200` with `{"as_of":{"min":t,"max":t},"entries":[...]}`. Each entry is an event object. `as_of` bounds the instants the per-partition slices were taken; they are not simultaneous.
- `GET /index/{host}/{service}` — `200` with one event object, `404` when absent or expired.
- `GET /events?q=&since=&limit=` — `200` with `{"events":[...]}` from the in-memory ring. Ordering is processing order within a partition; interleaving across partitions is best-effort.
- `GET /subscribe?q=&snapshot=true` — Server-Sent Events. When `snapshot=true`, the snapshot and the subscription are taken together, so no event falls between them. Delivery is at-most-once; a subscriber that falls behind its queue receives `event: lagged` carrying the count it missed.
- `GET /healthz` — `200` when the server is ready to accept traffic.
- `GET /metrics` — `200`, Prometheus text format, carrying at minimum the depth, capacity and dropped counter of every bounded queue named in `SCALE.md`.

**Rules.**

- `PUT /rules/{id}` — `200` when the content hash is unchanged, `201` when a new version is created. Body of either is the stored rule document with `version`. `400` on a rule that does not compile, with a message naming the node path or the expression that failed.
- `GET /rules` — `200` with an array of rule documents.
- `GET /rules/{id}` — `200` with the rule document plus `counters`, an object mapping each node path in the rule's tree to the number of events that node has passed downstream since the current version was installed, `404` when unknown. A rule that has never matched anything reports zero at every node, which is how a caller distinguishes a rule that is not firing from a rule that is firing into the index where nothing external can see it.
- `DELETE /rules/{id}` — `204` on delete, `404` when unknown.
- `POST /rules/{id}/dryrun` — `200` with `{"firings":[...]}`, one entry per sink stub delivery, each carrying the sink name, the event as the stub received it, and the node path. `404` when unknown.

No `/v1` prefix. Additional read paths may be added but MUST NOT collide with the set above.

## Rule document (normative)

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

| Key | Required | Meaning |
| --- | --- | --- |
| `id` | yes | Identifier, unique across rules. Matches the path segment on `PUT`. |
| `owner` | yes | An opaque string. How it resolves to a person or a team lives outside the core. |
| `partition` | no | `host` (default), `host,service`, or `global`. |
| `match` | yes | An expression. Only events for which it holds enter `stream`. |
| `stream` | yes | A combinator tree. |
| `bindings` | no | Object mapping a name to a sub-tree, referenced from `stream` as `{"ref":"name"}`. |
| `enabled` | no | Boolean, default `true`. A disabled rule compiles, holds no state, and never fires. |
| `expires_at` | no | Float seconds since epoch. Past that time the rule behaves as disabled. |
| `version` | server-owned | Monotonic integer per id. A client-supplied `version` is ignored. |

A `global` rule receives a copy of every event. A `host` or `host,service` rule sees only the events of the partition it runs on. A rule declaring `host` whose tree contains `coalesce` is refused at `PUT` with `400`, because a per-host partition cannot answer a fleet-wide fold.

**Combinators.** Each node is an object with `op` and, except for `splitp`, a `children` array.

| `op` | Keys | Contract |
| --- | --- | --- |
| `where` | `expr` | Passes an event downstream when the expression holds. |
| `by` | `fields` | Forks state per distinct tuple of those event fields. Children below the fork hold independent state per key. |
| `changed-state` | `initial` | Passes an event only when its `state` differs from the state the previous event left for this fork key. `initial` is the assumed prior state before any event. |
| `throttle` | `limit`, `window_seconds` | Passes at most `limit` events per fork key per window. Excess is discarded. |
| `splitp` | `test`, `branches`, `otherwise` | `test` is an expression containing `{}`, which each branch's `threshold` substitutes. The first branch whose test holds receives the event; `otherwise` receives it when none does. |
| `set` | `fields` | Each value is an expression evaluated against the incoming event. Produces a new event with those fields replaced. |
| `coalesce` | none | Holds the latest event per identity and emits the current set as `events` to its children whenever one changes. |
| `ddt` | none | Emits the rate of change of `metric` per second between consecutive events for a fork key. |
| `stable` | `duration_seconds`, `field` | Passes an event only after the named field has held one value for `duration_seconds`. |

Sink leaves are `{"sink":"ntfy"}`, `{"sink":"influx"}` and `{"sink":"index"}`. A `{"ref":"name"}` leaf splices the named binding in place.

**Deliberate divergence from the original.** Upstream's `test/riemann/streams_test.clj` is the semantic reference for every combinator above, and a generation should port those cases. Two behaviors depart from it on purpose. Where they disagree, this spec wins and the ported test is adjusted rather than the implementation.

- **`by` frees a fork.** Upstream never does, and says so: "`(by)` streams are never garbage-collected" (`src/riemann/streams.clj:1577`), so its fork table grows for the life of the process. riemann-go frees a fork once the key's expired event has passed through it and no timer still references it. The observable consequence is that an identity which expires and later returns arrives at a fresh fork, so a `changed-state` beneath the `by` compares against its `initial` rather than against the state that identity held before it expired. This is a heuristic, not an invariant. It carries two counters, forks live and forks freed, so its effect can be watched, and it retires if that re-firing turns out to be unwanted. The check is a scenario that expires an identity, re-ingests it with the state it last held, and expects an alert.

- **`stable` arms one timer per value change.** Upstream schedules several and lets them race: "it's simpler to just add N tasks during a flapping state and let them all fight it out" (`src/riemann/streams.clj:2018-2027`). riemann-go arms one generation-numbered entry and discards stale generations when they fire. The two agree while event times move forward, and differ only when event times go backwards, where riemann-go honors the most recent value change while upstream's outcome depends on task ordering. Property 12 is what makes the single-entry form reproducible under a scenario's timeline.

**Node paths.** A firing names the path it traversed, and the path is mechanical so a scenario can assert on it:

- The root of `stream` is `stream`; the root of a binding is `bindings/<name>`.
- The k-th entry of a node's `children` array appends `/<k>`.
- The k-th entry of a `splitp` node's `branches` appends `/branches/<k>`; `otherwise` appends `/otherwise`.
- A `{"ref":"name"}` leaf does not appear in the path. The nodes it splices in carry their binding-rooted path.

So the ntfy leaf in the rule above has the path `stream/0/0/0`.

## Expression language

Expressions are expr-lang syntax, compiled once at `PUT` and evaluated per event. The event's fields are top-level names: `host`, `service`, `state`, `metric`, `time`, `ttl`, `tags`, `attributes`, `description`, `source`. The engine supplies four more names, and nothing else:

- `tagged(name)` — true when `name` is in the event's `tags`.
- `now` — the engine's current time in float seconds.
- `expired` — true when the event was produced by index expiry rather than by ingest.
- `events` — inside a `coalesce` subtree, the current set of events. Undefined elsewhere.

The world is closed. An unknown top-level name is a compile error at `PUT`, which is a `400` rather than a rule that silently never fires.

Authors coming from upstream's query grammar need five changes: `=` becomes `==`; `not` binds tighter than comparison, so `not (host == "x")` needs the parentheses; `=~` has no equivalent and becomes `matches`, which is RE2 and unanchored; a missing attribute reads as `""`, so presence is `"key" in attributes`; and `and`/`or` are accepted alongside `&&`/`||`.

Compile cost was measured at 5 to 12 microseconds per expression and evaluation at 80 to 350 nanoseconds (observation, `probes/expr`, expr v1.17.8, Apple M3, Go 1.26.3). That margin is why compilation at `PUT` rather than caching at evaluation is adequate.

## Alert shape (normative)

Every firing at `{"sink":"ntfy"}` is one HTTP `POST` to the `--ntfy-url` root with `Content-Type: application/json` and a body whose keys are exactly `topic`, `title`, `message`, `priority` and `tags`.

- `topic` is the value of `--ntfy-topic`.
- `title` is `"<host> <service> <state>"`.
- `message` is the human line `"<host> <service> is <state> (<metric>)"`, then a newline, then one line beginning with the literal `riemann-go: ` followed by a compact JSON object with exactly these keys, in this order: `rule`, `version`, `owner`, `host`, `service`, `state`, `metric`, `prior_state`, `node`.
- `prior_state` is `null` when the path traversed no `changed-state` node. `metric` is `null` when the event carries none.
- `node` is the node path of the sink leaf, per `## Rule document`.
- `priority` maps from the event's state: `ok` and `info` to 2, `warning` to 4, `error`, `critical` and `emergency` to 5, anything else to 3. This is the mapping the fleet's Clojure sink already uses (`src/riemann/ntfy.clj`).
- `tags` MUST contain `rule:<id>` and `owner:<owner>`. It may carry more, including the state emoji short code.

Provenance rides inside `message` because ntfy discards what it does not recognize. A publish carrying an extra top-level `riemann` object returned `200` and came back without that key (observation, one `POST` to `http://mercy:2586/riemann-arena-test` on 2026-09-15). A subscriber would therefore never see provenance placed in a custom field, and neither would an agent reading the topic. Title and tags survive, but neither holds nine fields legibly, so the message body is the only honest place.

Every firing at `{"sink":"influx"}` is one line of InfluxDB v2 line protocol, appended to a batch that `POST`s to `<--influx-url>/api/v2/write?org=<org>&bucket=<bucket>&precision=ns`:

```
<service>,host=<host>[,state=<state>][,<attr-key>=<attr-value>]... metric=<metric> <time_ns>
```

The measurement is the event's `service`. The `state` tag is omitted when the state is empty. Each entry of `attributes` becomes one tag. Measurement names, tag keys and tag values are escaped per line protocol. An event with no `metric` is not written and increments the influx sink's `dropped` counter, because a point with no field is not a point.

A firing at `{"sink":"index"}` inserts the event into the index. It is observable through property 5 rather than at the sink receiver.

## Observability contract

Scenarios assert on `trace.jsonl`, one JSON object per line. Every line carries `seq`, `ts`, `event` and `source`.

Sink-receiver events, which are the oracle:

| `event` | Carries |
| --- | --- |
| `ntfy_post` | `rule`, `version`, `owner`, `host`, `service`, `state`, `metric`, `prior_state`, `node`, `topic`, `priority`, `tags`, and `raw` holding the verbatim request |
| `influx_write` | `measurement`, `tags`, `fields`, `timestamp`, and `raw` holding the verbatim request |

Harness-emitted events, used only where the sink cannot see the property:

| `event` | Carries |
| --- | --- |
| `ingest_response` | `status`, `accepted`, `rejected` |
| `query_response` | `kind`, plus the fields that query kind returns |
| `rule_response` | `kind`, `id`, `status`, `version` |

The extracted fields on `ntfy_post` are exactly the JSON object embedded in `message`, plus the ntfy envelope. The harness parses provenance out of the place this spec pins it, and nowhere else, so `## Alert shape` is a contract rather than a suggestion.

riemann-go's own counters reach the trace only through `GET /metrics` as a `query_response`. Its logs are debugging artifacts and no scenario reads them.

## Self-observation

riemann-go emits its own queue depths, drop counts, loop lag, index size and per-node firing counts into its own index as ordinary events tagged `riemann`, so a rule can alert on `riemann.sink.ntfy.dropped > 0` with no new mechanism. The sampling interval is a parameter owned by the process, default 10 seconds with a `ttl` of twice that, carried from upstream's instrumentation rule (`src/riemann/core.clj:44-46`).

The service names are part of the contract, because a rule matches on them. Each bounded queue named in `SCALE.md` emits `riemann.<queue>.depth`, `riemann.<queue>.capacity` and `riemann.<queue>.dropped`. Each shard emits `riemann.shard.loop_lag`, `riemann.shard.index_entries` and `riemann.shard.in_flight`. Ingest emits `riemann.ingest.accepted` and `riemann.ingest.rejected`.

One more is required, and it is the only one that is a claim rather than a reading. `riemann.accounting.residual` carries `accepted - (processed + dropped + queued + in_flight)` summed across the process. `SCALE.md` states that identity; this event is how a scenario checks it without the harness learning what a shard is. A correct implementation emits zero at every sample, so a rule matching a non-zero residual is a rule that never fires, and a scenario asserts the absence.

The core carries no dependency on any monitoring system to do this. It exposes a snapshot; whatever samples that snapshot and admits it through ingest lives at the edge. See `INVARIANTS.md` I8.

## Acceptance

A generation is accepted when it passes the scenario corpus under `scenarios/`. Each scenario names a timeline of stimuli and a set of expectations over the trace. The harness drives riemann-go over the HTTP surface above, stands up the sink receiver, and evaluates the expectations. See `HARNESS.md` for the matcher and `scenarios/README.md` for the grammar.

riemann-go's own responses are graded only where a property cannot be seen from a sink, and `INVARIANTS.md` I1 bounds how far that can go.

## Implementation guidance (non-binding)

What follows is the shape the probes measured. A generation may deviate from all of it and still be a valid riemann-go, provided the scenarios pass and `SCALE.md`'s floors hold. Nothing here is assertable, and no scenario checks for it.

The runtime that the evidence supports is one goroutine per shard, owning an inbox, a timer heap, its slice of the index, its ring, and a live instance of every rule's state for the keys it holds. Combinators stay closures over captured state, as in the Clojure original, and no lock protects that state because only the shard goroutine touches it. Timers are entries in the shard's own heap, fired by the same goroutine between events; firing them from anywhere else trips the race detector on the first run (observation, `probes/timers`, `go test -race -count=3`).

Expiry is a min-heap keyed by expiry time, with each item carrying the entry's generation so stale items are discarded on pop. That costs `O(log n)` per expiry instead of a scan of every entry on a fixed reaper tick, and it makes expiry latency the loop's timer granularity rather than the reaper interval. The fleet's Clojure server reaps every 60 seconds, so entries will expire sooner than they do today, which is a behavior change worth one observation before a rule depends on it.

The ring is a per-shard in-memory buffer serving dry run and `GET /events`, bounded by an event count and a byte count, whichever fills first, and lost on restart.

Measured throughput of that shape was 3.4 million events per second through `where -> by host -> changed-state -> counting sink` with expiry running, at 0.035 allocations per event and 6 MB of heap growth over a million events (observation, `probes/throughput`, Apple M3, 8 cores, Go 1.26.3, one goroutine). Loop and index cost about 250 nanoseconds per event; the four combinators add about 45. Not observed in that probe: garbage collection over minutes, index growth over minutes, and behavior under the race detector.

A generation that chooses a lock per combinator instead, and passes every scenario, is a valid riemann-go. `SCALE.md`'s flood floor is what keeps that choice honest.
