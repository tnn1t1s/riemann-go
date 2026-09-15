# HARNESS.md

> **If adding a riemann-go behavior requires editing Python in the harness, the harness is too smart.**

This document specifies the validation harness for the riemann-go arena. The harness records what an external process observed arrive and compares it to what a scenario declared should arrive. It knows nothing else, and that limit is the design.

## Architecture

Five layers. Each one is smaller and changes less often than the layer above it.

1. **The oracle.** `harness/sinks.py`, an HTTP server bound to 127.0.0.1 on an ephemeral port, standing in for ntfy and InfluxDB v2. It records every request verbatim and the implementation under test cannot read it, reconfigure it, or edit its record.

2. **Normalized trace.** `traces/<run>.jsonl`, one JSON object per line, built by `harness/observer.py` from the oracle's records plus a small set of harness-emitted events. The matcher reads this and nothing else, never a raw request body and never riemannd's log.

3. **Generic matcher.** `harness/matcher.py`, five operators over field-by-field equality. It is cue's matcher, code byte-identical; only the docstring differs, because the original names cue's domain nouns.

4. **Scenario YAML.** Stimulus and expectation. A new property is a new file.

5. **Adapter.** `harness/adapters/riemannd.py`, the only place riemann-go's surface appears. It supplies the start command, the ready check, and how to post events, put and delete a rule, and query the index. Process lifecycle belongs to the harness, not to it.

## The oracle

riemann-go's whole purpose is to send something somewhere when a condition holds, so the sinks are where its behavior becomes visible to a third party. Grading it on its own HTTP replies would let a generation satisfy itself; grading it on what an external receiver logged cannot be faked from inside the process.

The receiver accepts two surfaces:

| Request | Meaning |
| --- | --- |
| `POST /api/v2/write?org=&bucket=&precision=` | InfluxDB v2 write, body in line protocol |
| `POST` on any other path | ntfy publish, JSON body |

The ntfy shape follows `riemann/src/riemann/ntfy.clj`, which posts a JSON body carrying topic, title, message, priority and tags to the server's base URL. Line-protocol bodies are parsed into measurement, tags, fields and timestamp, with escaping and quoted string fields handled, so one write of several lines becomes several trace events.

Two faults can be injected. `set_delay(sink, seconds)` makes a sink slow, which is how `backpressure-sheds-and-counts` stalls the ntfy queue; `set_fail(sink, status)` makes it return 500, which is how shed-and-count behavior under a failing sink becomes observable. Both are Python method calls on an object the harness holds, deliberately not an HTTP control endpoint, because an endpoint would be a door the code under test could walk through to reconfigure its own grader.

## Trace vocabulary v0

Every line carries four keys, whatever else it holds:

- `seq`, a monotonically increasing integer assigned when the trace is built, so `order` is deterministic when two events share a timestamp.
- `ts`, float seconds. From the receiver's clock for sink events and from the harness clock for the rest, both `time.time()` in one process.
- `event`, one of the five names below.
- `source`, either `sink-receiver` or `harness`.

| event | fields | source |
| --- | --- | --- |
| `ntfy_post` | `topic`, `title`, `message`, `priority`, `tags`, `rule`, `version`, `owner`, `prior_state`, `node`, `host`, `service`, `state`, `metric`, `raw` | sink-receiver |
| `influx_write` | `measurement`, `host`, `service`, `state`, `metric`, `time`, `org`, `bucket`, `precision`, `raw` | sink-receiver |
| `ingest_response` | `status`, `accepted`, `rejected` | harness |
| `query_response` | `kind`, plus query-specific fields | harness |
| `rule_response` | `kind`, `id`, `status`, `version` | harness |

The split matters more than the field lists. A `ntfy_post` or an `influx_write` is evidence that something reached a process riemann-go does not control. The other three record what riemann-go said when the harness asked, which is weaker, and a scenario that leans on them is grading the implementation against itself. Two contracts genuinely live at the reply surface and belong there: the 202-with-an-accepted-count of SPEC.md's ingest path, and rule registration, where the PUT succeeding is the property. Everything else asserts on a sink.

Provenance extraction from an ntfy body is one function, `observer.ntfy_provenance`. It follows SPEC.md's observability contract and is the only place that changes if the placement of those fields moves; nothing else in the harness reads a rule id or a prior state out of a request.

Adding an event type is allowed and should stay rare. The threshold is a property that cannot be expressed with the existing types even by writing a new scenario.

## Matcher operators

```yaml
expect:
  trace:
    contains:                          # each pattern must match at least one event
      - { event: ntfy_post, service: ntfy.listen.up, state: expired }

    not_contains:                      # no pattern may match any event
      - { event: ingest_response, status: 429 }

    order:                             # before.seq < after.seq; both must match
      - before: { event: rule_response, kind: put, id: archive-all }
        after:  { event: influx_write, host: ghost }

    count:                             # bounded match counts
      - match: { event: ntfy_post, service: ntfy.listen.connected }
        equals: 1
      - match: { event: ntfy_post, rule: alert-all }
        min: 1
        max: 400

    field_exists:                      # match, then require a named field present
      - match: { event: ntfy_post, rule: atlas-cost }
        field: prior_state
```

An event matches a pattern when every key in the pattern equals the same key in the event; keys the pattern omits are ignored, and equality is exact, with no regex, ranges or prefixes. `order` compares `seq` rather than `ts`, and fails when either side matches nothing, because silently passing on absent evidence is the failure mode that makes a corpus worthless. `count` takes any subset of `equals`, `min` and `max`, all of which must hold. `field_exists` requires the named field present and non-null on every matching event, which is the right operator for a version number or a node path whose value is dynamic but whose presence is the contract.

### Why there is no `within_seconds`

The eight scenarios in this corpus are all expressible in the five operators, so the sixth was not added. cue deferred it and named riemann-go's time-domain combinators as the case that might justify it, and that case did come up, in one place.

`throttle-bounds-alerts` wants "at most 2 alerts in any 10-second window." What it asserts instead is "between 2 and 6 alerts over the whole run," derived from a stimulus timeline twenty seconds long plus a three-second settle, which touches at most three windows. The weaker form still fails against the bug that matters: twenty transitions arriving at an unthrottled sink produce twenty posts, far outside the ceiling. A generation that throttles at the wrong rate, say 4 per window instead of 2, would slip through at 12 posts only if the ceiling were raised, and it is not.

The condition for revisiting this: a scenario whose property is a rate that a fixed-length timeline cannot bound, or two scenarios that both want per-window arithmetic. Then `within_seconds` goes in, stated generically over `ts` with no monitoring vocabulary anywhere in the operator, and this section records why.

## Scenario grammar

```yaml
name: <string>
description: <string>

config:                     # SCALE.md parameter names, rendered by the adapter
  shard.inbox_capacity: 8   # as --set key=value. Neither half is interpreted.

settle_seconds: <number>    # wait after the last stimulus before reading the
                            # oracle's records. Default 5, recorded in the report.

ntfy_topic: <string>        # topic the adapter points riemannd at. Default arena.

rules:                      # seeded by PUT before the timeline starts
  - { id, owner, partition, match, stream, bindings? }

stimulus:                   # ordered timeline, offsets from the first event
  - at: <duration>
    emit: { events: [ <event>, ... ] }
  - at: <duration>
    emit_n: { count, batch_size, interval, template }   # {i} expands in strings
  - at: <duration>
    put_rule: { id: <string>, rule: { ... } }
  - at: <duration>
    delete_rule: <string>
  - at: <duration>
    query_index: <expr>
  - at: <duration>
    sink_delay: { sink: ntfy | influx, seconds: <duration> }
  - at: <duration>
    sink_fail: { sink: ntfy | influx, status: <int or null> }

expect:
  trace: { contains, not_contains, order, count, field_exists }
```

Stimulus is opaque. A rule body, a combinator tree, a throttle limit and a TTL go from the YAML to riemann-go unchanged, through an adapter that never inspects them. When a generation ignores `window_seconds`, the sink trace shows too many posts and the scenario fails, which is the coupling we want: the scenario and SPEC.md carry the contract, and the harness carries bytes.

`config` works the same way. `admission-429-on-loop-saturation` sets `shard.inbox_capacity` to 8 and `ingest.admission_deadline` to 1 ms; the adapter turns each pair into `--set key=value` and has no idea what an inbox is.

The settle window is written into every report next to the actual wall clock, so an absent event is interpretable. A missing alert means the property failed, or it means the harness stopped looking too early, and a reader who cannot tell the difference cannot act on the report.

## Process-lifecycle ownership

The harness starts the sink receiver, starts riemannd through the adapter with the receiver's URL, waits for ready, seeds the rules, drives the timeline, settles, stops riemannd, stops the receiver, builds the trace, runs the matcher, and writes the report. Every one of those steps is in `harness/run.py`.

The adapter owns four things and no more: the start command including every URL and port, the ready check, the event and rule surfaces, and the index query. It cannot skip a stop or start with different state, because it never decides when either happens.

Readiness is `GET /healthz` returning 200, which SPEC.md's HTTP surface pins as the readiness claim. The adapter asks that and nothing else.

## Score categories

| category | meaning |
| --- | --- |
| `compile_error` | binary missing or not executable |
| `start_error` | process did not pass the ready check before the timeout |
| `sink_connect_error` | started, but nothing ever reached the oracle |
| `observer_error` | the trace could not be built from what was recorded |
| `predicate_violation` | one or more matcher assertions failed |
| `GREEN` | every assertion held |

The matcher's verdict is the final arbiter. Categories classify a failure and never gate a pass, so a scenario whose assertions all held is GREEN even when no sink was posted to, because a scenario can assert exactly that silence.

## The rule

A new riemann-go capability should require a new scenario, always. It should almost never require new harness Python: when you catch yourself writing `check_throttle()` or `evaluate_expiry()`, the contract is moving into a file that nobody reviews as carefully as SPEC.md, which is how the harness quietly becomes the spec. An operator is justified when the property cannot be expressed in the five and the operator is general enough that later scenarios will reuse it. An event type is justified less often than that.

## Worked example: eight properties, five operators

Every scenario in `scenarios/` below, with the assertion that carries its property and the SPEC.md statement it comes from.

**`ingest-accepted`**, the ingest contract of "Wire and backpressure" and the sink contract of "Sinks":

```yaml
contains:
  - { event: ingest_response, status: 202, accepted: 1 }
  - { event: influx_write, host: ghost, service: agent.tokens.out, metric: 1234.0 }
```

**`expiry-becomes-event`**, invariant 2, carried from `src/riemann/core.clj:274-308`. One beat with a 2-second TTL, then silence:

```yaml
contains:
  - { event: ntfy_post, host: ghost, service: ntfy.listen.up, state: expired }
not_contains:
  - { event: ntfy_post, host: ghost, service: ntfy.listen.up, state: ok }
```

Without this scenario the index is invisible to the oracle, and an implementation that dropped every entry on insert would pass the other seven.

**`changed-state-transitions-only`**, from `streams_test.clj` changed-state-test. Five events in state `ok` and one in `critical`:

```yaml
count:
  - match: { event: ntfy_post, service: ntfy.listen.connected }
    equals: 1
```

**`throttle-bounds-alerts`**, from `streams_test.clj` throttle-test, and the shape the fleet's `ntfy.listen.connected` rule runs today. Twenty transitions through a throttle of 2 per 10 s:

```yaml
count:
  - match: { event: ntfy_post, service: ntfy.listen.connected }
    min: 2
    max: 6
```

**`provenance-on-alert`**, SPEC.md "Alert shape (normative)":

```yaml
field_exists:
  - { match: { event: ntfy_post, rule: atlas-cost }, field: version }
  - { match: { event: ntfy_post, rule: atlas-cost }, field: owner }
  - { match: { event: ntfy_post, rule: atlas-cost }, field: prior_state }
  - { match: { event: ntfy_post, rule: atlas-cost }, field: node }
```

**`rule-lifecycle`**, "Rules", Lifecycle. The DELETE half is the half that matters, and it is a silence, so the post-delete event gets a distinguishing state:

```yaml
not_contains:
  - { event: ntfy_post, service: ntfy.listen.connected, state: critical }
order:
  - before: { event: ntfy_post, service: ntfy.listen.connected }
    after:  { event: rule_response, kind: delete, id: listener-connected }
```

**`backpressure-sheds-and-counts`**, invariants 3, 4 and 6. The oracle's ntfy sink is slowed to 100 ms per post, then flooded:

```yaml
contains:
  - { event: ntfy_post, rule: shed-detector }
not_contains:
  - { event: ingest_response, status: 429 }
  - { event: ntfy_post, rule: accounting-violation }
```

The threshold comparison lives in the rule, not in the matcher. `shed-detector` matches `service == "riemann.sink.ntfy.dropped" && metric > 0`, so "drops were counted" becomes the plain existence of an alert, and the matcher needs no comparison operator to check it. Self-observation being an ordinary event stream is what makes that work, and it is why the decision to route the gauges through ingest pays off in validation rather than only on a dashboard.

**`admission-429-on-loop-saturation`**, invariant 9, with a 500-event batch offered to an inbox of 8 under a 1 ms deadline:

```yaml
contains:
  - { event: ingest_response, status: 429 }
field_exists:
  - { match: { event: ingest_response, status: 429 }, field: accepted }
  - { match: { event: ingest_response, status: 429 }, field: rejected }
```

There is no `check_expiry()`, no `check_throttle()` and no `check_backpressure()` anywhere. The harness has no idea what any of those words mean.

## Self-test

`python -m harness.run --dry` runs with no riemann-go binary and no network beyond the loopback. It starts the sink receiver, replays the recorded requests in `scenarios/fixtures/dry-run.json` at it over real HTTP, builds the trace from what the receiver captured, and evaluates `scenarios/fixtures/dry-run.yaml` over it. Replaying through the socket rather than handing the observer a dict is deliberate: it exercises the line-protocol parser and the query parsing, which is where a silent break would otherwise hide.

The self-test then runs a negative control, a pattern that must not match, and fails unless the satisfied scenario passes and the violated one fails. A matcher that always returns true would pass every scenario ever written, so the self-test checks for it directly.

## What this design intentionally lacks

- **A predicate registry.** Five operators, no plugins.
- **Streaming observation.** The trace is built after the settle window from what the receiver accumulated. SSE subscribe is a riemann-go read surface this corpus does not yet drive.
- **Per-property partial credit.** A scenario is GREEN or it is not; partial credit is spelled `count: {min: N}` in a scenario, not built into the scorer.
- **Cross-scenario state.** Each run gets a fresh riemannd, a fresh receiver, and its own temporary rule directory.

## What this corpus does not cover

Stated plainly, because a gap nobody wrote down is a gap nobody closes.

`admission-429-on-loop-saturation` reaches its 429 by shrinking the inbox and the deadline rather than by outrunning the loop. Driving a 3.4-million-events-per-second loop into saturation from Python is not something this harness can do, so the scenario tests the admission path and says nothing about throughput. SCALE.md's flood floor is the observation that covers the other half.

`backpressure-sheds-and-counts` asserts the accounting identity by its absence: a rule named `accounting-violation` matches `service == "riemann.accounting.residual" && metric != 0`, and the scenario requires that it never fires. SPEC.md's self-observation section names that metric and requires it to read zero at every sample, so a correct implementation makes this rule one that never fires.

Nothing here drives SSE subscribe, dry run, explain, the ring, or the global-partition shard. Those are milestones 4 and 5, and each wants its own scenario before the generation claiming them can be validated: a property with no scenario is not enforced, whatever SPEC.md says about it.
