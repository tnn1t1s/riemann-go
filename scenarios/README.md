# scenarios/

Each file is a complete arena fixture: the rules to seed, a stimulus timeline,
and the trace the run must produce. The grammar is defined in `../HARNESS.md`.

```yaml
name: <string>
description: <string>

config:                     # SCALE.md parameter names, passed to riemannd as
  shard.inbox_capacity: 8   # --set key=value. Opaque to the harness.

settle_seconds: <number>    # wait after the last stimulus before reading the
                            # sink records. Default 5. Recorded in the report.

ntfy_topic: <string>        # topic the adapter points riemannd at. Default arena.

rules:                      # seeded by PUT before the timeline starts
  - id: <string>
    owner: <string>
    partition: host | host,service | global
    match: <expr>
    stream: <combinator tree>

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
  trace:
    contains:      [ { event: ..., ... } ]
    not_contains:  [ { event: ..., ... } ]
    order:         [ { before: {...}, after: {...} } ]
    count:         [ { match: {...}, equals|min|max: <int> } ]
    field_exists:  [ { match: {...}, field: <name> } ]
```

## Time-domain scenarios

Throttle windows, stable windows and TTLs are counted on the wall clock in
seconds, and a scenario settles within ten. There is no test clock, and a
scenario never backdates the `time` field on a submitted event: timers fire on
the wall clock, so a future timestamp advances no window, and a clock endpoint
would let a generation pass under a fake clock with its real timers wrong.
Window length is a rule parameter the scenario picks; the fleet's stall rule
uses 600 seconds and a scenario uses 3, and both exercise the same combinator.
HARNESS.md carries the full reasoning.

## Authoring rules

- A rule body, a combinator tree, a threshold and a TTL are opaque to the
  harness and to the adapter. They are forwarded exactly as written. If the
  implementation ignores one, the sink trace shows it and the scenario fails.
- Prefer `ntfy_post` and `influx_write` assertions. Those are evidence that
  something arrived at an external process. `ingest_response`, `rule_response`
  and `query_response` are the implementation answering a question, which is
  weaker evidence; use them for the ingest and lifecycle contracts, where the
  reply itself is the property, and nowhere else.
- A scenario must fail for the right reason. Where the property is a silence,
  give the silent case a distinguishing value and assert `not_contains` on it,
  so an implementation that emits nothing at all cannot pass by accident.
- A new property is a new file. Adding a matcher operator is justified only
  when the property cannot be expressed in the five and the operator is
  general across many scenarios.

## The corpus

| File | Property, and where SPEC.md states it |
| --- | --- |
| `ingest-accepted.yaml` | An event is admitted with 202 and reaches the InfluxDB sink. "Wire and backpressure", "Sinks". |
| `expiry-becomes-event.yaml` | `time + ttl` produces a state `expired` event delivered to rules. Invariant 2. |
| `changed-state-transitions-only.yaml` | changed-state forwards on transitions only. `streams_test.clj` changed-state-test. |
| `throttle-bounds-alerts.yaml` | throttle passes at most `limit` per window. `streams_test.clj` throttle-test. |
| `provenance-on-alert.yaml` | Every alert names rule, version, owner, prior state and node path. "Sinks". |
| `rule-lifecycle.yaml` | PUT registers, DELETE deregisters. "Rules", Lifecycle. |
| `backpressure-sheds-and-counts.yaml` | A slow sink sheds and counts; no 429; the accounting identity holds. Invariants 3, 4, 6. |
| `admission-429-on-loop-saturation.yaml` | A full inbox past the deadline replies 429 and the emitter sees the counts. Invariant 9. |

`fixtures/` holds the recorded requests and the scenario for the harness
self-test. It asserts a property of the harness, not of riemann-go.
