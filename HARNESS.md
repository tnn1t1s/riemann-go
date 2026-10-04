# HARNESS.md

> **If adding a riemann-go behavior requires editing Python in the harness, the harness is too smart.**

This document specifies the validation harness for riemann-go. The harness records what an external process observed arrive and compares it to what a scenario declared should arrive. It knows nothing else, and that limit is the design.

Scenarios are Gherkin features under `features/`, run by pytest-bdd. Every stimulus and every assertion is a JSON doc string the harness forwards or evaluates without interpretation; the Gherkin step is the verb, the JSON is the data.

```sh
uv sync
uv run bin/check
uv run bin/trial --command '["/absolute/path/to/riemannd"]' \
  --spec /absolute/path/to/build/SPEC.md \
  --build-manifest /absolute/path/to/build/build-manifest.json
```

## Architecture

Five layers. Each one is smaller and changes less often than the layer above it.

1. **The oracle.** `riemann_harness.sinks`, an HTTP server bound to 127.0.0.1 on an ephemeral port, standing in for ntfy and InfluxDB v2. It records every request verbatim and the implementation under test cannot read it, reconfigure it, or edit its record.

2. **Normalized trace.** `trace.jsonl` in each case directory, one JSON object per line, built by `harness/observer.py` from the oracle's records plus a small set of harness-emitted events. The matcher reads this and nothing else, never a raw request body and never riemannd's log.

3. **Generic matcher.** `riemann_harness.matcher`, five operators over field-by-field equality. The code is the one riemann-graph and riemann-go have shared since cue's arena; it has no domain nouns.

4. **Gherkin features.** Stimulus and assertions, one feature per scenario. A new property is a new file. The step table in `harness/steps.py` maps each step onto one plan entry and nothing more; the arena, the trial options and the five assertion steps come from `riemann_harness.bdd` and `riemann_harness.steps`.

5. **Adapter.** `harness/adapters/riemannd.py`, the only place riemann-go's surface appears. It supplies the start command, the ready check, and how to post events, put and delete a rule, dry-run a rule, and query the index. Process lifecycle belongs to `harness/session.py`, not to it.

The shared package `riemann_harness` (tnn1t1s/riemann-harness) carries the layers that are the same for every riemann project: the sink receiver, the matcher, the score categories, the HTTP client, the launch adapter, the session lifecycle, the pytest-bdd arena and the generation scripts. This repository keeps what is riemann-go's: the riemannd flags, the observer that knows the alert shape, the plan checks, the session hooks (binding, receiver, rule seeding, timeline, counters, trace build) and the stimulus step table.

## The oracle

riemann-go's whole purpose is to send something somewhere when a condition holds, so the sinks are where its behavior becomes visible to a third party. Grading it on its own HTTP replies would let a generation satisfy itself; grading it on what an external receiver logged cannot be faked from inside the process.

The receiver accepts two surfaces:

| Request | Meaning |
| --- | --- |
| `POST /api/v2/write?org=&bucket=&precision=` | InfluxDB v2 write, body in line protocol |
| `POST` on any other path | ntfy publish, JSON body |

The ntfy shape follows `riemann/src/riemann/ntfy.clj`, which posts a JSON body carrying topic, title, message, priority and tags to the server's base URL. Line-protocol bodies are parsed into measurement, tags, fields and timestamp, with escaping and quoted string fields handled, so one write of several lines becomes several trace events.

Two faults can be injected. A delay makes a sink slow, which is how `backpressure-sheds-and-counts` stalls the ntfy queue; a failure status makes it return an error. Both are method calls on an object the session holds, reached only through the `sink delays` and `sink fails` steps below, deliberately not an HTTP control endpoint, because an endpoint would be a door the code under test could walk through to reconfigure its own grader.

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

The five operators and their Gherkin spellings. Each `Then` step carries a JSON array in a doc string; the array is the operator's operand list, in the same shape the YAML corpus used.

```gherkin
Then the recorded trace contains:          # each pattern must match at least one event
  """
  [{"event": "ntfy_post", "service": "ntfy.listen.up", "state": "expired"}]
  """
Then the recorded trace excludes:          # no pattern may match any event
  """
  [{"event": "ingest_response", "status": 429}]
  """
Then the recorded trace has this order:    # before.seq < after.seq; both must match
  """
  [{"before": {"event": "rule_response", "kind": "put", "id": "archive-all"},
    "after":  {"event": "influx_write", "host": "ghost"}}]
  """
Then the recorded trace has these counts:  # bounded match counts
  """
  [{"match": {"event": "ntfy_post", "service": "ntfy.listen.connected"}, "equals": 1},
   {"match": {"event": "ntfy_post", "rule": "alert-all"}, "min": 1, "max": 400}]
  """
Then the recorded trace has these fields:  # match, then require a named field present
  """
  [{"match": {"event": "ntfy_post", "rule": "atlas-cost"}, "field": "prior_state"}]
  """
```

An event matches a pattern when every key in the pattern equals the same key in the event; keys the pattern omits are ignored, and equality is exact, with no regex, ranges or prefixes. `order` compares `seq` rather than `ts`, and fails when either side matches nothing, because silently passing on absent evidence is the failure mode that makes a corpus worthless. `count` takes any subset of `equals`, `min` and `max`, all of which must hold. `field_exists` requires the named field present and non-null on every matching event, which is the right operator for a version number or a node path whose value is dynamic but whose presence is the contract.

The first `Then` step settles, stops riemannd, builds the trace and evaluates. Later `Then` steps evaluate the same frozen trace; a `When` after a `Then` is rejected. A scenario with no `Then` has no verdict and fails.

### Why there is no `within_seconds`

The corpus is expressible in the five operators, so the sixth was not added. cue deferred it and named riemann-go's time-domain combinators as the case that might justify it, and that case did come up, in one place.

`throttle-bounds-alerts` wants "at most 2 alerts in any 10-second window." What it asserts instead is "between 2 and 6 alerts over the whole run," derived from a stimulus timeline five seconds long plus a three-second settle, which touches at most three windows. The weaker form still fails against the bug that matters: twenty transitions arriving at an unthrottled sink produce twenty posts, far outside the ceiling. A generation that throttles at the wrong rate, say 4 per window instead of 2, would slip through at 12 posts only if the ceiling were raised, and it is not.

The condition for revisiting this: a scenario whose property is a rate that a fixed-length timeline cannot bound, or two scenarios that both want per-window arithmetic. Then `within_seconds` goes in, stated generically over `ts` with no monitoring vocabulary anywhere in the operator, and this section records why.

## Step table

Every step maps onto one entry of a plan the session executes. The plan has the same shape the YAML scenarios had: configuration, seeded rules, a stimulus timeline with offsets, and the assertion block.

| Step | Plan entry | Meaning |
| --- | --- | --- |
| `Given riemannd is configured with:` + JSON object | `config` | SCALE.md parameter names, rendered by the adapter as `--set key=value`. Neither half is interpreted. |
| `Given a settle window of N seconds` | `settle` | Seconds to wait after the last stimulus before reading the oracle's records. Default 5, recorded in the report. |
| `Given the ntfy topic is "t"` | `ntfy_topic` | Topic the adapter points riemannd at. Default `arena`. |
| `Given these rules are installed:` + JSON array | `rules` | Seeded by `PUT /rules/{id}` after ready and before the timeline starts. |
| `When at Ts the emitter posts:` + JSON array | `emit` | One `POST /events` with that batch. |
| `When at Ts the emitter posts N events in batches of B every Is from the template:` + JSON object | `emit_n` | `{i}` expands in string values; one `POST` per batch with `I` seconds between batches. |
| `When at Ts the client puts rule "id":` + JSON object | `put_rule` | `PUT /rules/{id}`. |
| `When at Ts the client deletes rule "id"` | `delete_rule` | `DELETE /rules/{id}`. |
| `When at Ts the client dry-runs rule "id":` + JSON object | `dryrun_rule` | `POST /rules/{id}/dryrun`. |
| `When at Ts the client queries the index with "expr"` | `query_index` | `GET /index?q=expr`; records status, match count and the CORS header. |
| `When at Ts the ntfy sink delays each reply by S seconds` | `sink_delay` | Oracle fault: per-request latency on that sink. |
| `When at Ts the ntfy sink fails every request with 500` | `sink_fail` | Oracle fault: that status on every request. |
| `When at Ts the ntfy sink replies normally again` | `sink_fail` with `null` | Clears the fault. |
| `Then the recorded trace contains / excludes / has this order / has these counts / has these fields:` | `expect.trace.<op>` | The five operators above. |

`T` is seconds from the first stimulus step, not from the previous one, so a timeline keeps its shape under scheduling jitter. Gherkin steps run in order, so offsets are written in order.

Stimulus is opaque. A rule body, a combinator tree, a throttle limit and a TTL go from the doc string to riemann-go unchanged, through an adapter that never inspects them. When a generation ignores `window_seconds`, the sink trace shows too many posts and the scenario fails, which is the coupling we want: the scenario and SPEC.md carry the contract, and the harness carries bytes.

`config` works the same way. `admission-accounts-for-every-event` sets `shard.inbox_capacity` to 8 and `ingest.admission_deadline` to 1; the adapter turns each pair into `--set key=value` and has no idea what an inbox is.

The settle window is written into every report next to the actual wall clock, so an absent event is interpretable. A missing alert means the property failed, or it means the harness stopped looking too early, and a reader who cannot tell the difference cannot act on the report.

### Tags

Each scenario carries the SPEC.md property numbers it asserts, as `@P1` through `@P17`, declared in `pytest.ini`. Properties stated outside the numbered list carry a section tag: `@http_surface`, `@expression_language`, and the combinator names `@coalesce`, `@ddt` and `@stable` for SEMANTICS.md behavior. `bin/trial -m P9` selects by tag and `-k throttle` by name. An empty selection is not a pass; pytest exits 5.

### Time-domain scenarios use short real windows

A throttle window, a stable window and a TTL are counted on the wall clock in seconds, and a scenario settles within ten. `throttle-bounds-alerts` uses `window_seconds: 2` and `expiry-becomes-event` a 2-second TTL, while the stable scenario in the held-out set uses 3. There is no test clock. A scenario does not backdate the `time` field to advance a timer, because a timer runs on the wall clock and a stale timestamp will not move it. Setting `time` deliberately is allowed where the timestamp is itself the data under test, as in an expiry scenario, which needs a controlled `time + ttl`. The distinction is whether the field is being used as data or as a substitute for waiting.

Two reasons, of which the first is the one that matters. A test-clock endpoint would be surface the spec does not have, and it would let a generation pass the whole corpus under a fake clock while its real timers were wrong, which is the failure the arena exists to catch. Backdating event timestamps substitutes for nothing, because timers fire on the wall clock in riemann-go as in upstream Clojure, so a future timestamp advances no throttle window.

The cost is that a scenario cannot assert a ten-minute stall window, and that is the right trade. Window length is a rule parameter the scenario chooses, while the combinator semantics are what the corpus tests: the fleet's stall rule uses 600 seconds and a scenario uses 3, and the same `stable` runs under both. Short windows are affordable because there is no cluster to wait for.

## Process-lifecycle ownership

The session starts the sink receiver, starts riemannd through the adapter with the receiver's URL in its own process group, waits for ready, seeds the rules, drives the timeline as the `When` steps arrive, and on the first `Then` settles, reads each seeded rule's counters, stops riemannd, stops the receiver, builds the trace and evaluates. The lifecycle is `riemann_harness.session.Session`; the riemann-go hooks are in `harness/session.py`.

The adapter owns four things and no more: the start command including every URL and port, the ready check, the event and rule surfaces, and the index query. It cannot skip a stop or start with different state, because it never decides when either happens.

Readiness is `GET /healthz` returning 200, which SPEC.md's HTTP surface pins as the readiness claim. The adapter asks that and nothing else. `bin/trial --ready-timeout` is 15 seconds by default, a parameter of the trial rather than of the product; the shared session's own default is 5.

Before launch, the session verifies the binding: the artifact at `command[0]` hashes to an entry in `build-manifest.json`, and the expected SPEC.md hashes to the manifest's `spec_sha256`. `harness/binding.py` accepts either the native `artifact_sha256` or any entry in the `artifacts` map, because one riemann-go generation produces the host-native `service` and three cross-compiled release assets from one source tree. A mismatch fails the case before riemannd starts, with no trace. `--unbound` waives the check and the report records the waiver; it exists for artifacts that predate the manifest and for fixtures, never for a promotion run.

## Score categories

| category | meaning |
| --- | --- |
| `start_error` | process did not pass the ready check before the timeout |
| `process_error` | riemannd exited during the trial |
| `ingest_error` | ready, but no event was ever admitted |
| `rule_error` | events admitted, but the rule surface never took a rule, or no rule ever fired |
| `sink_error` | rules fired, but nothing ever reached the oracle |
| `observer_error` | the trace could not be built from what was recorded |
| `predicate_violation` | one or more matcher assertions failed |
| `bdd_error` | a step raised before a verdict (an undefined step, malformed JSON, a surface error) |
| `invalid_scenario` | the scenario reached no final assertion |
| `GREEN` | every assertion held |

`compile_error` no longer appears in a report: a candidate that did not build never becomes an artifact the trial can launch, and `bin/generate` reports that failure in the attempt's `invocation.json` instead.

The three middle categories name riemann-go's own pipeline stages, so a failed report says how far a generation got before it stopped working.

`rule_error` means events were admitted and no rule ever fired. A firing is not visible at the sink receiver on its own: one that reaches a sink cannot be told apart from ordinary sink traffic, and one that goes to the `index` sink reaches no external process at all. After settle the session reads each seeded rule's node counters, which SPEC.md's `GET /rules/{id}` returns, and a rule whose every node reads zero has not fired. That read is harness evidence rather than oracle evidence. It is acceptable here because it classifies a failure and no scenario asserts on it; an assertion resting on it would be the implementation grading itself.

The matcher's verdict is the final arbiter. Categories classify a failure and never gate a pass, so a scenario whose assertions all held is GREEN even when no sink was posted to, because a scenario can assert exactly that silence. The category is written to `report.json` and to `summary.json`.

## Outputs

`bin/trial --out DIR` writes:

- `DIR/<case>-<key>/report.json`: scenario name, feature file and its SHA-256, category, the per-assertion matcher report, the score block, settle and wall-clock seconds, the sink request count, the command, the build binding, and the plan's SHA-256.
- `DIR/<case>-<key>/trace.jsonl`: the normalized trace the matcher read.
- `DIR/<case>-<key>/candidate.log`: riemannd's whole stdout and stderr, written even when empty so an absent file means the run did not get that far.
- `DIR/junit.xml`: pytest's JUnit report, one test case per scenario.
- `DIR/summary.json`: every scenario's category, assertion counts and case directory, and the green count.

Logs never determine correctness; the diagnostician reads them, the matcher does not.

## The rule

A new riemann-go capability should require a new feature file, always. It should almost never require new harness Python: when you catch yourself writing `check_throttle()` or `evaluate_expiry()`, the contract is moving into a file that nobody reviews as carefully as SPEC.md, which is how the harness quietly becomes the spec. An operator is justified when the property cannot be expressed in the five and the operator is general enough that later scenarios will reuse it. A step is justified when a stimulus the surface already offers has no spelling. An event type is justified less often than either.

## Worked example: eight properties, five operators

Scenarios from `features/`, with the assertion that carries the property and the SPEC.md statement it comes from. Patterns are shown as the matcher sees them.

**`ingest-accepted`**, the ingest contract of "HTTP surface" and the sink contract of "Alert shape":

```json
contains: [{"event": "ingest_response", "status": 202, "accepted": 1},
           {"event": "influx_write", "host": "ghost", "measurement": "agent.tokens.out", "metric": 1234.0}]
```

**`expiry-becomes-event`**, property 4, carried from `src/riemann/core.clj:274-308`. One beat with a 2-second TTL, then silence:

```json
contains:     [{"event": "ntfy_post", "host": "ghost", "service": "ntfy.listen.up", "state": "expired"}]
not_contains: [{"event": "ntfy_post", "host": "ghost", "service": "ntfy.listen.up", "state": "ok"}]
```

Without this scenario the index is invisible to the oracle, and an implementation that dropped every entry on insert would pass the other scenarios.

**`changed-state-transitions-only`**, from `streams_test.clj` changed-state-test. Five events in state `ok` and one in `critical`:

```json
count: [{"match": {"event": "ntfy_post", "service": "ntfy.listen.connected"}, "equals": 1}]
```

**`throttle-bounds-alerts`**, from `streams_test.clj` throttle-test, and the shape the fleet's `ntfy.listen.connected` rule runs today. Twenty transitions over five seconds through a throttle of 2 per 2 s:

```json
count: [{"match": {"event": "ntfy_post", "service": "ntfy.listen.connected"}, "min": 2, "max": 6}]
```

**`provenance-on-alert`**, SPEC.md "Alert shape (normative)":

```json
field_exists: [{"match": {"event": "ntfy_post", "rule": "atlas-cost"}, "field": "version"},
               {"match": {"event": "ntfy_post", "rule": "atlas-cost"}, "field": "owner"},
               {"match": {"event": "ntfy_post", "rule": "atlas-cost"}, "field": "prior_state"},
               {"match": {"event": "ntfy_post", "rule": "atlas-cost"}, "field": "node"}]
```

**`rule-lifecycle`**, property 14. The DELETE half is the half that matters, and it is a silence, so the post-delete event gets a distinguishing state:

```json
not_contains: [{"event": "ntfy_post", "service": "ntfy.listen.connected", "state": "critical"}]
order:        [{"before": {"event": "ntfy_post", "service": "ntfy.listen.connected"},
                "after":  {"event": "rule_response", "kind": "delete", "id": "listener-connected"}}]
```

**`backpressure-sheds-and-counts`**, invariants I4, I5 and the accounting identity in SCALE.md. The oracle's ntfy sink is slowed to 100 ms per post, then flooded:

```json
contains:     [{"event": "influx_write", "measurement": "riemann.sink.ntfy.dropped"}]
not_contains: [{"event": "ingest_response", "status": 429},
               {"event": "ntfy_post", "rule": "accounting-violation"}]
```

The threshold comparison lives in the rule, not in the matcher. `shed-detector` matches `service == "riemann.sink.ntfy.dropped" && metric > 0`, so "drops were counted" becomes the plain existence of a write, and the matcher needs no comparison operator to check it. Self-observation being an ordinary event stream is what makes that work, and it is why the decision to route the gauges through ingest pays off in validation rather than only on a dashboard.

**`admission-accounts-for-every-event`**, properties 2 and 16, with a 500-event batch offered to an inbox of 8 under a 1 ms deadline:

```json
contains:     [{"event": "ingest_response", "status": 202, "accepted": 1}]
field_exists: [{"match": {"event": "ingest_response", "status": 202}, "field": "accepted"}]
```

There is no `check_expiry()`, no `check_throttle()` and no `check_backpressure()` anywhere. The harness has no idea what any of those words mean.

## Self-test

`bin/check` first parses every feature under `features/` and `features/holdout/` through the step table without launching anything, validating each plan's shape, then runs `tests/`. The self-tests replay the recorded requests in `tests/fixtures/dry-run.json` at a live sink receiver over real HTTP, build the trace from what the receiver captured, and evaluate `tests/fixtures/dry-run-expect.json` over it, which exercises every operator against the line-protocol parser and the provenance extraction. Replaying through the socket rather than handing the observer a dict is deliberate: that is where a silent break would otherwise hide.

A negative control, a pattern that must not match, fails unless the satisfied expectation passes and the violated one fails. A matcher that always returns true would pass every scenario ever written, so the self-test checks for it directly.

`tests/riemannd_fixture.py` is a canned stand-in for riemannd that routes events to a rule's top-level sink and implements no combinator. The runner tests drive it in five modes and assert the category each one earns: GREEN, `predicate_violation` for a wrong state and for a missing provenance line, `sink_error` when nothing is posted, `start_error` when it exits at startup. Further tests cover an undefined step, an empty scenario, a scenario with no `Then`, an undefined step after a passing assertion, tag selection, an empty selection, a skipped case, holdout gating, build binding mismatches and the CMake build graph. None of them is evidence about a generated riemannd.

## What this design intentionally lacks

- **A predicate registry.** Five operators, no plugins.
- **Streaming observation.** The trace is built after the settle window from what the receiver accumulated. SSE subscribe is a riemann-go read surface this corpus does not yet drive.
- **Per-property partial credit.** A scenario is GREEN or it is not; partial credit is spelled `"min": N` in a count assertion, not built into the scorer.
- **Cross-scenario state.** Each scenario gets a fresh riemannd, a fresh receiver, and its own temporary rule directory.

## What this corpus does not cover

**The 429 path.** A 429 requires the partition inbox to stay full past the admission deadline, and the harness has no way to hold it there: a loop that drains while the handler offers admits the whole batch however small the inbox is. One generation produced a 429 under a tiny inbox and two did not, and all three were conforming. Forcing it would need a way to stall the loop from outside, which is test-only surface the spec does not have and should not grow. `admission-accounts-for-every-event` asserts what holds either way, that every event is accounted for and every reply says how many it took.

Driving a 3.4-million-events-per-second loop into saturation from Python is not something this harness can do, so no scenario says anything about throughput. SCALE.md's flood floor is the observation that covers the other half.

`backpressure-sheds-and-counts` asserts the accounting identity by its absence: a rule named `accounting-violation` matches `service == "riemann.accounting.residual" && metric != 0`, and the scenario requires that it never fires. SPEC.md's self-observation section names that metric and requires it to read zero at every sample, so a correct implementation makes this rule one that never fires.

Nothing here drives SSE subscribe, explain, the ring's bounds, or the global-partition shard. Each wants its own scenario before the generation claiming them can be validated: a property with no scenario is not enforced, whatever SPEC.md says about it. COVERAGE.md keeps the ledger.
