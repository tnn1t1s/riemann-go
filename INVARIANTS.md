# INVARIANTS.md

> Principles that hold across every riemann-go generation, regardless of which features the spec adds.

This document is a **generator-binding contract**. Every generation reads it alongside `SPEC.md`, `SCALE.md` and `HARNESS.md`. A generation that violates an invariant is not a v0 or a v1 implementation; it is the wrong shape, and the audit checklist at the end is what catches it before promotion.

The test for whether something belongs here: would the principle still hold if riemann-go grew a feature class nobody has imagined, say federated multi-node aggregation? If yes, it is an invariant. If it depends on a specific combinator, a specific sink, or a specific number, it belongs in `SPEC.md`, `SCALE.md`, or a scenario.

## Validation invariants

### I1. The sink receiver is the oracle

riemann-go is graded by what the harness-owned sink receiver observed arrive: every ntfy publish and every InfluxDB write, recorded verbatim. It is never graded by what riemann-go claims about itself in an HTTP reply, a log line, or its own metrics.

**Why.** A monitor that grades itself can satisfy itself. Anti-tautology depends on the contract being external to the generated implementation, and the sink receiver is external because it is written once, by hand, against `SPEC.md`'s alert shape, and is not regenerated with the thing it grades.

**Acid test.** Read any scenario's expectation block. If it asserts on `query_response` and `rule_response` more often than on `ntfy_post` and `influx_write`, that scenario has drifted toward self-grading. The exception is the read surface, which the sink genuinely cannot see; those properties use harness-emitted events sparingly, and a corpus where they dominate has drifted rather than grown.

### I2. The matcher has no domain concepts

The matcher's vocabulary is a small set of generic operators over normalized event types. A function named after a monitoring behavior, `check_throttle`, `check_expiry`, `assert_changed_state`, violates this.

**Why.** Once the harness encodes stream semantics, the harness becomes the hidden spec, and the real contract now lives in Python that nobody reviews as carefully as `SPEC.md`. Every new combinator would then require harness code, and the generator would be writing to a specification it cannot read.

**Acid test.** Grep `harness/` for combinator and monitoring vocabulary: `throttle`, `changed`, `expire`, `coalesce`, `splitp`, `ddt`, `stable`, `shard`. Any hit in a function or class name is a violation. A new scenario should almost never need new harness Python; when it does, the addition must be a generic operator that later scenarios will reuse.

### I3. The trace is the contract surface

Every scenario assertion goes through `trace.jsonl`, which holds sink-receiver events and a deliberately small set of harness-emitted events. riemann-go's log lines, its `GET /metrics` text, and its HTTP bodies are debugging artifacts for a human reading a failure, not inputs to the matcher except where they arrive as a `query_response` the spec named.

**Why.** Without one contract surface, each new dimension of observation tempts a new matcher path, and the matcher stops being auditable. Centralizing on the trace is also what lets a diagnostician read evidence instead of parsing logs.

**Acid test.** The matcher opens `trace.jsonl` and nothing else. No scenario names a log file, a process's stdout, or a raw HTTP body that did not pass through the trace.

## Architecture invariants

### I4. No unbounded queue

Every queue in the process declares three things: a capacity in events, a policy drawn from `{block-with-deadline, shed-newest}`, and a dropped counter. A channel or a slice used as a queue without all three does not conform. This holds for the ingest path, every sink, every subscriber, and anything a future feature adds.

**Why.** An unbounded queue converts a load problem into a memory problem, and memory problems surface as a process death with no counter explaining it. Upstream Riemann has exactly this shape: the Netty executor's task queue is unbounded (`src/riemann/transport.clj:137-160`), so what protects the process from a burst is available RAM rather than an admission decision.

**Acid test.** Enumerate every channel and every slice-used-as-buffer in the generated tree. Each one appears in `SCALE.md`'s parameter table with a capacity, a policy and a counter, or it is a violation. A generation that satisfies this can also be asked for its counters at `GET /metrics` and answer for every queue it has.

### I5. Every drop is counted and observable from outside

An event discarded anywhere increments exactly one counter, and that counter is readable without attaching a debugger: in the `sinks` object of a `202` reply, at `GET /metrics`, and as a self-observation event in riemann-go's own index. Silent loss is the failure mode the design exists to remove.

**Why.** Upstream's time-window family drops out-of-window events silently (`src/riemann/streams.clj:358,412,425`), and `async-queue!` rejections are visible only in an instrumentation event ten seconds later. A monitor whose own losses are invisible reports confidently on a fleet while losing that fleet's events.

**Acid test.** Drive a scenario that saturates a sink. The sum of events observed at the sink receiver plus the sink's `dropped` counter must equal the events that rule routed there. If the two do not close, some path drops without counting.

### I6. No fallbacks on required configuration

A required flag that is absent is a fatal error naming the flag, before any port is bound. riemann-go never substitutes a nearby value: not the listen address for a sink URL, not a default topic, not an environment variable that happens to be set.

**Why.** A fallback turns a loud misconfiguration into a quiet wrong action, and a wrong ntfy topic looks exactly like a right one from inside the process. The failure surfaces later, somewhere else, with nothing tying it back.

**Acid test.** Start the binary with each required flag omitted in turn. Every run exits non-zero with the flag named, and no run binds a socket or writes to a sink. A run that starts and then fails on the first publish is a violation even though it eventually errored.

### I7. Expiry is an event, not a deletion

When an index entry's `time + ttl` passes, riemann-go produces an event and delivers it to the rule set. Removing the entry is a consequence of that event, not a substitute for it. No code path deletes an entry without the event having been dispatched.

**Why.** The whole value of a TTL index for this fleet is that silence becomes a signal: a listener that stops heartbeating, an agent session that ends without saying so. If expiry were a garbage-collection detail, the one thing the index knows that nothing else knows would be unobservable.

**Acid test.** A rule matching `state == "expired"` fires at a sink after ingest has stopped, with no further input. Grep the generated tree for every site that removes an index entry; each one is downstream of the dispatch, never in place of it.

### I8. The core carries no deployment or monitoring dependency

The packages that hold the event model, the expression compiler, the combinators, the index and the engine import the standard library plus the expression library, and nothing else. The ntfy client, the InfluxDB client, the HTTP transport, the Prometheus endpoint and the self-metrics sampler are adapters at the edge, each with its own dependencies, importing the core and never each other.

**Why.** A reusable core that reaches for a specific monitoring stack cannot be reused, and it cannot be tested without standing that stack up. This is the researchable-software guide's rule 7: instrumentation must not expand the core's authority.

**Acid test.** Walk the generated module's import graph. Any edge from a core package to a sink client, an HTTP framework, a telemetry SDK, or another adapter is a violation. The generation should carry this test itself, so a later edit cannot reintroduce the edge quietly.

### I9. Emission never blocks the emitter past the admission deadline

An emitter's HTTP request is answered within the admission deadline, always. Past the deadline the answer is `429`, never a stall, and never a `202` bought by waiting longer. The emitter's contract is fire-and-forget: it counts what it lost and moves on.

**Why.** The atlas vocabulary's third rule is that emission never affects the agent (`~/Developer/riemann-atlas/VOCABULARY.md`). An agent that blocks on its monitor has had its behavior changed by being observed, which is worse than losing the observation. That is what fixes the meaning of backpressure here: the server sheds by declared policy and reports what it shed, rather than pushing the cost back onto the caller.

**Acid test.** Under a scenario that saturates the loop, no `POST /events` takes materially longer than the deadline, and every reply is either `202` or `429`. A reply that arrives late with `202` is a violation even though the events were admitted.

## Loop discipline invariants

### I10. Every property in SPEC.md has at least one scenario

A property with no scenario is not enforced, and adding a property without adding its scenario is a contract change with no check. A generation can violate such a property and still reach `releases/validated/`.

**Why.** The arena exists because spec text drifts and generated code re-rolls. The corpus is the executable contract; `SPEC.md` describes the corpus rather than the other way around.

**Acid test.** Map each of `SPEC.md`'s seventeen numbered properties to at least one scenario. An unmapped property is written up or removed, and a new property lands in the same commit as the scenario that asserts it.

### I11. Dry run is deterministic and inert

A dry run is a pure function of the rule, the clock, the ordered events it replays, and the sink stubs. Running it twice over the same input returns the same result, and neither run produces a single `ntfy_post` or `influx_write` at the sink receiver.

**Why.** Dry run is the mechanism that lets a rule author, and soon an agent author, test a rule before it can page anyone. If it can page, it is not a dry run; if it is not reproducible, its output is not evidence about the rule.

**Acid test.** A scenario runs the same dry run twice and diffs the responses, then asserts the trace holds no sink event from either. A generation that reaches the live sink during a dry run fails on the trace, not on inspection.

### I12. Generated code is disposable

The implementation under `releases/candidates/<gen-id>/src/` is a build output. A fix lands in `SPEC.md`, in `SCALE.md`, or in a new scenario, never as an edit to generated Go.

**Why.** The spec-as-source thesis collapses the moment the artifact gets patched, because the next generation re-rolls the patch away and the spec no longer describes anything. When a bug exists in generated code, the question is which spec edit would have prevented it.

**Acid test.** `git log` over any path under `releases/` shows scaffolding commits and manifest updates, never a substantive edit to generated source. A hot fix made to test a hypothesis lives in a scratch copy that gets thrown away.

## What is intentionally not in INVARIANTS.md

- Behavioral requirements, which belong in `SPEC.md`.
- Capacities, deadlines and rates, which belong in `SCALE.md`.
- Validation mechanics, which belong in `HARNESS.md`.
- Style preferences such as naming conventions, which belong in the generator's brief if they matter at all.
- Anything that might reasonably change as the spec evolves. If a principle is true for v0 but might not be for v1, it is not an invariant.

## Audit checklist for a candidate generation

Before promoting `releases/candidates/<gen-id>/` to `releases/validated/`:

- [ ] I1: No scenario asserts on a harness-emitted event except for a read-surface property the sink cannot observe.
- [ ] I2: Grep `harness/` for combinator and monitoring vocabulary in function and class names. Zero hits.
- [ ] I3: The matcher reads `trace.jsonl` only, confirmed by code review.
- [ ] I4: Every channel and buffer in the generated tree maps to a capacity, a policy and a counter in `SCALE.md`.
- [ ] I5: The saturation scenario's sink arrivals plus dropped counter equal the events routed.
- [ ] I6: Each required flag omitted in turn produces a non-zero exit naming that flag, with no socket bound.
- [ ] I7: The expiry scenario fires a sink with no further ingest, and every entry-removal site is downstream of dispatch.
- [ ] I8: The import-graph walk finds no edge from a core package to an adapter or a client library.
- [ ] I9: Under the flood scenario every reply is `202` or `429` and none exceeds the deadline materially.
- [ ] I10: All seventeen `SPEC.md` properties map to at least one scenario.
- [ ] I11: The dry-run scenario's two runs are identical and the trace holds no sink event from either.
- [ ] I12: `git log -- releases/<tag>/src/` shows no substantive edits since the generation commit.
