# DESIGN.md — the riemann-go arena

This document describes how riemann-go is built, validated and iterated. It does not describe what riemann-go does; that is `SPEC.md`. It is written for review.

## Problem

We want a single-node stream-processing monitor for a home cluster whose emitters are mostly agents. It needs the Riemann event model, an index keyed by identity where expiry becomes an event, per-key state transitions, rules submitted as data rather than compiled from a configuration language, and backpressure treated as a design requirement instead of an afterthought.

Rather than writing it by hand, we generate it from a specification using a coding agent, and treat the spec as the source of truth. The implementation is a build output, regenerated as the spec evolves and as models improve.

Three questions the design has to answer:

1. **How do we keep the spec honest**, so it does not become documentation about an artifact that moved on.
2. **How do we validate a generation** without the validation suite over-specifying the implementation and removing every design choice from the generator.
3. **How do we iterate**, so that a failing generation produces feedback that gets the next one closer rather than feedback that makes it worse.

## Framing: the sink surface as arena

riemann-go's job is to cause things to happen elsewhere: an ntfy topic receives an alert, an InfluxDB bucket receives a point. Those effects are where the arena lives. We stand up a harness-owned HTTP server that speaks the ntfy publish API and the InfluxDB v2 write API, record every request it receives verbatim, and grade a generation on what arrived.

The closest published analog is WebArena (Zhou et al., ICLR 2024, arXiv:2307.13854), which reframed self-hosted web applications as an agent environment whose state is the oracle for agent behavior. The move here is the same structurally, applied to a monitoring system rather than a web stack: fix the environment, regenerate the inhabitant, and read the grade off the environment.

Without an external arena, every generated system grades itself, and the grade means nothing.

## Architecture in one sentence

**The sink receiver is the oracle. A generation of riemann-go is only a hypothesis. The generator is `claude -p`. The build is iterative.**

## Why this oracle is honest

The sink receiver is not co-generated with riemann-go. It is written once, by hand, against `SPEC.md`'s `## Alert shape (normative)` section, and it changes only when that section changes. A generation cannot author its own contract, cannot pass by reporting success, and cannot pass by writing a convincing log line. It passes when an ntfy publish with the right provenance in the right place actually arrived.

The alert shape is what makes this work, which is why that section is normative and specific down to the ordering of keys in a JSON object. Provenance rides inside the ntfy `message` body because ntfy discards top-level keys it does not recognize: a publish carrying an extra `riemann` object returned `200` and came back without it (observation, one `POST` to `http://mercy:2586/riemann-arena-test`, 2026-09-15). A spec that left placement to the generator would have produced a different extraction path per generation, and the harness would have had to learn each one. Pinning it once means the oracle never has to guess.

The same reasoning binds the InfluxDB line protocol shape. The measurement, the tag set and the field set are pinned, so a write either parses into the expected point or it does not.

## The tension, stated plainly

Two things the oracle cannot do.

**It cannot see the read surface.** The index, the expression query, the SSE subscription and the rule lifecycle produce no sink traffic. A scenario asserting that `GET /index/{host}/{service}` returns the last event has to look at riemann-go's own reply, which is exactly the self-grading that `INVARIANTS.md` I1 exists to prevent. The compromise is the one cue reaches for with `cue_query_response`: a small, named set of harness-emitted trace events, `ingest_response`, `query_response` and `rule_response`, used only where the sink genuinely cannot observe the property. A scenario corpus that leans on them has drifted, and the discipline is to notice. The acid test in I1 names the ratio to watch.

There is a partial escape that the spec uses where it can. Expiry, which is an index property, is observable at the sink because a rule matching `state == "expired"` fires there with no further ingest. Where a read-surface property can be expressed as something that causes a sink effect, the scenario should be written that way, and the harness-emitted event should be the second choice rather than the first.

**It cannot see the runtime shape.** A generation that passes every scenario with one global mutex around a shared map is a valid riemann-go. So is one that spawns a goroutine per identity. The probes measured a shard-loop runtime at 3.4 million events per second, and `SPEC.md` names that shape as implementation guidance, but nothing in the corpus checks for it and nothing should. What keeps the choice honest is `SCALE.md`: a runtime that cannot hold the flood floor with exact accounting and bounded memory fails a scenario, whatever its internal shape. Behavior is the contract; the shape is the generator's problem.

## The artifacts

1. **`SPEC.md`** — the behavioral contract. What riemann-go must cause to happen, plus the wire surfaces marked normative because downstream consumers break when they drift. It does not specify the internal state model, the package layout, the concurrency model, or how rules are stored.

2. **`INVARIANTS.md`** — principles that hold across every generation regardless of features, each with an acid test, plus the checklist that gates promotion.

3. **`SCALE.md`** — backpressure and scale floors written as behavior, with every parameter named at its owning layer and every default carrying its reason.

4. **`scenarios/*.yaml`** — declarative tests. Each names a timeline of stimuli and a set of expectations over the trace. Expectations assert on what the sink receiver observed, and on harness-emitted events only where the sink is blind.

5. **`harness/`** — drives riemann-go, runs the sink receiver, evaluates expectations. Three responsibilities kept apart: stimulus, which adapts per generation and drives the HTTP surface; observation, which is the sink receiver and knows nothing about riemann-go's internals; and matching, which applies a scenario's expectations to the trace with generic operators only.

6. **`releases/`** — append-only generated artifacts at three trust levels, each carrying the generated source tree, the built binary, the trace, and a manifest pinning the spec hash, the model id, the prompt hash and the validation run.

## The two design moves

### Move 1: spec-as-source

No Go source is checked in at the repo root. The implementation is regenerated from the specification documents by `claude -p`.

The spec stays canonical as a result. A bug fix is a spec edit, a fork is a spec diff, and regenerating against a better model produces a better implementation with no spec change at all.

The precondition is that validation must be strong enough that two passing generations behave the same way where the spec says they must, and loose enough that each can choose what the spec left open. That precondition is why `SPEC.md` is kept small. A million-line system would not survive this discipline.

### Move 2: sink-as-oracle

Validation asserts on what arrived at the ntfy and InfluxDB endpoints, not on what riemann-go returned to the harness.

This frees the generator on everything the spec does not pin, and it makes anti-tautology real rather than aspirational. The couplings it requires are small and are all in `SPEC.md`: the alert must carry provenance in the pinned place, and the InfluxDB write must be the pinned line shape. Those two are the minimum needed for the oracle to attribute an arrival back to a rule and an event.

## The build pipeline

```
SPEC.md ─────┐
INVARIANTS.md├──► bin/prompt.md ──► claude -p ──► generated Go module ──► go build ──► riemannd
SCALE.md ────┤                                                                          │
knowledge/ ──┘                                                                          │
                                                                                        ▼
                            scenarios/*.yaml ──► harness ──► drive riemannd
                                                      │            │
                                                      │            ▼
                                                      │      sink receiver records
                                                      │      ntfy + influx requests
                                                      ▼            │
                                                 trace.jsonl ◄─────┘
                                                      │
                                                      ▼
                                              pass/fail per scenario
```

`bin/generate` creates a target directory, substitutes the spec paths into the prompt template, and runs `claude -p` with tools restricted to reading, writing and building. The generator's brief includes a `CLAUDE.md` placed in the target directory, describing the evaluation regime: what gets compiled, what scenarios run, what the sink receiver records. The generator writes for the test it knows is coming.

## The self-improvement loop

```
loop until score == 1.0 or iteration == N:
  generate  riemann-go  via claude -p (generator role)
  build                 via go build
  validate              via the harness running the scenario corpus
  score                 per the function below
  if score < 1.0:
    diagnose  failure   via claude -p (diagnostician role, separate context)
    act on the diagnosis by editing a spec document or adding a scenario
```

Two roles, deliberately split into separate `claude -p` invocations with separate contexts.

- **Generator** reads the spec documents and `knowledge/INDEX.md`. Writes Go. Knows nothing about prior runs except what the spec says, because there is no feedback side channel: a fix carried outside the spec regresses at the next regeneration, and a fix pinned in the spec does not.
- **Diagnostician** reads the spec, the failing scenario, the trace and the matcher report. Does not read the generated source on its first pass. It writes a behavioral diagnosis naming which expectation failed, what the trace says happened instead, and what kind of implementation mistake produces that pattern. Only after that diagnosis exists may a second pass read the source and write repair hints, which are hints and not corrections.

The staging keeps the epistemic boundary clean: behavioral reasoning happens against the oracle, and code reasoning is a downstream aid. The outer loop is bash. Determinism in the orchestration, reasoning only in the model roles.

## Scoring

The matcher's verdict is final. A run is GREEN when every expectation in every scenario holds.

```
scenario_score = scenarios_passed / scenarios_total
overall        = 1.0 if scenario_score == 1.0 and observer_ok else 0.0
```

Pipeline-stage attribution exists to classify a failure, not to contribute partial credit. A scenario that legitimately expects no sink traffic, say one asserting that a disabled rule never fires, reaches GREEN with zero arrivals at the sink receiver.

Failure categories the loop distinguishes:

- **observer_error** — the sink receiver or the matcher itself failed, so the run says nothing about the generation. Highest priority; it short-circuits everything else and is a harness bug until proven otherwise.
- **GREEN** — every expectation held.
- **compile_error** — `go build` failed.
- **start_error** — built but never reached `GET /healthz`, or crashed during the run.
- **ingest_error** — started and healthy, but `POST /events` never returned `202` for a well-formed batch.
- **sink_silent_error** — ingest succeeded and no request ever reached the sink receiver, while the scenario expected at least one. Categorized only after the matcher has already failed.
- **expectation_violation** — the pipeline worked and the wrong thing happened. This is the interesting category.

The distinction that matters most is `observer_error` against everything else: a harness bug must never be charged to a generation.

The loop terminates one of three ways. GREEN promotes the generation to `releases/validated/`. NEEDS-HUMAN stops after N iterations, default three, because a persistent failure is more likely a spec gap than a generation problem. DEGRADED stops early when the score falls across iterations, which is a signal that the diagnostician's feedback is making things worse.

## Trust levels

A score of 1.0 means a generation passed the current corpus. It does not mean the generated code is legible, or that this is the riemann-go anyone wants to run.

- **candidate** — generated and built, possibly not yet validated. Lives under `releases/candidates/<run-id>/`.
- **validated** — passed the full corpus against the sink receiver with `overall == 1.0`. Lives under `releases/validated/<tag>/`.
- **blessed** — a human reviewed the validated artifact for legibility, risk and product relevance. Lives under `releases/blessed/<tag>/`.

The loop may promote candidate to validated on its own. It may never promote validated to blessed. Anything pointed at the fleet's real ntfy topic runs from `blessed/`; experiments may run from `validated/`.

Scenario coverage is not product completeness. Blessing is where a human owns the question of whether this is the monitor we actually want.

## Failure promotion

Every discovered failure becomes a scenario, and every scenario becomes durable pressure that each later generation must survive.

When a generation fails for a reason the corpus did not directly assert, the diagnosis writes a new scenario isolating the property whose violation produced the failure. That scenario lands under `scenarios/` and joins the corpus. The failing trace is archived under `failures/promoted/<scenario-name>.md` with the evidence, the spec gap it closed if there was one, and the generation that first surfaced it. There is no quarantine: the corpus is the contract.

This converts debugging into curriculum construction. A failure caught once and not encoded is a failure the next generation rediscovers.

Some failures are spec gaps rather than implementation bugs. Closing a gap in `SPEC.md` is necessary but not sufficient, because spec text drifts and edits get lost across a rewrite. The scenario is what guarantees the lesson survives.

## The human role

Humans do not review generated Go line by line. The loop is autonomous over implementations.

Humans own the arena: what riemann-go must do, which behaviors are enforced, how observation and matching work, when a validated artifact becomes blessed, and whether a given alerting behavior is the one the fleet actually wants. Generated implementations are disposable unless the arena promotes them. Nobody argues with the loop about whether a particular generation is good; the argument is about whether the arena is right.

That separation is what lets this hold its shape. The arena evolves slowly under human judgment while implementations turn over freely under model improvement.

## What the design accepts as costs

1. **Generation is not free.** Each iteration costs model time, which is affordable only because the spec is small and bounded.

2. **Debugging a production incident needs the artifact.** Go source is checked in inside the release directory, just not at the repo root.

3. **Model drift over years.** A release pinned to a model that becomes unavailable can no longer be regenerated. The binary still runs, which is why `releases/` is append-only.

4. **The read surface is graded weakly.** Stated above rather than hidden. The index, the query path and the rule lifecycle lean on harness-emitted events, and the corpus can drift toward self-grading without anyone noticing unless the I1 acid test gets applied.

5. **The runtime shape is not graded at all.** Also stated above. `SCALE.md` is the mitigation, and it is a floor rather than a proof.

6. **Sink-receiver fidelity can masquerade as a riemann-go bug.** The receiver stands in for ntfy and InfluxDB, and it is honest only as long as it accepts what the real servers accept and rejects what they reject. When a scenario fails after a receiver change, the failure is triaged against the real API before it is charged to the generation.

## What is intentionally not in this design

- **A human gate on validation.** The loop promotes candidate to validated on its own. Humans gate blessing instead, which keeps autonomy where it is cheap and judgment where it is not.
- **Cross-generation regression testing.** Each generation is judged against the current corpus. Removing a scenario is a contract change, and that is the only way a behavior stops being enforced.
- **Partial credit.** A scenario passes or it does not. A soft score would let a generation be mostly right about whether an alert fired.
- **Multi-language targets.** Go is a hard constraint for operational reasons, not a parameter. A Rust riemann-go is a spec fork.
- **Grading riemann-go against the Clojure original.** Upstream's `test/riemann/streams_test.clj` is the semantic reference for the combinators and it is pointed at from `knowledge/INDEX.md`, but it is prior art for the generator to read rather than an oracle the harness runs. Two servers being diffed against each other is a migration check, not an arena.

## Open design questions

1. **Settle window per scenario.** After the last stimulus, how long does the harness wait before reading the trace? A fixed default is simplest; a per-scenario override is cleaner if a timer-heavy scenario needs one.
2. **How the harness drives virtual time.** `throttle` and `stable` are time-domain combinators, and a scenario that waits five real minutes for a throttle window is not a scenario anyone will run. Whether the harness manipulates event timestamps, or riemann-go exposes a test clock, is open, and the second option adds surface the spec does not currently have.
3. **Trace schema versioning.** The trace is a contract artifact. Additive-only is the obvious answer; it is worth confirming once the vocabulary has grown past its first version.
4. **Whether the diagnostician's output is structured or free-form.** Free-form risks being unactionable, and rigid structure risks missing the thing that mattered. Lean structured with a free-form field.
5. **Iteration exhaustion.** When N iterations end without GREEN, does the next attempt start clean or accumulate prior feedback? Clean restart with a NEEDS-HUMAN flag is the current answer, on the grounds that accumulated feedback outside the spec is the side channel the design rejects.
