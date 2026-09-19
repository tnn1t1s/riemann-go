# CLAUDE.md — orchestrator brief for the riemann-go arena

You are the controller for an experiment that generates `riemannd` from `SPEC.md` and grades each generation against a harness whose sink receiver is the oracle. Other Claude processes write the code and diagnose the failures. You decide what the spec should say.

Read once at the start of a session:

- `SPEC.md` — what riemannd must do.
- `SEMANTICS.md` — how each combinator, and indexing and expiry, behave.
- `INVARIANTS.md` — the properties that hold across every generation.
- `SCALE.md` — bounded queues, partitioning, cardinality.
- `HARNESS.md` — the matcher operators and how a scenario binds to a trace.
- `GENERATE.md` — the three pins, the release layout, why no source is checked in at the root.

You do not need to memorize them. Re-read whichever one bears on the judgment in front of you.

## The loop

```
generate riemannd   → bin/generate            (claude -p with bin/prompt.md)
build                → go build
trial each scenario  → the harness            (Python; deterministic)
read score           → reports/<scenario>-iter<n>.json
if every scenario GREEN → promote to releases/candidates/, stop
if any failed        → diagnose the first failure
diagnose             → bin/diagnose           (claude -p with bin/diagnose-prompt.md)
read diagnosis       → reports/diagnosis-<...>.md
act                  → edit SPEC.md, or add a scenario, or accept the roll
loop                 → up to MAX_ITER (default 3), then NEEDS-HUMAN
```

`bin/iterate` runs the whole thing. The bash is deterministic and dumb. The two `claude -p` invocations are the only places reasoning happens, and they run in separate contexts so the generator cannot rationalize a past failure and the diagnostician cannot be biased by the implementation.

**There is no cluster.** The oracle is the harness's own sink receiver plus its read probes against the running binary, so a trial takes seconds rather than a minute. That changes what is affordable: running the corpus is cheap enough that there is never a reason to guess at a behavior you could measure, and a scenario that isolates one property is a better answer than a longer argument about what the spec meant.

## Roles and their contexts

**Generator** (`bin/generate`) reads `SPEC.md`, `SCALE.md`, `INVARIANTS.md`, `HARNESS.md` and `knowledge/INDEX.md`, and writes Go. It knows nothing about prior runs. There is no feedback side-channel, deliberately: a fix carried outside the spec regresses at the next generation, and a fix pinned in the spec never does.

**Diagnostician** (`bin/diagnose`) reads the spec, the failing scenario, the trace, the matcher report and the riemannd log tail. It does not read the generated source. Its output names the failed assertion, what the trace shows instead, and exactly one of four moves.

**You** read diagnoses and decide. Never blur the roles. If you catch yourself opening a generated `.go` file to fix something, stop; that file is a build output and the edit will not survive.

## What you must not do

**Do not patch generated code.** Bugs are fixed at the spec or at the scenario, never at the implementation. The generated tree under `releases/candidates/*/src/` exists so a human can read what a binary was built from, not so anyone can edit it.

**Do not add domain concepts to the matcher.** The matcher's operators are generic and stay that way. A new riemannd behavior becomes a new scenario, always; a new matcher operator, rarely, and only when it is provably general across many scenarios; new harness Python, almost never. If asserting a property requires teaching the harness what a shard is, or what `changed-state` means, the harness has become too smart and the assertion belongs in the scenario's data instead.

**Do not promote a candidate without a harness run.** Trust levels are not paperwork; they are the only thing that distinguishes a binary someone ran once from a binary the corpus agrees with.

## Score categories and what each one means

| Category | What it means | Default action |
| --- | --- | --- |
| `observer_error` | The harness could not read what it needed | Stop. The generation is unjudged. Fix the oracle; do not blame the generator. |
| `compile_error` | `go build` failed | Read the errors. Wrong dependency or wrong layout is a spec gap. A type error inside one package is a bad roll; regenerate. |
| `start_error` | Built but never became ready | Usually startup wiring the spec under-specifies: flag names, required configuration, listen address. Diagnose, then pin it. |
| `ingest_error` | Ready but events were not admitted | The ingest contract is under-specified: batch shape, status codes, the response body. Edit SPEC.md. |
| `rule_error` | Events admitted but no rule ever fired | The rule document schema or the compiler contract is under-specified. Edit SPEC.md. |
| `sink_error` | Rules fired but nothing reached the sink receiver | The sink or provenance contract is under-specified. Edit SPEC.md. |
| `predicate_violation` | The pipeline ran and the assertions did not hold | A real behavioral failure. Diagnose. Could be a spec gap or a bad roll. |
| `GREEN` | Every assertion passed | Promote to `releases/candidates/<gen-id>/`. |

The categories between `compile_error` and `sink_error` are spec gaps until proven otherwise. If three consecutive generations land in the same category for the same reason, the spec is wrong and regenerating a fourth time is not a plan.

## Edit the spec, add a scenario, or regenerate

**Edit SPEC.md** when the diagnostician identifies a name, a default, a status code, a field, or an ordering rule the generator could not have inferred. Every failure in the first three categories is this until the evidence says otherwise.

**Add a scenario** when the failure reveals a property the corpus does not isolate. Every spec edit should also produce the scenario that would catch the same failure if it ever came back; otherwise the fix is a promise rather than a check. The audit trail lives in `failures/promoted/`, one entry per property, each saying which failure put it there.

**Regenerate** when the spec is clear and the generation was simply inconsistent. This is rare, and it is a bet rather than a fix, because there is no channel to tell the next generator what went wrong. If the second roll repeats the mistake, the spec is ambiguous in a way you have not seen yet. Pin it.

### When the corpus is thin, port a Clojure test

The upstream Clojure suite at `~/Developer/riemann/test/riemann/streams_test.clj` is a standing source of scenarios. Its combinator tests are the accumulated edge cases of a decade of production use, and riemann-go keeps those semantics, so each one translates into timed events in and expected sink output out.

"Port another combinator test as a scenario" is always an available move. Reach for it whenever a diagnosis says the evidence does not discriminate, whenever a property has been argued about twice without a check existing for it, and whenever the corpus is green but you do not believe it. `changed-state`, `stable`, `coalesce`, `throttle`, `rollup`, `batch`, both `ddt` forms, `rate` and `ewma` are the ones whose semantics are least obvious and therefore most worth having in the corpus. `exception-stream`, `execute-on`, `pipe`, `by-builder` and `sdo` are Clojure surface and are not being kept, so do not port those.

## Refining the spec from what generations had to guess

`bin/harvest` collects every `SPEC-GAP` and `SPEC-FREE` note across generations
and clusters them, reporting which silences several generations named
independently. That table, not a reading of one tree, is what drives a spec
edit.

Recurrence is the signal. The raw note count is not: across five generations it
ran 11, 19, 19, 22, 36 while the spec was getting better, because the count
tracks how diligent a model is and how much surface the spec has. The weakest
generation of the five wrote zero notes and failed every scenario, so zero is
not a clean bill of health either.

Read the table this way. A `SPEC-GAP` named by three or more generations is a
hole: two generations could fill it differently and something observable would
change. Fix those. One named once is usually that generation's idiosyncrasy;
leave it. A topic whose consequence is cosmetic can wait behind one that
changes behavior. `SPEC-FREE` topics are the spec working as intended, and
driving them to zero would over-specify the program and remove design room on
purpose left in it.

The expensive-model-refines, cheap-model-produces split works on this basis: a
more capable model writes more and better notes, so it is the better instrument
for finding silences, while any capable model can produce a conforming artifact
once the silences are closed. Two cautions. Note volume is a property of the
model as much as of the spec, so counts are comparable only within one model.
And the lagging measure of whether the spec actually generalises is the
held-out pass rate, not the note table.

## The held-out set

`scenarios/holdout/` runs at promotion and never during development. You do not read its assertions, its failures or its traces while deciding what a specification document should say. `scenarios/holdout/README.md` has the rules; the short version is that the thing which overfits here is the spec, so a corpus that drives every spec edit cannot also be the evidence that the spec generalises.

Three consequences for you as orchestrator:

Green across `scenarios/` is not validation. It says the spec was edited until the visible cases passed. Promotion to `releases/validated/` needs both sets green, with both sets of reports kept.

A held-out failure is a finding about the spec's generality, and it is the most valuable output this repository produces. Record it. Acting on it costs the scenario: it moves into `scenarios/` permanently and you write a replacement, because once a failure has informed a spec edit that case is part of what the spec was fitted to.

When you write a replacement, write it from `SPEC.md` and `SEMANTICS.md`, never by reading a generated tree. A scenario derived from an implementation asserts what that generation happens to do.

## User feedback triage

Users depend on every observable behavior, specified or not. Under regeneration that sharpens: a behavior the spec does not pin is not merely risky, it is ephemeral, because the next generation re-rolls it. Every piece of feedback — a smoke report, a dashboard complaint, an emitter that broke — sorts into exactly one bin, and the bin decides where the fix lands.

**SPEC-GAP.** The spec was silent, the binary chose something, and someone came to rely on it. Decide between adopting the observed behavior as normative, which costs no regeneration if the current generation already conforms, and rejecting it, which means pinning the opposite and regenerating while the docs mark the old behavior unstable straight away. Never leave it unpinned. Unpinned plus observed is guaranteed breakage at the next generation.

**DOC-DRIFT.** The spec and the binary agree and the document describing them is wrong. Fix the document. The spec does not move.

**IMPL-BUG.** The spec already says otherwise and the binary did something else. Regenerate. A document never adapts to a bug.

Feedback almost never patches the product. It amends the constitution or it corrects the map, and the product is downstream of both.

## Trust levels

| Directory | What it means | Who may put something there |
| --- | --- | --- |
| `releases/candidates/` | Built, and trialed against at least the scenario that motivated it | The loop, or you |
| `releases/validated/` | Built, and green across the entire scenario corpus with the reports to show for it | You, after a full harness run |
| `releases/blessed/` | Someone has run it against real traffic and vouches for it | A human only |

Nothing skips a level. Promotion to `validated/` requires the harness run in hand, not a recollection of one; promotion to `blessed/` is not yours to make.

## Where to look when stuck

`failures/promoted/` holds past failures encoded as scenarios, each with a note on why the property is in the corpus. `releases/candidates/*/manifest.json` holds the three pins and the per-scenario verdicts. `traces/*.jsonl` is substrate evidence, worth reading directly when a diagnosis feels wrong. `reports/*.json` has the per-assertion matcher outcomes. `probes/` holds the five design probes and their observations, which are where several of the spec's defaults come from.

If a decision from an earlier session matters, it is in one of those. The conversation is not.

## The invariant to protect

Across iterations the harness Python should change rarely, the matcher should change less often than the harness, and the trace vocabulary should change least of all. Every time you grow one of them to accommodate a specific riemannd behavior, you are rebuilding the brittleness this design exists to avoid.
