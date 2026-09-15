# GENERATE.md — producing a riemannd from the spec

riemann-go's source of truth is `SPEC.md`, with `SCALE.md`, `INVARIANTS.md` and `HARNESS.md` beside it. The Go implementation is a build output, regenerated from those documents by `bin/generate`. This file is the reproducibility recipe: what gets pinned, where a generation lands, and what the design accepts as the cost of working this way.

## Why no Go source is checked in at the repo root

Checking in generated code forks the spec from the artifact. The next fix lands in the code because that is where the bug is visible, the spec drifts one edit at a time, and within a few weeks the canonical artifact is whatever is in `*.go` right now. The markdown becomes documentation about something that has moved on.

Treating the spec as the source and the binary as the build output keeps the spec canonical, and two things follow from it.

As models improve, riemannd improves without a spec edit: regenerate against a newer model, run the harness, and if the corpus is still green, that candidate is the one to promote. The bug-fix loop becomes "fix the spec" instead of "fix the code and remember to update the spec."

Forks happen at the spec level. Someone who wants a different riemannd — a different shard policy, a different sink set, a different read surface — edits `SPEC.md` and regenerates, and the diff that describes their fork is a prose diff rather than a code diff. Prose diffs are smaller and a human can actually review one.

The bet has a precondition, and it is the harness: validation has to be strong enough that any two passing generations are behaviorally equivalent where the spec speaks, and loose enough that each generation can choose its own shard mechanism, index structure and package layout where the spec is silent. The oracle is the harness's sink receiver and its read probes, not riemannd's own account of itself, which is what makes the check non-tautological.

The generated source is not lost. It is checked in inside the release directory, under `releases/candidates/<gen-id>/src/`. The repo root is spec-only; the releases tree carries the artifact, so anyone debugging a running binary has the exact tree it was built from.

## The three pins

A generation is reproducible if and only if three things are recorded:

| Pin | How it is captured |
| --- | --- |
| Spec | `git rev-parse HEAD:SPEC.md` at generation time |
| Model | the exact model id the generator ran as |
| Prompt | the SHA-256 of the literal prompt text after substitution |

`bin/generate` prints the spec SHA and the prompt SHA on every run. Two generations against the same three pins are behaviorally equivalent, not byte-identical; generation is non-deterministic at the token level and the design does not pretend otherwise.

## Release directory layout

```
releases/candidates/<gen-id>/
  manifest.json     the three pins, harness run ids, per-scenario verdicts, timestamp
  src/              the generated Go module, exactly as generated
  riemannd          the built binary
  validation.log    the harness output that gated this directory
```

`<gen-id>` is the `gen-<timestamp>` name `bin/generate` chose, so a candidate traces back to its scratch directory and its generation log.

`manifest.json` carries at least:

```json
{"gen_id": "gen-20260915-143022",
 "spec_sha": "<git sha of SPEC.md>",
 "model": "<model id>",
 "prompt_sha": "<sha256 of the substituted prompt>",
 "scenarios": {"expiry-event": "GREEN", "shed-newest": "GREEN"},
 "spec_gaps": 4,
 "created": "2026-09-15T14:30:22Z"}
```

Releases are append-only. A red generation never lands in `releases/`; it stays in `scratch/` with its report and its trace, and `scratch/` is gitignored.

The three trust levels are directories, and the promotion rules are in `CLAUDE.md`. In short: `candidates/` is built and trialed, `validated/` is green across the whole corpus, and only a human moves anything into `blessed/`.

## Harvesting SPEC-GAP comments

The generation prompt requires a `// SPEC-GAP:` comment at every decision the spec left open. Those comments are the highest-value output of a generation after the binary, because each one is a place where the next regeneration will roll differently and something may already depend on the current roll.

```
grep -rn "SPEC-GAP:" releases/candidates/<gen-id>/src/
```

`bin/generate` prints the same list at the end of a run. Work the list this way: a gap that appears in one generation is a note; a gap that appears in the same place across three generations is a real ambiguity, and it is the highest-priority spec edit available. Resolving it in `SPEC.md` removes it from every future generation at once.

## Failure modes this design accepts

**Generation is not free.** Each cycle costs model time and human review. Spec-as-source is worth it only because the spec is small and bounded. A system with a million lines would not survive the discipline.

**A model that goes away takes the regeneration path with it.** A release pinned to a model that is later retired still has a working binary and a checked-in source tree, but it can no longer be re-rolled. That is the reason `releases/` is append-only rather than a cache that gets cleaned.

**Unpinned behavior is ephemeral.** Anything the spec does not fix, the next generation re-rolls. This is Hyrum's Law with the clock sped up: a user who comes to depend on an unspecified response field will lose it, and the loss will look like a regression. The triage that handles this is in `CLAUDE.md`.

**The harness can be wrong.** An `observer_error` means the oracle misread, and the generation it was grading is unjudged. The loop stops rather than charging a harness defect to the generator.
