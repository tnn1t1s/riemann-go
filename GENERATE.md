# GENERATE.md: producing a riemannd from the spec

riemann-go's source of truth is `SPEC.md`, with `SEMANTICS.md`, `SCALE.md` and `INVARIANTS.md` beside it. The Go implementation is a build output, regenerated from those documents by a coding agent the build invokes. This file is the reproducibility recipe: what gets pinned, where a generation lands, and what the design accepts as the cost of working this way.

## Why no Go source is checked in at the repo root

Checking in generated code forks the spec from the artifact. The next fix lands in the code because that is where the bug is visible, the spec drifts one edit at a time, and within a few weeks the canonical artifact is whatever is in `*.go` right now. The markdown becomes documentation about something that has moved on.

Treating the spec as the source and the binary as the build output keeps the spec canonical, and two things follow from it.

As models improve, riemannd improves without a spec edit: regenerate against a newer model, run the harness, and if the corpus is still green, that candidate is the one to promote. The bug-fix loop becomes "fix the spec" instead of "fix the code and remember to update the spec."

Forks happen at the spec level. Someone who wants a different riemannd, a different shard policy, a different sink set, a different read surface, edits `SPEC.md` and regenerates, and the diff that describes their fork is a prose diff rather than a code diff.

The bet has a precondition, and it is the harness: validation has to be strong enough that any two passing generations are behaviorally equivalent where the spec speaks, and loose enough that each generation can choose its own shard mechanism, index structure and package layout where the spec is silent. The oracle is the harness's sink receiver and its read probes, not riemannd's own account of itself, which is what makes the check non-tautological.

The generated source is not lost. A release carries it under `src/`, so anyone debugging a running binary has the exact tree it was built from.

## Configure the agent once

`agent-command.json` at the repository root is a JSON argv array. Each `{model}` placeholder is replaced with the configured model; at least one is required so the selected model is actually passed to the command. No shell interpolation is performed.

```json
["claude", "-p", "--model", "{model}", "--permission-mode", "acceptEdits",
 "--allowedTools", "Read,Write,Edit,Bash", "--max-turns", "60"]
```

The process receives the prompt on stdin, runs with the prepared input directory as its working directory, and must not return until `output/service`, the three cross-compiled binaries and `output/build-manifest.json` exist. A remote job submission alone is not completion. Authentication belongs to the installed runner; this repository assumes no provider and no credential. A different agent is a different command file, passed to CMake as `RG_AGENT_COMMAND_FILE`.

## Build and test

Requires CMake 3.24 or later, Make or Ninja, Python 3.11 or later with `uv sync` run, Go 1.25 or later on the agent's PATH, and the agent command above.

```sh
uv sync
cmake -S . -B build -DRG_MODEL=claude-fable-5-1 \
  -DRG_AGENT_COMMAND_FILE="$PWD/agent-command.json" \
  -DPython3_EXECUTABLE="$PWD/.venv/bin/python"
cmake --build build
ctest --test-dir build --output-on-failure
```

The custom command's dependencies are `SPEC.md`, `SEMANTICS.md`, `SCALE.md`, `INVARIANTS.md`, `knowledge/INDEX.md`, `bin/prompt.md`, `bin/generate`, the command file and the configured model and generation id. It invokes `bin/generate`, which calls `riemann-generate` from the shared package with riemann-go's input list, prompt and manifest contract. That script prepares a fresh attempt, runs the agent, checks that no input byte changed, verifies the manifest's digests against the artifact and the SPEC.md bytes, and publishes `build/service`, `build/build-manifest.json` and `build/SPEC.md`. The agent performs the Go compilation using the build instructions it writes. CMake owns dependency scheduling; nothing here is a second build system.

An unchanged build does not call the model. A changed input triggers a new attempt. To deliberately sample another generation from the same inputs:

```sh
cmake -S . -B build -DRG_GENERATION_ID=experiment-002
cmake --build build
```

Attempt evidence stays under `build/generations/gen-*/`: the copied inputs with `manifest.json` recording their digests, `invocation.json` with the exact command, the model, the exit status and the binding, `agent.log`, and `input/output/` with the source tree, `BUILD.md` and all four binaries. A failed attempt removes the active outputs so CTest fails rather than testing stale output, and keeps its evidence. CTest never invokes a model.

`ctest` runs the development corpus through `bin/test-build`, which is `bin/trial` with the build's own `service`, `SPEC.md` and `build-manifest.json`. A successful build is not a passing conformance result; the two are separate claims and `report.json` carries the second.

`cmake --build build --target check-harness` runs `bin/check`.

## The pins

A generation is reproducible if and only if its inputs are recorded. `build/generations/<attempt>/input/manifest.json` carries:

| Pin | How it is captured |
| --- | --- |
| Inputs | `inputs_sha256`, the SHA-256 of every file the generator read, including SPEC.md |
| Prompt | `prompt_sha256`, the SHA-256 of `bin/prompt.md` as handed to the agent |
| Model | `model`, the exact identifier passed through `{model}` |
| Repository | `repository_commit`, HEAD when the attempt was prepared |

`build-manifest.json`, written by the agent and verified by the runner, binds the outputs to one of those inputs:

```json
{"contract": "riemann-go/spec-v1",
 "spec_sha256": "<sha256 of SPEC.md>",
 "artifact_sha256": "<sha256 of output/service>",
 "artifacts": {"riemannd-linux-arm64": "<sha256>",
               "riemannd-linux-amd64": "<sha256>",
               "riemannd-darwin-arm64": "<sha256>"}}
```

The trial refuses an artifact whose digest is not listed, or an expected SPEC.md whose digest differs, before riemannd starts. Two generations against the same pins are behaviorally equivalent where the corpus speaks, not byte-identical; generation is non-deterministic at the token level and the design does not pretend otherwise. Changing the runner installation without changing its command file requires a new `RG_GENERATION_ID`.

## Prepare inputs without running an agent

```sh
uv run riemann-prepare-generation --model claude-fable-5-1 --target scratch/gen-001 \
  --root . --prompt bin/prompt.md --contract riemann-go/spec-v1 \
  --input SPEC.md --input SEMANTICS.md --input SCALE.md --input INVARIANTS.md --input knowledge/INDEX.md
```

This copies the inputs, writes `PROMPT.md` and `manifest.json`, and invokes no model. Point an agent at that directory by hand when the CMake path is not wanted. Nothing from `features/`, `harness/`, `tests/` or `releases/` is copied; copying is not an access boundary, so restrict the runner's readable workspace when isolation matters.

## Release directory layout

```
releases/<trust>/<gen-id>/
  manifest.json          the pins above, the release tag, trust level, per-corpus counts
  build-manifest.json    the artifact binding, verbatim from the generation
  src/                   the generated Go module, exactly as generated
  reports/               the trial case directories that gated this directory
```

The binaries are release assets on GitHub, named by generation, one per target; the release carries both manifests beside them. `<gen-id>` is the generation name chosen at promotion, so a release traces back to its attempt directory and its agent log. Releases are append-only. A red generation never lands in `releases/`; it stays in `build/generations/` with its report.

The three trust levels are directories, and the promotion rules are in `CLAUDE.md`. In short: `candidates/` is built and trialed, `validated/` is green across the development corpus and the held-out set with both sets of reports kept, and only a human moves anything into `blessed/`.

## The loop

`bin/iterate --model <id>` runs generate, trial and diagnose up to three times, stopping on GREEN, on `observer_error`, or when fewer scenarios pass than the iteration before (DEGRADED). `bin/diagnose <case-dir>` runs the diagnostician over one failed case in its own context with read-only access. `bin/harvest` collects the `SPEC-GAP` and `SPEC-FREE` notes across attempts and clusters them; DESIGN.md says how to read that table.

## Failure modes this design accepts

**Generation is not free.** Each cycle costs model time and human review. Spec-as-source is worth it only because the spec is small and bounded.

**A model that goes away takes the regeneration path with it.** A release pinned to a model that is later retired still has a working binary and a checked-in source tree, but it can no longer be re-rolled. That is the reason `releases/` is append-only rather than a cache that gets cleaned.

**Unpinned behavior is ephemeral.** Anything the spec does not fix, the next generation re-rolls. A user who comes to depend on an unspecified response field will lose it, and the loss will look like a regression. The triage that handles this is in `CLAUDE.md`.

**The harness can be wrong.** An `observer_error` means the oracle misread, and the generation it was grading is unjudged. The loop stops rather than charging a harness defect to the generator.
