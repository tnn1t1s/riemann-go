# riemann-go

A single-node stream-processing monitor for a cluster whose emitters are mostly agents. It accepts events over HTTP, keeps an index of the last event per `(host, service)` where expiry becomes an event rather than a deletion, evaluates rules submitted as JSON, and drives two sinks: an ntfy topic and an InfluxDB v2 bucket.

Every queue is bounded, every drop is counted, and the emitter is told.

## The architecture in one line

**The sink receiver is the oracle. A generation of riemann-go is only a hypothesis.**

Validation does not assert on riemann-go's HTTP replies. It asserts on what arrived at a harness-owned server standing in for ntfy and InfluxDB, recorded verbatim. That frees a generation to choose anything the spec left open, and it keeps the correctness check external to the code being checked.

## What this repo contains

- [`SPEC.md`](./SPEC.md): the behavioral contract, with the wire surfaces marked normative.
- [`SEMANTICS.md`](./SEMANTICS.md): how each combinator, and indexing and expiry, behave.
- [`INVARIANTS.md`](./INVARIANTS.md): principles binding every generation, each with an acid test.
- [`SCALE.md`](./SCALE.md): backpressure and scale floors written as behavior.
- [`DESIGN.md`](./DESIGN.md): how riemann-go is built, validated and iterated.
- [`HARNESS.md`](./HARNESS.md): the Gherkin step table, the trace vocabulary and the matcher.
- [`GENERATE.md`](./GENERATE.md): the CMake build, the pins and the release layout.
- [`COVERAGE.md`](./COVERAGE.md): which SPEC.md property each scenario covers, and what nothing covers.
- [`knowledge/INDEX.md`](./knowledge/INDEX.md): pointers to the authoritative documents a generator should read.
- `features/`: the Gherkin corpus: eleven development scenarios, and fifteen held out under `features/holdout/`.
- `harness/`: the riemannd flags, the observer, the session hooks and the stimulus step table. The receiver, matcher, session lifecycle, pytest-bdd arena and generation scripts come from [riemann-harness](https://github.com/tnn1t1s/riemann-harness).
- `releases/`: generated artifacts at three trust levels.
- [`probes/`](./probes/): standalone programs that measured one design question each. Evidence, not product code; nothing under it is imported by the server.

No implementation source is checked in at the repo root. riemann-go is a build output, regenerated from the specification documents. The language is Go for operational reasons rather than as part of the contract.

## Build from the specification

`agent-command.json` holds the coding agent's argv with a `{model}` placeholder. Then:

```sh
uv sync
cmake -S . -B build -DRG_MODEL=claude-fable-5-1 \
  -DRG_AGENT_COMMAND_FILE="$PWD/agent-command.json" \
  -DPython3_EXECUTABLE="$PWD/.venv/bin/python"
cmake --build build
ctest --test-dir build --output-on-failure
```

The build invokes the agent once, with pinned inputs, and verifies that the artifact it produced is bound by digest to the SPEC.md it read. Unchanged inputs reuse the artifact. CTest runs the development corpus against it; a successful build is not a passing trial.

## Trial a candidate

```sh
uv run bin/check
uv run bin/trial --command '["/absolute/path/to/riemannd"]' \
  --spec /absolute/path/to/SPEC.md \
  --build-manifest /absolute/path/to/build-manifest.json
```

`bin/trial --holdout` adds the held-out set, which is a promotion run. Reports, traces and the candidate's log land under `reports/trial/` by default. Python 3.11 or later and loopback sockets are the only requirements; no Riemann, ntfy or InfluxDB is contacted.

## Known limitations

Stream state does not survive a restart. On restart the index is empty, rules start fresh, and entries live before the restart never produce their expiry event. The event ring that serves dry run and `GET /events` is in memory and lost with the process.

## Product, not instance

This repository is what riemann-go *is*: the specification, the semantics, the invariants, the validation harness, the scenario corpus, the generation loop, and the releases those produce. It does not know which host runs it, on which port, against which bus topic or which InfluxDB, and it holds no rule set that any fleet runs.

Those belong to an instance. The medios fleet's is [deploy-riemann](https://github.com/tnn1t1s/deploy-riemann), which pins a release tag from here and renders it into a fennel app.

The seam is a published release. Because the binary is a build output of `SPEC.md`, a release tag names a generation rather than a version, and the manifests published beside it record the inputs, the model and the artifact digests that produced it. That is what makes "which riemann-go is running" a question with an answer.

## License

Apache-2.0.
