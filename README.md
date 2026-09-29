# riemann-go

A single-node stream-processing monitor for a cluster whose emitters are mostly agents. It accepts events over HTTP, keeps an index of the last event per `(host, service)` where expiry becomes an event rather than a deletion, evaluates rules submitted as JSON, and drives two sinks: an ntfy topic and an InfluxDB v2 bucket.

Every queue is bounded, every drop is counted, and the emitter is told.

## The architecture in one line

**The sink receiver is the oracle. A generation of riemann-go is only a hypothesis.**

Validation does not assert on riemann-go's HTTP replies. It asserts on what arrived at a harness-owned server standing in for ntfy and InfluxDB, recorded verbatim. That frees a generation to choose anything the spec left open, and it keeps the correctness check external to the code being checked.

## What this repo contains

- [`SPEC.md`](./SPEC.md) — the behavioral contract, with the wire surfaces marked normative.
- [`INVARIANTS.md`](./INVARIANTS.md) — principles binding every generation, each with an acid test.
- [`SCALE.md`](./SCALE.md) — backpressure and scale floors written as behavior.
- [`DESIGN.md`](./DESIGN.md) — how riemann-go is built, validated and iterated.
- [`knowledge/INDEX.md`](./knowledge/INDEX.md) — pointers to the authoritative documents a generator should read.
- `scenarios/` — declarative tests over the trace.
- `harness/` — drives riemann-go, runs the sink receiver, evaluates expectations.
- `releases/` — generated artifacts at three trust levels.
- [`probes/`](./probes/) — standalone programs that measured one design question each. Evidence, not product code; nothing under it is imported by the server.

No implementation source is checked in at the repo root. riemann-go is a build output, regenerated from the specification documents. The language is Go for operational reasons rather than as part of the contract.

## Known limitations

Stream state does not survive a restart. On restart the index is empty, rules start fresh, and entries live before the restart never produce their expiry event. The event ring that serves dry run and `GET /events` is in memory and lost with the process.

## Product, not instance

This repository is what riemann-go *is*: the specification, the semantics, the
invariants, the validation harness, the scenario corpus, the generation loop,
and the releases those produce. It does not know which host runs it, on which
port, against which bus topic or which InfluxDB, and it holds no rule set that
any fleet runs.

Those belong to an instance. The medios fleet's is
[deploy-riemann](https://github.com/tnn1t1s/deploy-riemann), which pins a
release tag from here and renders it into a fennel app.

The seam is a published release. Because the binary is a build output of
`SPEC.md`, a release tag names a generation rather than a version, and the
manifest published beside it records the spec, model and prompt that produced
it. That is what makes "which riemann-go is running" a question with an answer.

## License

Apache-2.0.
