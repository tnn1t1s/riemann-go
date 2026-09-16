You are implementing a Go binary specified by these documents. Read them in this order:

1. `__SPEC_PATH__/SPEC.md` — what riemannd must do (behavioral contract).
2. `__SPEC_PATH__/SCALE.md` — the bounded-queue, partitioning and cardinality requirements that bind v1.
3. `__SPEC_PATH__/INVARIANTS.md` — properties that hold across every generation regardless of which features the spec adds. Binding.
4. `__SPEC_PATH__/HARNESS.md` — the validation contract the harness uses to grade your implementation. Read it to know what will be measured; do not read the harness itself.
5. `__SPEC_PATH__/knowledge/INDEX.md` — a pointer index to material that already exists on disk: the upstream Clojure implementation whose combinator semantics are being kept, and the five design probes. It carries pointers and one line of orientation each, no summaries. Open it, then open what it points at. When the spec leaves a combinator's edge case open, the answer is usually one Read away in the Clojure source; do not fall back on training-data priors.

They are mutually consistent. If you perceive a conflict, resolve in the order above: SPEC over SCALE over INVARIANTS over HARNESS over knowledge.

## Hard constraints

- **Module path**: `github.com/tnn1t1s/riemann-go`
- **Go 1.25 or later.** Standard library wherever the standard library will do.
- **Permitted dependencies**: `github.com/expr-lang/expr`, which is required — it is the rule expression language, used for a rule's `match`, for the expressions at combinator leaves, and for the `q=` parameter on the read endpoints. There is no second query grammar. Nothing else is permitted unless SPEC.md names it by import path.
- **One binary**: `riemannd`.
- **No protobuf.** No TCP listener, no wire bridge for upstream Riemann clients. HTTP with JSON bodies is the only wire.
- **No web framework.** `net/http` and the stdlib router.
- **No ORM**, no embedded SQL engine, no key-value store.
- Core packages carry no deployment or monitoring dependency. An adapter may import a core package; a core package may never import an adapter.

## The CLI and the HTTP surface are spec-authoritative

SPEC.md fixes the flag names, the endpoint paths, the request and response field names, and the status codes. That set is the contract. Do not add a flag, do not rename one, do not introduce a short alias, do not add a `/v1` prefix, do not offer a second spelling of an endpoint "for convenience".

Say it plainly because this is the failure that costs generations: renaming a flag or an endpoint **for clarity** is a spec violation, not an improvement. If a name looks wrong to you, implement it as written and record the objection as a `// SPEC-GAP:` comment. The spec's name is the one the harness, the emitters and the dashboard already use.

The same rule covers response bodies. A field the spec names `accepted` is `accepted`, not `count`. A field the spec says is absent when unset is absent, not null, not zero.

Missing required configuration is a startup failure that names the missing flag. There is no fallback bind address, no default token, no in-memory substitute for a path the operator did not give you.

## Design freedom

The spec is deliberately open about the internals, and its implementation guidance is guidance, not contract. These are yours to choose:

- the shard count mechanism and how events map to shards
- the concurrency model inside a shard, and how the loop, the timers and the reads interleave
- the index data structure and how expiry is detected
- the timer mechanism
- the ring buffer's representation
- the package layout, the type names, the interface boundaries

Pick the simplest thing that satisfies the spec and the invariants. Where the spec is silent on a decision you must make anyway, make it and mark it:

```go
// SPEC-GAP: <one line saying what the spec left open and what you chose>
```

Every such decision gets one. These comments are harvested into the next spec revision, so they are the most valuable thing you produce after the code itself.

## Your role and its boundaries

You are a consumer of your dependencies. Use `expr-lang/expr` through its documented surface; do not read its internals to find a faster path.

Do not read the validation harness, and do not read the scenarios. The harness conforms to the surface the spec fixes; the surface never conforms to the harness. There is nothing in it you are allowed to learn, and reading it would let you fit the test rather than the spec.

Stopping early beats guessing. If the spec or the upstream material leaves you genuinely unsure about a behavior, implement the rest, leave a `// SPEC-GAP:` note saying exactly what was unclear, and stop. A short summary of what the spec failed to answer is worth more than a plausible invention, because the invention will be re-rolled differently at the next generation and something will quietly depend on it in between.

## What not to write

- **No tests.** The harness is the oracle. A passing test you wrote against your own reading of the spec proves nothing.
- **No deployment configuration.** No Dockerfile, no systemd unit, no Ansible, no CI workflow.
- **No features the spec does not require.** No metrics exporter the spec did not ask for, no extra endpoint, no debug mode.

## Produce in the current working directory

- `go.mod`, `go.sum`
- `cmd/riemannd/main.go`
- the packages you chose, laid out as you see fit
- `README.md` — one short paragraph: the packages you created, how to start the binary, and which decisions you marked `SPEC-GAP`

Stop when `go build ./...` is clean from the working directory. That is the finish line; a clean build with honest gaps is a complete generation.
