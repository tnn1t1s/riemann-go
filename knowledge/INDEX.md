# knowledge/ — pointer index for the generator

A curated list of authoritative documents that already exist on disk. Every entry is a path to open directly. There are no summaries here, only pointers with one orientation line each.

Guardrails:

- **Open the file at each pointer.** Do not paraphrase from a section title, and do not answer from a remembered prior. The index exists to move you off priors and onto verified source.
- **Read the Clojure for semantics, not for structure.** Upstream Riemann is the reference for what a combinator means. It is not a codebase to port line by line, and its concurrency model is the thing riemann-go is deliberately not reproducing.
- If none of the material below answers a question, mark the choice in a comment prefixed `// SPEC-GAP:` and pick the simplest thing consistent with the rest of the spec. Do not invent a convention.

## The expression language

- `~/go/pkg/mod/github.com/expr-lang/expr@v1.17.8/docs/language-definition.md` — the full syntax and the builtin functions. This is the language rule authors write in, and the one `SPEC.md`'s expression section is a subset of.
- `~/go/pkg/mod/github.com/expr-lang/expr@v1.17.8/docs/environment.md` — how an environment struct binds to top-level names, which is how event fields become identifiers.
- `~/go/pkg/mod/github.com/expr-lang/expr@v1.17.8/docs/functions.md` — supplying engine functions such as `tagged`.
- `~/go/pkg/mod/github.com/expr-lang/expr@v1.17.8/docs/visitor.md` — walking a compiled program's AST, which is how a rule's static read set is extracted.

## Combinator semantics, from the original

- `~/Developer/riemann/src/riemann/streams.clj` — every combinator as a closure over captured state. `by` at line 1577, `changed-state` at 1655, `throttle` through `part-time-simple` at 595, `stable` at 1936, `coalesce` at 1209.
- `~/Developer/riemann/test/riemann/streams_test.clj` — the tested behaviors, which are the actual specification of what each combinator means. Timed events in, expected sink deliveries out, under a controlled clock. `throttle-test` at 1354, the six `stable-test` cases at 1452.
- `~/Developer/riemann/test/riemann/query_test.clj` — the predicate cases the expression language has to cover, carried over from the retired query grammar.

## Index and expiry, from the original

- `~/Developer/riemann/src/riemann/index.clj` — the index as a map keyed by `[host service]`, its reaper, and why search is a full scan.
- `~/Developer/riemann/src/riemann/core.clj:274-308` — expiry synthesising an event that keeps only host and service and running it through the streams. This is the behavior `SPEC.md` property 4 preserves.

## Contracts riemann-go has to meet

- `~/Developer/riemann-atlas/VOCABULARY.md` — the emitter contract: cumulative counters, no content, fire-and-forget. Rule 3 there, that emission never affects the agent, is where `INVARIANTS.md` I9 comes from.
- `~/Developer/pyntfy/docs/metrics.md` — what the ntfy listeners emit, with names, kinds, cadences and TTLs. These are the events riemann-go's first rules match on.
- `~/Developer/riemann/src/riemann/ntfy.clj` — the fleet's current ntfy sink, including the state-to-priority mapping that `SPEC.md`'s alert shape keeps.

## Measured evidence in this repo

- `probes/README.md` — what each probe asks and how to run it.
- `probes/throughput/` — events per second through one loop with index, expiry heap and a four-combinator pipeline.
- `probes/expr/` — whether expr-lang expresses every predicate and transform the fleet's rules need, and whether read sets come out of the AST.
- `probes/timers/` — whether loop-owned heap timers pass the upstream `stable` and `throttle` tests without a lock.
- `probes/admission/` — what the HTTP ingest returns under burst with a slow sink, and how few lines an emitter needs.
