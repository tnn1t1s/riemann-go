# knowledge/INDEX.md — pointers for the generator

Pointers only. No summaries live here, because a summary is a second copy of a fact that goes stale without anyone noticing. Open the files.

Combinator behavior is not here. It is in `SEMANTICS.md`, extracted once from the upstream Clojure and frozen, so that every generation reads the same words. What follows is the material behind that document plus the fleet contracts the spec depends on.

Nothing here overrides the spec. Where this material and `SPEC.md` disagree, the spec wins, and the disagreement is worth a `// SPEC-GAP:` comment.

## Upstream Clojure implementation

Provenance, not required reading. `SEMANTICS.md` was extracted from these files, reviewed and frozen, and it is where combinator behavior is stated for implementation purposes. Read the Clojure only when `SEMANTICS.md` records an open question against it, or when you believe `SEMANTICS.md` is wrong; in the second case the finding is a spec edit rather than something to work around.

Re-reading these on every generation is what `SEMANTICS.md` exists to stop. Four thousand lines interpreted afresh each time is an unpinned input, and two generations from the same spec can disagree because they read it differently.

| Path | What it is |
| --- | --- |
| `~/Developer/riemann/src/riemann/streams.clj` | The combinators, in the original. |
| `~/Developer/riemann/src/riemann/index.clj`, `core.clj` | Indexing, the reaper, and the shape of an expiry event. |
| `~/Developer/riemann/test/riemann/streams_test.clj` | Behavior as timed input and expected output, under a controlled clock. |
| `~/Developer/riemann/test/riemann/query_test.clj` | The predicate forms the expression syntax has to cover. |

## The fleet this runs for

What the emitters already send, and what the existing sink already does. The spec pins behavior that comes from these files; a generator that has not read them will invent a different answer and be wrong in a way no compiler catches.

| Path | What to read it for |
| --- | --- |
| `~/Developer/riemann-atlas/VOCABULARY.md` | The emitter contract: cumulative counters, no content, fire-and-forget. Its rule that emission never affects the agent is why backpressure here means shedding and counting rather than blocking a caller. |
| `~/Developer/pyntfy/docs/metrics.md` | The names, kinds, cadences and TTLs the ntfy listeners emit, including the 30 second heartbeat with a 90 second TTL that makes expiry the alert rather than the beat. |
| `~/Developer/riemann/src/riemann/ntfy.clj` | The fleet's current ntfy sink, including the state-to-priority mapping that `SPEC.md` keeps normative. |

## Design probes

Five standalone programs under `probes/`, each answering one question, each its own Go module, none imported by the server. `probes/README.md` says what each one measured and how to run it. Read them for the reasoning behind a default, not as code to copy: they are evidence, and several of the spec's parameter values cite them.

`probes/timers/` is the closest thing to a worked example of the loop-owned timer heap. `probes/expr/` shows the expression language actually compiling fleet predicates and extracting read sets from the AST. `probes/admission/` is where the 429 semantics came from. `probes/throughput/` is the per-event cost measurement.

## Expression language

`github.com/expr-lang/expr`, used through its documented surface at <https://expr-lang.org/docs/language-definition>. Run `go doc -all github.com/expr-lang/expr` once for the API rather than reading the package source.
