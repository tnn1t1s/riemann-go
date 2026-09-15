# knowledge/INDEX.md — pointers for the generator

Pointers only. No summaries live here, because a summary is a second copy of a fact that goes stale without anyone noticing. Open the files.

This index exists for one reason: riemann-go keeps the semantics of an implementation that already exists and has a decade of edge cases in it. When `SPEC.md` is silent on how a combinator behaves at a boundary, the answer is usually one Read away rather than a guess.

Nothing here overrides the spec. Where this material and `SPEC.md` disagree, the spec wins, and the disagreement is worth a `// SPEC-GAP:` comment.

## Upstream Clojure implementation

The semantics being kept, in the original.

| Path | What to read it for |
| --- | --- |
| `~/Developer/riemann/src/riemann/streams.clj` | Every combinator: `changed-state`, `stable`, `coalesce`, `throttle`, `rollup`, `batch`, `ddt`, `rate`, `ewma`, `by`, `splitp`, `project`, `top`. The edge cases are in the bodies, not the docstrings. |
| `~/Developer/riemann/src/riemann/core.clj` | The index reaper and the shape of an expiry event. |
| `~/Developer/riemann/src/riemann/index.clj` | Index keying and lookup. |
| `~/Developer/riemann/src/riemann/common.clj` | Event field defaults, especially how a missing `time` is filled. |
| `~/Developer/riemann/test/riemann/streams_test.clj` | The behavior of each combinator stated as timed input and expected output. The clearest statement of intent in the whole tree. |
| `~/Developer/riemann/test/riemann/query_test.clj` | The query predicates that the expr syntax has to cover. |

Two cautions. Upstream's `by` never frees a fork, and riemann-go does; upstream's `stable` schedules racing tasks, and riemann-go uses one generation-numbered heap entry. `SPEC.md` states both departures, and where it does, the spec is right and the Clojure is context.

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
