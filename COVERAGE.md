# Coverage ledger

SPEC.md is the contract; its seventeen numbered properties are the rows. The development corpus is `features/*.feature`, eleven scenarios. The held-out set is `features/holdout/*.feature`, fifteen scenarios that run at promotion only. A row with no scenario is a property nobody enforces, whatever SPEC.md says about it.

| Property | Development scenarios | Held-out scenarios | Remaining evidence |
| --- | --- | --- | --- |
| P1 routed events reach their sink | `ingest-accepted` | | ntfy and influx asserted on the same event in one scenario |
| P2 ingest answers truthfully | `ingest-accepted`, `admission-accounts-for-every-event`, `backpressure-sheds-and-counts` | | the 429 body under real loop saturation; see HARNESS.md |
| P3 validation fails fast | none | none | a batch with no host, no service, over `max_batch_events`, over `max_batch_bytes` |
| P4 expiry is an event | `expiry-becomes-event` | `expiry-event-carries-only-identity`, `expired-name-means-the-reaper-only` | |
| P5 index only through an index leaf | `expiry-becomes-event` | | an event matching no rule never expires; `GET /index/{host}/{service}` 404 after expiry |
| P6 index query by expression | `read-surface-allows-a-browser` | | `as_of` bounds; a query that selects a subset |
| P7 by partitions state | | `by-gives-throttle-a-window-per-key`, `by-tuple-fork-delivers-every-event`, `changed-state-per-key-independence` | a development scenario; every P7 check is held out |
| P8 changed-state suppresses repeats | `changed-state-transitions-only` | `by-tuple-fork-delivers-every-event`, `changed-state-per-key-independence` | |
| P9 throttle bounds firings per window | `throttle-bounds-alerts`, `throttle-window-reopens-after-boundary` | `by-gives-throttle-a-window-per-key`, `throttle-window-anchors-on-the-first-event` | `riemann.rule.discarded` counted per discard |
| P10 splitp selects one branch | `provenance-on-alert` | `splitp-branch-state-is-per-branch`, `splitp-takes-the-first-branch-only` | |
| P11 set rewrites before the sink | `provenance-on-alert` | `set-derives-fields-from-the-incoming-event`, `set-reads-pre-rewrite-values` | |
| P12 timers ordered against events | | `stable-releases-buffer-and-elides-spike` | a scenario where a timer due at T and an event stamped T are distinguishable at the sink |
| P13 every alert carries provenance | `provenance-on-alert`, `changed-state-transitions-only` | `splitp-takes-the-first-branch-only` (node path by value) | `priority` mapping and `tags` contents |
| P14 rules idempotent by content | `rule-lifecycle` | | a second PUT with the same body returns 200 and the same version; a changed body increments and resets state |
| P15 dry run touches nothing live | | `dryrun-tests-a-rule-that-is-not-installed` | determinism: two identical dry runs return identical bodies; a timer past the last replayed event does not fire |
| P16 a drop is counted | `backpressure-sheds-and-counts`, `admission-accounts-for-every-event` | | influx drop of a metricless event; `GET /metrics` carrying every queue |
| P17 missing configuration is fatal | none | none | each required flag omitted in turn exits non-zero naming it, with no socket bound |

Outside the numbered list:

| Section | Scenarios | Remaining evidence |
| --- | --- | --- |
| HTTP surface: CORS | `read-surface-allows-a-browser` | the `OPTIONS` preflight |
| HTTP surface: predicate typing at PUT | `where-refuses-a-non-boolean-predicate` | a non-boolean `match` and `splitp` `test` |
| Expression language | `where-refuses-a-non-boolean-predicate`; held out: `expired-name-means-the-reaper-only`, `where-partitions-with-no-else` | `tagged`, `matches` anchoring and the closed world in a development scenario |
| SEMANTICS.md coalesce, ddt, stable | held out only | development scenarios; `rate`, `ewma`, `rollup`, `batch` have none at all |
| Self-observation | `backpressure-sheds-and-counts` | the per-shard and ingest gauges by name |
| SSE subscribe, `GET /events` paging, the ring's bounds | none | scenarios, and a streaming observer in the harness |

The harness runs a candidate in a fresh temporary directory with its own process group, but that alone does not prove independence from absolute paths or network access. The build manifest binds the supplied bytes; it does not attest the builder. Build and evaluation inputs should remain immutable during a trial.

The self-tests under `tests/` check harness transport and matcher behavior against a canned stand-in. They impose no requirement on riemannd and are not product validation.
