# riemann-go — combinator and index semantics

This document states what each combinator does, what state it holds, when its timers fire, and what it does at the edges. It is the normative behavioral reference for a generation. A generation reads this file and does not read upstream Riemann's Clojure.

`SPEC.md` owns the wire: the JSON shape of a rule document, the parameter names, the event model, the alert shape, and the HTTP surface. This document owns behavior. Where a parameter name appears here it is `SPEC.md`'s name, and where the two documents appear to disagree `SPEC.md` wins and this file is the one that gets corrected.

Provenance lines name a file and line range in upstream Riemann (`~/Developer/riemann`, the fleet's current checkout). They exist so a human can check the extraction. Nothing in a generation's pipeline reads them.

## Why this file exists

The generator's reproducibility pins are the spec, the model and the prompt. Telling the generator to read four thousand lines of Clojure adds a fourth input that is unpinned, unreviewed and re-interpreted on every roll, so two generations from one spec can disagree because they read `streams.clj` differently. Extracting the behavior once, in prose a human has reviewed, removes that input. The Clojure becomes provenance for people rather than required reading for a model.

## Conventions used throughout

**Event time against wall time.** Every combinator below that compares times compares the `time` field of events, not the process clock, unless this document says otherwise. Upstream frequently mixes the two: `stable`'s timeout callback reads the wall clock while its arrival path reads event time (`src/riemann/streams.clj:1968-1986`), and `throttle`'s window boundaries come from a wall-clock anchor taken when the stream was constructed (`src/riemann/streams.clj:598-599`). riemann-go uses event time, because `INVARIANTS.md` and `SPEC.md` property 12 make the timeline of a scenario the authority on ordering. A combinator whose boundaries depend on when the process happened to start is not reproducible under that timeline.

**Fork key.** Several combinators hold state "per fork key". The fork key is the tuple of field values named by the nearest enclosing `by` node on the path from the root of `stream` to this node. When no `by` encloses the node, there is exactly one fork key for that node, shared by every event the rule matched. This matters most for `changed-state`, which upstream bundles with a `by [:host :service]` and `SPEC.md` does not; see that section.

**Downstream.** A node "passes an event downstream" by delivering it to every entry of its `children` array, in array order. A node that passes nothing delivers nothing; there is no separate else path. Upstream's `where` and `where*` support an `(else ...)` clause (`src/riemann/streams.clj:1765-1811`); `SPEC.md`'s rule document has no equivalent key, so riemann-go has no else branch. An author who wants one writes a second `where` with the negated expression.

**Expired events traverse rules normally.** An event carrying `state` equal to `expired`, whether synthesized by the index reaper or sent by a client, is an ordinary event to every combinator in this document. No combinator filters it, skips it, or treats it as a lifecycle signal. The one place expiry has combinator-visible meaning is `coalesce`, which drops an expired identity from its held set after emitting it once, and the `by` fork-freeing departure described at the end of this file.

**Classification.** Choices are labelled with the categories in `~/.claude/guides/writing-researchable-software.md`: invariant, observation, hypothesis, heuristic, parameter, default, research question. A behavior with no label is a required behavior, which is to say an invariant that a scenario enforces.

## `where`

**What it does.** Evaluates its `expr` against the incoming event. When the expression is true the event passes downstream unchanged. When it is false the event is discarded at this node.

**Parameters.** `expr`, a string holding an expr-lang expression, compiled once at `PUT`. Its name domain is closed, per `SPEC.md`'s expression language section: the ten event fields plus `tagged`, `now`, `expired` and `events`.

**State.** None. `where` is stateless, holds no fork state, and behaves identically for every fork key.

**Timing.** Owns no timer.

**Edge cases.**

- A `where` expression whose type is not boolean fails to compile, which is a `400` at `PUT` naming the expression, per `SPEC.md`'s rule surface. It is not a truthiness coercion at evaluation time. Upstream's `where` returns the value of the expression and treats any truthy value as a match (`src/riemann/streams.clj:1802-1810`), so `(where metric)` there matches every event with a non-nil metric. A predicate that is accidentally a projection fires on everything, and that is the failure mode hardest to notice once a rule is live.
- An event with no `metric` evaluating an expression that references `metric` is a per-expression concern, not a `where` concern. `SPEC.md` fixes the reading of an absent attribute as `""`; it does not fix the reading of an absent metric. See Open questions.
- An event whose `state` is `expired` is passed or dropped by the expression like any other. `where` adds no filter of its own.
- The event delivered downstream is the same event, not a copy with fields normalized.

**Worked example.** Rule matched all events; node is `{"op":"where","expr":"metric > 10","children":[{"sink":"ntfy"}]}`.

| t | host | service | metric | passes |
| --- | --- | --- | --- | --- |
| 0 | a | cpu | 5 | no |
| 1 | a | cpu | 15 | yes |
| 2 | a | cpu | 10 | no |
| 3 | a | cpu | absent | see Open questions |

Two firings would be one, at t=1, if the absent-metric row is a non-match.

**Provenance.** `src/riemann/streams.clj:1765-1811`, and `where-rewrite` at `1688-1724` for the symbol set upstream exposes.

## `by`

**What it does.** Partitions the stream. For each distinct tuple of the event's `fields` values, `by` maintains an independent instance of the subtree below it, and delivers the event to the instance for that event's tuple. The event itself is unchanged; what differs is which copy of the downstream state it meets.

**Parameters.** `fields`, an array of event field names. The tuple is formed by reading those fields from the event in array order.

**State.** A table from fork key to subtree instance. Each instance holds its own copy of every stateful node beneath it, so a `changed-state` under `by ["host","service"]` remembers a prior state per identity rather than one prior state for the rule. `SPEC.md` property 7 is exactly this. Fork lifetime is a riemann-go departure from upstream, described in the departures section below; upstream never frees a fork.

**Timing.** `by` owns no timer of its own, but the timers owned by nodes beneath it are per fork key. A `throttle` under a `by` has one window per key, and a `stable` under a `by` has one buffer and one armed timer per key.

**Edge cases.**

- A field named in `fields` that the event does not carry reads as that field's documented default from `SPEC.md`'s event model. An absent `state` reads as `""`, so events with no state share one fork rather than each getting their own. An absent `metric` is the one field with no default value, which makes `by ["metric"]` ill-defined; see Open questions.
- Forking on a high-cardinality field creates one subtree instance per distinct value. Upstream warns about this and provides no bound (`src/riemann/streams.clj:1577-1580`). riemann-go's fork-freeing rule bounds it only for keys that expire; a `by ["attributes.run_id"]` over an unbounded id space is bounded by nothing. That is a research question, recorded below rather than solved here.
- The first event for a key creates the fork and is then delivered to it, so the first event is not lost. Creation and delivery are one step.
- Nested `by` nodes compose. The fork key of an inner `by` is the tuple of its own fields, scoped within its parent's fork, so the effective key is the concatenation.
- An event whose fork key was freed and then recreated meets a fresh subtree. This is the observable consequence of the departure and is the subject of a scenario.

**Worked example.** `{"op":"by","fields":["host","service"],"children":[{"op":"changed-state","initial":"ok","children":[{"sink":"ntfy"}]}]}`.

| t | host | service | state | fires |
| --- | --- | --- | --- | --- |
| 0 | a | cpu | ok | no, equals `initial` for key (a,cpu) |
| 1 | b | cpu | warning | yes, key (b,cpu) first transition from `ok` |
| 2 | a | cpu | warning | yes, key (a,cpu) transition ok to warning |
| 3 | b | cpu | warning | no, key (b,cpu) unchanged |

Without the `by`, rows at t=1 and t=3 would share one prior state and the firing count would differ.

**Provenance.** `src/riemann/streams.clj:1556-1612`, tests at `test/riemann/streams_test.clj:718-767`.

## `changed-state`

**What it does.** Remembers the `state` of the last event it saw for a fork key and passes an event downstream only when the incoming `state` differs from it. The remembered state is updated on every event, whether or not the event passes.

**Parameters.** `initial`, a string. It is the state assumed to be remembered before any event has arrived for a fork key. Upstream's option is named `:init` and takes an arbitrary value (`src/riemann/streams.clj:1614-1653`); `SPEC.md` names it `initial` and it is a state string.

**State.** One remembered state string per fork key. Nothing else. There is no count, no timestamp, no prior event retained beyond the state string, and no timer to reset it. The remembered state is reset only by the fork being freed or the rule version changing, since `SPEC.md` property 14 says a new rule version does not carry the previous instance's state.

**Timing.** Owns no timer.

**Edge cases.**

- The first event for a fork key compares against `initial`, not against nothing. An `initial` of `"ok"` and a first event with state `"ok"` produce no firing, which is usually what a rule author wants, and an `initial` of `""` and a first event with no `state` also produce no firing, because an absent `state` reads as `""`. That second case is easy to write by accident.
- The remembered state advances even when the event is suppressed. Three consecutive `warning` events remember `warning` once and fire once, and the fourth event carrying `ok` fires because it differs from `warning`, not because it differs from `initial`.
- An expired event carries state `expired`, which differs from any ordinary state, so it fires. It then becomes the remembered state, so an identity that comes back with the state it held before expiring fires again. Under the fork-freeing departure the fork is gone by then and the comparison is against `initial` instead; either way there is a firing, which is what the departure's scenario checks.
- A firing records the state it replaced as `prior_state` in the alert, per `SPEC.md` property 8 and the alert shape. On the first event for a key, `prior_state` is `initial`, not null. Null is reserved for a path that traversed no `changed-state` node at all.
- Upstream bundles the fork into the combinator: `changed-state` is a macro expanding to `(by [:host :service] (changed :state ...))` (`src/riemann/streams.clj:1655-1659`). `SPEC.md` separates them. A riemann-go `changed-state` with no enclosing `by` holds one remembered state for every event the rule matched, which is a legal rule and a firing pattern nobody wants. Faithfully reproducing upstream means writing `by ["host","service"]` above it, and every example in `SPEC.md` does.
- Upstream's `changed` compares the result of an arbitrary function and offers a `:pairs?` option that delivers the previous and current events as a pair. Neither is in `SPEC.md`. riemann-go compares the `state` field only, and delivers the current event only.

**Worked example.** `{"op":"changed-state","initial":"ok"}` under `by ["host","service"]`, one identity.

| t | state | fires | `prior_state` in the alert | remembered after |
| --- | --- | --- | --- | --- |
| 0 | ok | no | | ok |
| 1 | warning | yes | ok | warning |
| 2 | warning | no | | warning |
| 3 | critical | yes | warning | critical |
| 4 | ok | yes | critical | ok |

**Provenance.** `src/riemann/streams.clj:1614-1659`, tests at `test/riemann/streams_test.clj:1141-1202`.

## `throttle`

**What it does.** Bounds how many events pass downstream per fork key per window. It counts events arriving in the current window, passes the first `limit` of them downstream immediately, and discards the rest. Nothing is buffered and nothing is delayed.

**Parameters.** `limit`, an integer count of events. `window_seconds`, a float number of seconds. Upstream's arguments are positional `n` and `dt` (`src/riemann/streams.clj:1102-1118`).

**State.** Per fork key: a count of events admitted in the current window, and the window's end time. The count resets to zero when a window closes.

**Timing.** A window opens when an event arrives for a fork key with no open window, and it closes `window_seconds` of event time later. The count resets at the close. No firing happens at the close: a window boundary with no event arriving produces nothing downstream, and a fork key that stops receiving events simply has no open window.

Upstream does this differently in a way worth stating, because it is invisible in the docstring. Upstream's windows are fixed divisions of wall-clock time anchored at the instant the stream was constructed, computed by `next-tick` (`src/riemann/time.clj:83-90`) from an anchor taken at construction (`src/riemann/streams.clj:598-599`). The first window after a stream is built is therefore shorter than `dt` by an arbitrary amount, and two throttles built a second apart have boundaries a second apart. riemann-go opens the window at the first event for the key instead. Classification: this is a heuristic replacing an accident. Upstream's anchoring is not a designed behavior, it is a consequence of `part-time-simple` dividing wall time into bins, and no upstream test depends on the anchor's value. The riemann-go rule is reproducible under a scenario timeline; upstream's is not. See Open questions for the part of this that is a genuine spec decision rather than a correction.

**Edge cases.**

- Exactly `limit` events pass per window. Upstream increments before testing and passes while the running count does not exceed `n` (`src/riemann/streams.clj:1110-1114`), so with `limit` of 3 the third event passes and the fourth does not.
- An event beyond the limit is discarded, not deferred to the next window. `SPEC.md` property 9 states this, and it is the behavior that distinguishes `throttle` from upstream's `rollup`, which does defer.
- An event arriving exactly at a window boundary belongs to the new window. The window covering `[t0, t0 + window_seconds)` is half open, so an event stamped `t0 + window_seconds` opens the next window and is its first admitted event.
- Every discarded event increments the throttle node's drop accounting. `INVARIANTS.md` I5 requires that no event is lost without a counter, and a throttle discard is a loss. The node's `counters` entry on `GET /rules/{id}` reports events passed downstream, so passed plus discarded is the count that arrived.
- An expired event is counted and throttled like any other. It has no privileged passage. Upstream's throttle test drives four `expired` events through a throttle and drops one of them (`test/riemann/streams_test.clj:1354-1373`), which is the clearest statement that expiry is not special here.
- A window whose events are all discarded still closes on schedule. Discarding does not extend the window.

**Worked example.** `limit` 3, `window_seconds` 2, one fork key. Windows open at the first event.

| t | event | count in window | passes |
| --- | --- | --- | --- |
| 0 | e1 | 1 | yes, opens window ending at 2 |
| 0 | e2 | 2 | yes |
| 0 | e3 | 3 | yes |
| 1 | e4 | 4 | no, discarded |
| 2 | e5 | 1 | yes, opens window ending at 4 |
| 3 | e6 | 2 | yes |
| 3 | e7 | 3 | yes |
| 3 | e8 | 4 | no, discarded |
| 5 | e9 | 1 | yes, opens window ending at 7 |

Seven of nine events pass. This is upstream's own throttle test with its wall-clock anchor replaced by the first-event anchor; under both rules the same seven pass, because that test's stream is constructed at t=0.

**Provenance.** `src/riemann/streams.clj:1102-1118` and `595-661` for `part-time-simple`, test at `test/riemann/streams_test.clj:1354-1373`.

## `splitp`

**What it does.** Routes an event to exactly one of several branches. It evaluates the `test` expression once per branch, in branch order, with that branch's `threshold` substituted for the `{}` placeholder, and delivers the event to the first branch whose test holds. When no branch's test holds the event goes to `otherwise`.

**Parameters.** `test`, an expression string containing the literal `{}`. `branches`, an ordered array whose entries each carry a `threshold` and a subtree. `otherwise`, a subtree. `splitp` is the one combinator with no `children` array; its subtrees hang off `branches` and `otherwise`.

**State.** None. `splitp` is stateless. Branch subtrees beneath it may be stateful, and each branch's state is independent of the other branches because they are separate subtrees, not separate fork keys. An event that takes branch 0 does not touch branch 1's state at all, which means a `changed-state` inside a branch sees only the events that reached that branch and is blind to events that took a sibling.

**Timing.** Owns no timer. Timers inside branch subtrees behave as they would anywhere else.

**Edge cases.**

- Order decides. The branches are tested in array order and the first hit wins, so a rule whose thresholds are written in the wrong order silently routes everything to the loosest branch. Nothing detects that at `PUT`, because "loosest" is not a property of the expression, it is a property of the substituted values.
- Exactly one subtree receives the event. `SPEC.md` property 10 requires this, and the node path recorded in the alert names which one, as `.../branches/<k>` or `.../otherwise`.
- Upstream's `splitp` throws `IllegalArgumentException` when no clause matches and no default stream is present (`src/riemann/streams.clj:1855-1916`, tested at `test/riemann/streams_test.clj:394-398`), while its sibling `split*` silently drops the event in the same situation (`src/riemann/streams.clj:1818-1835`). Two names for one idea disagreeing on the no-match case is an accident, not a design. See Open questions for which riemann-go keeps.
- The test expression is evaluated per branch, not once. A `test` with a side-effect-free expression is the only kind expr-lang can express, so this costs evaluation time and nothing else. Upstream evaluates the subject expression once and applies a binary predicate per clause, which is the same outcome.
- A `test` string with no `{}` is a compile error at `PUT`. A branch with no `threshold` is a compile error at `PUT`. Both are `400` with the node path named, per `SPEC.md`'s rule surface.
- Upstream evaluates each branch's child stream once, at rule construction, not once per event (`test/riemann/streams_test.clj:399-410`). In riemann-go this is the statement that branch subtrees hold state across events rather than being rebuilt per event.

**Worked example.** `test` is `"metric >= {}"`, branches with thresholds 10 and 5, plus `otherwise`.

| t | metric | branch taken | node path suffix |
| --- | --- | --- | --- |
| 0 | 15 | first, 15 >= 10 | `/branches/0` |
| 1 | 8 | second, 8 >= 5 | `/branches/1` |
| 2 | 2 | none hold | `/otherwise` |
| 3 | 10 | first, 10 >= 10 | `/branches/0` |
| 4 | 5 | second, 5 >= 5 | `/branches/1` |

The boundary rows at t=3 and t=4 are the reason the test carries its own comparison operator rather than the spec fixing one.

**Provenance.** `src/riemann/streams.clj:1855-1916`, tests at `test/riemann/streams_test.clj:372-410`.

## `set`

**What it does.** Builds a new event from the incoming one by evaluating an expression per named field and replacing that field's value with the result. Every other field is carried over unchanged. The new event is what the children see, and what a sink downstream of the node writes.

**Parameters.** `fields`, an object mapping a field name to an expression string. Upstream's equivalent is `with`, which takes literal values rather than expressions (`src/riemann/streams.clj:1346-1391`); the expression form is riemann-go's, and `SPEC.md` names it `set`.

**State.** None. `set` is stateless and holds nothing across events.

**Timing.** Owns no timer.

**Edge cases.**

- Every expression is evaluated against the incoming event, not against the partially rewritten one. Two fields that each reference the other's old value both read old values, and the order of keys in the `fields` object cannot change the result. Upstream's `with` has the same property, since it reduces over an immutable map (`src/riemann/streams.clj:1367-1372`), but it is worth stating because the obvious sequential implementation gets it wrong.
- The incoming event is not modified. A `set` on one branch of a `splitp` does not change what a sibling branch or a later node on another path sees, because the rewritten event exists only below this node.
- Rewriting `host` or `service` changes the event's identity for every node below, including a `{"sink":"index"}` leaf, which will then index under the new identity. Nothing prevents this and there is a legitimate use for it, such as folding a per-container host into a per-service one.
- A `set` producing an empty `host` or `service` produces an event with no usable identity. Ingest rejects such an event at the door with `400`, per `SPEC.md` property 3, but `set` is downstream of ingest and no check runs there. See Open questions.
- Upstream's `with` deletes a key when the value is nil rather than setting it (`src/riemann/streams.clj:1364-1366,1387-1390`). That behavior exists because upstream distinguishes an absent key from a nil one in a protobuf-backed record. riemann-go's event model gives every field a default except `metric`, so deletion has no meaning for most fields; what an expression evaluating to null should do is in Open questions.
- Alert provenance reports the rewritten values. `SPEC.md` property 11 requires that the value observed at the sink is the rewritten one, and the provenance object is extracted from the event the sink leaf received.

**Worked example.** `{"op":"set","fields":{"state":"metric > 90 ? \"critical\" : \"ok\"","service":"service + \".derived\""}}`.

| t | in: service | in: metric | in: state | out: service | out: state | out: metric |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | cpu | 95 | ok | cpu.derived | critical | 95 |
| 1 | cpu | 10 | critical | cpu.derived | ok | 10 |

`metric` and every unnamed field pass through untouched, and the alert at a downstream ntfy leaf carries `cpu.derived` as its service.

**Provenance.** `src/riemann/streams.clj:1346-1391`, tests at `test/riemann/streams_test.clj:684-717`.

## `coalesce`

**What it does.** Holds the most recent event per identity and, on every arrival, emits the whole held set to its children as the `events` name. Children see a set rather than a single event, and the expression language exposes `events` only inside a `coalesce` subtree.

**Parameters.** None.

**State.** A table from identity, the `(host, service)` pair, to the most recent event for that identity. Scoped per fork key like any other state, so a `coalesce` under a `by` folds only within its fork. A rule declaring `partition` of `host` and containing a `coalesce` is refused at `PUT` with `400`, per `SPEC.md`'s rule document section, because a per-host partition cannot answer a fleet-wide fold.

**Timing.** Owns no timer. This is a departure from upstream worth naming explicitly, because the two upstream functions differ and `SPEC.md` picks the one the docstrings do not describe. Upstream's `coalesce` emits periodically, every `dt` seconds with `dt` defaulting to 1, driven by `periodically-until-expired` (`src/riemann/streams.clj:1209-1241`). Upstream's `coalesce-with-event` emits on every arrival (`src/riemann/streams.clj:1187-1207`). `SPEC.md` says "emits the current set as `events` to its children whenever one changes", which is the per-arrival form, and `SPEC.md` carries no `dt` parameter. riemann-go implements the per-arrival form. The consequence is that a `coalesce` with a hundred identities arriving at a hundred events per second emits a hundred sets of a hundred events per second, and a rule author who wants a periodic fold does not have one in v0.

**Edge cases.**

- The arriving event is in the emitted set. It is inserted before the set is emitted, so the fold always includes the event that triggered it.
- An expired identity appears in the emitted set exactly once and is then removed. Upstream partitions the held table on every arrival, emits the expired entries alongside the live ones, and retains only the live ones (`src/riemann/streams.clj:1191-1207`). A downstream consumer therefore sees an identity's final state once, and never again, which is what makes a fold like "how many hosts are in state critical" converge rather than counting a dead host forever.
- Expired entries are emitted before live ones in upstream's ordering. That ordering is an artifact of a `concat`, not a designed property, and no upstream test asserts on it. riemann-go does not promise an order within the emitted set.
- An arriving event that is itself expired is inserted, immediately classified as expired, emitted in the set once, and not retained.
- Expiry here is decided by the event's own `state` and by its `time` plus `ttl` against the current time, which is upstream's `expired?` (`src/riemann/streams.clj:50-59`). An identity that stops reporting ages out of a `coalesce` fold on its own, without the index reaper's involvement, because `coalesce` re-evaluates the whole table on each arrival. A `coalesce` that stops receiving any events stops re-evaluating and holds its stale set indefinitely.
- The first event to reach a `coalesce` emits a set of one. There is no warm-up period and no minimum set size.

**Worked example.** One `coalesce` with no enclosing `by`. Every arrival emits.

| t | arriving | held set after | emitted `events` |
| --- | --- | --- | --- |
| 0 | (a, cpu) ok, ttl 2 | {(a,cpu)} | one event |
| 1 | (b, cpu) ok, ttl 60 | {(a,cpu), (b,cpu)} | two events |
| 4 | (c, cpu) ok, ttl 60 | {(b,cpu), (c,cpu)} | three events, including (a,cpu) whose ttl lapsed at t=2 |
| 5 | (c, cpu) warning | {(b,cpu), (c,cpu)} | two events |

The emission at t=4 carries three events and the one at t=5 carries two, because (a, cpu) was delivered once on its way out.

**Provenance.** `src/riemann/streams.clj:1187-1241`, test at `test/riemann/streams_test.clj:1417-1450`.

## `ddt`

**What it does.** Converts a series of metric readings into a rate. For each pair of consecutive events for a fork key it emits the current event with its `metric` replaced by the change in metric divided by the change in time, in units of metric per second.

**Parameters.** None. Upstream's `ddt` takes an optional leading number that switches it to a different combinator entirely, emitting on a timer rather than per event (`src/riemann/streams.clj:824-839`). `SPEC.md` has no such parameter, so riemann-go implements only the per-event form, upstream's `ddt-events`.

**State.** Per fork key: the previous event that carried a metric. Nothing else, and no timer to age it out. The previous event is retained indefinitely, so a fork key that goes quiet for an hour and then reports once produces a rate computed over that hour.

**Timing.** Owns no timer.

**Edge cases.**

- An event with no `metric` is ignored entirely. It produces no output and it does not become the previous event, so the next event with a metric differentiates against the last event that had one, skipping the gap. Upstream's test drives four metric-less events through and expects no output (`test/riemann/streams_test.clj:986-987`).
- A metric of zero is a metric. The check is presence, not truthiness, and upstream's test differentiates a run beginning with two zeroes (`test/riemann/streams_test.clj:991-999`).
- The first event for a fork key produces no output. There is nothing to differentiate against, so it is recorded as the previous event and nothing is emitted.
- Two consecutive events with the same `time` produce no output, because the time delta is zero and the rate is undefined. The second event still becomes the previous event, so the gap is one emission rather than a stall. Upstream guards this with an explicit zero check (`src/riemann/streams.clj:819-822`).
- Time moving backwards produces a negative denominator and therefore a sign-flipped rate. Upstream does not guard it. riemann-go does not either, because the timer-ordering invariant does not forbid an out-of-order event and silently dropping one would hide a clock problem at the emitter rather than surfacing it. Classification: a hypothesis about which failure is more useful, not a measured result.
- The emitted event is the current event with only `metric` replaced. Its `time`, `host`, `service`, `state`, `tags` and `attributes` are the current event's, so a `ddt` feeding an ntfy leaf produces an alert whose state is the raw event's state and whose metric is a rate.
- A rate is emitted per event received, not per second. A fork key receiving ten events a second produces ten rate events a second.

**Worked example.** One fork key.

| t | metric in | metric out | note |
| --- | --- | --- | --- |
| 0 | 0 | nothing | first event |
| 1 | 0 | 0 | (0 - 0) / 1 |
| 2 | 2 | 2 | (2 - 0) / 1 |
| 2 | 7 | nothing | zero time delta |
| 4 | -4 | -5.5 | (-4 - 7) / 2, differentiated against the t=2 metric of 7 |
| 5 | absent | nothing | ignored, previous event unchanged |
| 6 | -4 | 0 | (-4 - -4) / 2, differentiated against t=4 |

**Provenance.** `src/riemann/streams.clj:809-839`, test at `test/riemann/streams_test.clj:984-999`.

## `stable`

**What it does.** Suppresses events whose named field is still changing, and releases them once that field has held one value for long enough. Events arriving after a change are buffered rather than dropped, and the whole buffer is released when the value has been stable across `duration_seconds` of event time. A value that changes again before then discards the buffer.

**Parameters.** `duration_seconds`, a float number of seconds of event time. `field`, the name of the event field whose value is watched. Upstream takes `dt` and an arbitrary function of the event (`src/riemann/streams.clj:1936-2030`); `SPEC.md` restricts the function to reading one named field.

**State.** Per fork key: the last observed value of `field`, a buffer of events awaiting release, and at most one armed timer with a generation number. The buffer is bounded by how many events arrive within `duration_seconds`, which is not bounded by this node; a fork key receiving a million events a second while flapping buffers a million events a second. Classification: a research question, recorded below, because the right bound depends on a workload nobody has measured.

**Timing.** A timer is armed when the watched value changes, due at the first buffered event's `time` plus `duration_seconds`. When it fires, the buffer is released downstream in arrival order if the value has not changed since. Under `SPEC.md` property 12 the timer fires before any event stamped at or after its due time is dispatched, which is what makes the release deterministic rather than dependent on scheduling.

riemann-go arms one timer per value change and stamps it with a generation number, discarding a timer whose generation is stale when it fires. Upstream arms a fresh task on every value change and never cancels the old ones, letting them fire and find nothing to do: "it's simpler to just add N tasks during a flapping state and let them all fight it out" (`src/riemann/streams.clj:2018-2027`). The two agree while event times move forward. They differ only when event times go backwards, where riemann-go honours the most recent value change and upstream's outcome depends on which task the scheduler runs first. This is the second of the two departures `SPEC.md` states.

**Edge cases.**

- The first event for a fork key is always buffered, never passed immediately. Upstream initialises the remembered value to a sentinel that no field value can equal (`src/riemann/streams.clj:1937`), so the first event takes the "value changed" path. A `stable` node therefore emits nothing for at least `duration_seconds` after a fork key appears.
- Once the buffer has been released, subsequent events carrying the same value pass through immediately, one at a time, with no further delay. Stability is a state the node enters and stays in until the value changes.
- The release condition on the arrival path compares the arriving event's `time` against the first buffered event's `time`, and the comparison is inclusive. An event arriving exactly `duration_seconds` after the first buffered event releases the buffer, including that arriving event. Upstream's second test case relies on this: with `dt` of 3 and events at times 0, 1 and 3, all three are emitted (`test/riemann/streams_test.clj:1457-1460`).
- Released events carry their original times, not the release time. A buffer of three events released at once appears downstream as three events whose timestamps are already in the past. A sink sees a burst, and an ntfy leaf under a `stable` node sends three notifications at once.
- A value change discards the buffer's contents. The buffered events are neither emitted nor deferred, and they are gone. This is the whole point of the combinator and it is also a silent loss, so those discards are counted like any other, per `INVARIANTS.md` I5.
- The timer path matters only when the value stops changing and no further events arrive. With events still arriving, the arrival path releases the buffer first. Upstream's fourth test case exercises the timer alone: a value that changes at t=1 and receives no further event releases at t=11 with `dt` of 10 (`test/riemann/streams_test.clj:1489-1495`).
- Upstream's timer checks the wall clock against the buffered event's `time` (`src/riemann/streams.clj:1968-1971`), which is why a task armed for an earlier change can fire, find the buffer head too young, and do nothing. riemann-go's generation number makes that check unnecessary.

**Worked example.** `duration_seconds` 3, `field` `state`, one fork key. This is upstream's spike test.

| t | state | buffer after | emitted |
| --- | --- | --- | --- |
| 0 | ok | [t=0] | nothing, first event |
| 3 | ok | [] | t=0 and t=3, released together |
| 4 | warning | [t=4] | nothing, value changed |
| 5 | warning | [t=4, t=5] | nothing, 5 - 4 is under 3 |
| 6 | ok | [t=6] | nothing, value changed again; t=4 and t=5 discarded |
| 9 | ok | [] | t=6 and t=9, released together |

The warning spike never reaches a sink. Four of six events are emitted, two are discarded, and the two released pairs arrive as bursts.

**Provenance.** `src/riemann/streams.clj:1936-2030`, tests at `test/riemann/streams_test.clj:1452-1507`.

## Indexing

**What the index holds.** The most recently indexed event for each identity, where identity is the `(host, service)` pair. One entry per identity, replaced in full on each insert. The index stores whole events, not a reduced summary, so `GET /index/{host}/{service}` returns every field the indexed event carried.

**What puts an event there.** A `{"sink":"index"}` leaf in a rule's tree. There is no implicit indexing: an event that matches no rule, or that matches a rule whose tree has no index leaf, is never indexed. An event may traverse several rules and reach several index leaves; the last one to run wins, and which that is depends on rule evaluation order, which `SPEC.md` does not pin.

**What an insert does.** It replaces any existing entry for the identity, including the entry's expiry deadline, which becomes the new event's `time` plus its `ttl`. An insert of an event whose `time` is older than the stored entry's still replaces it, because the index holds the last event indexed, not the newest event by timestamp. Upstream does the same, storing unconditionally into a map keyed by host and service (`src/riemann/index.clj:98-101`).

**An event whose state is already `expired`.** Inserting it deletes the entry for that identity rather than storing it. Upstream branches on this in `insert` itself (`src/riemann/index.clj:98-100`). The consequence is that a client can retire an identity from the index by sending one event with state `expired`, and that a synthesized expiry event routed back into an index leaf removes rather than re-adds the entry. Removing an entry this way does not synthesize a second expiry event: the event that caused the removal has already been dispatched to the rules, which is what `INVARIANTS.md` I7 requires, and re-dispatching would loop.

**What `GET` returns.** `SPEC.md` property 5: the most recently indexed event for the identity, or `404` when there is no entry or the entry has expired. An entry whose deadline has passed but whose expiry has not yet been processed reads as absent, so the read surface never returns an entry the reaper is about to remove.

**Comparison at the boundary.** Upstream expires an entry when its age is strictly greater than its ttl (`src/riemann/index.clj:80-84`), so an entry read at exactly `time + ttl` is still present. riemann-go keeps that: the entry is live on the closed interval `[time, time + ttl]` and expires after it. Classification: a default carried from upstream, not an invariant. Nothing in the fleet's rules depends on the boundary, and the value that would revise it is a rule whose alert timing sits inside one ttl.

**`ttl` of zero or negative.** Upstream's expiry check applies the same comparison, so a ttl of zero expires the entry at the first reap after insertion, and a negative ttl expires it immediately. Neither is rejected at ingest. `SPEC.md`'s event model gives `ttl` a default of 60 and does not constrain its range. See Open questions.

**Provenance.** `src/riemann/index.clj:60-113`, defaults at `src/riemann/index.clj:44`.

## Expiry

**When it fires.** When an indexed entry's `time` plus `ttl` passes. riemann-go decides this against the same clock it uses to order timers, so `SPEC.md` property 12 governs the interleaving: an expiry due at or before time T is delivered before an event stamped T.

Upstream instead runs a reaper on a fixed interval, ten seconds by default and sixty in the fleet's configuration, which scans the index and expires everything it finds overdue (`src/riemann/core.clj:274-308`). An entry's expiry event therefore arrives somewhere between zero and one reaper interval after its deadline. riemann-go's expiry latency is the loop's timer granularity instead, which means entries will expire sooner than they do today. That is a behavior change, and it is worth one observation before a rule depends on it.

**The shape of the synthesized event.** Upstream copies only `host` and `service` from the expiring entry, sets `state` to `expired`, and sets `time` to the instant of expiry (`src/riemann/core.clj:295-305`, with `keep-keys` defaulting to `[:host :service]`). Everything else is dropped: `metric`, `ttl`, `tags`, `attributes` and `description` are absent from the synthesized event and take the defaults in `SPEC.md`'s event model. So the expiry event for an entry that carried a metric of 1234 and a tag of `agent-obs` carries no metric and no tags, and a rule matching `tagged("agent-obs") and state == "expired"` never fires. That is the single most commonly missed fact about expiry, and it is invisible in any docstring.

The expired event's `ttl` is 0, not the expiring entry's ttl and not the model default. `SPEC.md` property 4 pins it: the event's content is that an identity stopped being live, and a sixty second lease would assert the opposite. The schema's absent-field default does not apply, because the event is synthesized rather than ingested. Nothing re-indexes it unless a rule routes it to an index leaf, in which case the insert deletes rather than stores, per the indexing section above.

**Delivery.** The synthesized event goes to the rule set exactly as an ingested event does. It is matched by each rule's `match` expression, traverses combinators with no special casing, and reaches sinks. `SPEC.md` property 4 is this, and a rule matching `state == "expired"` fires at a sink with no further ingest, which is `INVARIANTS.md` I7's acid test.

**The `expired` name.** `SPEC.md`'s expression language supplies `expired` as a boolean that is true when the event was produced by index expiry rather than by ingest. That is a stronger statement than `state == "expired"`, which a client can also produce by sending such an event. An author who wants to catch both writes the state comparison; one who wants only the index's own signal writes `expired`. Upstream has no equivalent distinction: its `expired?` predicate is true both for state `expired` and for any event whose `time` plus `ttl` has passed (`src/riemann/streams.clj:50-59`), which means an upstream rule cannot tell a reaper event from a stale one.

**Removal ordering.** The entry is removed as a consequence of the expiry event having been dispatched, never in place of it. `INVARIANTS.md` I7 requires that every removal site sit downstream of the dispatch.

**Provenance.** `src/riemann/core.clj:274-308`, `src/riemann/index.clj:74-88`, `src/riemann/streams.clj:50-59`.

## The two departures from upstream

`SPEC.md`'s rule document section names both and this section states them behaviorally. Where upstream and riemann-go differ, riemann-go is right.

### `by` frees a fork

Upstream never frees one, and says so: "`(by)` streams are never garbage-collected" (`src/riemann/streams.clj:1577-1580`). Its fork table grows for the life of the process, so a fleet that cycles through container hostnames leaks a subtree per hostname until restart.

riemann-go frees a fork once the key's expired event has passed through it and no timer beneath it still references it. The timer condition matters: a `stable` node with an armed timer and a buffer of events holds a release that has not happened, and freeing the fork under it would discard those events without the node ever deciding to.

The observable consequence is a re-firing. An identity that expires and later returns arrives at a fresh fork, so a `changed-state` beneath the `by` compares the returning event against `initial` rather than against the state that identity held before it expired. Concretely: a host reporting `critical`, then falling silent until it expires, then returning with `critical`, produces an alert on its return. Under upstream it produces none, because the fork remembered `critical` across the silence.

Classification: a heuristic, not an invariant. It carries two counters, forks live and forks freed, so its effect can be watched, and it retires if that re-firing turns out to be unwanted. The check is a scenario that expires an identity, re-ingests it with the state it last held, and expects an alert.

The rule is stated in terms of "the key's expired event", which is well defined when the `by` forks on `host`, on `service`, or on both, because an expiry event carries those two fields and nothing else. It is not well defined when the `by` forks on any other field, since the synthesized expiry event does not carry that field's original value. See Open questions.

### `stable` arms one timer per value change

Upstream schedules several timers and lets them race, by design and with a comment saying so (`src/riemann/streams.clj:2018-2027`). Each value change adds a task, no task is ever cancelled, and a stale task fires, finds the buffer head too young or already flushed, and returns.

riemann-go arms one generation-numbered entry per value change and discards stale generations when they fire. The two agree while event times move forward, which is every case upstream's tests cover. They differ when event times go backwards: riemann-go honours the most recent value change, and upstream's outcome depends on which of its several pending tasks the scheduler runs first, which is to say it has no defined outcome.

Classification: a correction, not a preference. `SPEC.md` property 12 is what makes the single-entry form reproducible under a scenario's timeline, and a behavior that depends on task ordering cannot be asserted in a scenario at all.

## Open questions

Each of these is a gap this extraction found and did not fill. An invented answer would re-roll differently on every generation, so each one is a spec edit rather than an implementation decision.

1. **An expression referencing `metric` on an event that carries none.** `SPEC.md` fixes the reading of an absent attribute as `""` and gives every other event field a default, but `metric` is documented as "absent, which is distinct from `0`" with no reading fixed for an expression. `where` with `expr` of `"metric > 10"` on a metric-less event could be a non-match, a compile-time-detectable error, or a runtime error that drops the event and counts it. Read: `SPEC.md` event model table and expression language section.

2. **Whether `throttle`'s window anchor is per fork key or per node.** This document states per fork key, opened by the first event for that key, on the grounds that a node-wide anchor is either wall-clock derived and unreproducible or arbitrary. But a per-key anchor means two identities under one `by` have windows offset from each other, and a rule author reasoning about "at most 5 alerts per 5 minutes" across a fleet gets a different bound than they expect. `SPEC.md` property 9 says "in any `window_seconds` interval", which reads as a sliding window and is neither of these. Read: `src/riemann/streams.clj:595-661`, `src/riemann/time.clj:83-90`, `SPEC.md` property 9.

3. **What `splitp` does when no branch matches and `otherwise` is absent.** `SPEC.md`'s combinator table lists `otherwise` among `splitp`'s keys without marking it optional or required. Upstream's two implementations disagree: `splitp` throws (`src/riemann/streams.clj:1855-1916`) and `split*` drops silently (`src/riemann/streams.clj:1818-1835`). The candidates for riemann-go are to require `otherwise` at `PUT` and reject a rule without it, or to allow its absence and drop the event while counting the drop. The first is closer to `INVARIANTS.md` I5, since a dropped event needs a counter and a node that exists only implicitly has nowhere to put one. Read: `SPEC.md` rule document combinator table.

4. **What a `set` expression evaluating to null does.** Upstream's `with` deletes the key (`src/riemann/streams.clj:1364-1366`). riemann-go's event model has a default for every field except `metric`, so deletion and "set to the default" are the same thing for nine of ten fields and differ only for `metric`. Whether a `set` on `metric` evaluating to null produces an event with no metric, which an influx leaf would then refuse to write and count as dropped, is unstated. Read: `SPEC.md` event model and alert shape sections.

5. **Whether `set` may produce an empty `host` or `service`.** Ingest rejects such an event with `400` per property 3, but a `set` node is downstream of that check and nothing re-validates. The resulting event has no usable identity, and routing it to an index leaf would create an entry keyed on an empty string. The candidates are to reject the expression at `PUT`, which is not decidable, or to check at evaluation and drop with a counter. Read: `SPEC.md` property 3 and property 11.

6. **Which fork a non-identity `by` frees.** The fork-freeing rule is stated in terms of the key's expired event, and a synthesized expiry event carries only `host` and `service`. A `by ["attributes.run_id"]` therefore has no expiry event that names its key, and its forks are freed by nothing. The candidates are to free forks only when the `by` fields are a subset of host and service, and to say so, or to give `by` a separate idle-timeout parameter. Read: `src/riemann/core.clj:295-305`, `SPEC.md` rule document departures section.

7. **Whether a `ttl` of zero or a negative `ttl` is accepted at ingest.** `SPEC.md`'s event model gives `ttl` a default of 60 and no range. Upstream accepts both and expires the entry at the first reap. Under riemann-go's timer-driven expiry a zero ttl expires the entry effectively at insertion, which makes a `{"sink":"index"}` leaf into an immediate expiry generator, and a negative ttl expires it in the past. Read: `SPEC.md` event model table, `src/riemann/index.clj:80-84`.

8. **Whether `stable`'s buffer is bounded.** The buffer grows with the arrival rate during a flapping period and is bounded by nothing in this node. `INVARIANTS.md` I4 requires that every queue in the process declare a capacity, a policy and a counter, and a `stable` buffer is a queue by that definition. `SCALE.md` does not name it. Whether that is an omission in `SCALE.md` or a decision that the buffer is not a queue is unresolved. Read: `INVARIANTS.md` I4, `SPEC.md` combinator table, `src/riemann/streams.clj:1936-2030`.

9. **Rule evaluation order when several rules index the same identity.** The index holds the last event indexed for an identity, and two rules whose trees both reach an index leaf for one event both write. `SPEC.md` does not pin the order in which rules are evaluated, so the stored entry depends on an order that is the implementation's choice. Whether that is intentional, and whether a scenario could ever assert on it, is unresolved. Read: `SPEC.md` property 5 and the rule document section.
