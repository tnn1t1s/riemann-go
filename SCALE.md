# SCALE.md

> **Backpressure is behavior, not a tuning exercise. Every queue is bounded, every drop is counted, and the emitter is told.**

This document states riemann-go's operational floors as behavior a scenario can assert, and names every parameter that can change that behavior, with its owning layer, its units, its default and the reason for that default. Defaults here are provisional: each names the observation that would revise it.

Read alongside `SPEC.md` (what riemann-go must do), `INVARIANTS.md` (I4, I5 and I9 in particular), and `HARNESS.md` (how the flood is driven and graded).

## Principle

The mechanism is the generator's choice. The floors are not. A generation may hash events by host across several loops, run one loop, or put a mutex around a shared map, and the scenarios cannot tell the difference. What they can tell is whether a sustained burst produces counted drops with exact accounting and bounded memory, or whether it produces a stall, a memory climb, or a number that does not add up.

That is the point of writing scale as behavior. The sink receiver cannot see the runtime shape, so `INVARIANTS.md` I1's oracle says nothing about it. The flood floor below is what keeps the shape honest without the arena having to inspect generated code.

## The admission path

```
emitter --POST /events--> ingest handler --offer(deadline)--> partition inbox (bounded)
             ^                  |                                      |
             |                  | inbox full past deadline             v
         429 + Retry-After <----+                              processing loop
             |                                                        |
         202 {accepted, sinks}                                         v
                                                       sink queues (bounded, shed-newest)
                                                                       |
                                                                       v
                                                            ntfy / influx / subscribers
```

Ingest never processes. The handler decodes the batch, routes each event to a partition, and offers it with a deadline. While the offer waits the handler goroutine is parked, which is TCP backpressure to the client at no cost to the process. If every event is admitted the reply is `202`; if the deadline passes on any inbox the reply is `429` with `Retry-After`, and the body says how many were admitted before the stall. Admitted events are never rolled back.

Nothing between the socket and the sink queues drops. The sinks shed, and shedding is counted.

## Bounded stages

Each row is a queue that must declare its capacity, its policy and its counter, per `INVARIANTS.md` I4.

| Boundary | Bound | Behavior when full | What the emitter sees | Counters |
| --- | --- | --- | --- | --- |
| HTTP server | `ingest.max_inflight_requests` | new connections wait in the kernel accept queue | a slow connect | in-flight gauge |
| Request body | `ingest.max_batch_events`, `ingest.max_batch_bytes` | `413` | an error status | rejected count |
| Partition inbox | `shard.inbox_capacity` | offer blocks up to `ingest.admission_deadline`, then `429` | `429` with `Retry-After: 1` | depth, capacity, stalled offers, rejected events |
| Processing loop | one unit of concurrency per partition | never blocks on a sink | nothing | events processed, loop lag |
| Sink queue | `sink.<name>.queue_capacity` | shed newest and count it | the `sinks` object of the next `202` | depth, capacity, dropped |
| Subscriber queue | `subscribe.queue_capacity` | shed and send an SSE `lagged` frame carrying the count | not applicable | dropped per subscriber |

## Parameters

Every default below is provisional. The flood floor is the observation that revises most of them.

| Parameter | Owner | Units | Default | Reason |
| --- | --- | --- | --- | --- |
| `ingest.max_inflight_requests` | HTTP transport | requests | 256 | Bounds parked handler goroutines. Beyond it, new connections wait in the kernel accept queue rather than allocating in the process. |
| `ingest.max_batch_events` | HTTP transport | events | 1000 | One atlas fan-out burst fits in a single batch; larger bodies are a client bug and get `413`. |
| `ingest.max_batch_bytes` | HTTP transport | bytes | 1 MiB | A body cap independent of event count, so one pathological event cannot carry an arbitrary payload. |
| `ingest.admission_deadline` | HTTP transport | milliseconds | 200 | Shorter than any HTTP client's default timeout, and longer than an estimated 50 ms rule burst, so an ordinarily busy loop does not surface as `429`. The 50 ms is an estimate, not a measurement. |
| `shard.inbox_capacity` | partition | events | 4096 | An agent session's tool-call flurry is tens of events, so the deadline rather than the inbox is what handles a thousand saturating sessions. The admission probe filled 4096 only when the loop was given 1 to 5 ms of work per event. |
| `sink.ntfy.queue_capacity` | sink | events | 1000 | Carried from the fleet's current configuration. |
| `sink.influx.queue_capacity` | sink | events | 10000 | Carried from the fleet's current configuration, an order above ntfy because writes batch and alerts do not. |
| `subscribe.queue_capacity` | HTTP transport | events per subscriber | 1000 | The same order as the ntfy sink; a subscriber past it receives a `lagged` frame rather than slowing the loop. |
| `stable.buffer_capacity` | rule node | events per fork key | 1024 | `stable` holds events while a value is unsettled, and a flapping key can hold them for as long as it keeps flapping, so the buffer is a queue and `INVARIANTS.md` I4 requires it to declare itself. Policy is drop-oldest, since a release is meant to show the recent run of a settled value rather than the whole history. Each eviction counts as `riemann.rule.discarded`. Provisional: nothing has measured a real buffer depth. |
| `index.max_entries_per_shard` | index | entries | 1000000 | At an estimated 300 bytes per entry this is 300 MB per partition, which is right for a hot partition and wrong for a small host. On overflow the insert is refused and counted as `riemann.index.rejected`, never silently evicted. |
| `shard.ring_events` | partition | events | 100000 | The ring serves dry run and `GET /events` and nothing else. |
| `shard.ring_bytes` | partition | bytes | 64 MiB | Whichever of the two ring bounds fills first wins, so one large event cannot defeat the count bound. |
| `engine.shards` | engine | partitions | `runtime.GOMAXPROCS(0)` | Set by `--shards`. Changing it requires a restart. |
| `self.interval` | process | seconds | 10 | The sampling period for self-observation events, with a `ttl` of twice the interval, carried from upstream's instrumentation rule (`src/riemann/core.clj:44-46`). |

Per-source ingest quotas are a research question rather than a parameter in v0. The trigger that would open it is one emitter observed starving others at an inbox.

## The accounting identity

For each sink, over any window:

```
accepted = processed + dropped + queued + in_flight
```

`accepted` counts events routed to that sink by a rule. `processed` counts those the sink receiver observed. `dropped` counts those shed at the sink's bounded queue. `queued` is the queue's depth at the instant of measurement. `in_flight` counts the events held between those states: one in the loop mid-dispatch, one in the sink worker mid-request.

The identity is exact, not approximate. The admission probe found it off by exactly two without in-flight gauges: one event held by the loop, one by the sink worker (observation, `probes/admission`, single partition, Apple M3, macOS 23.6.0, Go 1.26.3). A generation that omits the gauges produces a number that is close and wrong, which is the failure mode this floor exists to catch.

At the ingest boundary the identity is simpler and equally exact: across a batch, `accepted + rejected` equals the number of events in the body. The same probe held that in all four of its cases.

## The flood floor

A sustained burst of 1,000 events per second for 60 seconds, against a sink slow enough to saturate, must produce:

1. Counted drops. The sink's `dropped` counter is non-zero, and every discarded event incremented exactly one counter.
2. Exact accounting. The identity above closes at the end of the window, with no residue.
3. Bounded memory. Resident memory at the end of the window is within a stated bound of resident memory at the start. Growth proportional to events ingested is a failure.
4. A live read surface. `GET /index` and `GET /subscribe` keep answering throughout. A monitor that goes blind under the load it is monitoring has failed at its job.
5. No stall. Every `POST /events` is answered within the admission deadline with `202` or `429`, per `INVARIANTS.md` I9.

1,000 events per second is an estimate of the fleet's burst, derived from emitter cadences in source rather than measured: nine hosts, a heartbeat per session every 30 seconds, roughly seven events per agent turn, and a fan-out factor the atlas warns about at eight subagents. It is the floor riemann-go must not fail, not a capacity claim.

The margin over that floor is large. One loop carried 3.4 million events per second through `where -> by host -> changed-state -> counting sink` with expiry running, at 0.035 allocations per event and 6 MB of heap growth over a million events (observation, `probes/throughput`, Apple M3, 8 cores, Go 1.26.3, one goroutine, 1,000,000 events from 1,000 hosts, synthetic clock advancing 1 ms per event). Three orders of magnitude of headroom is why the flood floor is about accounting and memory rather than about throughput.

What that probe did not observe: garbage collection over minutes, index growth over minutes, and behavior under the race detector. Its runs were 0.3 seconds long. The flood scenario is where those get observed.

## What a 429 means, and what it does not

A `429` reports loop saturation only. A slow sink alone never produces one, because the loop never blocks on a sink.

The admission probe measured this directly. With a sink sleeping 50 milliseconds per event and no work in the loop, 10,000 events in 100 batches produced 100 replies of `202`, zero replies of `429`, a maximum inbox depth of 98, and 8,998 events dropped at the sink (observation, `probes/admission`, four cases, single partition, Apple M3, Go 1.26.3, stdlib Python client). Only loop cost filled the inbox: adding 5 ms of work per event turned the same load into 41 replies of `202` and 59 of `429`, with the inbox pinned at its 4,096 capacity.

| Case | 202 | 429 | Accepted | Rejected | Max inbox | Sink dropped | Sink processed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Slow sink only | 100 | 0 | 10,000 | 0 | 98 | 8,998 | 1 |
| Sink sleep zero | 100 | 0 | 10,000 | 0 | 98 | 0 | 10,000 |
| Slow sink plus 5 ms loop work | 41 | 59 | 6,327 | 3,673 | 4,096 | 992 | 238 |
| Slow sink plus 1 ms loop work, 8 clients | 41 | 59 | 5,538 | 4,462 | 4,096 | 409 | 32 |

The consequence for anyone reading a `202`: sink saturation is visible through the `dropped` counter and the `sinks` object of the reply, and nowhere else. This is why `SPEC.md` puts that object in the `202` body. A reply that said only `{"accepted":100}` would be true and useless, since an emitter could not distinguish delivery from shedding.

## Index cardinality

Each partition's index is bounded by `index.max_entries_per_shard`. On overflow the insert is refused and counted as `riemann.index.rejected`; nothing is evicted to make room, because an eviction policy would silently decide which identity stops being monitored.

The peak is a research question rather than a known number. One agent session is one host, holding roughly ten services, and a session's entries expire within one TTL after it ends. How many dead sessions sit in the index at fan-out scale has not been measured. The triggers that would open the question are `riemann.index.rejected` going above zero, or the milestone 1 mirror showing growth that does not flatten.

Expiry is `O(log n)` per entry rather than a scan of every entry on a fixed tick, which changes when an entry expires as well as what it costs. The fleet's Clojure server reaps every 60 seconds (`ansible/roles/medios_riemann/templates/riemann.config.j2`), so entries will expire sooner than they do today by up to that interval. That is a behavior change, and it deserves one observation before a rule depends on expiry timing.

## The ring

The ring is per-partition, in memory, bounded by `shard.ring_events` and `shard.ring_bytes` with the first bound to fill winning. It serves dry run and `GET /events`, and nothing else. It is lost on restart, which is stated in `README.md` as a known limitation rather than hidden.

An on-disk segment log behind the ring is a research question. Its triggers: a restart observed to cause a missed alert that mattered, or a dry run over more than the ring's window requested twice. Nothing in the fleet's current rules needs more than a few minutes of history.

## What is intentionally not here

- **Multi-node and replication.** riemann-go is single-node. Restart is the recovery mechanism, and every emitter re-states on a heartbeat within one TTL.
- **Persistence of stream state across restart.** The cost is a serializer for every combinator's state, timer reconstruction against a clock that jumped, and a defined meaning for a window that straddles the gap. The benefit on this fleet is small. Recorded as a research question whose trigger is a missed alert attributable to a restart.
- **Per-source quotas.** Recorded above, not built.
- **Latency targets on the read path.** The read surface answers from memory and no scenario has found a number worth pinning. A floor lands when a read is observed to be slow, not before.
- **Horizontal scale of the sinks.** ntfy and InfluxDB are somebody else's capacity problem. riemann-go's obligation stops at a bounded queue and an honest counter.
