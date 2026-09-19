package engine

import (
	"strconv"

	"github.com/tnn1t1s/riemann-go/rules"
	"github.com/tnn1t1s/riemann-go/sinkqueue"
)

// Sample is one self-observation reading. Service is the contract name a rule
// matches on; Attrs distinguish readings that share a service.
type Sample struct {
	Service string
	Attrs   map[string]string
	Value   float64
}

// AddSamples registers an edge component's gauges (the HTTP transport's
// in-flight count, say) so they appear in the same snapshot.
func (e *Engine) AddSamples(fn func() []Sample) {
	e.extraMu.Lock()
	e.extra = append(e.extra, fn)
	e.extraMu.Unlock()
}

// SinkStats is a reading of every sink queue, keyed by sink name.
func (e *Engine) SinkStats() map[string]sinkqueue.Stats {
	out := make(map[string]sinkqueue.Stats, len(e.sinks))
	for name, s := range e.sinks {
		out[name] = s.Stats()
	}
	return out
}

// Snapshot is the core's whole self-observation surface. The core exposes it
// and depends on nothing that samples it (INVARIANTS.md I8): the sampler that
// turns it into events and the handler that renders it as Prometheus text
// both live at the edge.
//
// SPEC-GAP: the spec fixes `riemann.<queue>.depth|capacity|dropped` and gives
// `riemann.sink.ntfy.dropped` as the only worked queue name. Chosen queue
// names: `sink.ntfy`, `sink.influx`, `shard.inbox` (one reading per partition,
// told apart by the `shard` attribute), `subscribe` (depth summed over
// subscribers, capacity per subscriber) and `stable.buffer` (depth summed over
// fork keys, capacity per fork key).
func (e *Engine) Snapshot() []Sample {
	var out []Sample
	add := func(service string, v float64, attrs map[string]string) {
		out = append(out, Sample{Service: service, Attrs: attrs, Value: v})
	}
	queue := func(name string, depth, capacity int, dropped int64, attrs map[string]string) {
		add("riemann."+name+".depth", float64(depth), attrs)
		add("riemann."+name+".capacity", float64(capacity), attrs)
		add("riemann."+name+".dropped", float64(dropped), attrs)
	}

	var residual int64
	for _, name := range []string{rules.SinkNtfy, rules.SinkInflux} {
		s := e.sinks[name]
		if s == nil {
			continue
		}
		st := s.Stats()
		queue("sink."+name, st.Depth, st.Capacity, st.Dropped, nil)
		residual += st.Residual()
	}
	// The one claim among the readings: accepted - (processed + dropped +
	// queued + in_flight), summed over the sinks. Each sink's terms are read
	// under one lock, so a correct queue contributes exactly zero.
	add("riemann.accounting.residual", float64(residual), nil)

	var indexRejected int64
	for _, s := range e.shards {
		attrs := map[string]string{"shard": strconv.Itoa(s.id)}
		// The inbox's policy is block-with-deadline, so its `dropped` is the
		// events refused with 429 at this inbox; they sum to
		// riemann.ingest.rejected.
		queue("shard.inbox", len(s.inbox), cap(s.inbox), s.rejected.Load(), attrs)
		add("riemann.shard.inbox.stalled", float64(s.stalled.Load()), attrs)
		add("riemann.shard.loop_lag", float64(s.loopLagNs.Load())/1e9, attrs)
		add("riemann.shard.index_entries", float64(s.idx.Len()), attrs)
		add("riemann.shard.in_flight", float64(s.inFlight.Load()), attrs)
		add("riemann.shard.processed", float64(s.processed.Load()), attrs)
		indexRejected += s.idx.Rejected()
	}
	add("riemann.index.rejected", float64(indexRejected), nil)
	queue("subscribe", e.subs.depth(), e.Params.SubscribeQueueCapacity, e.subs.dropped.Load(), nil)
	shared := e.Store.Shared
	// A stable eviction is counted once, at its node, as
	// riemann.rule.discarded; this `dropped` is the same evictions summed, a
	// second view of that counter rather than a second count.
	queue("stable.buffer", int(shared.StableDepth.Load()), shared.StableBufferCapacity, shared.StableEvicted.Load(), nil)
	add("riemann.ingest.accepted", float64(e.accepted.Load()), nil)
	add("riemann.ingest.rejected", float64(e.rejected.Load()), nil)

	for _, c := range e.Store.Current().Rules {
		for _, path := range c.Live.Paths {
			ctr := c.Live.ByPath[path]
			// SPEC-GAP: the spec says riemann.rule.discarded carries "the
			// rule id, the rule version and the node path as attributes"
			// without naming the keys. Chosen: `rule`, `version`, `node`,
			// the names the alert provenance uses for the same three things.
			attrs := map[string]string{"rule": c.ID, "version": strconv.Itoa(c.Version), "node": path}
			discarded := ctr.Discarded.Load()
			switch ctr.Op {
			case "throttle", "stable":
				add("riemann.rule.discarded", float64(discarded), attrs)
			default:
				if discarded > 0 {
					add("riemann.rule.discarded", float64(discarded), attrs)
				}
			}
			if ctr.Op == "by" {
				// SPEC-GAP: the fork-freeing heuristic "carries two counters,
				// forks live and forks freed" with no names given. Chosen:
				// riemann.rule.forks_live and riemann.rule.forks_freed.
				add("riemann.rule.forks_live", float64(ctr.ForksLive.Load()), attrs)
				add("riemann.rule.forks_freed", float64(ctr.ForksFreed.Load()), attrs)
			}
		}
		if n := c.Live.MatchErrors.Load(); n > 0 {
			add("riemann.rule.match_errors", float64(n),
				map[string]string{"rule": c.ID, "version": strconv.Itoa(c.Version)})
		}
	}

	e.extraMu.Lock()
	extra := append([]func() []Sample(nil), e.extra...)
	e.extraMu.Unlock()
	for _, fn := range extra {
		out = append(out, fn()...)
	}
	return out
}
