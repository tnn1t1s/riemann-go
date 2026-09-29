package engine

import "strconv"

// Reading is one number the process reports about itself, under the service
// name a self-observation event carries. The metrics endpoint renders the
// same readings, so the two cannot disagree on a name.
type Reading struct {
	// Service is the event's service, for example riemann.sink.ntfy.dropped.
	Service string
	// Attributes distinguish readings that share a service: the shard, or
	// the rule, version and node.
	Attributes map[string]string
	Value      float64
	// Counter is true for a count that only grows and false for a gauge.
	Counter bool
	// PerNode marks a reading that exists once per rule node. A sampler
	// leaves out the ones still at zero, since a node that has discarded
	// nothing has nothing to report.
	PerNode bool
}

func queueReadings(queue string, attrs map[string]string, q QueueStats) []Reading {
	return []Reading{
		{Service: "riemann." + queue + ".depth", Attributes: attrs, Value: float64(q.Depth)},
		{Service: "riemann." + queue + ".capacity", Attributes: attrs, Value: float64(q.Capacity)},
		{Service: "riemann." + queue + ".dropped", Attributes: attrs, Value: float64(q.Dropped), Counter: true},
	}
}

// Readings lists the snapshot as named numbers. Readings that share a service
// are adjacent.
//
// SPEC-GAP: the spec fixes `riemann.<queue>.depth|capacity|dropped` and gives
// one queue name by example, `sink.ntfy`. Chosen for the rest:
// `sink.influx`, `shard.inbox` with a `shard` attribute, `subscribe` summed
// over subscribers, and `stable.buffer` summed over stable nodes. The spec
// names no service for per-node firing counts, for the two fork counters, or
// for the index's refusals beyond `riemann.index.rejected`. Chosen:
// `riemann.rule.passed` with the attributes `riemann.rule.discarded`
// carries, `riemann.by.forks_live` and `riemann.by.forks_freed`. The
// attribute keys of `riemann.rule.discarded` are not named either. Chosen:
// `rule`, `version` and `node`, the keys the alert's provenance uses.
func (snap Snapshot) Readings() []Reading {
	var out []Reading
	for _, name := range snap.SinkNames {
		st := snap.Sinks[name]
		out = append(out, queueReadings("sink."+name, nil, QueueStats{Depth: st.Depth, Capacity: st.Capacity, Dropped: st.Dropped})...)
	}

	shardAttrs := make([]map[string]string, len(snap.Shards))
	for i, s := range snap.Shards {
		shardAttrs[i] = map[string]string{"shard": strconv.Itoa(s.Shard)}
	}
	perShard := func(service string, counter bool, value func(ShardStats) float64) {
		for i, s := range snap.Shards {
			out = append(out, Reading{Service: service, Attributes: shardAttrs[i], Value: value(s), Counter: counter})
		}
	}
	perShard("riemann.shard.inbox.depth", false, func(s ShardStats) float64 { return float64(s.Inbox.Depth) })
	perShard("riemann.shard.inbox.capacity", false, func(s ShardStats) float64 { return float64(s.Inbox.Capacity) })
	perShard("riemann.shard.inbox.dropped", true, func(s ShardStats) float64 { return float64(s.Inbox.Dropped) })
	perShard("riemann.shard.inbox.stalled_offers", true, func(s ShardStats) float64 { return float64(s.StalledOffers) })
	perShard("riemann.shard.loop_lag", false, func(s ShardStats) float64 { return s.LoopLag })
	perShard("riemann.shard.index_entries", false, func(s ShardStats) float64 { return float64(s.IndexEntries) })
	perShard("riemann.shard.in_flight", false, func(s ShardStats) float64 { return float64(s.InFlight) })
	perShard("riemann.shard.processed", true, func(s ShardStats) float64 { return float64(s.Processed) })

	out = append(out, queueReadings("subscribe", nil, snap.Subscribe)...)
	out = append(out, queueReadings("stable.buffer", nil, snap.StableBuffer)...)
	out = append(out,
		Reading{Service: "riemann.ingest.accepted", Value: float64(snap.IngestAccepted), Counter: true},
		Reading{Service: "riemann.ingest.rejected", Value: float64(snap.IngestRejected), Counter: true},
		Reading{Service: "riemann.index.rejected", Value: float64(snap.IndexRejected), Counter: true},
		Reading{Service: "riemann.by.forks_live", Value: float64(snap.ForksLive)},
		Reading{Service: "riemann.by.forks_freed", Value: float64(snap.ForksFreed), Counter: true},
	)
	for _, name := range snap.SinkNames {
		st := snap.Sinks[name]
		out = append(out,
			Reading{Service: "riemann.sink." + name + ".accepted", Value: float64(st.Accepted), Counter: true},
			Reading{Service: "riemann.sink." + name + ".processed", Value: float64(st.Processed), Counter: true},
			Reading{Service: "riemann.sink." + name + ".in_flight", Value: float64(st.InFlight)},
		)
	}
	out = append(out, Reading{Service: "riemann.accounting.residual", Value: float64(snap.Residual)})

	nodeAttrs := make([]map[string]string, len(snap.Rules))
	for i, n := range snap.Rules {
		nodeAttrs[i] = map[string]string{"rule": n.Rule, "version": strconv.Itoa(n.Version), "node": n.Node}
	}
	for i, n := range snap.Rules {
		out = append(out, Reading{Service: "riemann.rule.passed", Attributes: nodeAttrs[i], Value: float64(n.Passed), Counter: true, PerNode: true})
	}
	for i, n := range snap.Rules {
		if n.Discards {
			out = append(out, Reading{Service: "riemann.rule.discarded", Attributes: nodeAttrs[i], Value: float64(n.Discarded), Counter: true, PerNode: true})
		}
	}
	return out
}
