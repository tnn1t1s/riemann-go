package engine

import (
	"math"
	"sort"
)

// QueueStats is the depth, capacity and dropped counter of one bounded queue.
type QueueStats struct {
	Depth    int
	Capacity int
	Dropped  uint64
}

// ShardStats is one partition's readings.
type ShardStats struct {
	Shard int
	// Inbox.Dropped counts events refused at this inbox after the admission
	// deadline. The inbox itself never discards an admitted event.
	Inbox QueueStats
	// StalledOffers counts offers that found the inbox full and had to wait.
	StalledOffers uint64
	Processed     uint64
	InFlight      int64
	LoopLag       float64 // seconds between admission and dispatch, last event
	IndexEntries  int64
	IndexRejected uint64
}

// RuleNodeStats is one node of one installed rule.
type RuleNodeStats struct {
	Rule      string
	Version   int
	Node      string
	Passed    uint64
	Discarded uint64
	Discards  bool
}

// Snapshot is everything the engine reports about itself. The core exposes
// it and does nothing with it; the metrics endpoint and the self-observation
// sampler are adapters that read it.
type Snapshot struct {
	Shards    []ShardStats
	SinkNames []string
	Sinks     map[string]SinkStats
	Subscribe QueueStats
	// StableBuffer sums every stable node's buffers. Capacity is per fork
	// key, as stable.buffer_capacity is. Dropped counts events a buffer let
	// go without releasing: evictions at capacity and the contents discarded
	// when the watched value changed.
	StableBuffer   QueueStats
	IngestAccepted uint64
	IngestRejected uint64
	IndexRejected  uint64
	ForksLive      int64
	ForksFreed     uint64
	Rules          []RuleNodeStats
	// Residual is accepted - (processed + dropped + queued + in_flight),
	// summed over the sinks.
	Residual int64
}

// Snapshot reads the engine's counters.
func (e *Engine) Snapshot() Snapshot {
	snap := Snapshot{
		Sinks:          map[string]SinkStats{},
		IngestAccepted: e.accepted.Load(),
		IngestRejected: e.rejected.Load(),
		ForksLive:      e.forksLive.Load(),
		ForksFreed:     e.forksFreed.Load(),
	}
	for _, s := range e.shards {
		st := ShardStats{
			Shard: s.id,
			Inbox: QueueStats{
				Depth:    len(s.inbox),
				Capacity: cap(s.inbox),
				Dropped:  s.rejected.Load(),
			},
			StalledOffers: s.stalled.Load(),
			Processed:     s.processed.Load(),
			InFlight:      s.inFlight.Load(),
			LoopLag:       math.Float64frombits(s.loopLag.Load()),
			IndexEntries:  s.index.size.Load(),
			IndexRejected: s.index.rejected.Load(),
		}
		snap.IndexRejected += st.IndexRejected
		snap.Shards = append(snap.Shards, st)
	}
	for name, sink := range e.sinks {
		st := sink.Stats()
		snap.Sinks[name] = st
		snap.SinkNames = append(snap.SinkNames, name)
		snap.Residual += int64(st.Accepted) - int64(st.Processed) - int64(st.Dropped) - int64(st.Depth) - int64(st.InFlight)
	}
	sort.Strings(snap.SinkNames)

	e.subsMu.Lock()
	for sub := range e.subs {
		snap.Subscribe.Depth += len(sub.queue)
	}
	e.subsMu.Unlock()
	snap.Subscribe.Capacity = e.params.SubscribeQueueCapacity
	snap.Subscribe.Dropped = e.subscribeDropped.Load()

	snap.StableBuffer.Capacity = e.params.StableBufferCapacity
	e.rulesMu.RLock()
	ids := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := e.rules[id]
		for _, n := range r.prog.Stats() {
			snap.Rules = append(snap.Rules, RuleNodeStats{
				Rule:      id,
				Version:   r.prog.Version,
				Node:      n.Path,
				Passed:    n.Passed,
				Discarded: n.Discarded,
				Discards:  n.Discards,
			})
			if n.Stable {
				snap.StableBuffer.Depth += int(n.Buffered)
				snap.StableBuffer.Dropped += n.Discarded
			}
		}
	}
	e.rulesMu.RUnlock()
	return snap
}
