package engine

// ShardStats is a snapshot of one partition.
type ShardStats struct {
	ID            int
	InboxDepth    int
	InboxCapacity int
	Stalled       int64
	Rejected      int64
	Processed     int64
	LoopLag       float64 // seconds, last processed event
	InFlight      int64
	IndexEntries  int
	IndexRejected int64
	RingEvents    int
	RingBytes     int64
}

// QueueStats is a snapshot of the global loop's inbox.
type QueueStats struct {
	Depth     int
	Capacity  int
	Dropped   int64
	Processed int64
	InFlight  int64
	LoopLag   float64
}

// RuleStats carries one rule's discard and fork counters.
type RuleStats struct {
	ID         string
	Version    int64
	Paths      []string
	Discards   []int64
	ForksLive  int64
	ForksFreed int64
}

// Stats is a process-wide snapshot for GET /metrics and self-observation.
type Stats struct {
	Sinks               []SinkStats
	Shards              []ShardStats
	Global              QueueStats
	Subscribers         int
	SubscribeCapacity   int
	SubscribeDropped    int64
	IngestAccepted      int64
	IngestRejected      int64
	InflightRequests    int64
	MaxInflightRequests int
	StableCapacity      int
	Rules               []RuleStats
	// Residual is SCALE.md's accepted - (processed + dropped + queued +
	// in_flight), summed over the sinks. A correct process reads zero.
	Residual int64
}

// Stats takes the snapshot.
func (e *Engine) Stats() Stats {
	st := Stats{
		SubscribeCapacity:   e.P.SubscribeQueueCapacity,
		IngestAccepted:      e.IngestAccepted.Load(),
		IngestRejected:      e.IngestRejected.Load(),
		InflightRequests:    e.InflightRequests.Load(),
		MaxInflightRequests: e.P.IngestMaxInflightRequests,
		StableCapacity:      e.P.StableBufferCapacity,
	}
	for _, name := range []string{SinkNtfy, SinkInflux} {
		s := e.sinks[name].Stats()
		st.Sinks = append(st.Sinks, s)
		st.Residual += s.Residual()
	}
	for _, s := range e.shards {
		re, rb := s.ring.stats()
		st.Shards = append(st.Shards, ShardStats{
			ID: s.shardID, InboxDepth: len(s.inbox), InboxCapacity: cap(s.inbox),
			Stalled: s.stalled.Load(), Rejected: s.rejected.Load(), Processed: s.processed.Load(),
			LoopLag: s.loopLag(), InFlight: s.inFlight.Load(),
			IndexEntries: s.idx.Len(), IndexRejected: s.idx.Rejected.Load(),
			RingEvents: re, RingBytes: rb,
		})
	}
	g := e.global
	st.Global = QueueStats{Depth: len(g.inbox), Capacity: cap(g.inbox), Dropped: g.dropped.Load(), Processed: g.processed.Load(), InFlight: g.inFlight.Load(), LoopLag: g.loopLag()}
	e.subs.mu.RLock()
	st.Subscribers = len(e.subs.set)
	e.subs.mu.RUnlock()
	st.SubscribeDropped = e.subs.dropped.Load()
	for _, c := range e.compiledRules() {
		paths, counts := c.Discards()
		st.Rules = append(st.Rules, RuleStats{ID: c.ID, Version: c.Version, Paths: paths, Discards: counts, ForksLive: c.ForksLive.Load(), ForksFreed: c.ForksFreed.Load()})
	}
	return st
}
