// Package engine owns the shard set, admission, the rule set pointer, the
// sink registry and the read path. It imports core packages only; ports,
// tokens and URLs live in cmd.
package engine

import (
	"hash/fnv"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/index"
	"github.com/tnn1t1s/riemann-go/shard"
	"github.com/tnn1t1s/riemann-go/sink"
	"github.com/tnn1t1s/riemann-go/stream"
)

// DefaultShards is the number of host shards.
// Parameter: engine.shards. Owner: engine. Units: shards. Default
// runtime.GOMAXPROCS(0); restart to change. Provisional: the milestone 3
// flood decides whether one shard suffices.
func DefaultShards() int { return runtime.GOMAXPROCS(0) }

// Config sizes the engine.
type Config struct {
	Shards     int          // host shards; 0 means DefaultShards()
	Shard      shard.Config // applied to every host shard
	DefaultTTL float64      // event.default_ttl; 0 means event.DefaultTTL
}

// RuleSet is the immutable set of streams installed on every shard. Host
// builds a host shard's root; Global builds the global shard's root and may
// be nil, which installs a no-op. The rule API that produces these is
// milestone 2; this milestone installs DefaultRules.
type RuleSet struct {
	Host   func(s *shard.Shard) stream.Stream
	Global func(s *shard.Shard) stream.Stream
}

// DefaultRules is the implicit rule set: index everything, no global rules.
func DefaultRules() RuleSet {
	return RuleSet{Host: func(s *shard.Shard) stream.Stream { return s.IndexSink() }}
}

// Engine is the shard set plus what surrounds it.
type Engine struct {
	cfg     Config
	clock   stream.Clock
	shards  []*shard.Shard // host shards, hashed by host
	global  *shard.Shard   // receives a copy of every event; no ring, no reads
	sinks   []*sink.Queue  // in registration order
	rules   atomic.Pointer[RuleSet]
	started bool

	accepted      atomic.Uint64 // events admitted to their host shard
	rejected      atomic.Uint64 // events refused after the admission deadline
	globalDropped atomic.Uint64 // global copies refused after the deadline
}

// New builds an engine; Start runs it.
func New(cfg Config, clock stream.Clock) *Engine {
	if cfg.Shards == 0 {
		cfg.Shards = DefaultShards()
	}
	if cfg.DefaultTTL == 0 {
		cfg.DefaultTTL = event.DefaultTTL
	}
	e := &Engine{cfg: cfg, clock: clock}
	for i := 0; i < cfg.Shards; i++ {
		e.shards = append(e.shards, shard.New(clock, cfg.Shard))
	}
	// The global shard keeps no ring: reads come from host shards only, so
	// a second copy of every event would only double memory.
	gcfg := cfg.Shard
	gcfg.RingEvents = 0
	e.global = shard.New(clock, gcfg)
	rs := DefaultRules()
	e.rules.Store(&rs)
	return e
}

// DefaultTTL is the ttl given to events that arrive without one.
func (e *Engine) DefaultTTL() float64 { return e.cfg.DefaultTTL }

// Clock is the engine's clock.
func (e *Engine) Clock() stream.Clock { return e.clock }

// AddSink registers s behind a bounded queue. Call before Start.
func (e *Engine) AddSink(s sink.Sink, capacity int) *sink.Queue {
	q := sink.NewQueue(s, capacity)
	e.sinks = append(e.sinks, q)
	return q
}

// Sink returns the registered queue named name.
func (e *Engine) Sink(name string) *sink.Queue {
	for _, q := range e.sinks {
		if q.Name() == name {
			return q
		}
	}
	return nil
}

// Start runs every shard loop and sink worker and installs the rule set.
func (e *Engine) Start() {
	for _, q := range e.sinks {
		q.Start()
	}
	for _, s := range e.shards {
		go s.Run()
	}
	go e.global.Run()
	e.started = true
	e.install(*e.rules.Load())
}

// Stop ends the loops and workers.
func (e *Engine) Stop() {
	for _, s := range e.shards {
		s.Stop()
	}
	e.global.Stop()
	for _, q := range e.sinks {
		q.Stop()
	}
}

// SetRules installs rs on every shard, or records it for Start if the
// engine is not running yet. Each shard builds its own instance inside its
// loop, so stream state is never shared. The previous instance's state does
// not carry across: a replaced rule forgets.
func (e *Engine) SetRules(rs RuleSet) {
	e.rules.Store(&rs)
	if e.started {
		e.install(rs)
	}
}

func (e *Engine) install(rs RuleSet) {
	for _, s := range e.shards {
		s := s
		s.Do(func() { s.SetRoot(rs.Host(s)) })
	}
	e.global.Do(func() {
		if rs.Global == nil {
			e.global.SetRoot(func(event.Event) {})
			return
		}
		e.global.SetRoot(rs.Global(e.global))
	})
}

// Rules is the installed rule set.
func (e *Engine) Rules() *RuleSet { return e.rules.Load() }

// ShardFor hashes host to its shard.
func (e *Engine) ShardFor(host string) *shard.Shard {
	h := fnv.New32a()
	h.Write([]byte(host))
	return e.shards[h.Sum32()%uint32(len(e.shards))]
}

// Shards are the host shards, for tests and metrics.
func (e *Engine) Shards() []*shard.Shard { return e.shards }

// Global is the global shard.
func (e *Engine) Global() *shard.Shard { return e.global }

// Admit offers each event to its host shard, then a copy to the global
// shard. The deadline is per batch, armed at the first stalled offer, as in
// the admission probe. It returns how many events were admitted and how many
// were refused; admitted events are not rolled back. A global copy that
// misses the deadline is counted separately and does not refuse the event.
func (e *Engine) Admit(events []event.Event, deadline time.Duration) (accepted, rejected int) {
	var due <-chan time.Time
	var timer *time.Timer
	arm := func() <-chan time.Time {
		if due == nil {
			timer = time.NewTimer(deadline)
			due = timer.C
		}
		return due
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for i, ev := range events {
		s := e.ShardFor(ev.Host)
		if !s.TryOffer(ev) && !s.Offer(ev, arm()) {
			rejected = len(events) - i
			break
		}
		accepted++
		if !e.global.TryOffer(ev) && !e.global.Offer(ev, arm()) {
			e.globalDropped.Add(1)
		}
	}
	e.accepted.Add(uint64(accepted))
	e.rejected.Add(uint64(rejected))
	return accepted, rejected
}

// AsOf is the (min, max) shard snapshot time of a scatter-gather read.
type AsOf struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

func (a *AsOf) add(t float64) {
	if a.Min == 0 || t < a.Min {
		a.Min = t
	}
	if t > a.Max {
		a.Max = t
	}
}

// Query gathers every indexed event for which pred holds. Each shard's
// slice is a snapshot between two events of its loop; slices are not
// simultaneous (invariant 10).
func (e *Engine) Query(pred func(event.Event) bool) ([]event.Event, AsOf) {
	var out []event.Event
	var asOf AsOf
	for _, s := range e.shards {
		s := s
		s.Do(func() {
			asOf.add(event.Seconds(e.clock.Now()))
			s.Index().Each(func(en index.Entry) {
				if pred(en.Event) {
					out = append(out, en.Event)
				}
			})
		})
	}
	return out, asOf
}

// Lookup returns one entry from its shard at one instant.
func (e *Engine) Lookup(host, service string) (index.Entry, float64, bool) {
	var en index.Entry
	var ok bool
	var at float64
	s := e.ShardFor(host)
	s.Do(func() {
		at = event.Seconds(e.clock.Now())
		en, ok = s.Index().Lookup(host, service)
	})
	return en, at, ok
}

// Events reads from every shard's ring: events with time >= since for which
// pred holds, merged by time, newest last, at most limit. Interleaving
// across shards is by event time and best-effort.
func (e *Engine) Events(pred func(event.Event) bool, since float64, limit int) []event.Event {
	var out []event.Event
	for _, s := range e.shards {
		s := s
		s.Do(func() {
			s.Ring().Each(func(ev event.Event) bool {
				if ev.Time >= since && pred(ev) {
					out = append(out, ev)
				}
				return true
			})
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time < out[j].Time })
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// Subscribe attaches a subscription to every host shard, returning the
// snapshot if requested. Each shard's snapshot and registration are one step
// inside that shard's loop.
func (e *Engine) Subscribe(pred func(event.Event) bool, snapshot bool, capacity int) ([]event.Event, *shard.Subscription) {
	sub := shard.NewSubscription(pred, capacity)
	var snap []event.Event
	for _, s := range e.shards {
		part, _ := s.Attach(sub, snapshot)
		snap = append(snap, part...)
	}
	return snap, sub
}

// Unsubscribe detaches sub from every shard.
func (e *Engine) Unsubscribe(sub *shard.Subscription) {
	for _, s := range e.shards {
		s.Detach(sub)
	}
}

// Snapshot is the engine's self-observation.
type Snapshot struct {
	Ingest IngestMetrics           `json:"ingest"`
	Shards []shard.Metrics         `json:"shards"`
	Global shard.Metrics           `json:"global"`
	Sinks  map[string]sink.Metrics `json:"sinks"`
}

// IngestMetrics are the admission counters.
type IngestMetrics struct {
	Accepted      uint64 `json:"accepted"`
	Rejected      uint64 `json:"rejected"`
	GlobalDropped uint64 `json:"global_dropped"`
}

// Metrics reads every gauge without entering any loop.
func (e *Engine) Metrics() Snapshot {
	snap := Snapshot{
		Ingest: IngestMetrics{Accepted: e.accepted.Load(), Rejected: e.rejected.Load(), GlobalDropped: e.globalDropped.Load()},
		Global: e.global.Metrics(),
		Sinks:  e.SinkMetrics(),
	}
	for _, s := range e.shards {
		snap.Shards = append(snap.Shards, s.Metrics())
	}
	return snap
}

// SinkMetrics is the sinks block of the 202 reply.
func (e *Engine) SinkMetrics() map[string]sink.Metrics {
	m := make(map[string]sink.Metrics, len(e.sinks))
	for _, q := range e.sinks {
		m[q.Name()] = q.Metrics()
	}
	return m
}
