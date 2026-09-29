// Package engine runs the partitions: admission, the processing loops, the
// index, the ring, timers, and the rule instances. It is a core package. The
// sinks reach it through the Sink interface, which adapters implement.
package engine

import (
	"container/heap"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/config"
	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

// SinkStats is one sink's counters, read at one instant. At every instant
// Accepted equals Processed + Dropped + Depth + InFlight.
type SinkStats struct {
	// Accepted counts firings a rule routed to the sink.
	Accepted uint64
	// Processed counts firings the sink sent and the far end answered.
	Processed uint64
	// Dropped counts firings that were shed or lost.
	Dropped uint64
	// Depth is the queue's length and Capacity its bound, in firings.
	Depth    int
	Capacity int
	// InFlight counts firings a worker has taken and not yet finished.
	InFlight int
}

// Sink is a bounded, non-blocking destination for firings.
type Sink interface {
	// Offer never blocks. A full queue sheds the offered firing and counts it.
	Offer(f rule.Firing)
	Stats() SinkStats
}

type installed struct {
	doc  *rule.Document
	prog *rule.Program
}

// Engine is the process's event machinery.
type Engine struct {
	params config.Params
	shards []*shard
	global *runtime
	sinks  map[string]Sink

	rulesMu  sync.RWMutex
	rules    map[string]*installed
	versions map[string]int

	seq atomic.Uint64

	accepted atomic.Uint64
	rejected atomic.Uint64

	forksLive  atomic.Int64
	forksFreed atomic.Uint64

	subsMu           sync.Mutex
	subs             map[*Subscriber]struct{}
	subscribeDropped atomic.Uint64

	stop chan struct{}
	wg   sync.WaitGroup
}

// New builds an engine. sinks must hold an entry for rule.SinkNtfy and one
// for rule.SinkInflux.
func New(params config.Params, sinks map[string]Sink) (*Engine, error) {
	for _, name := range []string{rule.SinkNtfy, rule.SinkInflux} {
		if sinks[name] == nil {
			return nil, fmt.Errorf("engine: no sink named %q was provided", name)
		}
	}
	if params.EngineShards < 1 {
		return nil, fmt.Errorf("engine: shards is %d; it must be 1 or greater", params.EngineShards)
	}
	e := &Engine{
		params:   params,
		sinks:    sinks,
		rules:    map[string]*installed{},
		versions: map[string]int{},
		subs:     map[*Subscriber]struct{}{},
		stop:     make(chan struct{}),
	}
	e.global = newRuntime(e)
	for i := 0; i < params.EngineShards; i++ {
		e.shards = append(e.shards, newShard(e, i))
	}
	return e, nil
}

// Start launches one goroutine per partition and one for the global
// partition's timers.
func (e *Engine) Start() {
	for _, s := range e.shards {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			s.run(e.stop)
		}()
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.global.run(e.stop)
	}()
}

// Stop ends the loops and waits for them.
func (e *Engine) Stop() {
	close(e.stop)
	e.wg.Wait()
}

// Params returns the parameters the engine was built with.
func (e *Engine) Params() config.Params { return e.params }

// SPEC-FREE: how events map to partitions. Chosen: FNV-1a of the host,
// modulo the partition count.
func (e *Engine) route(host string) *shard {
	h := fnv.New64a()
	h.Write([]byte(host))
	return e.shards[h.Sum64()%uint64(len(e.shards))]
}

// Admit offers each event to its partition's inbox, in order, waiting on a
// full inbox until the deadline. It returns how many were admitted. Once the
// deadline has passed every remaining event is rejected; an admitted event
// is never rolled back.
//
// SPEC-GAP: SCALE.md gives the offer a deadline without saying whether it is
// per event or per request. Chosen: per request, measured from the call,
// because INVARIANTS.md I9 bounds the request's answer by it.
func (e *Engine) Admit(events []*event.Event, deadline time.Duration) (accepted int) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		e.accepted.Add(uint64(accepted))
		e.rejected.Add(uint64(len(events) - accepted))
	}()
	now := time.Now()
	for i, ev := range events {
		s := e.route(ev.Host)
		item := inboxItem{ev: ev, admitted: now}
		select {
		case s.inbox <- item:
			accepted++
			continue
		default:
		}
		s.stalled.Add(1)
		if timer == nil {
			timer = time.NewTimer(deadline - time.Since(now))
		}
		select {
		case s.inbox <- item:
			accepted++
		case <-timer.C:
			for _, rest := range events[i:] {
				e.route(rest.Host).rejected.Add(1)
			}
			return accepted
		}
	}
	return accepted
}

func (e *Engine) deliver(f rule.Firing) {
	switch f.Sink {
	case rule.SinkIndex:
		s := e.route(f.Event.Host)
		s.index.insert(f.Event, s.rt.clock.now())
		// The owning loop may be asleep on a later deadline.
		s.rt.signal()
	default:
		e.sinks[f.Sink].Offer(f)
	}
}

// ErrInvalidRule marks a rule the engine refused: one that does not parse,
// does not compile, or does not match the id it was put under.
var ErrInvalidRule = errors.New("invalid rule")

func invalid(err error) error { return fmt.Errorf("%w: %v", ErrInvalidRule, err) }

// LoadRules installs the documents of a --rules file, each at version 1.
func (e *Engine) LoadRules(body []byte) error {
	docs, err := rule.ParseDocuments(body)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, doc := range docs {
		if seen[doc.ID] {
			return fmt.Errorf("rule id %q appears more than once", doc.ID)
		}
		seen[doc.ID] = true
		if _, _, err := e.put(doc); err != nil {
			return fmt.Errorf("rule %q: %v", doc.ID, err)
		}
	}
	return nil
}

// PutRule creates or replaces a rule. created is false when the body's
// canonical form hashes to the stored rule's hash, in which case nothing
// changes and the stored document is returned.
func (e *Engine) PutRule(id string, body []byte) (stored map[string]any, created bool, err error) {
	doc, err := rule.ParseDocument(body)
	if err != nil {
		return nil, false, invalid(err)
	}
	if doc.ID != id {
		return nil, false, invalid(fmt.Errorf("rule id %q does not match the path segment %q", doc.ID, id))
	}
	return e.put(doc)
}

func (e *Engine) put(doc *rule.Document) (map[string]any, bool, error) {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	if cur, ok := e.rules[doc.ID]; ok && cur.doc.Hash == doc.Hash {
		return cur.doc.Stored(cur.prog.Version), false, nil
	}
	// SPEC-GAP: the spec calls version "monotonic integer per id" and does
	// not say what a PUT after a DELETE gets. Chosen: the count continues,
	// so a version number never names two different documents within one
	// process lifetime.
	version := e.versions[doc.ID] + 1
	prog, err := rule.Compile(doc, version)
	if err != nil {
		return nil, false, invalid(err)
	}
	e.versions[doc.ID] = version
	e.rules[doc.ID] = &installed{doc: doc, prog: prog}
	e.install(doc.ID, prog)
	return doc.Stored(version), true, nil
}

// install swaps a rule's instances. A disabled rule compiles and gets no
// instance, so it holds no state.
func (e *Engine) install(id string, prog *rule.Program) {
	var partitioned, global *rule.Program
	if prog != nil && prog.Doc.Enabled {
		if prog.Doc.Partition == rule.PartitionGlobal {
			global = prog
		} else {
			partitioned = prog
		}
	}
	for _, s := range e.shards {
		s.rt.install(id, partitioned)
	}
	e.global.install(id, global)
}

// DeleteRule removes a rule. It reports whether the rule existed.
func (e *Engine) DeleteRule(id string) bool {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	if _, ok := e.rules[id]; !ok {
		return false
	}
	delete(e.rules, id)
	e.install(id, nil)
	return true
}

// Rules returns every stored rule document, ordered by id.
func (e *Engine) Rules() []map[string]any {
	e.rulesMu.RLock()
	defer e.rulesMu.RUnlock()
	ids := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		r := e.rules[id]
		out = append(out, r.doc.Stored(r.prog.Version))
	}
	return out
}

// Rule returns one stored document and its per-node counters.
func (e *Engine) Rule(id string) (stored map[string]any, counters map[string]uint64, ok bool) {
	e.rulesMu.RLock()
	defer e.rulesMu.RUnlock()
	r, ok := e.rules[id]
	if !ok {
		return nil, nil, false
	}
	return r.doc.Stored(r.prog.Version), r.prog.Counters(), true
}

// IndexGet returns the entry for an identity, or nil when there is none or
// it has expired.
func (e *Engine) IndexGet(host, service string) *event.Event {
	s := e.route(host)
	return s.index.get(event.Key{Host: host, Service: service}, s.rt.clock.now())
}

func sortEvents(events []*event.Event) {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Host != events[j].Host {
			return events[i].Host < events[j].Host
		}
		return events[i].Service < events[j].Service
	})
}

// IndexQuery returns the live entries the filter selects, ordered by
// identity, with the earliest and latest instants at which a partition's
// slice was taken.
//
// SPEC-GAP: the spec does not say which clock `as_of` is read from. Chosen:
// each partition's engine time, the clock its entries' liveness was judged
// against.
func (e *Engine) IndexQuery(filter Filter) (entries []*event.Event, asOfMin, asOfMax float64) {
	entries = []*event.Event{}
	for i, s := range e.shards {
		now := s.rt.clock.now()
		if i == 0 || now < asOfMin {
			asOfMin = now
		}
		if i == 0 || now > asOfMax {
			asOfMax = now
		}
		for _, ev := range s.index.slice(now) {
			if filter == nil || filter(ev, now) {
				entries = append(entries, ev)
			}
		}
	}
	sortEvents(entries)
	return entries, asOfMin, asOfMax
}

func (e *Engine) ringEntries() []RingEntry {
	var all []RingEntry
	for _, s := range e.shards {
		all = append(all, s.ring.snapshot()...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Seq < all[j].Seq })
	return all
}

// Recent returns events from the rings in processing order. since, when
// given, keeps events whose time is at or after it. limit, when positive,
// keeps the most recent that many.
func (e *Engine) Recent(filter Filter, since *float64, limit int) []*event.Event {
	nows := make([]float64, len(e.shards))
	for i, s := range e.shards {
		nows[i] = s.rt.clock.now()
	}
	out := []*event.Event{}
	for _, en := range e.ringEntries() {
		if since != nil && en.Event.Time < *since {
			continue
		}
		if filter != nil && !filter(en.Event, nows[en.Shard]) {
			continue
		}
		out = append(out, en.Event)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// dryHost is the rule.Host of a dry run. Its clock is the latest event time
// replayed and nothing else, its timers fire only as replayed events pass
// them, and its sinks are a list.
type dryHost struct {
	mark     float64
	timers   timerHeap
	seq      uint64
	capacity int
	out      *[]rule.Firing
}

func (h *dryHost) Now() float64 { return h.mark }

func (h *dryHost) Schedule(at float64, fn func()) {
	h.seq++
	heap.Push(&h.timers, timerEntry{at: at, seq: h.seq, fn: fn})
}

func (h *dryHost) Deliver(f rule.Firing) { *h.out = append(*h.out, f) }

func (h *dryHost) StableBufferCapacity() int { return h.capacity }

func (h *dryHost) ForksChanged(int, int) {}

func (h *dryHost) advance(t float64, first bool) {
	if first || t > h.mark {
		h.mark = t
	}
	for len(h.timers) > 0 && h.timers[0].at <= h.mark {
		heap.Pop(&h.timers).(timerEntry).fn()
	}
}

// DryRun replays the rings through a fresh instance of the rule, with stubs
// in place of all three sinks, and returns what the stubs received.
//
// SPEC-GAP: the spec does not say what a dry run replays beyond "the ring
// content", nor what clock it runs on. Chosen: every ring entry in processing
// order; a partitioned rule gets one fresh instance per partition and a
// global rule one, as live; the clock is the latest replayed event time, so a
// timer still pending after the last entry does not fire and the wall clock
// never enters the result. A disabled or expired rule is replayed as if
// enabled, since the point of a dry run is to see what a rule would do.
func (e *Engine) DryRun(id string) ([]rule.Firing, bool) {
	e.rulesMu.RLock()
	r, ok := e.rules[id]
	e.rulesMu.RUnlock()
	if !ok {
		return nil, false
	}
	// Compile again so the replay shares no counters with the live rule.
	prog, err := rule.Compile(r.doc, r.prog.Version)
	if err != nil {
		return nil, false
	}
	firings := []rule.Firing{}
	type replay struct {
		host    *dryHost
		inst    *rule.Instance
		started bool
	}
	replays := map[int]*replay{}
	for _, en := range e.ringEntries() {
		slot := en.Shard
		if prog.Doc.Partition == rule.PartitionGlobal {
			slot = 0
		}
		rp := replays[slot]
		if rp == nil {
			h := &dryHost{capacity: e.params.StableBufferCapacity, out: &firings}
			rp = &replay{host: h, inst: prog.NewInstance(h)}
			replays[slot] = rp
		}
		rp.host.advance(en.Event.Time, !rp.started)
		rp.started = true
		rp.inst.Handle(en.Event)
	}
	return firings, true
}
