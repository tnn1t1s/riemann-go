// Package engine is the core: partitions, the global loop, the index, the
// ring, the rule store, bounded sink queues and the read surface. It imports
// the standard library, the expression library and the sibling core packages,
// and never an adapter (INVARIANTS.md I8).
package engine

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/exprs"
	"github.com/tnn1t1s/riemann-go/internal/index"
	"github.com/tnn1t1s/riemann-go/internal/params"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

// Sink names.
const (
	SinkNtfy   = "ntfy"
	SinkInflux = "influx"
	SinkIndex  = "index"
)

// Engine is the process's stream-processing core.
type Engine struct {
	P params.Params

	shards  []*loop
	global  *loop
	sinks   map[string]*SinkQueue
	ringSeq atomic.Uint64

	rulesMu     sync.Mutex
	rules       map[string]*rule.Compiled
	lastVersion map[string]int64

	subs *subscribers

	IngestAccepted   atomic.Int64
	IngestRejected   atomic.Int64
	InflightRequests atomic.Int64

	started bool
}

// New builds an engine. Nothing runs until Start.
func New(p params.Params) *Engine {
	e := &Engine{
		P:           p,
		sinks:       map[string]*SinkQueue{},
		rules:       map[string]*rule.Compiled{},
		lastVersion: map[string]int64{},
	}
	e.sinks[SinkNtfy] = NewSinkQueue(SinkNtfy, p.SinkNtfyQueueCapacity)
	e.sinks[SinkInflux] = NewSinkQueue(SinkInflux, p.SinkInfluxQueueCapacity)
	e.subs = newSubscribers(p.SubscribeQueueCapacity)
	for i := 0; i < p.Shards; i++ {
		e.shards = append(e.shards, &loop{
			eng:     e,
			shardID: i,
			inbox:   make(chan inboxItem, p.ShardInboxCapacity),
			ctl:     make(chan ctlMsg),
			idx:     index.New(p.IndexMaxEntriesPerShard),
			ring:    newRing(p.ShardRingEvents, p.ShardRingBytes),
		})
	}
	// SPEC-GAP: the global loop's inbox is a queue SCALE.md does not name.
	// It takes shard.inbox_capacity, sheds newest, and reports as `global`.
	e.global = &loop{
		eng:     e,
		shardID: -1,
		inbox:   make(chan inboxItem, p.ShardInboxCapacity),
		ctl:     make(chan ctlMsg),
	}
	return e
}

// Sink returns a sink queue by name, for an adapter to drain.
func (e *Engine) Sink(name string) *SinkQueue { return e.sinks[name] }

// Start launches the loops.
func (e *Engine) Start() {
	if e.started {
		return
	}
	e.started = true
	for _, s := range e.shards {
		go s.run()
	}
	go e.global.run()
}

// shardFor routes a host to a partition. SPEC-FREE: FNV-1a over host modulo
// the shard count.
func (e *Engine) shardFor(host string) int {
	h := fnv.New32a()
	h.Write([]byte(host))
	return int(h.Sum32() % uint32(len(e.shards)))
}

// Admit offers events to their partition inboxes, each offer bounded by the
// shared deadline. It returns how many were admitted and how many were not;
// admitted events are never rolled back.
func (e *Engine) Admit(evs []*event.Event, deadline time.Time) (accepted, rejected int) {
	var timer *time.Timer
	var tc <-chan time.Time
	for i, ev := range evs {
		l := e.shards[e.shardFor(ev.Host)]
		it := inboxItem{ev: ev, enq: time.Now()}
		select {
		case l.inbox <- it:
			accepted++
			continue
		default:
		}
		l.stalled.Add(1)
		if timer == nil {
			timer = time.NewTimer(time.Until(deadline))
			tc = timer.C
		}
		select {
		case l.inbox <- it:
			accepted++
		case <-tc:
			rejected = len(evs) - i
			l.rejected.Add(int64(rejected))
			e.IngestAccepted.Add(int64(accepted))
			e.IngestRejected.Add(int64(rejected))
			return
		}
	}
	if timer != nil {
		timer.Stop()
	}
	e.IngestAccepted.Add(int64(accepted))
	return accepted, 0
}

// forwardGlobal hands an event a partition has finished with to the global
// loop. The partition loop never blocks, so a full global inbox sheds.
func (e *Engine) forwardGlobal(ev *event.Event) {
	select {
	case e.global.inbox <- inboxItem{ev: ev, enq: time.Now()}:
	default:
		e.global.dropped.Add(1)
	}
}

// emit is the live delivery of a firing to a sink.
func (e *Engine) emit(c *rule.Compiled, sink, path string, ev *event.Event, fr rule.Frame) {
	switch sink {
	case SinkIndex:
		e.shards[e.shardFor(ev.Host)].idx.Insert(ev)
	case SinkInflux:
		q := e.sinks[SinkInflux]
		if ev.Metric == nil {
			// A point with no field is not a point: not written, counted.
			q.Reject()
			return
		}
		q.Offer(Firing{Sink: sink, Rule: c.ID, Version: c.Version, Owner: c.Owner, Node: path, PriorState: fr.PriorState, Event: ev})
	case SinkNtfy:
		e.sinks[SinkNtfy].Offer(Firing{Sink: sink, Rule: c.ID, Version: c.Version, Owner: c.Owner, Node: path, PriorState: fr.PriorState, Event: ev})
	}
}

// --- rules ---

// ErrNotFound is returned for an unknown rule id.
var ErrNotFound = errors.New("rule not found")

func (e *Engine) ruleOptions() rule.Options {
	return rule.Options{StableBufferCapacity: e.P.StableBufferCapacity}
}

// PutRule installs or replaces a rule. created is false when the body's
// content hash matches the stored rule, in which case nothing changes.
func (e *Engine) PutRule(body []byte, id string) (doc map[string]any, created bool, err error) {
	c, err := rule.Parse(body, id, e.ruleOptions())
	if err != nil {
		return nil, false, err
	}
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	if existing, ok := e.rules[c.ID]; ok && existing.Hash == c.Hash {
		return existing.Doc, false, nil
	}
	// SPEC-GAP: whether version restarts after a DELETE is not stated. It
	// continues from the last version ever issued for the id, so it is
	// monotonic for the life of the process.
	v := e.lastVersion[c.ID] + 1
	e.lastVersion[c.ID] = v
	c.Version = v
	c.Doc["version"] = v
	e.rules[c.ID] = c
	e.installLocked(c)
	return c.Doc, true, nil
}

// installLocked sends the compiled rule to every loop. Each loop installs a
// fresh instance if the rule's partition belongs to it and drops any stale
// instance otherwise.
func (e *Engine) installLocked(c *rule.Compiled) {
	for _, l := range e.allLoops() {
		done := make(chan struct{})
		l.ctl <- ctlMsg{install: c, done: done}
		<-done
	}
}

func (e *Engine) allLoops() []*loop {
	return append(append([]*loop{}, e.shards...), e.global)
}

// Rules returns every stored document in ascending id order.
func (e *Engine) Rules() []map[string]any {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	ids := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		out = append(out, e.rules[id].Doc)
	}
	return out
}

// Rule returns one document with its per-node counters.
func (e *Engine) Rule(id string) (doc map[string]any, counters map[string]int64, err error) {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	c, ok := e.rules[id]
	if !ok {
		return nil, nil, ErrNotFound
	}
	return c.Doc, c.Counters(), nil
}

// DeleteRule removes a rule from the store and from every loop.
func (e *Engine) DeleteRule(id string) error {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	if _, ok := e.rules[id]; !ok {
		return ErrNotFound
	}
	delete(e.rules, id)
	for _, l := range e.allLoops() {
		done := make(chan struct{})
		l.ctl <- ctlMsg{remove: id, done: done}
		<-done
	}
	return nil
}

// compiledRules snapshots the store for the stats sampler.
func (e *Engine) compiledRules() []*rule.Compiled {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	out := make([]*rule.Compiled, 0, len(e.rules))
	for _, c := range e.rules {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// --- reads ---

// IndexLookup returns the live indexed event for an identity.
func (e *Engine) IndexLookup(host, service string) (*event.Event, bool) {
	return e.shards[e.shardFor(host)].idx.Lookup(host, service, nowf())
}

// AsOf bounds the instants at which per-partition slices were taken.
type AsOf struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

// IndexQuery returns the live entries for which q holds. An empty q selects
// every entry.
func (e *Engine) IndexQuery(q string) ([]*event.Event, AsOf, error) {
	var prog *exprs.Program
	if q != "" {
		p, err := exprs.CompilePredicate(q, false)
		if err != nil {
			return nil, AsOf{}, err
		}
		prog = p
	}
	var out []*event.Event
	var asOf AsOf
	for i, s := range e.shards {
		t := nowf()
		if i == 0 || t < asOf.Min {
			asOf.Min = t
		}
		if t > asOf.Max {
			asOf.Max = t
		}
		for _, ev := range s.idx.Snapshot(t) {
			if prog == nil || prog.Bool(ev.Env(t, nil)) {
				out = append(out, ev)
			}
		}
	}
	if out == nil {
		out = []*event.Event{}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].Service < out[j].Service
	})
	return out, asOf, nil
}

// RecentEvents reads the rings: events whose time is at or after since and
// for which q holds, keeping the most recent limit in processing order.
func (e *Engine) RecentEvents(q string, since float64, limit int) ([]*event.Event, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}
	var prog *exprs.Program
	if q != "" {
		p, err := exprs.CompilePredicate(q, false)
		if err != nil {
			return nil, err
		}
		prog = p
	}
	now := nowf()
	var all []RingEntry
	for _, s := range e.shards {
		all = append(all, s.ring.snapshot()...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Seq < all[j].Seq })
	matched := make([]*event.Event, 0, len(all))
	for _, re := range all {
		if re.Event.Time < since {
			continue
		}
		if prog != nil && !prog.Bool(re.Event.Env(now, nil)) {
			continue
		}
		matched = append(matched, re.Event)
	}
	if len(matched) > limit {
		matched = matched[len(matched)-limit:]
	}
	return matched, nil
}

// indexSnapshot collects every live entry across partitions.
func (e *Engine) indexSnapshot() []*event.Event {
	now := nowf()
	var out []*event.Event
	for _, s := range e.shards {
		out = append(out, s.idx.Snapshot(now)...)
	}
	return out
}
