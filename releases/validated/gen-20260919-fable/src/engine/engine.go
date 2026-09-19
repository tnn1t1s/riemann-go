// Package engine runs the partitions: admission into bounded inboxes, one
// processing loop per partition with its timers, index slice and ring, the
// rule registry, the global-rule executor, dry run, and the snapshot that
// self-observation and the metrics endpoint read.
package engine

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/rule"
)

// Config carries the parameters the engine, the partition and the index own.
// Names in comments are SCALE.md's.
type Config struct {
	Shards          int   // engine.shards, honoured exactly
	InboxCapacity   int   // shard.inbox_capacity, events
	RingEvents      int   // shard.ring_events, events
	RingBytes       int64 // shard.ring_bytes, bytes
	IndexMaxEntries int   // index.max_entries_per_shard, entries
	Rule            rule.Config
	// Sinks maps the sink leaf names ntfy and influx to their destinations.
	Sinks map[string]rule.Sink
}

// ruleSet is an immutable snapshot of the installed rules.
// SPEC-GAP: rules are evaluated in ascending id order (SEMANTICS.md open
// question 9), partition-local rules before global ones.
type ruleSet struct {
	local   []*rule.Rule
	global  []*rule.Rule
	members map[*rule.Rule]bool
}

// Engine is the core's entry point.
type Engine struct {
	cfg    Config
	clock  *Clock
	shards []*shard
	gauges rule.Gauges
	seq    atomic.Uint64

	accepted atomic.Int64 // riemann.ingest.accepted
	rejected atomic.Int64 // riemann.ingest.rejected

	rulesMu     sync.Mutex
	rules       map[string]*rule.Rule
	lastVersion map[string]int // survives DELETE, so version stays monotonic per id
	set         atomic.Pointer[ruleSet]

	// The global executor: one instance per global rule for the whole
	// process, run by whichever partition loop holds globalMu. Its timers are
	// fired by partition 0 when idle, and by any partition ahead of an event.
	globalMu     sync.Mutex
	globalSet    *ruleSet
	globalInst   map[*rule.Rule]*rule.Instance
	globalTimers timerHeap

	subsMu  sync.RWMutex
	subs    map[uint64]func(*event.Event)
	subNext uint64

	queuesMu sync.Mutex
	queues   []registeredQueue
}

type registeredQueue struct {
	name string
	fn   func() QueueStat
}

// New builds an engine. Start launches its loops.
func New(cfg Config) (*Engine, error) {
	if cfg.Shards < 1 {
		return nil, errors.New("engine.shards must be 1 or greater")
	}
	for _, name := range []string{rule.SinkNtfy, rule.SinkInflux} {
		if cfg.Sinks[name] == nil {
			return nil, fmt.Errorf("no sink configured for %q", name)
		}
	}
	e := &Engine{cfg: cfg, clock: &Clock{}, rules: map[string]*rule.Rule{}, lastVersion: map[string]int{},
		globalInst: map[*rule.Rule]*rule.Instance{}, subs: map[uint64]func(*event.Event){}}
	e.set.Store(&ruleSet{members: map[*rule.Rule]bool{}})
	for i := 0; i < cfg.Shards; i++ {
		e.shards = append(e.shards, newShard(e, i))
	}
	return e, nil
}

// Start launches one processing loop per partition.
func (e *Engine) Start() {
	for _, s := range e.shards {
		go s.run()
	}
}

// Now is the engine's current time.
func (e *Engine) Now() float64 { return e.clock.Now() }

// shardFor routes by host, so every event for one host lands in the same
// partition for the life of the process.
func (e *Engine) shardFor(host string) *shard {
	h := fnv.New32a()
	h.Write([]byte(host))
	return e.shards[int(h.Sum32()%uint32(len(e.shards)))]
}

// Admit offers events to their partition inboxes in order. An offer to a full
// inbox blocks until the deadline, which covers the whole batch; once it has
// passed, the remaining events are rejected. Admitted events are never rolled
// back, and accepted + rejected equals len(events).
func (e *Engine) Admit(events []*event.Event, deadline time.Duration) (accepted, rejected int) {
	var timeout *time.Timer
	expired := false
	for i, ev := range events {
		s := e.shardFor(ev.Host)
		it := inboxItem{ev: ev, enqueued: time.Now()}
		select {
		case s.inbox <- it:
			accepted++
			continue
		default:
		}
		if !expired {
			if timeout == nil {
				timeout = time.NewTimer(deadline)
				defer timeout.Stop()
			}
			s.stalled.Add(1)
			select {
			case s.inbox <- it:
				accepted++
				continue
			case <-timeout.C:
				expired = true
			}
		}
		// Reject this event and everything after it, each counted against
		// the inbox it was bound for.
		for _, rest := range events[i:] {
			e.shardFor(rest.Host).rejected.Add(1)
		}
		rejected = len(events) - i
		break
	}
	e.accepted.Add(int64(accepted))
	e.rejected.Add(int64(rejected))
	return accepted, rejected
}

// fire delivers a firing to its destination. It never blocks: a sink's Offer
// sheds when its queue is full, and an index insert takes a short lock.
func (e *Engine) fire(f rule.Firing, from *shard) {
	if f.Sink == rule.SinkIndex {
		owner := e.shardFor(f.Event.Host)
		owner.index.Insert(f.Event)
		if owner != from {
			owner.poke() // its next expiry deadline may have moved
		}
		return
	}
	e.cfg.Sinks[f.Sink].Offer(f)
}

// ---- rules ----

// ErrCompile marks an error that is the client's: a 400 at PUT.
type ErrCompile struct{ Err error }

func (e ErrCompile) Error() string { return e.Err.Error() }

// PutRule installs a rule document. When the canonical content hash matches
// the stored rule it is a no-op and created is false. Otherwise the version
// increments and a fresh instance replaces the old one, carrying no state.
func (e *Engine) PutRule(id string, body []byte) (doc map[string]any, created bool, err error) {
	d, err := rule.ParseDoc(body, id)
	if err != nil {
		return nil, false, ErrCompile{err}
	}
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	if cur, ok := e.rules[d.ID]; ok && cur.Doc.Hash == d.Hash {
		return cur.Doc.Document(cur.Version), false, nil
	}
	version := e.lastVersion[d.ID] + 1
	r, err := rule.Compile(d, version)
	if err != nil {
		return nil, false, ErrCompile{err}
	}
	e.rules[d.ID] = r
	e.lastVersion[d.ID] = version
	e.publishRules()
	return d.Document(version), true, nil
}

// DeleteRule removes a rule and reports whether it existed.
func (e *Engine) DeleteRule(id string) bool {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	if _, ok := e.rules[id]; !ok {
		return false
	}
	delete(e.rules, id)
	e.publishRules()
	return true
}

// publishRules swaps in a new snapshot and wakes every loop so instances of
// replaced rules are retired promptly. Caller holds rulesMu.
func (e *Engine) publishRules() {
	ids := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	set := &ruleSet{members: map[*rule.Rule]bool{}}
	for _, id := range ids {
		r := e.rules[id]
		set.members[r] = true
		if r.Global() {
			set.global = append(set.global, r)
		} else {
			set.local = append(set.local, r)
		}
	}
	e.set.Store(set)
	for _, s := range e.shards {
		s.poke()
	}
}

// Rules returns every stored rule document, in id order.
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
		out = append(out, e.rules[id].Doc.Document(e.rules[id].Version))
	}
	return out
}

// Rule returns one rule document plus counters.
func (e *Engine) Rule(id string) (map[string]any, bool) {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	r, ok := e.rules[id]
	if !ok {
		return nil, false
	}
	doc := r.Doc.Document(r.Version)
	doc["counters"] = r.Counters()
	return doc, true
}

// ---- the global executor ----

type globalHost struct{ e *Engine }

func (h globalHost) Now() float64       { return h.e.clock.Now() }
func (h globalHost) Fire(f rule.Firing) { h.e.fire(f, nil) }
func (h globalHost) Arm(due float64, fn func()) {
	// Runs under globalMu: a global instance is only ever entered with it held.
	h.e.globalTimers.arm(due, fn)
	h.e.shards[0].poke()
}

// reconcileGlobal retires instances of rules no longer installed and creates
// instances for new ones. A disabled rule holds no state. Caller holds globalMu.
func (e *Engine) reconcileGlobal(set *ruleSet) {
	if e.globalSet == set {
		return
	}
	e.globalSet = set
	for r, in := range e.globalInst {
		if !set.members[r] {
			in.Close()
			delete(e.globalInst, r)
		}
	}
	for _, r := range set.global {
		if _, ok := e.globalInst[r]; !ok && r.Doc.Enabled {
			e.globalInst[r] = rule.NewInstance(r, globalHost{e}, e.cfg.Rule, &e.gauges)
		}
	}
}

// fireGlobalDue fires global timers due at or before limit. Caller holds globalMu.
func (e *Engine) fireGlobalDue(limit float64) {
	for {
		t, ok := e.globalTimers.popDue(limit)
		if !ok {
			return
		}
		e.advance(t.due, nil)
		t.fn()
	}
}

// dispatchGlobal hands one event to every global rule, after the global
// timers due at or before its stamp.
func (e *Engine) dispatchGlobal(set *ruleSet, ev *event.Event) {
	e.globalMu.Lock()
	defer e.globalMu.Unlock()
	e.reconcileGlobal(set)
	e.fireGlobalDue(ev.Time)
	now := e.clock.Now()
	for _, r := range set.global {
		if in := e.globalInst[r]; in != nil && r.Active(now) {
			in.Handle(ev)
		}
	}
}

// idleGlobal is partition 0's duty: reconcile and fire global timers by the
// clock. It returns the next global timer's due time.
func (e *Engine) idleGlobal() (float64, bool) {
	e.globalMu.Lock()
	defer e.globalMu.Unlock()
	e.reconcileGlobal(e.set.Load())
	e.fireGlobalDue(e.clock.Now())
	return e.globalTimers.next()
}

// advance moves the clock to t and, when it moved, wakes the other loops so
// they re-read their timers against the new time.
func (e *Engine) advance(t float64, from *shard) {
	if e.clock.AdvanceTo(t) {
		for _, s := range e.shards {
			if s != from {
				s.poke()
			}
		}
	}
}

// ---- reads ----

// IndexGet returns the live entry for an identity.
func (e *Engine) IndexGet(host, service string) (*event.Event, bool) {
	return e.shardFor(host).index.Get(host, service, e.clock.Now())
}

// IndexQuery returns the live entries keep accepts, with the earliest and
// latest instants at which a partition's slice was taken.
func (e *Engine) IndexQuery(keep func(*event.Event, float64) bool) (entries []*event.Event, asOfMin, asOfMax float64) {
	entries = []*event.Event{}
	for i, s := range e.shards {
		now := e.clock.Now()
		if i == 0 || now < asOfMin {
			asOfMin = now
		}
		if i == 0 || now > asOfMax {
			asOfMax = now
		}
		for _, ev := range s.index.Live(now) {
			if keep(ev, now) {
				entries = append(entries, ev)
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Host != entries[j].Host {
			return entries[i].Host < entries[j].Host
		}
		return entries[i].Service < entries[j].Service
	})
	return entries, asOfMin, asOfMax
}

// ringItems merges every partition's ring in processing order.
func (e *Engine) ringItems() []ringItem {
	var all []ringItem
	for _, s := range e.shards {
		all = append(all, s.ring.snapshot()...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].seq < all[j].seq })
	return all
}

// Events returns ring events keep accepts, in processing order. limit keeps
// the most recent; zero or less means no limit.
func (e *Engine) Events(keep func(*event.Event, float64) bool, limit int) []*event.Event {
	now := e.clock.Now()
	out := []*event.Event{}
	for _, it := range e.ringItems() {
		if keep(it.ev, now) {
			out = append(out, it.ev)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// Subscribe registers fn to receive every event the loops process, ingested
// or synthesized by expiry, after its rules have run. fn is called on a
// partition loop and must not block. With snapshot set, the live index
// entries are read while publication is held off, so no event falls between
// the snapshot and the subscription; an event may appear in both.
// SPEC-GAP: the spec does not say what a subscription streams or what its
// snapshot holds; the stream is processed events and the snapshot is the index.
func (e *Engine) Subscribe(fn func(*event.Event), snapshot bool) (entries []*event.Event, cancel func()) {
	e.subsMu.Lock()
	if snapshot {
		entries, _, _ = e.IndexQuery(func(*event.Event, float64) bool { return true })
	}
	e.subNext++
	id := e.subNext
	e.subs[id] = fn
	e.subsMu.Unlock()
	return entries, func() {
		e.subsMu.Lock()
		delete(e.subs, id)
		e.subsMu.Unlock()
	}
}

func (e *Engine) publish(ev *event.Event) {
	e.subsMu.RLock()
	for _, fn := range e.subs {
		fn(ev)
	}
	e.subsMu.RUnlock()
}

// ---- snapshot ----

// QueueStat is one bounded queue's reading. Shard is -1 for a queue that is
// not per partition.
type QueueStat struct {
	Name     string // SCALE.md's queue name, for example sink.ntfy
	Shard    int
	Depth    int64
	Capacity int64
	Dropped  int64
}

// ShardStat is one partition's reading.
type ShardStat struct {
	ID           int
	LoopLag      float64 // seconds the most recently dispatched event waited in the inbox
	IndexEntries int64
	InFlight     int64
	Processed    int64
	Stalled      int64 // offers that had to wait at a full inbox
}

// NodeStat is one node path's counts for the installed version of a rule.
type NodeStat struct {
	Rule      string
	Version   int
	Node      string
	Passed    int64
	Discarded int64
}

// Snapshot is what the engine exposes about itself. Whatever samples it and
// turns it into events or metrics text lives at the edge (I8).
type Snapshot struct {
	Queues         []QueueStat
	Shards         []ShardStat
	Sinks          map[string]rule.SinkStats
	Nodes          []NodeStat
	IngestAccepted int64
	IngestRejected int64
	IndexRejected  int64
	ForksLive      int64
	ForksFreed     int64
	// Residual is accepted - (processed + dropped + queued + in_flight),
	// summed over the sinks. Each sink's five counts are read together, so a
	// correct sink contributes exactly zero.
	Residual int64
}

// RegisterQueue adds a bounded queue an adapter owns to the snapshot, so every
// queue in the process answers for itself at the same places.
func (e *Engine) RegisterQueue(name string, fn func() QueueStat) {
	e.queuesMu.Lock()
	defer e.queuesMu.Unlock()
	e.queues = append(e.queues, registeredQueue{name, fn})
}

// SinkStats reads each sink's queue.
func (e *Engine) SinkStats() map[string]rule.SinkStats {
	out := make(map[string]rule.SinkStats, len(e.cfg.Sinks))
	for name, s := range e.cfg.Sinks {
		out[name] = s.Stats()
	}
	return out
}

// Snapshot reads every counter the engine keeps.
func (e *Engine) Snapshot() Snapshot {
	snap := Snapshot{Sinks: e.SinkStats(), IngestAccepted: e.accepted.Load(), IngestRejected: e.rejected.Load(),
		ForksLive: e.gauges.ForksLive.Load(), ForksFreed: e.gauges.ForksFreed.Load()}
	names := make([]string, 0, len(snap.Sinks))
	for name := range snap.Sinks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		st := snap.Sinks[name]
		snap.Residual += st.Accepted - (st.Processed + st.Dropped + st.Depth + st.InFlight)
		snap.Queues = append(snap.Queues, QueueStat{Name: "sink." + name, Shard: -1, Depth: st.Depth, Capacity: st.Capacity, Dropped: st.Dropped})
	}
	for _, s := range e.shards {
		snap.Queues = append(snap.Queues, QueueStat{Name: "shard.inbox", Shard: s.id, Depth: int64(len(s.inbox)),
			Capacity: int64(cap(s.inbox)), Dropped: s.rejected.Load()})
		snap.Shards = append(snap.Shards, ShardStat{ID: s.id, LoopLag: float64(s.lagNanos.Load()) / 1e9,
			IndexEntries: s.index.Len(), InFlight: s.inFlight.Load(), Processed: s.processed.Load(), Stalled: s.stalled.Load()})
		snap.IndexRejected += s.index.Rejected()
	}
	snap.Queues = append(snap.Queues, QueueStat{Name: "stable.buffer", Shard: -1, Depth: e.gauges.StableDepth.Load(),
		Capacity: int64(e.cfg.Rule.StableBufferCapacity), Dropped: e.gauges.StableDropped.Load()})
	e.queuesMu.Lock()
	for _, q := range e.queues {
		st := q.fn()
		st.Name, st.Shard = q.name, -1
		snap.Queues = append(snap.Queues, st)
	}
	e.queuesMu.Unlock()

	e.rulesMu.Lock()
	ids := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := e.rules[id]
		passed, discards := r.Counters(), r.Discards()
		paths := make([]string, 0, len(passed))
		for p := range passed {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		for _, p := range paths {
			snap.Nodes = append(snap.Nodes, NodeStat{Rule: id, Version: r.Version, Node: p, Passed: passed[p], Discarded: discards[p]})
		}
	}
	e.rulesMu.Unlock()
	return snap
}

// InstallRules compiles the boot-time rule file's documents. Any failure is
// fatal to startup; the caller reports it.
func (e *Engine) InstallRules(docs [][]byte) error {
	for i, body := range docs {
		if _, _, err := e.PutRule("", body); err != nil {
			return fmt.Errorf("rule %d: %v", i, err)
		}
	}
	return nil
}
