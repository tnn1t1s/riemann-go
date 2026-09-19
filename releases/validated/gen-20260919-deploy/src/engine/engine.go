package engine

import (
	"context"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/index"
	"github.com/tnn1t1s/riemann-go/rules"
	"github.com/tnn1t1s/riemann-go/sinkqueue"
)

// Sink is the destination behind an ntfy or influx leaf. Offer never blocks:
// the loop never waits on a sink.
type Sink interface {
	Offer(f rules.Firing)
	Stats() sinkqueue.Stats
}

// Engine is the process's event machinery.
type Engine struct {
	Params Params
	Store  *rules.Store

	shards []*shard
	global *globalExec
	sinks  map[string]Sink
	subs   subscribers
	seq    atomic.Uint64 // processing order across partitions, for the ring

	accepted atomic.Int64
	rejected atomic.Int64

	extraMu sync.Mutex
	extra   []func() []Sample
}

// New builds an engine. sinks maps rules.SinkNtfy and rules.SinkInflux to
// their destinations.
func New(p Params, sinks map[string]Sink) *Engine {
	e := &Engine{Params: p, sinks: sinks}
	e.Store = rules.NewStore(&rules.Shared{StableBufferCapacity: p.StableBufferCapacity})
	e.global = &globalExec{eng: e, wake: make(chan struct{}, 1), instances: map[string]*rules.Instance{}}
	for i := 0; i < p.EngineShards; i++ {
		s := &shard{id: i, eng: e,
			// The partition inbox: capacity shard.inbox_capacity, policy
			// block-with-deadline, counters stalled and rejected.
			inbox:     make(chan inboxItem, p.ShardInboxCapacity),
			wake:      make(chan struct{}, 1),
			ring:      &ring{maxEvents: p.ShardRingEvents, maxBytes: p.ShardRingBytes},
			instances: map[string]*rules.Instance{},
		}
		s.idx = index.New(p.IndexMaxEntriesPerShard, s.poke)
		e.shards = append(e.shards, s)
	}
	return e
}

// Run starts every partition loop and the global executor's timer loop, and
// returns when ctx is done.
func (e *Engine) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, s := range e.shards {
		wg.Add(1)
		go func() { defer wg.Done(); s.run(ctx) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); e.global.run(ctx) }()
	wg.Wait()
}

// Now is the engine's wall clock in float seconds.
func Now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// SPEC-FREE: how events map to partitions. Chosen: FNV-1a of `host` modulo
// the partition count.
func (e *Engine) shardFor(host string) *shard {
	h := fnv.New32a()
	h.Write([]byte(host))
	return e.shards[int(h.Sum32()%uint32(len(e.shards)))]
}

// Admit offers a batch to the partition inboxes under one admission deadline
// and returns how many events were admitted. Admitted events are never rolled
// back. Once the deadline passes on any inbox the rest of the batch is
// rejected, so the reply is never later than the deadline.
func (e *Engine) Admit(evs []*event.Event) (accepted int) {
	var deadline *time.Timer
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()
	for i, ev := range evs {
		s := e.shardFor(ev.Host)
		item := inboxItem{ev: ev, enqueued: time.Now()}
		select {
		case s.inbox <- item:
			accepted++
			continue
		default:
		}
		if deadline == nil {
			deadline = time.NewTimer(e.Params.IngestAdmissionDeadline)
		}
		select {
		case s.inbox <- item:
			accepted++
			continue
		case <-deadline.C:
		}
		s.stalled.Add(1)
		for _, rest := range evs[i:] {
			e.shardFor(rest.Host).rejected.Add(1)
		}
		break
	}
	e.accepted.Add(int64(accepted))
	e.rejected.Add(int64(len(evs) - accepted))
	return accepted
}

// fire routes one firing to its destination. It runs on the thread of
// whichever owner holds the rule's state and never blocks.
func (e *Engine) fire(f rules.Firing) {
	if f.Sink == rules.SinkIndex {
		// The slice is chosen by the event's host as it is now, so an event
		// whose host a `set` rewrote is indexed where reads will look for it.
		e.shardFor(f.Event.Host).idx.Insert(f.Event)
		return
	}
	if s := e.sinks[f.Sink]; s != nil {
		s.Offer(f)
	}
}

// ---- partition ----

type inboxItem struct {
	ev       *event.Event
	enqueued time.Time
}

// SPEC-FREE: the concurrency model inside a partition. Chosen: one goroutine
// per partition owns the rule state and the timer heap, with no lock on
// either. The index slice and the ring sit behind their own locks so reads
// are served without entering the inbox.
type shard struct {
	id    int
	eng   *Engine
	inbox chan inboxItem
	wake  chan struct{}
	idx   *index.Slice
	ring  *ring

	timers    timerHeap
	now       float64
	seenSet   *rules.Set
	order     []*rules.Instance
	instances map[string]*rules.Instance

	processed atomic.Int64
	stalled   atomic.Int64 // offers that waited out the admission deadline
	rejected  atomic.Int64 // events refused at this inbox
	inFlight  atomic.Int64
	loopLagNs atomic.Int64
}

// poke wakes the loop when another thread moved this partition's earliest
// index deadline.
func (s *shard) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// rules.Host, for the instances this partition owns.
func (s *shard) Now() float64                   { return s.now }
func (s *shard) Schedule(at float64, fn func()) { s.timers.schedule(at, fn) }
func (s *shard) Fire(f rules.Firing)            { s.eng.fire(f) }

// minTimerSleep keeps a deadline that lands exactly on the clock's reading
// from spinning the loop; one millisecond is below anything a rule can
// observe.
const minTimerSleep = time.Millisecond

func (s *shard) run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		s.fireDue(Now())
		var wakeAt <-chan time.Time
		next, ok := s.timers.next()
		if d, dok := s.idx.NextDeadline(); dok && (!ok || d < next) {
			next, ok = d, true
		}
		if ok {
			timer.Reset(sleepFor(next))
			wakeAt = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-wakeAt:
		case <-s.wake:
		case item := <-s.inbox:
			s.process(item)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func sleepFor(at float64) time.Duration {
	d := time.Duration((at - Now()) * float64(time.Second))
	if d < minTimerSleep {
		return minTimerSleep
	}
	return d
}

func (s *shard) process(item inboxItem) {
	s.inFlight.Store(1)
	s.loopLagNs.Store(int64(time.Since(item.enqueued)))
	wall := Now()
	// Property 12: a timer due at or before T fires before an event stamped T
	// is dispatched, even when T is ahead of the wall clock.
	s.fireDue(math.Max(wall, item.ev.Time))
	s.now = wall
	s.dispatch(item.ev)
	s.processed.Add(1)
	s.inFlight.Store(0)
}

// fireDue runs, in due order, every rule timer due at or before limit and
// every index expiry whose deadline is strictly before limit. An entry is
// live on the closed interval [time, time+ttl].
//
// SPEC-GAP: the spec orders timers against event time and also requires an
// expiry to fire with no further ingest, without saying what clock joins the
// two. Chosen: timers fire when the wall clock reaches them, and additionally
// ahead of any event stamped later than them. `now` reads the wall clock,
// except inside a timer fired ahead of it, where it reads that event's time.
func (s *shard) fireDue(limit float64) {
	for {
		tAt, tok := s.timers.next()
		tok = tok && tAt <= limit
		dAt, dok := s.idx.NextDeadline()
		dok = dok && dAt < limit
		switch {
		case tok && (!dok || tAt <= dAt):
			t, _ := s.timers.popDue(limit)
			s.now = limit
			t.fn()
		case dok:
			due, ok := s.idx.PopDue(limit)
			if !ok {
				continue
			}
			s.now = limit
			// The expiry event carries host and service and nothing else of
			// the entry; ttl is 0 and there is no metric.
			s.dispatch(&event.Event{Host: due.Key.Host, Service: due.Key.Service,
				State: event.StateExpired, Time: limit, TTL: 0, Expired: true})
			// Removal is downstream of the dispatch (INVARIANTS.md I7).
			s.idx.Remove(due)
		default:
			return
		}
	}
}

// dispatch delivers one event, ingested or synthesized, to the ring, to every
// rule this partition runs, to the global rules, and to subscribers.
func (s *shard) dispatch(ev *event.Event) {
	s.ring.add(ev, s.eng.seq.Add(1))
	set := s.eng.Store.Current()
	if set != s.seenSet {
		s.seenSet = set
		s.order = reconcile(set, s.instances, s, s.eng.Store.Shared, false)
	}
	for _, in := range s.order {
		in.Handle(ev)
	}
	s.eng.global.dispatch(ev, set)
	s.eng.subs.publish(ev, s.now)
}

// reconcile brings an owner's instances in line with the installed rules: a
// new version gets fresh state, a removed or disabled rule loses its state.
func reconcile(set *rules.Set, have map[string]*rules.Instance, host rules.Host, shared *rules.Shared, global bool) []*rules.Instance {
	order := make([]*rules.Instance, 0, len(set.Rules))
	keep := make(map[string]bool, len(set.Rules))
	for _, c := range set.Rules {
		if !c.Enabled || (c.Partition == rules.PartitionGlobal) != global {
			continue
		}
		keep[c.ID] = true
		in := have[c.ID]
		if in == nil || in.Rule != c {
			if in != nil {
				in.Kill()
			}
			in = c.NewInstance(host, c.Live, shared, false)
			have[c.ID] = in
		}
		order = append(order, in)
	}
	for id, in := range have {
		if !keep[id] {
			in.Kill()
			delete(have, id)
		}
	}
	return order
}

// ---- global executor ----

// globalExec holds the single instance of every `global` rule. Partitions
// hand it a copy of each event under one mutex, which is the whole
// concurrency model: state for the process, one thread of control at a time.
type globalExec struct {
	eng  *Engine
	mu   sync.Mutex
	wake chan struct{}

	timers    timerHeap
	now       float64
	seenSet   *rules.Set
	order     []*rules.Instance
	instances map[string]*rules.Instance
	hasRules  atomic.Bool

	seenAtomic atomic.Pointer[rules.Set]
}

func (g *globalExec) Now() float64 { return g.now }
func (g *globalExec) Schedule(at float64, fn func()) {
	g.timers.schedule(at, fn)
	select {
	case g.wake <- struct{}{}:
	default:
	}
}
func (g *globalExec) Fire(f rules.Firing) { g.eng.fire(f) }

func (g *globalExec) dispatch(ev *event.Event, set *rules.Set) {
	if set == g.loadSeen() && !g.hasRules.Load() {
		return // no global rule installed; stay off the shared lock
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	wall := Now()
	g.fireDueLocked(math.Max(wall, ev.Time))
	g.now = wall
	if cur := g.eng.Store.Current(); cur != g.seenSet {
		g.order = reconcile(cur, g.instances, g, g.eng.Store.Shared, true)
		g.hasRules.Store(len(g.order) > 0)
		g.setSeen(cur)
	}
	for _, in := range g.order {
		in.Handle(ev)
	}
}

// seenAtomic mirrors seenSet for the lock-free fast path in dispatch.
func (g *globalExec) loadSeen() *rules.Set { return g.seenAtomic.Load() }
func (g *globalExec) setSeen(s *rules.Set) { g.seenSet = s; g.seenAtomic.Store(s) }

func (g *globalExec) fireDueLocked(limit float64) {
	for {
		t, ok := g.timers.popDue(limit)
		if !ok {
			return
		}
		g.now = limit
		t.fn()
	}
}

func (g *globalExec) run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		g.mu.Lock()
		g.fireDueLocked(Now())
		next, ok := g.timers.next()
		g.mu.Unlock()
		var wakeAt <-chan time.Time
		if ok {
			timer.Reset(sleepFor(next))
			wakeAt = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-wakeAt:
		case <-g.wake:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}
