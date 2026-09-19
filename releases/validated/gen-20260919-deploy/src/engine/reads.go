package engine

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/exprs"
	"github.com/tnn1t1s/riemann-go/rules"
)

// CompileQuery compiles a `q=` expression. There is no second query grammar:
// it is the rule expression language with the same closed names, and an empty
// q selects everything.
func CompileQuery(q string) (*exprs.Program, error) {
	if q == "" {
		return nil, nil
	}
	return exprs.CompileBool(q, false)
}

// matches evaluates a query against one event. An expression that cannot be
// evaluated on the event does not select it.
func matches(q *exprs.Program, ev *event.Event, now float64) bool {
	if q == nil {
		return true
	}
	ok, err := q.Bool(exprs.Env(ev, now, nil))
	return err == nil && ok
}

// AsOf bounds the instants the per-partition slices were taken.
type AsOf struct{ Min, Max float64 }

// QueryIndex returns the live entries the query selects.
func (e *Engine) QueryIndex(q *exprs.Program) ([]*event.Event, AsOf) {
	var out []*event.Event
	var asOf AsOf
	for i, s := range e.shards {
		t := Now()
		if i == 0 || t < asOf.Min {
			asOf.Min = t
		}
		if t > asOf.Max {
			asOf.Max = t
		}
		out = append(out, s.idx.Snapshot(t, func(ev *event.Event) bool { return matches(q, ev, t) })...)
	}
	sortEvents(out)
	return out, asOf
}

// sortEvents gives a read a stable order: by host, then service.
func sortEvents(evs []*event.Event) {
	sort.Slice(evs, func(i, j int) bool {
		if evs[i].Host != evs[j].Host {
			return evs[i].Host < evs[j].Host
		}
		return evs[i].Service < evs[j].Service
	})
}

// Lookup returns the indexed event for an identity, or nil when there is no
// entry or it has expired.
func (e *Engine) Lookup(host, service string) *event.Event {
	return e.shardFor(host).idx.Get(host, service, Now())
}

// RecentEvents reads the rings.
//
// SPEC-GAP: the spec names `since` and `limit` on GET /events without defining
// them. Chosen: `since` keeps events whose `time` is at or after it, `limit`
// keeps the most recent that many, and the result is in processing order,
// oldest first. Across partitions the order is the engine's processing
// sequence, which is best-effort as the spec allows.
func (e *Engine) RecentEvents(q *exprs.Program, since *float64, limit int) []*event.Event {
	items := e.ringItems()
	now := Now()
	out := make([]*event.Event, 0, len(items))
	for _, it := range items {
		if since != nil && it.ev.Time < *since {
			continue
		}
		if matches(q, it.ev, now) {
			out = append(out, it.ev)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func (e *Engine) ringItems() []ringItem {
	var items []ringItem
	for _, s := range e.shards {
		items = append(items, s.ring.snapshot()...)
	}
	if len(e.shards) > 1 {
		sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })
	}
	return items
}

// ---- subscriptions ----

// Subscriber is one SSE client's bounded queue: capacity
// subscribe.queue_capacity, policy shed-newest, with the shed count delivered
// to the client as a `lagged` frame.
type Subscriber struct {
	C      chan *event.Event
	query  *exprs.Program
	missed atomic.Int64
}

// TakeMissed returns and clears the count of events shed since the last call.
func (s *Subscriber) TakeMissed() int64 { return s.missed.Swap(0) }

type subscribers struct {
	mu      sync.RWMutex
	set     map[*Subscriber]struct{}
	dropped atomic.Int64
}

// Subscribe registers a subscriber and, when snapshot is true, returns the
// index entries the query selects. The subscriber is registered first and
// the snapshot taken second, so an event indexed between the two is at worst
// seen twice and never missed.
func (e *Engine) Subscribe(q *exprs.Program, snapshot bool) (*Subscriber, []*event.Event) {
	sub := &Subscriber{C: make(chan *event.Event, e.Params.SubscribeQueueCapacity), query: q}
	e.subs.mu.Lock()
	if e.subs.set == nil {
		e.subs.set = map[*Subscriber]struct{}{}
	}
	e.subs.set[sub] = struct{}{}
	e.subs.mu.Unlock()
	var snap []*event.Event
	if snapshot {
		snap, _ = e.QueryIndex(q)
	}
	return sub, snap
}

// Unsubscribe removes a subscriber.
func (e *Engine) Unsubscribe(sub *Subscriber) {
	e.subs.mu.Lock()
	delete(e.subs.set, sub)
	e.subs.mu.Unlock()
}

// publish runs on a partition loop and never blocks.
func (s *subscribers) publish(ev *event.Event, now float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for sub := range s.set {
		if !matches(sub.query, ev, now) {
			continue
		}
		select {
		case sub.C <- ev:
		default:
			sub.missed.Add(1)
			s.dropped.Add(1)
		}
	}
}

func (s *subscribers) depth() (n int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for sub := range s.set {
		n += len(sub.C)
	}
	return n
}

// ---- dry run ----

// DryFiring is one sink stub delivery.
type DryFiring struct {
	Sink  string
	Node  string
	Event *event.Event
}

// dryHost is the clock, timer heap and sink stubs of a dry run. Its clock is
// the replayed events' own times, so the result is a function of the rule and
// the ring content and of nothing else.
type dryHost struct {
	now     float64
	timers  timerHeap
	firings []DryFiring
}

func (h *dryHost) Now() float64                   { return h.now }
func (h *dryHost) Schedule(at float64, fn func()) { h.timers.schedule(at, fn) }
func (h *dryHost) Fire(f rules.Firing) {
	h.firings = append(h.firings, DryFiring{Sink: f.Sink, Node: f.Node, Event: f.Event})
}

// DryRun replays the ring through fresh state for the rule, against stubs for
// all three sink leaves. Nothing live is touched: no sink queue, no index
// slice, no live counter. A `host` or `host,service` rule is replayed once
// per partition over that partition's ring, as it runs live; a `global` rule
// is replayed once over every ring in processing order.
//
// SPEC-GAP: the spec does not say what a dry run does with timers still
// pending after the last replayed event. Chosen: they do not fire, because in
// the replay's clock their time has not come.
func (e *Engine) DryRun(c *rules.Compiled) []DryFiring {
	var replays [][]ringItem
	if c.Partition == rules.PartitionGlobal {
		replays = [][]ringItem{e.ringItems()}
	} else {
		for _, s := range e.shards {
			replays = append(replays, s.ring.snapshot())
		}
	}
	firings := []DryFiring{}
	for _, items := range replays {
		h := &dryHost{}
		shared := &rules.Shared{StableBufferCapacity: e.Params.StableBufferCapacity}
		in := c.NewInstance(h, c.NewCounterSet(), shared, true)
		for _, it := range items {
			for {
				t, ok := h.timers.popDue(it.ev.Time)
				if !ok {
					break
				}
				h.now = t.at
				t.fn()
			}
			h.now = it.ev.Time
			in.Handle(it.ev)
		}
		firings = append(firings, h.firings...)
	}
	return firings
}
