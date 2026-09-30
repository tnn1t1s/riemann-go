package engine

import (
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/index"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

func nowf() float64 { return float64(time.Now().UnixNano()) / 1e9 }

type inboxItem struct {
	ev  *event.Event
	enq time.Time
}

type ctlMsg struct {
	install *rule.Compiled
	remove  string
	done    chan struct{}
}

// loop is one unit of concurrency: a partition, or the single global loop.
// SPEC-FREE: one goroutine per partition owning its inbox, timers, index
// slice, ring and rule state, so no lock guards combinator state.
type loop struct {
	eng     *Engine
	shardID int // -1 for the global loop
	inbox   chan inboxItem
	ctl     chan ctlMsg
	idx     *index.Index // nil for the global loop
	ring    *ring        // nil for the global loop
	tm      timers
	rules   []*rule.Instance // ascending id

	processed atomic.Int64
	inFlight  atomic.Int64
	lagBits   atomic.Uint64 // float64 seconds, last processed event
	stalled   atomic.Int64  // offers that had to wait on this inbox
	rejected  atomic.Int64  // events answered 429 at this inbox
	dropped   atomic.Int64  // global loop only: forwards shed because the inbox was full
}

// Now implements rule.Runtime with the wall clock. It is never moved by an
// event's timestamp, per SPEC.md property 12.
func (l *loop) Now() float64 { return nowf() }

// Schedule implements rule.Runtime.
func (l *loop) Schedule(due float64, fn func()) { l.tm.schedule(due, fn) }

// Emit implements rule.Runtime for the live sinks.
func (l *loop) Emit(c *rule.Compiled, sink, path string, ev *event.Event, fr rule.Frame) {
	l.eng.emit(c, sink, path, ev, fr)
}

func (l *loop) run() {
	var tmr *time.Timer
	var tmrC <-chan time.Time
	var wakeC chan struct{}
	if l.idx != nil {
		wakeC = l.idx.Wake
	}
	for {
		due, ok := l.tm.next()
		if l.idx != nil {
			// An entry is live through its deadline and expires after it, so
			// the wake lands one millisecond past the deadline.
			if d, ok2 := l.idx.NextDeadline(); ok2 && (!ok || d+0.001 < due) {
				due, ok = d+0.001, true
			}
		}
		if ok {
			d := time.Duration((due - nowf()) * float64(time.Second))
			if d < 0 {
				d = 0
			}
			if tmr == nil {
				tmr = time.NewTimer(d)
			} else {
				tmr.Reset(d)
			}
			tmrC = tmr.C
		} else {
			if tmr != nil {
				tmr.Stop()
			}
			tmrC = nil
		}

		select {
		case it := <-l.inbox:
			l.inFlight.Store(1)
			l.lagBits.Store(math.Float64bits(time.Since(it.enq).Seconds()))
			l.fireDue()
			l.dispatch(it.ev)
			l.inFlight.Store(0)
		case <-tmrC:
			l.fireDue()
		case <-wakeC:
		case m := <-l.ctl:
			l.control(m)
		}
	}
}

// fireDue runs every timer due now and expires every index entry whose
// deadline has passed, dispatching each expiry event before removing its
// entry (INVARIANTS.md I7).
func (l *loop) fireDue() {
	now := nowf()
	l.tm.fireDue(now)
	if l.idx == nil {
		return
	}
	for _, d := range l.idx.PopDue(now) {
		ev := &event.Event{
			Host: d.Host, Service: d.Service, State: event.ExpiredState,
			Time: now, TTL: 0, Tags: []string{}, Attributes: map[string]string{},
			Expired: true,
		}
		l.dispatch(ev)
		l.idx.Remove(d)
	}
}

// dispatch runs one event through this loop's rules in ascending id order,
// then forwards it to the global loop and to subscribers.
func (l *loop) dispatch(ev *event.Event) {
	if l.ring != nil {
		l.ring.add(l.eng.ringSeq.Add(1), ev)
	}
	for _, r := range l.rules {
		r.Offer(ev)
	}
	if l.shardID >= 0 {
		l.eng.forwardGlobal(ev)
		l.eng.subs.publish(ev, nowf())
	}
	l.processed.Add(1)
}

func (l *loop) control(m ctlMsg) {
	defer close(m.done)
	if m.remove != "" {
		l.removeRule(m.remove)
		return
	}
	c := m.install
	isGlobal := c.Partition == "global"
	if isGlobal != (l.shardID < 0) {
		// The rule belongs to the other kind of loop; make sure no stale
		// instance from a previous version lingers here.
		l.removeRule(c.ID)
		return
	}
	inst := rule.NewInstance(c, l)
	for i, r := range l.rules {
		if r.C.ID == c.ID {
			l.rules[i] = inst // fresh instance; no state carries across versions
			return
		}
	}
	l.rules = append(l.rules, inst)
	sort.Slice(l.rules, func(i, j int) bool { return l.rules[i].C.ID < l.rules[j].C.ID })
}

func (l *loop) removeRule(id string) {
	for i, r := range l.rules {
		if r.C.ID == id {
			l.rules = append(l.rules[:i], l.rules[i+1:]...)
			return
		}
	}
}

func (l *loop) loopLag() float64 { return math.Float64frombits(l.lagBits.Load()) }
