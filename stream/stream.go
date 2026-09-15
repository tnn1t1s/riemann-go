// Package stream holds combinators as functions over events, in the Clojure
// shape: a Stream takes an event and calls its children, closing over its
// own state. No combinator locks, because the shard loop that owns a stream
// is the only goroutine that calls it; timers go through a Scheduler owned
// by that same loop.
package stream

import (
	"time"

	"github.com/tnn1t1s/riemann-go/event"
)

// Stream is a combinator.
type Stream func(event.Event)

// Clock is what a loop reads. StdClock delegates to package time; inside a
// synctest bubble package time is virtual, so tests drive it with time.Sleep.
type Clock interface {
	Now() time.Time
	// At returns a channel that receives once at t (immediately if t is not
	// after Now) and a stop function.
	At(t time.Time) (<-chan time.Time, func())
}

// StdClock is the real clock.
type StdClock struct{}

func (StdClock) Now() time.Time { return time.Now() }
func (StdClock) At(t time.Time) (<-chan time.Time, func()) {
	tm := time.NewTimer(time.Until(t))
	return tm.C, func() { tm.Stop() }
}

// Scheduler is the loop-owned timer facility a stateful combinator uses.
// gen is opaque to the scheduler: the callback receives it back and decides
// whether the entry is stale.
type Scheduler interface {
	Now() time.Time
	Schedule(at time.Time, gen uint64, fn func(gen uint64))
}

func emit(children []Stream, e event.Event) {
	for _, c := range children {
		c(e)
	}
}

// Fanout sends every event to every child.
func Fanout(children ...Stream) Stream {
	return func(e event.Event) { emit(children, e) }
}

// Where passes events for which pred holds.
func Where(pred func(event.Event) bool, children ...Stream) Stream {
	return func(e event.Event) {
		if pred(e) {
			emit(children, e)
		}
	}
}

// By forks a child per key, built by mk on first sight. Forks are never
// freed in this milestone; freeing on expiry is milestone 2 and carries the
// forks-live and forks-freed metrics with it.
func By(key func(event.Event) string, mk func() Stream) Stream {
	forks := map[string]Stream{}
	return func(e event.Event) {
		k := key(e)
		s, ok := forks[k]
		if !ok {
			s = mk()
			forks[k] = s
		}
		s(e)
	}
}

// ChangedState passes an event when its state differs from the previous
// event's; the first comparison is against initial.
func ChangedState(initial string, children ...Stream) Stream {
	prev := initial
	return func(e event.Event) {
		if e.State != prev {
			prev = e.State
			emit(children, e)
		}
	}
}

// Set passes each event through fn before its children.
func Set(fn func(event.Event) event.Event, children ...Stream) Stream {
	return func(e event.Event) { emit(children, fn(e)) }
}

// Collector is a sink stub: it appends what it receives. It is for tests
// and dry run, and is only touched from the owning loop.
type Collector struct{ Events []event.Event }

func (c *Collector) Stream() Stream {
	return func(e event.Event) { c.Events = append(c.Events, e) }
}

type unknownValue struct{}

// Stable ports riemann.streams/stable (streams.clj:1936-2030). Events pass
// only once f(event) has been equal across successive events for at least dt.
//
// Difference from the Clojure: instead of N once! tasks racing, each
// idempotent, one heap entry per value change carries a generation and a
// stale entry is skipped. With monotone event times the two are equivalent,
// because a stale task's flush check (dt <= now - first.Time) is always false
// for a newer buffer. With non-monotone event times a stale Clojure task could
// flush a newer buffer early; the generation version cannot. Recorded as a
// deliberate divergence.
func Stable(s Scheduler, dt time.Duration, f func(event.Event) any, children ...Stream) Stream {
	var prev any = unknownValue{}
	var buffer []event.Event
	var gen uint64

	flush := func() {
		out := buffer
		buffer = nil
		for _, e := range out {
			emit(children, e)
		}
	}
	timeout := func(g uint64) {
		if g != gen || len(buffer) == 0 {
			return // flushed already, or buffer replaced
		}
		if s.Now().Sub(buffer[0].At()) >= dt {
			flush()
		}
	}
	return func(e event.Event) {
		v := f(e)
		if v == prev {
			if len(buffer) == 0 {
				emit(children, e) // stable: pass immediately
				return
			}
			buffer = append(buffer, e)
			if e.At().Sub(buffer[0].At()) >= dt {
				flush()
			}
			return
		}
		prev = v
		buffer = []event.Event{e}
		gen++
		s.Schedule(e.At().Add(dt), gen, timeout)
	}
}

// nextTick ports riemann.time/next-tick: the first instant after now that is
// an exact multiple of dt from anchor. When now is on a boundary, now+dt.
func nextTick(anchor time.Time, dt time.Duration, now time.Time) time.Time {
	return now.Add(dt - now.Sub(anchor)%dt)
}

// Throttle ports riemann.streams/throttle over part-time-simple
// (streams.clj:595-661, 1102-1118): at most n events per fixed dt window
// anchored at construction time; the rest are dropped. The first event of a
// window schedules the window's end tick, which resets the count.
func Throttle(s Scheduler, n int, dt time.Duration, children ...Stream) Stream {
	anchor := s.Now()
	sent := 0
	scheduled := false
	return func(e event.Event) {
		sent++
		if !scheduled {
			scheduled = true
			s.Schedule(nextTick(anchor, dt, s.Now()), 0, func(uint64) {
				sent = 0
				scheduled = false
			})
		}
		if sent <= n {
			emit(children, e)
		}
	}
}
