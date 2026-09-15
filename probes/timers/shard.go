// Package probe3 is a probe of the shard-loop timer design for riemann-go.
//
// One goroutine owns an inbox and a timer heap. Combinators are closures
// func(Event) that call their children; a stateful combinator that needs a
// timer pushes an entry on the shard's heap instead of scheduling on a thread
// pool. The loop fires due timers between events in the same goroutine, so
// combinator state has no lock.
//
// Invariant (tested in TestNoConcurrentTimerAndEvent): a timer callback never
// runs concurrently with, or re-entrantly inside, event processing.
package probe3

import (
	"container/heap"
	"time"
)

// Event is a minimal event: the fields the ported tests use. Time is absolute.
type Event struct {
	Time  time.Time
	State string
	X     int
}

// Stream is a combinator: takes an event, calls children.
type Stream func(Event)

// Clock is what the loop reads. StdClock delegates to package time; inside a
// synctest bubble package time is virtual, so tests drive it with time.Sleep.
type Clock interface {
	Now() time.Time
	// NewTimer returns a channel that receives once at time t (or immediately
	// if t is not after Now), plus a stop function.
	At(t time.Time) (<-chan time.Time, func())
}

type StdClock struct{}

func (StdClock) Now() time.Time { return time.Now() }
func (StdClock) At(t time.Time) (<-chan time.Time, func()) {
	tm := time.NewTimer(time.Until(t))
	return tm.C, func() { tm.Stop() }
}

// timerEntry is one heap entry. gen is opaque to the shard: the callback
// receives it and decides whether the entry is stale. seq breaks ties so
// timers with equal fire time run in scheduling order, as Riemann's
// ConcurrentSkipListSet ordered by (t, id) does.
type timerEntry struct {
	at  time.Time
	seq uint64
	gen uint64
	fn  func(gen uint64)
}

type timerHeap []timerEntry

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		return h[i].seq < h[j].seq
	}
	return h[i].at.Before(h[j].at)
}
func (h timerHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)         { *h = append(*h, x.(timerEntry)) }
func (h *timerHeap) Pop() any           { o := *h; n := len(o); e := o[n-1]; *h = o[:n-1]; return e }

// Shard is one single-threaded loop. Only the loop goroutine touches heap,
// busy, and the state captured by the streams it runs.
type Shard struct {
	clock Clock
	inbox chan msg
	heap  timerHeap
	seq   uint64
	root  Stream

	// busy is true while the loop is inside a stream or timer callback.
	// It is read and written only by the loop goroutine; the race detector
	// enforces that claim.
	busy bool
}

type msg struct {
	ev    *Event
	query func() // runs on the loop goroutine; used for reads and for setting root
}

func NewShard(clock Clock, inboxDepth int) *Shard {
	return &Shard{clock: clock, inbox: make(chan msg, inboxDepth)}
}

// Send enqueues an event.
func (s *Shard) Send(e Event) { s.inbox <- msg{ev: &e} }

// Do runs f on the loop goroutine and waits for it. This is the read path:
// reads are serialised with writes by construction, not by a lock.
func (s *Shard) Do(f func()) {
	done := make(chan struct{})
	s.inbox <- msg{query: func() { f(); close(done) }}
	<-done
}

// Schedule pushes a timer. Must be called from the loop goroutine (i.e. from
// inside a stream or timer callback); the busy flag asserts that.
func (s *Shard) Schedule(at time.Time, gen uint64, fn func(gen uint64)) {
	if !s.busy {
		panic("Schedule called from outside the loop goroutine")
	}
	s.seq++
	heap.Push(&s.heap, timerEntry{at: at, seq: s.seq, gen: gen, fn: fn})
}

func (s *Shard) Now() time.Time { return s.clock.Now() }

// fireDue runs every timer whose fire time is <= now, in (time, seq) order.
// A callback may push new timers; if they are already due they run too.
func (s *Shard) fireDue() {
	for len(s.heap) > 0 {
		now := s.clock.Now()
		if s.heap[0].at.After(now) {
			return
		}
		e := heap.Pop(&s.heap).(timerEntry)
		s.enter()
		e.fn(e.gen)
		s.leave()
	}
}

func (s *Shard) enter() {
	if s.busy {
		panic("re-entrant or concurrent dispatch")
	}
	s.busy = true
}
func (s *Shard) leave() { s.busy = false }

// Run is the loop. It returns when ctxDone is closed.
func (s *Shard) Run(ctxDone <-chan struct{}) {
	for {
		s.fireDue()
		var wake <-chan time.Time
		stop := func() {}
		if len(s.heap) > 0 {
			wake, stop = s.clock.At(s.heap[0].at)
		}
		select {
		case <-ctxDone:
			stop()
			return
		case <-wake:
			// loop around to fireDue
		case m := <-s.inbox:
			stop()
			// Timers due at or before now fire before this event, matching
			// controlled.clj advance!, which runs tasks with t <= target
			// before run-stream feeds the event at that time.
			s.fireDue()
			s.enter()
			if m.ev != nil {
				s.root(*m.ev)
			} else {
				m.query()
			}
			s.leave()
		}
	}
}
