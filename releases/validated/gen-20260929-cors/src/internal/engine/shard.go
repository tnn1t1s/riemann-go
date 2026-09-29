package engine

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

type inboxItem struct {
	ev *event.Event
	// admitted is when the ingest path offered the event, for loop lag.
	admitted time.Time
}

// shard is one partition: an inbox, a slice of the index, a ring, timers,
// and an instance of every partitioned rule.
type shard struct {
	id    int
	e     *Engine
	inbox chan inboxItem
	rt    *runtime
	index *index
	ring  *ring

	processed atomic.Uint64
	inFlight  atomic.Int64
	loopLag   atomic.Uint64 // float64 bits, seconds
	stalled   atomic.Uint64
	rejected  atomic.Uint64
}

func newShard(e *Engine, id int) *shard {
	return &shard{
		id:    id,
		e:     e,
		inbox: make(chan inboxItem, e.params.ShardInboxCapacity),
		rt:    newRuntime(e),
		index: newIndex(e.params.IndexMaxEntriesPerShard),
		ring:  newRing(e.params.ShardRingEvents, e.params.ShardRingBytes),
	}
}

// fireDue fires, in due order, every timer due at or before limit and every
// expiry whose deadline lies before it.
func (s *shard) fireDue(limit float64) {
	for {
		timerAt, hasTimer := s.rt.nextTimer()
		deadline, hasExpiry := s.index.nextDeadline()
		timerDue := hasTimer && timerAt <= limit
		expiryDue := hasExpiry && limit > deadline
		switch {
		case timerDue && (!expiryDue || timerAt <= deadline):
			s.rt.fireOne(limit)
		case expiryDue:
			s.expireOne(limit)
		default:
			return
		}
	}
}

// expireOne synthesizes the expiry event for the most overdue entry, delivers
// it to the rules exactly as an ingested event, and only then removes the
// entry.
func (s *shard) expireOne(limit float64) {
	entry, gen, ok := s.index.overdue(limit)
	if !ok {
		return
	}
	now := s.rt.clock.now()
	expired := &event.Event{
		Host:    entry.Host,
		Service: entry.Service,
		State:   event.StateExpired,
		Time:    now,
		TTL:     0,
		Expired: true,
	}
	s.process(expired)
	s.index.remove(expired, gen, now)
}

// process records an event in the ring and offers it to this partition's
// rules and to the global rules.
//
// SPEC-GAP: the spec does not say whether the ring holds expiry events.
// Chosen: it does, since they are delivered to rules exactly as ingested
// events are, and a dry run over the ring should see what the live rules saw.
func (s *shard) process(ev *event.Event) {
	s.ring.append(RingEntry{Seq: s.e.seq.Add(1), Shard: s.id, Event: ev})
	s.rt.dispatch(ev)
	s.e.global.dispatch(ev)
}

func (s *shard) dispatch(it inboxItem) {
	s.inFlight.Store(1)
	s.loopLag.Store(math.Float64bits(time.Since(it.admitted).Seconds()))
	// Property 12: everything due at or before the event's time fires
	// before the event is dispatched.
	s.rt.clock.advance(it.ev.Time)
	s.fireDue(s.rt.clock.now())
	s.process(it.ev)
	s.processed.Add(1)
	s.inFlight.Store(0)
}

// nextDue is the earliest instant at which the loop has work with no event
// arriving.
func (s *shard) nextDue() (float64, bool) {
	timerAt, hasTimer := s.rt.nextTimer()
	deadline, hasExpiry := s.index.nextDeadline()
	switch {
	case hasTimer && hasExpiry:
		return math.Min(timerAt, deadline), true
	case hasTimer:
		return timerAt, true
	case hasExpiry:
		return deadline, true
	}
	return 0, false
}

// SPEC-FREE: the timer mechanism. Chosen: a heap per partition, fired by the
// partition's own goroutine between events. The loop sleeps on one
// time.Timer set to the earliest due time.
func (s *shard) run(stop <-chan struct{}) {
	for {
		s.fireDue(s.rt.clock.now())
		// Take whatever is already waiting before arming a timer.
		select {
		case <-stop:
			return
		case it := <-s.inbox:
			s.dispatch(it)
			continue
		default:
		}
		var due <-chan time.Time
		var timer *time.Timer
		if at, ok := s.nextDue(); ok {
			timer = time.NewTimer(untilDue(at, s.rt.clock.now()))
			due = timer.C
		}
		select {
		case <-stop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-due:
		case <-s.rt.wake:
			if timer != nil {
				timer.Stop()
			}
		case it := <-s.inbox:
			if timer != nil {
				timer.Stop()
			}
			s.dispatch(it)
		}
	}
}
