package engine

import (
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/index"
	"github.com/tnn1t1s/riemann-go/rule"
)

// timerGranularity is the shortest sleep a loop takes before re-reading its
// timers. It bounds how late a clock-driven timer or expiry fires, and keeps
// a deadline that is due "now" from spinning the loop. Default; nothing has
// measured a need for finer.
const timerGranularity = time.Millisecond

type inboxItem struct {
	ev       *event.Event
	enqueued time.Time
}

// shard is one partition: its own inbox, slice of the index, ring and timers,
// all driven by one goroutine. Rule state for host and host,service rules
// lives here and is touched by that goroutine alone.
type shard struct {
	eng   *Engine
	id    int
	inbox chan inboxItem // capacity shard.inbox_capacity, policy block-with-deadline, dropped counter rejected
	// wake carries no data and holds at most one pending poke; it is a signal,
	// not a queue.
	wake   chan struct{}
	index  *index.Slice
	ring   *ring
	timers timerHeap

	set  *ruleSet
	inst map[*rule.Rule]*rule.Instance

	rejected  atomic.Int64 // events refused at this inbox after the deadline
	stalled   atomic.Int64 // offers that found this inbox full
	processed atomic.Int64
	inFlight  atomic.Int64
	lagNanos  atomic.Int64
}

func newShard(e *Engine, id int) *shard {
	return &shard{eng: e, id: id, inbox: make(chan inboxItem, e.cfg.InboxCapacity), wake: make(chan struct{}, 1),
		index: index.New(e.cfg.IndexMaxEntries), ring: newRing(e.cfg.RingEvents, e.cfg.RingBytes),
		inst: map[*rule.Rule]*rule.Instance{}}
}

func (s *shard) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// rule.Host, for the instances this partition owns.
func (s *shard) Now() float64               { return s.eng.clock.Now() }
func (s *shard) Fire(f rule.Firing)         { s.eng.fire(f, s) }
func (s *shard) Arm(due float64, fn func()) { s.timers.arm(due, fn) }

func (s *shard) run() {
	sleep := time.NewTimer(time.Hour)
	defer sleep.Stop()
	for {
		// An event already admitted is ordered ahead of timers that are due
		// only by the clock: its own stamp decides which timers precede it.
		select {
		case it := <-s.inbox:
			s.process(it)
			continue
		default:
		}
		wait := s.idle()
		if !sleep.Stop() {
			select {
			case <-sleep.C:
			default:
			}
		}
		sleep.Reset(wait)
		select {
		case it := <-s.inbox:
			s.process(it)
		case <-s.wake:
		case <-sleep.C:
		}
	}
}

// idle fires what the clock says is due and returns how long to sleep.
func (s *shard) idle() time.Duration {
	s.reconcile(s.eng.set.Load())
	now := s.eng.clock.Now()
	s.fireDue(now)
	next, ok := s.timers.next()
	if d, has := s.index.NextDeadline(); has && (!ok || d < next) {
		next, ok = d, true
	}
	if s.id == 0 {
		if d, has := s.eng.idleGlobal(); has && (!ok || d < next) {
			next, ok = d, true
		}
	}
	if !ok {
		return time.Hour // a poke or an event ends the sleep early
	}
	wait := time.Duration((next - s.eng.clock.Now()) * float64(time.Second))
	if wait < timerGranularity {
		wait = timerGranularity
	}
	return wait
}

// fireDue fires, in due order, this partition's timers due at or before limit
// and its index entries whose deadline is strictly before limit. An entry is
// live on the closed interval [time, time+ttl], so an event stamped exactly at
// the deadline still finds it.
func (s *shard) fireDue(limit float64) {
	for {
		tdue, tok := s.timers.next()
		tok = tok && tdue <= limit
		ddue, dok := s.index.NextDeadline()
		dok = dok && ddue < limit
		switch {
		case tok && (!dok || tdue <= ddue):
			t, _ := s.timers.popDue(limit)
			s.eng.advance(t.due, s)
			t.fn()
		case dok:
			due, ok := s.index.BeginExpire(limit)
			if !ok {
				continue
			}
			s.expire(due)
		default:
			return
		}
	}
}

// expire synthesizes the expiry event and delivers it to the rule set exactly
// as an ingested event. Only then is the entry removed.
func (s *shard) expire(due index.Due) {
	s.eng.advance(due.Deadline, s)
	t := s.eng.clock.Now()
	if t < due.Deadline {
		t = due.Deadline
	}
	// host, service, state expired, the instant of expiry, ttl 0, no metric.
	ev := &event.Event{Host: due.Host, Service: due.Service, State: event.StateExpired, Time: t, TTL: 0, Expired: true}
	s.dispatch(ev)
	s.index.FinishExpire(due)
	s.record(ev)
}

func (s *shard) process(it inboxItem) {
	s.inFlight.Store(1)
	s.lagNanos.Store(int64(time.Since(it.enqueued)))
	s.reconcile(s.eng.set.Load())
	s.fireDue(it.ev.Time) // property 12: timers due at or before T precede the event stamped T
	s.eng.advance(it.ev.Time, s)
	s.dispatch(it.ev)
	s.record(it.ev)
	s.processed.Add(1)
	s.inFlight.Store(0)
}

// record puts a processed event in the ring and hands it to subscribers.
// SPEC-GAP: synthesized expiry events enter the ring alongside ingested ones,
// so a dry run of a rule matching expired has something to replay.
func (s *shard) record(ev *event.Event) {
	s.ring.add(s.eng.seq.Add(1), ev)
	s.eng.publish(ev)
}

// reconcile retires instances of rules that are no longer installed and
// creates instances for new ones. A disabled rule holds no state.
func (s *shard) reconcile(set *ruleSet) {
	if s.set == set {
		return
	}
	s.set = set
	for r, in := range s.inst {
		if !set.members[r] {
			in.Close()
			delete(s.inst, r)
		}
	}
	for _, r := range set.local {
		if _, ok := s.inst[r]; !ok && r.Doc.Enabled {
			s.inst[r] = rule.NewInstance(r, s, s.eng.cfg.Rule, &s.eng.gauges)
		}
	}
}

func (s *shard) dispatch(ev *event.Event) {
	set := s.eng.set.Load()
	s.reconcile(set)
	now := s.eng.clock.Now()
	for _, r := range set.local {
		if in := s.inst[r]; in != nil && r.Active(now) {
			in.Handle(ev)
		}
	}
	if len(set.global) > 0 {
		s.eng.dispatchGlobal(set, ev)
	}
}
