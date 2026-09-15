// Package shard is one single-threaded loop that owns an inbox, a timer
// heap, a slice of the index, a ring, and the stream state of the rules it
// runs. Only the loop goroutine touches any of that; other goroutines reach
// it through the inbox, either with an event or with a closure (the read
// path). Timers fire between events on the same goroutine, so combinator
// state has no lock.
//
// Invariant (TestNoConcurrentTimerAndEvent): a timer callback never runs
// concurrently with, or re-entrantly inside, event processing.
// Invariant (TestTimersFireBeforeSameTimestampEvent): timers due at or before
// T fire before an event stamped T is dispatched.
package shard

import (
	"container/heap"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/index"
	"github.com/tnn1t1s/riemann-go/stream"
)

// DefaultInboxCapacity bounds a shard's inbox.
// Parameter: shard.inbox_capacity. Owner: shard. Units: events. Default 4096:
// tens of events per tool-call flurry; the admission deadline, not the inbox,
// handles saturation. The admission probe filled it only with 1 to 5 ms of
// loop work per event. Provisional until the milestone 3 flood.
const DefaultInboxCapacity = 4096

// DefaultRingEvents caps the ring by count.
// Parameter: shard.ring_events. Owner: shard. Units: events. Default 100,000,
// provisional.
const DefaultRingEvents = 100_000

// DefaultRingBytes caps the ring by estimated bytes.
// Parameter: shard.ring_bytes. Owner: shard. Units: bytes. Default 64 MiB,
// provisional.
const DefaultRingBytes = 64 << 20

// Config sizes one shard. Zero values take the defaults above except
// RingEvents, where 0 means no ring.
type Config struct {
	InboxCapacity   int
	RingEvents      int
	RingBytes       int
	MaxIndexEntries int
}

type msg struct {
	ev       *event.Event
	fn       func()    // runs on the loop goroutine: reads, root swaps, subscriptions
	enqueued time.Time // for loop lag
}

// timerEntry is one heap entry; seq breaks ties so equal fire times run in
// scheduling order, as Riemann's ConcurrentSkipListSet ordered by (t, id).
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
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(timerEntry)) }
func (h *timerHeap) Pop() any     { o := *h; n := len(o); e := o[n-1]; *h = o[:n-1]; return e }

// Shard is one loop. Fields below the metrics block are loop-private.
type Shard struct {
	clock stream.Clock
	inbox chan msg
	done  chan struct{}

	// Metrics, written by the loop or by Offer, read by anyone.
	accepted  atomic.Uint64 // events admitted to the inbox
	processed atomic.Uint64 // events taken from the inbox and run
	expired   atomic.Uint64 // expired events synthesised and run
	inflight  atomic.Int64  // 1 while the loop holds an inbox event
	lagLast   atomic.Int64  // nanoseconds, admission to processing, last event
	lagMax    atomic.Int64  // nanoseconds, maximum seen
	timerLen  atomic.Int64
	indexLen  atomic.Int64
	subsLen   atomic.Int64

	timers timerHeap
	seq    uint64
	idx    *index.Index
	ring   *Ring
	root   stream.Stream
	subs   []*Subscription

	// busy is true while the loop is inside a stream or timer callback. It
	// is plain memory: the race detector enforces the single-goroutine claim.
	busy bool
}

// New builds a shard with a no-op root. Run must be called before Offer.
func New(clock stream.Clock, cfg Config) *Shard {
	if cfg.InboxCapacity == 0 {
		cfg.InboxCapacity = DefaultInboxCapacity
	}
	if cfg.RingBytes == 0 {
		cfg.RingBytes = DefaultRingBytes
	}
	if cfg.MaxIndexEntries == 0 {
		cfg.MaxIndexEntries = index.DefaultMaxEntries
	}
	return &Shard{
		clock: clock,
		inbox: make(chan msg, cfg.InboxCapacity),
		done:  make(chan struct{}),
		idx:   index.New(cfg.MaxIndexEntries),
		ring:  NewRing(cfg.RingEvents, cfg.RingBytes),
		root:  func(event.Event) {},
	}
}

// TryOffer admits e if the inbox has room, without waiting.
func (s *Shard) TryOffer(e event.Event) bool {
	select {
	case s.inbox <- msg{ev: &e, enqueued: s.clock.Now()}:
		s.accepted.Add(1)
		return true
	default:
		return false
	}
}

// Offer admits e, waiting until deadline fires. It returns false if the
// deadline passed first; the event is then gone.
func (s *Shard) Offer(e event.Event, deadline <-chan time.Time) bool {
	if s.TryOffer(e) {
		return true
	}
	select {
	case s.inbox <- msg{ev: &e, enqueued: s.clock.Now()}:
		s.accepted.Add(1)
		return true
	case <-deadline:
		return false
	}
}

// Do runs fn on the loop goroutine and waits for it. Reads are serialised
// with writes by construction, not by a lock. It returns false without
// running fn if the shard has stopped.
func (s *Shard) Do(fn func()) bool {
	finished := make(chan struct{})
	select {
	case s.inbox <- msg{fn: func() { fn(); close(finished) }, enqueued: s.clock.Now()}:
	case <-s.done:
		return false
	}
	select {
	case <-finished:
		return true
	case <-s.done:
		return false
	}
}

// SetRoot installs the stream every event runs through. Called from inside
// Do or from the loop.
func (s *Shard) SetRoot(root stream.Stream) { s.root = root }

// Index is the shard's slice of the index. Loop goroutine only.
func (s *Shard) Index() *index.Index { return s.idx }

// Ring is the shard's ring. Loop goroutine only.
func (s *Shard) Ring() *Ring { return s.ring }

// Now reads the shard's clock.
func (s *Shard) Now() time.Time { return s.clock.Now() }

// InboxDepth is the number of messages waiting in the inbox.
func (s *Shard) InboxDepth() int { return len(s.inbox) }

// InboxCapacity is the inbox's declared capacity.
func (s *Shard) InboxCapacity() int { return cap(s.inbox) }

// Schedule pushes a timer. Must be called from the loop goroutine, from
// inside a stream or timer callback; the busy flag asserts that.
func (s *Shard) Schedule(at time.Time, gen uint64, fn func(gen uint64)) {
	if !s.busy {
		panic("Schedule called from outside the loop goroutine")
	}
	s.seq++
	heap.Push(&s.timers, timerEntry{at: at, seq: s.seq, gen: gen, fn: fn})
}

func (s *Shard) enter() {
	if s.busy {
		panic("re-entrant or concurrent dispatch")
	}
	s.busy = true
}
func (s *Shard) leave() { s.busy = false }

// IndexSink returns the in-process "index" sink: insert into this shard's
// index (an expired event deletes), then publish to every subscriber whose
// predicate matches. Refused inserts are counted by the index.
func (s *Shard) IndexSink() stream.Stream {
	return func(e event.Event) {
		s.idx.Insert(e)
		for _, sub := range s.subs {
			if sub.pred(e) {
				sub.offer(e)
			}
		}
	}
}

// run dispatches one event through the ring and the root.
func (s *Shard) run(e event.Event) {
	s.ring.Push(e)
	s.enter()
	s.root(e)
	s.leave()
}

// fireDue runs every timer whose fire time is <= now, in (time, seq) order,
// then every index expiry due at now, and repeats until nothing is due,
// because a callback or an expired event may schedule more.
func (s *Shard) fireDue() {
	for {
		now := s.clock.Now()
		fired := false
		for len(s.timers) > 0 && !s.timers[0].at.After(now) {
			e := heap.Pop(&s.timers).(timerEntry)
			s.enter()
			e.fn(e.gen)
			s.leave()
			fired = true
		}
		for _, e := range s.idx.Expire(event.Seconds(now)) {
			s.expired.Add(1)
			s.run(e)
			fired = true
		}
		if !fired {
			return
		}
	}
}

// nextWake is the earliest pending timer or expiry.
func (s *Shard) nextWake() (time.Time, bool) {
	var at time.Time
	ok := false
	if len(s.timers) > 0 {
		at, ok = s.timers[0].at, true
	}
	if t, has := s.idx.NextExpiry(); has {
		if tt := event.FromSeconds(t); !ok || tt.Before(at) {
			at, ok = tt, true
		}
	}
	return at, ok
}

// Run is the loop. It returns after Stop.
func (s *Shard) Run() {
	for {
		s.fireDue()
		s.timerLen.Store(int64(len(s.timers)))
		s.indexLen.Store(int64(s.idx.Len()))
		var wake <-chan time.Time
		stop := func() {}
		if at, ok := s.nextWake(); ok {
			wake, stop = s.clock.At(at)
		}
		select {
		case <-s.done:
			stop()
			return
		case <-wake:
		case m := <-s.inbox:
			stop()
			// Timers due at or before now fire before this event, matching
			// controlled.clj advance!, which runs tasks with t <= target
			// before run-stream feeds the event at that time.
			s.fireDue()
			lag := s.clock.Now().Sub(m.enqueued).Nanoseconds()
			s.lagLast.Store(lag)
			if lag > s.lagMax.Load() {
				s.lagMax.Store(lag)
			}
			if m.ev != nil {
				s.inflight.Store(1)
				s.run(*m.ev)
				s.processed.Add(1)
				s.inflight.Store(0)
			} else {
				s.enter()
				m.fn()
				s.leave()
			}
		}
	}
}

// Stop ends the loop. Events still in the inbox are dropped uncounted;
// stopping is not a shed path.
func (s *Shard) Stop() { close(s.done) }

// Metrics is a shard's self-observation snapshot.
type Metrics struct {
	InboxDepth    int    `json:"inbox_depth"`
	InboxCapacity int    `json:"inbox_capacity"`
	Accepted      uint64 `json:"accepted"`
	Processed     uint64 `json:"processed"`
	Expired       uint64 `json:"expired"`
	InFlight      int64  `json:"in_flight"`
	LoopLagLastNs int64  `json:"loop_lag_last_ns"`
	LoopLagMaxNs  int64  `json:"loop_lag_max_ns"`
	TimerHeap     int64  `json:"timer_heap"`
	IndexEntries  int64  `json:"index_entries"`
	Subscribers   int64  `json:"subscribers"`
}

// Metrics reads the shard's gauges without entering the loop.
func (s *Shard) Metrics() Metrics {
	return Metrics{
		InboxDepth:    len(s.inbox),
		InboxCapacity: cap(s.inbox),
		Accepted:      s.accepted.Load(),
		Processed:     s.processed.Load(),
		Expired:       s.expired.Load(),
		InFlight:      s.inflight.Load(),
		LoopLagLastNs: s.lagLast.Load(),
		LoopLagMaxNs:  s.lagMax.Load(),
		TimerHeap:     s.timerLen.Load(),
		IndexEntries:  s.indexLen.Load(),
		Subscribers:   s.subsLen.Load(),
	}
}
