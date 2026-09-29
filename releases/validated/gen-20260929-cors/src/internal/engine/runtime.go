package engine

import (
	"container/heap"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

func wallNow() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

type timerEntry struct {
	at  float64
	seq uint64
	fn  func()
}

// timerHeap orders timers by due time, and by scheduling order among equals.
type timerHeap []timerEntry

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].at == h[j].at {
		return h[i].seq < h[j].seq
	}
	return h[i].at < h[j].at
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(timerEntry)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = timerEntry{}
	*h = old[:n-1]
	return e
}

// clock is a partition's notion of time.
//
// SPEC-GAP: the spec names "the engine's current time" and orders timers
// against event time (property 12), but does not say what the engine's time
// is when an event's `time` and the wall clock disagree. Chosen: the wall
// clock, except that it never reads earlier than the latest event time the
// partition has dispatched. An event stamped ahead of the wall clock
// therefore fires every timer and expiry due at or before its stamp, and the
// clock holds there until the wall clock catches up. An event stamped behind
// the wall clock moves nothing. One emitter with a clock far in the future
// expires its whole partition; the spec has no guard for that.
type clock struct {
	watermark atomic.Uint64 // float64 bits
}

func (c *clock) mark() float64 { return math.Float64frombits(c.watermark.Load()) }

func (c *clock) now() float64 { return math.Max(wallNow(), c.mark()) }

func (c *clock) advance(t float64) {
	for {
		old := c.watermark.Load()
		if math.Float64frombits(old) >= t {
			return
		}
		if c.watermark.CompareAndSwap(old, math.Float64bits(t)) {
			return
		}
	}
}

type liveInstance struct {
	id   string
	prog *rule.Program
	inst *rule.Instance
}

// runtime holds the rule instances and the timers of one partition, or of
// the global partition.
//
// SPEC-FREE: the concurrency model inside a partition. Chosen: one goroutine
// per partition runs the loop, and each runtime carries a mutex. A
// partition's own mutex is uncontended except while a rule is being
// installed. The global partition's is what serialises the copies of events
// that every partition's loop hands it, so a global rule holds one state for
// the process without a second queue between the loops and the sinks.
type runtime struct {
	e     *Engine
	clock clock

	mu        sync.Mutex
	timers    timerHeap
	seq       uint64
	instances []*liveInstance

	// wake tells the goroutine that sleeps on this runtime's next timer that
	// the earliest due time may have changed. It carries no data and holds
	// at most one token.
	wake chan struct{}
}

func newRuntime(e *Engine) *runtime {
	return &runtime{e: e, wake: make(chan struct{}, 1)}
}

func (rt *runtime) signal() {
	select {
	case rt.wake <- struct{}{}:
	default:
	}
}

// liveHost is the rule.Host of a live instance.
type liveHost struct{ rt *runtime }

func (h liveHost) Now() float64 { return h.rt.clock.now() }

// Schedule is called with rt.mu held, from inside Handle or a timer.
func (h liveHost) Schedule(at float64, fn func()) {
	h.rt.seq++
	heap.Push(&h.rt.timers, timerEntry{at: at, seq: h.rt.seq, fn: fn})
	h.rt.signal()
}

func (h liveHost) Deliver(f rule.Firing) { h.rt.e.deliver(f) }

func (h liveHost) StableBufferCapacity() int { return h.rt.e.params.StableBufferCapacity }

func (h liveHost) ForksChanged(liveDelta, freed int) {
	h.rt.e.forksLive.Add(int64(liveDelta))
	if freed > 0 {
		h.rt.e.forksFreed.Add(uint64(freed))
	}
}

// nextTimer returns the due time of the earliest timer.
func (rt *runtime) nextTimer() (float64, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.timers) == 0 {
		return 0, false
	}
	return rt.timers[0].at, true
}

// fireOne fires the earliest timer if it is due at or before limit.
func (rt *runtime) fireOne(limit float64) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.fireOneLocked(limit)
}

func (rt *runtime) fireOneLocked(limit float64) bool {
	if len(rt.timers) == 0 || rt.timers[0].at > limit {
		return false
	}
	t := heap.Pop(&rt.timers).(timerEntry)
	t.fn()
	return true
}

// dispatch offers one event to every instance, after firing the timers due
// at or before the event's time.
func (rt *runtime) dispatch(ev *event.Event) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.instances) == 0 && len(rt.timers) == 0 {
		return
	}
	rt.clock.advance(ev.Time)
	limit := rt.clock.now()
	for rt.fireOneLocked(limit) {
	}
	// SPEC-GAP: SEMANTICS.md open question 9, the order in which rules see
	// an event. Chosen: ascending rule id, so that when two rules index one
	// identity the one with the greater id wins.
	for _, li := range rt.instances {
		// SPEC-GAP: the spec does not say which clock `expires_at` is read
		// against. Chosen: the engine's time in the partition evaluating
		// the rule, the same clock as `now`.
		if doc := li.prog.Doc; doc.HasExpiresAt && limit > doc.ExpiresAt {
			li.inst.Kill()
			continue
		}
		li.inst.Handle(ev)
	}
}

// install replaces the instance for a rule id. A nil program removes it.
func (rt *runtime) install(id string, prog *rule.Program) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	next := make([]*liveInstance, 0, len(rt.instances)+1)
	for _, li := range rt.instances {
		if li.id == id {
			li.inst.Kill()
			continue
		}
		next = append(next, li)
	}
	if prog != nil {
		next = append(next, &liveInstance{id: id, prog: prog, inst: prog.NewInstance(liveHost{rt: rt})})
		sort.Slice(next, func(i, j int) bool { return next[i].id < next[j].id })
	}
	rt.instances = next
}

// run sleeps on the runtime's timers and fires them as the wall clock passes
// them. The global partition uses it; a shard's loop does the same work
// itself, interleaved with its inbox and its index.
func (rt *runtime) run(stop <-chan struct{}) {
	for {
		for rt.fireOne(rt.clock.now()) {
		}
		var due <-chan time.Time
		var timer *time.Timer
		if at, ok := rt.nextTimer(); ok {
			timer = time.NewTimer(untilDue(at, rt.clock.now()))
			due = timer.C
		}
		select {
		case <-stop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-due:
		case <-rt.wake:
			if timer != nil {
				timer.Stop()
			}
		}
	}
}

// maxSleep caps one sleep of a loop waiting on a timer, so that a due time
// too far ahead to express as a Duration cannot overflow one. It changes no
// behavior: the loop recomputes and sleeps again.
const maxSleep = time.Hour

func untilDue(at, now float64) time.Duration {
	d := at - now
	if d <= 0 {
		return 0
	}
	if d > maxSleep.Seconds() {
		return maxSleep
	}
	// Round up to the next microsecond so the loop does not wake a moment
	// before the due time and spin.
	return time.Duration(d*1e9) + time.Microsecond
}
