package engine

import (
	"container/heap"
	"sync"
	"time"
)

// Clock is the engine's one notion of the current time, in float seconds
// since the epoch. Timers, index expiry, the `now` name, receive-time
// stamping and as_of all read it.
//
// SPEC-GAP: the spec says combinators run on event time and that a timer due
// at or before T fires before an event stamped T, but does not say what the
// engine's clock is when an event is stamped ahead of the wall clock. Here the
// clock is the wall clock plus an offset that is never negative. Dispatching
// an event stamped later than the clock moves the offset forward so the clock
// reads that stamp, and from there it keeps advancing at wall rate. A timeline
// stamped in the future therefore plays out in order, with expiry and timers
// following it. The cost is that one emitter with a fast clock moves the
// engine's time forward for the life of the process. Event stamps in the
// distant past, such as small synthetic epochs, are not supported: their
// leases have lapsed by the time they arrive.
type Clock struct {
	mu     sync.Mutex
	offset float64
}

func wallSeconds() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// Now returns the engine's current time.
func (c *Clock) Now() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return wallSeconds() + c.offset
}

// AdvanceTo moves the clock forward to t when t is ahead of it, and reports
// whether it moved.
func (c *Clock) AdvanceTo(t float64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := wallSeconds()
	if t > w+c.offset {
		c.offset = t - w
		return true
	}
	return false
}

// timer is one entry in a loop-owned timer heap.
type timer struct {
	due float64
	seq uint64 // arming order breaks ties, so equal due times fire in a fixed order
	fn  func()
}

type timerHeap struct {
	items []timer
	seq   uint64
}

func (h *timerHeap) Len() int { return len(h.items) }
func (h *timerHeap) Less(i, j int) bool {
	if h.items[i].due != h.items[j].due {
		return h.items[i].due < h.items[j].due
	}
	return h.items[i].seq < h.items[j].seq
}
func (h *timerHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *timerHeap) Push(x any)    { h.items = append(h.items, x.(timer)) }
func (h *timerHeap) Pop() any {
	old := h.items
	t := old[len(old)-1]
	old[len(old)-1] = timer{}
	h.items = old[:len(old)-1]
	return t
}

func (h *timerHeap) arm(due float64, fn func()) {
	h.seq++
	heap.Push(h, timer{due: due, seq: h.seq, fn: fn})
}

func (h *timerHeap) next() (float64, bool) {
	if len(h.items) == 0 {
		return 0, false
	}
	return h.items[0].due, true
}

// popDue removes and returns the earliest timer due at or before limit.
func (h *timerHeap) popDue(limit float64) (timer, bool) {
	if len(h.items) == 0 || h.items[0].due > limit {
		return timer{}, false
	}
	return heap.Pop(h).(timer), true
}
