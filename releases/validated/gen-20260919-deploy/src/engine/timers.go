package engine

import (
	"container/heap"
	"sync"

	"github.com/tnn1t1s/riemann-go/event"
)

// SPEC-FREE: the timer mechanism. Chosen: a min-heap per owner of rule state
// (each partition, and the global executor), fired by that owner between
// events. Ties run in scheduling order.
type timerEntry struct {
	at  float64
	seq uint64
	fn  func()
}

type timerHeap struct {
	items []timerEntry
	seq   uint64
}

func (h *timerHeap) Len() int { return len(h.items) }
func (h *timerHeap) Less(i, j int) bool {
	if h.items[i].at == h.items[j].at {
		return h.items[i].seq < h.items[j].seq
	}
	return h.items[i].at < h.items[j].at
}
func (h *timerHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *timerHeap) Push(x any)    { h.items = append(h.items, x.(timerEntry)) }
func (h *timerHeap) Pop() any {
	n := len(h.items)
	it := h.items[n-1]
	h.items[n-1] = timerEntry{}
	h.items = h.items[:n-1]
	return it
}

func (h *timerHeap) schedule(at float64, fn func()) {
	h.seq++
	heap.Push(h, timerEntry{at: at, seq: h.seq, fn: fn})
}

// next is the due time of the earliest timer.
func (h *timerHeap) next() (float64, bool) {
	if len(h.items) == 0 {
		return 0, false
	}
	return h.items[0].at, true
}

// popDue removes and returns the earliest timer due at or before limit.
func (h *timerHeap) popDue(limit float64) (timerEntry, bool) {
	if len(h.items) == 0 || h.items[0].at > limit {
		return timerEntry{}, false
	}
	return heap.Pop(h).(timerEntry), true
}

// SPEC-FREE: the ring's representation. Chosen: a slice used as a deque
// behind a mutex, trimmed from the front when either bound is exceeded.
type ringItem struct {
	ev  *event.Event
	seq uint64
}

type ring struct {
	mu        sync.Mutex
	items     []ringItem
	head      int
	bytes     int
	maxEvents int
	maxBytes  int
}

func (r *ring) add(ev *event.Event, seq uint64) {
	r.mu.Lock()
	r.items = append(r.items, ringItem{ev, seq})
	r.bytes += ev.SizeBytes()
	for n := len(r.items) - r.head; n > 0 && (n > r.maxEvents || r.bytes > r.maxBytes); n-- {
		r.bytes -= r.items[r.head].ev.SizeBytes()
		r.items[r.head] = ringItem{}
		r.head++
	}
	// Reclaim the trimmed prefix once it is at least as long as what is live.
	if r.head > 0 && r.head >= len(r.items)-r.head {
		n := copy(r.items, r.items[r.head:])
		clear(r.items[n:])
		r.items = r.items[:n]
		r.head = 0
	}
	r.mu.Unlock()
}

// snapshot copies the ring's content in processing order.
func (r *ring) snapshot() []ringItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ringItem(nil), r.items[r.head:]...)
}
