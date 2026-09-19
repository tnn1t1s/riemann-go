package engine

import (
	"sync"

	"github.com/tnn1t1s/riemann-go/event"
)

// ringItem is one processed event. seq is the process-wide processing order,
// which is what lets a dry run and GET /events merge partitions the same way
// every time.
type ringItem struct {
	seq  uint64
	ev   *event.Event
	size int64
}

// ring is one partition's in-memory buffer of processed events, bounded by an
// event count and a byte count, whichever fills first. It serves dry run and
// GET /events and nothing else, and it overwrites its oldest item by design:
// it is a window, not a queue, so an overwrite is not a drop.
type ring struct {
	mu        sync.Mutex
	items     []ringItem
	head      int // index of the oldest item
	count     int
	bytes     int64
	maxEvents int
	maxBytes  int64
}

func newRing(maxEvents int, maxBytes int64) *ring {
	return &ring{maxEvents: maxEvents, maxBytes: maxBytes}
}

func (r *ring) add(seq uint64, ev *event.Event) {
	it := ringItem{seq: seq, ev: ev, size: ev.Size()}
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.count > 0 && (r.count >= r.maxEvents || r.bytes+it.size > r.maxBytes) {
		r.bytes -= r.items[r.head].size
		r.items[r.head] = ringItem{}
		r.head = (r.head + 1) % len(r.items)
		r.count--
	}
	if r.count == len(r.items) {
		// Grow towards maxEvents rather than allocating it up front.
		grown := make([]ringItem, max(16, 2*len(r.items)))
		for i := 0; i < r.count; i++ {
			grown[i] = r.items[(r.head+i)%len(r.items)]
		}
		r.items, r.head = grown, 0
	}
	r.items[(r.head+r.count)%len(r.items)] = it
	r.count++
	r.bytes += it.size
}

func (r *ring) snapshot() []ringItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ringItem, r.count)
	for i := 0; i < r.count; i++ {
		out[i] = r.items[(r.head+i)%len(r.items)]
	}
	return out
}
