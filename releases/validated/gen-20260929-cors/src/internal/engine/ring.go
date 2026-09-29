package engine

import (
	"sync"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

// RingEntry is one event a partition processed. Seq is process-wide and
// increases in processing order, so entries from several partitions merge
// into one reproducible order.
type RingEntry struct {
	Seq   uint64
	Shard int
	Event *event.Event
}

// ring is a partition's in-memory record of the events it processed, bounded
// by shard.ring_events and shard.ring_bytes, whichever fills first. It serves
// dry run and GET /events and is lost on restart.
//
// SPEC-FREE: the ring's representation. Chosen: a circular buffer of
// pointers with a running byte total, overwriting the oldest entry.
//
// The ring is history rather than a queue: nothing waits on it and nothing
// is delivered from it, so overwriting the oldest entry loses no event on
// its way to a sink and is not counted as a drop.
type ring struct {
	mu       sync.Mutex
	buf      []RingEntry
	head     int // index of the oldest entry
	count    int
	bytes    int
	maxBytes int
}

func newRing(maxEvents, maxBytes int) *ring {
	return &ring{buf: make([]RingEntry, maxEvents), maxBytes: maxBytes}
}

func (r *ring) evictOldest() {
	r.bytes -= r.buf[r.head].Event.Size()
	r.buf[r.head] = RingEntry{}
	r.head = (r.head + 1) % len(r.buf)
	r.count--
}

func (r *ring) append(en RingEntry) {
	size := en.Event.Size()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == len(r.buf) {
		r.evictOldest()
	}
	for r.count > 0 && r.bytes+size > r.maxBytes {
		r.evictOldest()
	}
	r.buf[(r.head+r.count)%len(r.buf)] = en
	r.count++
	r.bytes += size
}

// snapshot returns the ring's content, oldest first.
func (r *ring) snapshot() []RingEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RingEntry, r.count)
	for i := 0; i < r.count; i++ {
		out[i] = r.buf[(r.head+i)%len(r.buf)]
	}
	return out
}
