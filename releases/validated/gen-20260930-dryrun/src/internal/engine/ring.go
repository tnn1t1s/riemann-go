package engine

import (
	"sync"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

// RingEntry is one event in a partition's ring with its process-wide
// processing sequence number.
type RingEntry struct {
	Seq   uint64
	Event *event.Event
	size  int64
}

// ring is a per-partition in-memory buffer bounded by an event count and a
// byte count, whichever fills first. SPEC-FREE: a circular slice.
type ring struct {
	mu        sync.Mutex
	buf       []RingEntry
	head, n   int
	bytes     int64
	maxEvents int
	maxBytes  int64
}

func newRing(maxEvents int, maxBytes int64) *ring {
	return &ring{buf: make([]RingEntry, maxEvents), maxEvents: maxEvents, maxBytes: maxBytes}
}

func (r *ring) add(seq uint64, ev *event.Event) {
	size := ev.Size()
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.n > 0 && (r.n >= r.maxEvents || r.bytes+size > r.maxBytes) {
		r.bytes -= r.buf[r.head].size
		r.buf[r.head] = RingEntry{}
		r.head = (r.head + 1) % r.maxEvents
		r.n--
	}
	if r.n >= r.maxEvents {
		return
	}
	r.buf[(r.head+r.n)%r.maxEvents] = RingEntry{Seq: seq, Event: ev, size: size}
	r.n++
	r.bytes += size
}

func (r *ring) snapshot() []RingEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RingEntry, r.n)
	for i := 0; i < r.n; i++ {
		out[i] = r.buf[(r.head+i)%r.maxEvents]
	}
	return out
}

func (r *ring) stats() (events int, bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n, r.bytes
}
