package shard

import "github.com/tnn1t1s/riemann-go/event"

// Ring is a bounded buffer of recent events in processing order, capped by
// count and by estimated bytes, whichever fills first. It serves GET /events
// and dry run, is touched only by the loop goroutine, and is lost on restart.
type Ring struct {
	buf       []event.Event
	head      int // index of the oldest event
	n         int
	bytes     int
	maxEvents int
	maxBytes  int
}

// NewRing returns a ring holding at most maxEvents events or maxBytes
// estimated bytes. maxEvents 0 disables the ring.
func NewRing(maxEvents, maxBytes int) *Ring {
	return &Ring{buf: make([]event.Event, maxEvents), maxEvents: maxEvents, maxBytes: maxBytes}
}

// Push appends e, evicting the oldest events until both caps hold.
func (r *Ring) Push(e event.Event) {
	if r.maxEvents == 0 {
		return
	}
	sz := e.Size()
	for r.n > 0 && (r.n >= r.maxEvents || r.bytes+sz > r.maxBytes) {
		r.bytes -= r.buf[r.head].Size()
		r.buf[r.head] = event.Event{}
		r.head = (r.head + 1) % r.maxEvents
		r.n--
	}
	r.buf[(r.head+r.n)%r.maxEvents] = e
	r.n++
	r.bytes += sz
}

// Each calls fn from oldest to newest and stops when fn returns false.
func (r *Ring) Each(fn func(event.Event) bool) {
	for i := 0; i < r.n; i++ {
		if !fn(r.buf[(r.head+i)%r.maxEvents]) {
			return
		}
	}
}

func (r *Ring) Len() int   { return r.n }
func (r *Ring) Bytes() int { return r.bytes }
