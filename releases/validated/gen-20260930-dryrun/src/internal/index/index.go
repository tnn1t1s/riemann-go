// Package index holds one partition's slice of the last-event-per-identity
// index, with expiry detected by a min-heap of deadlines.
package index

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

type key struct{ host, service string }

type entry struct {
	ev       *event.Event
	deadline float64 // time + ttl; live on [time, deadline], expired after
	gen      uint64
}

type heapItem struct {
	deadline float64
	gen      uint64
	k        key
}

type expHeap []heapItem

func (h expHeap) Len() int            { return len(h) }
func (h expHeap) Less(i, j int) bool  { return h[i].deadline < h[j].deadline }
func (h expHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *expHeap) Push(x any)         { *h = append(*h, x.(heapItem)) }
func (h *expHeap) Pop() any           { old := *h; n := len(old); it := old[n-1]; *h = old[:n-1]; return it }
func (h expHeap) peek() (heapItem, bool) {
	if len(h) == 0 {
		return heapItem{}, false
	}
	return h[0], true
}

// Index is one partition's index. Reads and writes may come from any
// goroutine; the owning loop is the only one that expires entries.
type Index struct {
	mu      sync.Mutex
	entries map[key]*entry
	heap    expHeap
	gen     uint64
	max     int

	// Rejected counts inserts refused because the partition was full.
	Rejected atomic.Int64
	// Wake receives a signal when a deadline may have moved earlier, so the
	// owning loop can re-arm its timer. Capacity one; sends never block.
	Wake chan struct{}
}

// New returns an index bounded at max entries.
func New(max int) *Index {
	return &Index{entries: map[key]*entry{}, max: max, Wake: make(chan struct{}, 1)}
}

// Due is an entry whose deadline has passed and whose expiry event the loop
// must dispatch before calling Remove.
type Due struct {
	Host, Service string
	gen           uint64
}

// Insert stores ev as the entry for its identity, replacing any existing one.
// An event whose state is expired deletes the entry instead. It returns false
// when the insert was refused because the partition is full.
func (x *Index) Insert(ev *event.Event) bool {
	k := key{ev.Host, ev.Service}
	x.mu.Lock()
	defer x.mu.Unlock()
	if ev.State == event.ExpiredState {
		delete(x.entries, k)
		return true
	}
	if _, exists := x.entries[k]; !exists && len(x.entries) >= x.max {
		x.Rejected.Add(1)
		return false
	}
	x.gen++
	e := &entry{ev: ev, deadline: ev.Time + ev.TTL, gen: x.gen}
	x.entries[k] = e
	heap.Push(&x.heap, heapItem{deadline: e.deadline, gen: e.gen, k: k})
	select {
	case x.Wake <- struct{}{}:
	default:
	}
	return true
}

// Lookup returns the live entry for an identity. An entry whose deadline has
// passed reads as absent even before the loop has processed its expiry.
func (x *Index) Lookup(host, service string, now float64) (*event.Event, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	e, ok := x.entries[key{host, service}]
	if !ok || now > e.deadline {
		return nil, false
	}
	return e.ev, true
}

// Snapshot returns every live entry.
func (x *Index) Snapshot(now float64) []*event.Event {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := make([]*event.Event, 0, len(x.entries))
	for _, e := range x.entries {
		if now > e.deadline {
			continue
		}
		out = append(out, e.ev)
	}
	return out
}

// Len is the number of entries held, live or awaiting expiry.
func (x *Index) Len() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.entries)
}

// NextDeadline is the earliest deadline of any current entry.
func (x *Index) NextDeadline() (float64, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for {
		it, ok := x.heap.peek()
		if !ok {
			return 0, false
		}
		e, live := x.entries[it.k]
		if !live || e.gen != it.gen {
			heap.Pop(&x.heap) // stale: replaced or deleted
			continue
		}
		return it.deadline, true
	}
}

// PopDue removes from the heap every item whose deadline has passed and
// returns the ones still current. Entries stay in the table until Remove, so
// removal is downstream of the dispatch the caller performs in between.
func (x *Index) PopDue(now float64) []Due {
	x.mu.Lock()
	defer x.mu.Unlock()
	var due []Due
	for {
		it, ok := x.heap.peek()
		if !ok || !(now > it.deadline) {
			return due
		}
		heap.Pop(&x.heap)
		e, live := x.entries[it.k]
		if !live || e.gen != it.gen {
			continue
		}
		due = append(due, Due{Host: it.k.host, Service: it.k.service, gen: it.gen})
	}
}

// Remove deletes the entry named by d if it has not been replaced since.
func (x *Index) Remove(d Due) {
	x.mu.Lock()
	defer x.mu.Unlock()
	k := key{d.Host, d.Service}
	if e, ok := x.entries[k]; ok && e.gen == d.gen {
		delete(x.entries, k)
	}
}
