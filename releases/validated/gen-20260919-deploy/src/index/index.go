// Package index holds one partition's slice of the index: the last indexed
// event per (host, service), with an expiry heap.
//
// SPEC-FREE: the index structure. Chosen: a map plus a min-heap of deadlines
// whose items carry the entry's generation so stale items are discarded on
// pop, behind one RWMutex per slice so the read surface never queues behind a
// saturated inbox (SCALE.md flood floor 4).
package index

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
)

// Key is an identity.
type Key struct{ Host, Service string }

type entry struct {
	ev       *event.Event
	deadline float64
	gen      uint64
}

type heapItem struct {
	deadline float64
	key      Key
	gen      uint64
}

type expiryHeap []heapItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].deadline < h[j].deadline }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(heapItem)) }
func (h *expiryHeap) Pop() any {
	o := *h
	n := len(o)
	it := o[n-1]
	*h = o[:n-1]
	return it
}

// Slice is one partition's index.
type Slice struct {
	mu         sync.RWMutex
	entries    map[Key]*entry
	heap       expiryHeap
	gen        uint64
	maxEntries int
	rejected   atomic.Int64
	// onInsert tells the owning partition its earliest deadline may have
	// moved. It must not block.
	onInsert func()
}

// New returns an empty slice bounded by maxEntries
// (index.max_entries_per_shard).
func New(maxEntries int, onInsert func()) *Slice {
	return &Slice{entries: make(map[Key]*entry), maxEntries: maxEntries, onInsert: onInsert}
}

// Insert stores ev as the entry for its identity, replacing any entry and its
// deadline. An event whose state is already `expired` deletes the entry
// instead; that removal is downstream of the event's own dispatch, since the
// event reached this leaf through a rule. It returns false when the slice is
// full and the identity is new, in which case the insert is refused and
// counted.
func (s *Slice) Insert(ev *event.Event) bool {
	k := Key{ev.Host, ev.Service}
	s.mu.Lock()
	if ev.State == event.StateExpired {
		delete(s.entries, k)
		s.mu.Unlock()
		return true
	}
	if _, exists := s.entries[k]; !exists && len(s.entries) >= s.maxEntries {
		s.mu.Unlock()
		s.rejected.Add(1)
		return false
	}
	s.gen++
	e := &entry{ev: ev, deadline: ev.Time + ev.TTL, gen: s.gen}
	s.entries[k] = e
	heap.Push(&s.heap, heapItem{deadline: e.deadline, key: k, gen: e.gen})
	s.mu.Unlock()
	if s.onInsert != nil {
		s.onInsert()
	}
	return true
}

// Get returns the entry for an identity, or nil when there is none or its
// deadline has passed. An entry is live on the closed interval
// [time, time+ttl], a default carried from upstream.
func (s *Slice) Get(host, service string, now float64) *event.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.entries[Key{host, service}]
	if e == nil || now > e.deadline {
		return nil
	}
	return e.ev
}

// Snapshot returns the live entries for which keep holds.
func (s *Slice) Snapshot(now float64, keep func(*event.Event) bool) []*event.Event {
	s.mu.RLock()
	live := make([]*event.Event, 0, len(s.entries))
	for _, e := range s.entries {
		if now <= e.deadline {
			live = append(live, e.ev)
		}
	}
	s.mu.RUnlock()
	// keep runs outside the lock: an expression evaluation must not hold up
	// an insert.
	out := live[:0]
	for _, ev := range live {
		if keep == nil || keep(ev) {
			out = append(out, ev)
		}
	}
	return out
}

// Len is the number of entries held, expired-but-unreaped included.
func (s *Slice) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Rejected is the count of inserts refused at the cardinality bound.
func (s *Slice) Rejected() int64 { return s.rejected.Load() }

// NextDeadline returns the earliest live deadline.
func (s *Slice) NextDeadline() (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropStale()
	if len(s.heap) == 0 {
		return 0, false
	}
	return s.heap[0].deadline, true
}

func (s *Slice) dropStale() {
	for len(s.heap) > 0 {
		top := s.heap[0]
		if e := s.entries[top.key]; e != nil && e.gen == top.gen {
			return
		}
		heap.Pop(&s.heap)
	}
}

// Due is an entry whose deadline has passed and whose expiry event has not
// been dispatched yet.
type Due struct {
	Key      Key
	Gen      uint64
	Deadline float64
}

// PopDue returns the earliest entry whose deadline is strictly before limit.
// The entry itself stays in the slice: the caller dispatches the expiry event
// and only then calls Remove (INVARIANTS.md I7).
func (s *Slice) PopDue(limit float64) (Due, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropStale()
	if len(s.heap) == 0 || !(s.heap[0].deadline < limit) {
		return Due{}, false
	}
	it := heap.Pop(&s.heap).(heapItem)
	return Due{Key: it.key, Gen: it.gen, Deadline: it.deadline}, true
}

// Remove deletes the entry a dispatched expiry event was about, unless a rule
// re-indexed the identity while that event was being dispatched.
func (s *Slice) Remove(d Due) {
	s.mu.Lock()
	if e := s.entries[d.Key]; e != nil && e.gen == d.Gen {
		delete(s.entries, d.Key)
	}
	s.mu.Unlock()
}
