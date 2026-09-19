// Package index holds one partition's slice of the index: the last event
// indexed per (host, service), and the deadlines at which entries expire.
//
// A slice is guarded by a mutex rather than owned by its shard goroutine,
// because a set node may rewrite host and so index into another partition's
// slice, and because the read surface reads every slice.
package index

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
)

type key struct{ host, service string }

type entry struct {
	ev       *event.Event
	deadline float64 // time + ttl; live on the closed interval [time, deadline]
	gen      uint64
	heapIdx  int // -1 while the entry's expiry event is being dispatched
	key      key
}

type expiryHeap []*entry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].deadline < h[j].deadline }
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx, h[j].heapIdx = i, j
}
func (h *expiryHeap) Push(x any) {
	e := x.(*entry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}
func (h *expiryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	e.heapIdx = -1
	return e
}

// Slice is one partition's index.
type Slice struct {
	mu         sync.Mutex
	entries    map[key]*entry
	heap       expiryHeap
	gen        uint64
	maxEntries int // index.max_entries_per_shard
	rejected   atomic.Int64
	size       atomic.Int64
}

// New returns an empty slice bounded at maxEntries.
func New(maxEntries int) *Slice {
	return &Slice{entries: map[key]*entry{}, maxEntries: maxEntries}
}

// Insert stores ev, replacing any entry for its identity along with that
// entry's deadline. An event whose state is already expired deletes the entry
// instead; the event causing that removal has already been dispatched to the
// rules, so no second expiry event follows. It returns false when the insert
// was refused because the slice is full, which is counted and never evicts.
func (s *Slice) Insert(ev *event.Event) bool {
	k := key{ev.Host, ev.Service}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[k]
	if ev.State == event.StateExpired {
		if e != nil {
			s.remove(e)
		}
		return true
	}
	if e == nil {
		if len(s.entries) >= s.maxEntries {
			s.rejected.Add(1)
			return false
		}
		e = &entry{key: k, heapIdx: -1}
		s.entries[k] = e
		s.size.Store(int64(len(s.entries)))
	}
	s.gen++
	e.ev, e.deadline, e.gen = ev, ev.Time+ev.TTL, s.gen
	if e.heapIdx >= 0 {
		heap.Fix(&s.heap, e.heapIdx)
	} else {
		heap.Push(&s.heap, e)
	}
	return true
}

func (s *Slice) remove(e *entry) {
	if e.heapIdx >= 0 {
		heap.Remove(&s.heap, e.heapIdx)
	}
	delete(s.entries, e.key)
	s.size.Store(int64(len(s.entries)))
}

// Get returns the entry for an identity. An entry whose deadline has passed
// reads as absent even when its expiry has not been processed yet.
func (s *Slice) Get(host, service string, now float64) (*event.Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[key{host, service}]
	if e == nil || now > e.deadline {
		return nil, false
	}
	return e.ev, true
}

// Live returns every entry that has not passed its deadline at now.
func (s *Slice) Live(now float64) []*event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*event.Event, 0, len(s.entries))
	for _, e := range s.entries {
		if now <= e.deadline {
			out = append(out, e.ev)
		}
	}
	return out
}

// NextDeadline returns the earliest pending deadline.
func (s *Slice) NextDeadline() (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.heap) == 0 {
		return 0, false
	}
	return s.heap[0].deadline, true
}

// Due identifies an entry whose expiry event must now be dispatched.
type Due struct {
	Host, Service string
	Deadline      float64
	gen           uint64
}

// BeginExpire takes the earliest entry whose deadline is strictly before
// limit off the deadline heap. The entry itself stays until FinishExpire, so
// that removal is a consequence of the expiry event's dispatch (I7).
func (s *Slice) BeginExpire(limit float64) (Due, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.heap) == 0 || !(s.heap[0].deadline < limit) {
		return Due{}, false
	}
	e := heap.Pop(&s.heap).(*entry)
	return Due{Host: e.key.host, Service: e.key.service, Deadline: e.deadline, gen: e.gen}, true
}

// FinishExpire removes the entry after its expiry event has been dispatched,
// unless a rule indexed the identity afresh in the meantime.
func (s *Slice) FinishExpire(d Due) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entries[key{d.Host, d.Service}]; e != nil && e.gen == d.gen {
		s.remove(e)
	}
}

// Len is the number of entries held, including any awaiting FinishExpire.
func (s *Slice) Len() int64 { return s.size.Load() }

// Rejected counts inserts refused at the bound: riemann.index.rejected.
func (s *Slice) Rejected() int64 { return s.rejected.Load() }
