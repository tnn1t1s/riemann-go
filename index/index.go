// Package index is one shard's slice of the index: the last event per
// (host, service) plus a min-heap of expiry times. Insert pushes a heap item
// carrying the entry's generation; Expire pops due items and discards stale
// generations, so expiry is O(log n) per entry rather than a scan. Nothing
// here locks; the owning shard loop is the only caller.
package index

import (
	"container/heap"

	"github.com/tnn1t1s/riemann-go/event"
)

// DefaultMaxEntries is the cardinality cap per shard.
// Parameter: index.max_entries_per_shard. Owner: index. Units: entries.
// Default 1,000,000: at an estimated 300 bytes per entry that is 300 MB per
// shard; provisional until the milestone 1 mirror observes real cardinality.
// On overflow an insert is refused and counted, never silently evicted.
const DefaultMaxEntries = 1_000_000

// Entry is one indexed event with its last state transition time.
type Entry struct {
	Event          event.Event
	LastTransition float64 // event time at which State last changed, float seconds
	gen            uint64
}

type heapItem struct {
	at  float64 // expiry time, float seconds
	key event.Key
	gen uint64
}

type expiryHeap []heapItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(heapItem)) }
func (h *expiryHeap) Pop() any          { o := *h; n := len(o); it := o[n-1]; *h = o[:n-1]; return it }

// Index is one shard's slice.
type Index struct {
	entries    map[event.Key]*Entry
	heap       expiryHeap
	gen        uint64
	maxEntries int
	rejected   uint64
	expired    uint64
}

// New returns an empty index capped at maxEntries live entries.
func New(maxEntries int) *Index {
	return &Index{entries: map[event.Key]*Entry{}, maxEntries: maxEntries}
}

// Insert records e as the current event for its key and schedules its
// expiry at time + ttl. An event whose state is "expired" deletes the key
// instead, as upstream index.clj insert does. Insert returns false when the
// cardinality cap refuses a new key; the refusal is counted.
func (ix *Index) Insert(e event.Event) bool {
	k := e.Key()
	if e.Expired() {
		delete(ix.entries, k)
		return true
	}
	en, ok := ix.entries[k]
	if !ok {
		if len(ix.entries) >= ix.maxEntries {
			ix.rejected++
			return false
		}
		en = &Entry{LastTransition: e.Time}
		ix.entries[k] = en
	} else if en.Event.State != e.State {
		en.LastTransition = e.Time
	}
	ix.gen++
	en.Event = e
	en.gen = ix.gen
	heap.Push(&ix.heap, heapItem{at: e.Time + e.TTL, key: k, gen: ix.gen})
	return true
}

// Lookup returns the entry for (host, service).
func (ix *Index) Lookup(host, service string) (Entry, bool) {
	en, ok := ix.entries[event.Key{Host: host, Service: service}]
	if !ok {
		return Entry{}, false
	}
	return *en, true
}

// Each calls fn for every live entry, in map order.
func (ix *Index) Each(fn func(Entry)) {
	for _, en := range ix.entries {
		fn(*en)
	}
}

// Len is the number of live entries.
func (ix *Index) Len() int { return len(ix.entries) }

// Rejected is the number of inserts refused by the cardinality cap.
func (ix *Index) Rejected() uint64 { return ix.rejected }

// Expired is the number of entries expired so far.
func (ix *Index) Expired() uint64 { return ix.expired }

// HeapLen is the number of pending expiry items, stale ones included.
func (ix *Index) HeapLen() int { return len(ix.heap) }

// NextExpiry returns the earliest pending expiry time, if any. It may be a
// stale generation; Expire skips those.
func (ix *Index) NextExpiry() (float64, bool) {
	if len(ix.heap) == 0 {
		return 0, false
	}
	return ix.heap[0].at, true
}

// Expire removes every entry whose time + ttl is at or before now and
// returns, for each, the synthesised expired event: host and service kept,
// state "expired", time now, nothing else, as upstream core.clj:274-308.
// Upstream's reaper expires strictly after ttl has elapsed (index.clj expire
// tests ttl < age); this expires at exactly time + ttl so a wake timer armed
// at that instant is not a spin.
func (ix *Index) Expire(now float64) []event.Event {
	var out []event.Event
	for len(ix.heap) > 0 && ix.heap[0].at <= now {
		it := heap.Pop(&ix.heap).(heapItem)
		en, ok := ix.entries[it.key]
		if !ok || en.gen != it.gen {
			continue // stale generation: the key was re-inserted or deleted
		}
		delete(ix.entries, it.key)
		ix.expired++
		out = append(out, event.Event{Host: it.key.Host, Service: it.key.Service, State: event.StateExpired, Time: now})
	}
	return out
}
