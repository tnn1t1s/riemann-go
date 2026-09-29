package engine

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

type indexEntry struct {
	ev  *event.Event
	gen uint64
}

type expiryItem struct {
	deadline float64
	key      event.Key
	gen      uint64
}

type expiryHeap []expiryItem

func (h expiryHeap) Len() int { return len(h) }
func (h expiryHeap) Less(i, j int) bool {
	if h[i].deadline == h[j].deadline {
		return h[i].gen < h[j].gen
	}
	return h[i].deadline < h[j].deadline
}
func (h expiryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)   { *h = append(*h, x.(expiryItem)) }
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = expiryItem{}
	*h = old[:n-1]
	return it
}

// index is one partition's slice of the index.
//
// SPEC-FREE: the index structure and how expiry is detected. Chosen: a map
// from identity to the last event indexed, and a min-heap of deadlines whose
// items carry the entry's generation, so an item left behind by a replaced
// entry is discarded when it surfaces. The slice has its own lock rather than
// being owned by the loop, because a `set` that rewrites `host` and a global
// rule both insert into a slice from outside its partition's goroutine, and
// because a read then never waits behind the inbox.
type index struct {
	mu      sync.RWMutex
	entries map[event.Key]*indexEntry
	expiry  expiryHeap
	gen     uint64
	max     int
	subs    []*Subscriber

	rejected atomic.Uint64
	size     atomic.Int64
}

func newIndex(maxEntries int) *index {
	return &index{entries: map[event.Key]*indexEntry{}, max: maxEntries}
}

// insert stores the event as the entry for its identity, or, when the event's
// state is expired, removes that entry. now is the partition's time.
func (ix *index) insert(ev *event.Event, now float64) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	key := ev.Key()
	if ev.State == event.StateExpired {
		// This removal is downstream of a dispatch: the event that causes
		// it reached this leaf through a rule. No second expiry event is
		// synthesized for it.
		if _, ok := ix.entries[key]; ok {
			delete(ix.entries, key)
			ix.size.Store(int64(len(ix.entries)))
		}
		ix.publish(ev, now)
		return
	}
	if _, ok := ix.entries[key]; !ok && len(ix.entries) >= ix.max {
		ix.rejected.Add(1)
		return
	}
	ix.gen++
	ix.entries[key] = &indexEntry{ev: ev, gen: ix.gen}
	heap.Push(&ix.expiry, expiryItem{deadline: ev.Deadline(), key: key, gen: ix.gen})
	ix.size.Store(int64(len(ix.entries)))
	ix.publish(ev, now)
}

// live reports whether an entry is within its lease. The entry is live on the
// closed interval [time, time + ttl] and expires after it.
func live(ev *event.Event, now float64) bool { return now <= ev.Deadline() }

// get returns the entry for an identity, or nil when there is none or its
// deadline has passed, whether or not the expiry has been processed yet.
func (ix *index) get(key event.Key, now float64) *event.Event {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	en, ok := ix.entries[key]
	if !ok || !live(en.ev, now) {
		return nil
	}
	return en.ev
}

// slice returns every live entry.
func (ix *index) slice(now float64) []*event.Event {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.sliceLocked(now)
}

func (ix *index) sliceLocked(now float64) []*event.Event {
	out := make([]*event.Event, 0, len(ix.entries))
	for _, en := range ix.entries {
		if live(en.ev, now) {
			out = append(out, en.ev)
		}
	}
	return out
}

// nextDeadline returns the earliest deadline of a current entry, dropping
// heap items that a replaced or removed entry left behind.
func (ix *index) nextDeadline() (float64, bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for len(ix.expiry) > 0 {
		top := ix.expiry[0]
		if en, ok := ix.entries[top.key]; ok && en.gen == top.gen {
			return top.deadline, true
		}
		heap.Pop(&ix.expiry)
	}
	return 0, false
}

// overdue returns the entry with the earliest deadline if that deadline lies
// before limit. The entry stays in the map, where reads already treat it as
// absent, until remove is called after its expiry event has been dispatched.
func (ix *index) overdue(limit float64) (*event.Event, uint64, bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for len(ix.expiry) > 0 {
		top := ix.expiry[0]
		en, ok := ix.entries[top.key]
		if !ok || en.gen != top.gen {
			heap.Pop(&ix.expiry)
			continue
		}
		if !(limit > top.deadline) {
			return nil, 0, false
		}
		heap.Pop(&ix.expiry)
		return en.ev, en.gen, true
	}
	return nil, 0, false
}

// remove deletes an expired entry after its expiry event was dispatched. It
// does nothing when a rule has since replaced or removed the entry. The
// expiry event is published to subscribers here, so one that a rule routed to
// an index leaf, which published it already, is not sent twice.
func (ix *index) remove(expired *event.Event, gen uint64, now float64) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	key := expired.Key()
	en, ok := ix.entries[key]
	if !ok || en.gen != gen {
		return
	}
	delete(ix.entries, key)
	ix.size.Store(int64(len(ix.entries)))
	ix.publish(expired, now)
}

// publish offers an index change to every subscriber. Called with mu held.
func (ix *index) publish(ev *event.Event, now float64) {
	for _, sub := range ix.subs {
		sub.offer(ev, now)
	}
}

// subscribe registers a subscriber and, when snapshot is set, returns the
// live entries as of the same instant, under the same lock, so no index
// change falls between the two.
func (ix *index) subscribe(sub *Subscriber, snapshot bool, now float64) []*event.Event {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var entries []*event.Event
	if snapshot {
		entries = ix.sliceLocked(now)
	}
	ix.subs = append(ix.subs, sub)
	return entries
}

func (ix *index) unsubscribe(sub *Subscriber) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	next := make([]*Subscriber, 0, len(ix.subs))
	for _, s := range ix.subs {
		if s != sub {
			next = append(next, s)
		}
	}
	ix.subs = next
}
