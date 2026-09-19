// Package boundedq is the bounded, shed-newest queue a sink puts between the
// processing loop and its HTTP client. It imports the standard library only.
//
// Every transition moves an event between exactly two of the five counts
// under one lock, so a reading taken under that lock satisfies
// accepted = processed + dropped + queued + in_flight with no residue.
package boundedq

import "sync"

// Stats is one consistent reading.
type Stats struct {
	Depth     int64
	Capacity  int64
	Dropped   int64
	Accepted  int64
	Processed int64
	InFlight  int64
}

// Queue holds up to capacity items. Offer never blocks.
type Queue[T any] struct {
	mu       sync.Mutex
	nonEmpty *sync.Cond
	items    []T
	capacity int

	accepted, processed, dropped, inFlight int64
}

// New returns a queue bounded at capacity.
func New[T any](capacity int) *Queue[T] {
	q := &Queue[T]{capacity: capacity}
	q.nonEmpty = sync.NewCond(&q.mu)
	return q
}

// Offer enqueues v, or sheds it and counts the drop when the queue is full.
func (q *Queue[T]) Offer(v T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.accepted++
	if len(q.items) >= q.capacity {
		q.dropped++
		return false
	}
	q.items = append(q.items, v)
	q.nonEmpty.Signal()
	return true
}

// Refuse counts an event that was routed here and cannot be delivered at all.
func (q *Queue[T]) Refuse() {
	q.mu.Lock()
	q.accepted++
	q.dropped++
	q.mu.Unlock()
}

// Take blocks until at least one item is queued, then moves up to max items
// into flight and returns them.
func (q *Queue[T]) Take(max int) []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		q.nonEmpty.Wait()
	}
	n := min(max, len(q.items))
	out := make([]T, n)
	copy(out, q.items)
	var zero T
	for i := 0; i < n; i++ {
		q.items[i] = zero
	}
	q.items = q.items[n:]
	if len(q.items) == 0 {
		q.items = nil // let the backing array go once drained
	}
	q.inFlight += int64(n)
	return out
}

// Done settles n items taken earlier: delivered ones count as processed, the
// rest as dropped.
func (q *Queue[T]) Done(n int, delivered bool) {
	q.mu.Lock()
	q.inFlight -= int64(n)
	if delivered {
		q.processed += int64(n)
	} else {
		q.dropped += int64(n)
	}
	q.mu.Unlock()
}

// Stats reads all five counts together.
func (q *Queue[T]) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{Depth: int64(len(q.items)), Capacity: int64(q.capacity), Dropped: q.dropped,
		Accepted: q.accepted, Processed: q.processed, InFlight: q.inFlight}
}
