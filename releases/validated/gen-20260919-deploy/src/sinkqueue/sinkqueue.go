// Package sinkqueue is the bounded queue in front of a sink. Its policy is
// shed-newest, and every state an item can be in is counted under one lock so
// the accounting identity of SCALE.md,
//
//	accepted = processed + dropped + queued + in_flight
//
// holds at every instant a snapshot can be taken, not just at rest.
package sinkqueue

import (
	"context"
	"sync"
)

// Stats is one consistent reading of a queue.
type Stats struct {
	Depth     int
	Capacity  int
	Accepted  int64
	Processed int64
	Dropped   int64
	InFlight  int64
}

// Residual is accepted - (processed + dropped + queued + in_flight). A correct
// queue reports zero.
func (s Stats) Residual() int64 {
	return s.Accepted - (s.Processed + s.Dropped + int64(s.Depth) + s.InFlight)
}

// Queue is a bounded FIFO with one consumer.
type Queue[T any] struct {
	mu        sync.Mutex
	buf       []T
	head, n   int
	accepted  int64
	processed int64
	dropped   int64
	inFlight  int64
	notify    chan struct{}
}

// New returns a queue holding at most capacity items.
func New[T any](capacity int) *Queue[T] {
	return &Queue[T]{buf: make([]T, capacity), notify: make(chan struct{}, 1)}
}

// Offer routes one item to the sink. It never blocks: when the queue is full
// the item is shed and counted, and Offer returns false.
func (q *Queue[T]) Offer(item T) bool {
	q.mu.Lock()
	q.accepted++
	if q.n == len(q.buf) {
		q.dropped++
		q.mu.Unlock()
		return false
	}
	q.buf[(q.head+q.n)%len(q.buf)] = item
	q.n++
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

// Refuse counts an item that was routed to the sink and cannot be written at
// all, such as an influx point with no field. It is accepted and dropped in
// one step.
func (q *Queue[T]) Refuse() {
	q.mu.Lock()
	q.accepted++
	q.dropped++
	q.mu.Unlock()
}

// Stats returns a consistent reading.
func (q *Queue[T]) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{Depth: q.n, Capacity: len(q.buf), Accepted: q.accepted,
		Processed: q.processed, Dropped: q.dropped, InFlight: q.inFlight}
}

func (q *Queue[T]) take(max int) []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	k := q.n
	if k > max {
		k = max
	}
	if k == 0 {
		return nil
	}
	out := make([]T, k)
	var zero T
	for i := 0; i < k; i++ {
		out[i] = q.buf[q.head]
		q.buf[q.head] = zero
		q.head = (q.head + 1) % len(q.buf)
	}
	q.n -= k
	q.inFlight += int64(k)
	return out
}

func (q *Queue[T]) settle(k int, delivered bool) {
	q.mu.Lock()
	q.inFlight -= int64(k)
	if delivered {
		q.processed += int64(k)
	} else {
		q.dropped += int64(k)
	}
	q.mu.Unlock()
}

// Run is the consumer. It hands deliver up to maxBatch items at a time and
// returns when ctx is done.
//
// SPEC-GAP: the spec does not say what happens to a firing whose delivery
// fails (connection refused, a non-2xx reply). Chosen: no retry; the items
// count as dropped, so the identity still closes and the loss is visible.
func (q *Queue[T]) Run(ctx context.Context, maxBatch int, deliver func(context.Context, []T) error) {
	for {
		batch := q.take(maxBatch)
		if batch == nil {
			select {
			case <-ctx.Done():
				return
			case <-q.notify:
			}
			continue
		}
		err := deliver(ctx, batch)
		q.settle(len(batch), err == nil)
		if ctx.Err() != nil {
			return
		}
	}
}
