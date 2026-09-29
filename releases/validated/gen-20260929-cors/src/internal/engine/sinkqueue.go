package engine

import (
	"sync"

	"github.com/tnn1t1s/riemann-go/internal/rule"
)

// SinkQueue is the bounded queue in front of a sink, with the counters the
// accounting identity needs. Its policy is shed-newest: a firing offered to a
// full queue is dropped and counted. Every counter moves under one lock, so
// Stats reads a state in which the identity holds exactly, including for the
// firings a worker has taken and not finished.
type SinkQueue struct {
	mu        sync.Mutex
	buf       []rule.Firing
	head      int
	count     int
	accepted  uint64
	processed uint64
	dropped   uint64
	inFlight  int

	// ready holds at most one token and tells the worker the queue is not
	// empty. It carries no firing.
	ready chan struct{}
}

// NewSinkQueue returns a queue holding at most capacity firings.
func NewSinkQueue(capacity int) *SinkQueue {
	return &SinkQueue{buf: make([]rule.Firing, capacity), ready: make(chan struct{}, 1)}
}

// Offer enqueues a firing, or sheds it when the queue is full. It never
// blocks.
func (q *SinkQueue) Offer(f rule.Firing) {
	q.mu.Lock()
	q.accepted++
	if q.count == len(q.buf) {
		q.dropped++
		q.mu.Unlock()
		return
	}
	q.buf[(q.head+q.count)%len(q.buf)] = f
	q.count++
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// Refuse counts a firing that was routed to the sink and cannot be sent, such
// as an influx point with no field.
func (q *SinkQueue) Refuse() {
	q.mu.Lock()
	q.accepted++
	q.dropped++
	q.mu.Unlock()
}

// Take waits for the queue to hold a firing and returns up to max of them,
// now counted as in flight. It returns nil when stop closes.
func (q *SinkQueue) Take(max int, stop <-chan struct{}) []rule.Firing {
	for {
		q.mu.Lock()
		if q.count > 0 {
			n := min(q.count, max)
			out := make([]rule.Firing, n)
			for i := range out {
				out[i] = q.buf[q.head]
				q.buf[q.head] = rule.Firing{}
				q.head = (q.head + 1) % len(q.buf)
			}
			q.count -= n
			q.inFlight += n
			q.mu.Unlock()
			return out
		}
		q.mu.Unlock()
		select {
		case <-stop:
			return nil
		case <-q.ready:
		}
	}
}

// Done settles n firings a worker took: processed when the far end answered,
// dropped when they were lost.
func (q *SinkQueue) Done(n int, processed bool) {
	q.mu.Lock()
	q.inFlight -= n
	if processed {
		q.processed += uint64(n)
	} else {
		q.dropped += uint64(n)
	}
	q.mu.Unlock()
}

// Stats reads the counters at one instant.
func (q *SinkQueue) Stats() SinkStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return SinkStats{
		Accepted:  q.accepted,
		Processed: q.processed,
		Dropped:   q.dropped,
		Depth:     q.count,
		Capacity:  len(q.buf),
		InFlight:  q.inFlight,
	}
}
