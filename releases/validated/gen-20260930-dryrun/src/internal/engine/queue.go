package engine

import (
	"sync"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

// Firing is one event reaching one sink leaf through one rule.
type Firing struct {
	Sink       string
	Rule       string
	Version    int64
	Owner      string
	Node       string
	PriorState *string
	Event      *event.Event
}

// SinkStats is a snapshot of one sink queue's accounting.
type SinkStats struct {
	Name      string
	Depth     int
	Capacity  int
	Accepted  int64
	Processed int64
	Dropped   int64
	Failed    int64
	InFlight  int64
}

// Residual is accepted - (processed + dropped + queued + in_flight).
func (s SinkStats) Residual() int64 {
	return s.Accepted - (s.Processed + s.Dropped + int64(s.Depth) + s.InFlight)
}

// SinkQueue is a bounded, shed-newest queue between the loops and a sink
// worker. One mutex guards both the buffer and the counters so a snapshot
// sees every event in exactly one of queued, in-flight, processed or dropped,
// which is what closes SCALE.md's accounting identity.
type SinkQueue struct {
	name string
	mu   sync.Mutex
	cond *sync.Cond
	buf  []Firing
	head int
	n    int

	accepted  int64
	processed int64
	dropped   int64
	failed    int64
	inflight  int64
	closed    bool
}

// NewSinkQueue returns a queue of the given capacity.
func NewSinkQueue(name string, capacity int) *SinkQueue {
	q := &SinkQueue{name: name, buf: make([]Firing, capacity)}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Offer admits a firing, or sheds it and counts the drop when full.
func (q *SinkQueue) Offer(f Firing) {
	q.mu.Lock()
	q.accepted++
	if q.n == len(q.buf) {
		q.dropped++
		q.mu.Unlock()
		return
	}
	q.buf[(q.head+q.n)%len(q.buf)] = f
	q.n++
	q.mu.Unlock()
	q.cond.Signal()
}

// Reject records a firing that the sink refuses by contract before it is
// queued, such as an influx point with no metric: accepted and dropped both
// count it, so the identity still closes.
func (q *SinkQueue) Reject() {
	q.mu.Lock()
	q.accepted++
	q.dropped++
	q.mu.Unlock()
}

// Run drains the queue in batches of at most maxBatch and hands each batch to
// deliver. A batch that deliver fails counts as dropped, since the sink
// receiver did not observe it. Run returns when Close is called.
func (q *SinkQueue) Run(maxBatch int, deliver func([]Firing) error) {
	if maxBatch < 1 {
		maxBatch = 1
	}
	for {
		q.mu.Lock()
		for q.n == 0 && !q.closed {
			q.cond.Wait()
		}
		if q.n == 0 && q.closed {
			q.mu.Unlock()
			return
		}
		k := q.n
		if k > maxBatch {
			k = maxBatch
		}
		batch := make([]Firing, k)
		for i := 0; i < k; i++ {
			batch[i] = q.buf[q.head]
			q.buf[q.head] = Firing{}
			q.head = (q.head + 1) % len(q.buf)
		}
		q.n -= k
		q.inflight += int64(k)
		q.mu.Unlock()

		err := deliver(batch)

		q.mu.Lock()
		q.inflight -= int64(k)
		if err != nil {
			// SPEC-GAP: SCALE.md's identity has no term for a delivery the
			// sink refused. Counting it as dropped keeps the identity exact
			// and keeps the loss visible; `failed` records it separately.
			q.dropped += int64(k)
			q.failed += int64(k)
		} else {
			q.processed += int64(k)
		}
		q.mu.Unlock()
	}
}

// Close stops Run once the queue drains.
func (q *SinkQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// Stats snapshots the queue under its lock.
func (q *SinkQueue) Stats() SinkStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return SinkStats{
		Name: q.name, Depth: q.n, Capacity: len(q.buf),
		Accepted: q.accepted, Processed: q.processed, Dropped: q.dropped,
		Failed: q.failed, InFlight: q.inflight,
	}
}
