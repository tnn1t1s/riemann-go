// Package sink holds the Sink interface and the bounded queue every sink
// sits behind. The queue's policy is shed-newest with a dropped counter; a
// shard loop offering into it never blocks. It imports only event.
package sink

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
)

// Sink delivers one event somewhere. Send may block; it runs on the queue's
// worker goroutine, never on a shard loop.
type Sink interface {
	Name() string
	Send(ctx context.Context, e event.Event) error
}

// Queue is a bounded queue in front of a Sink with one worker goroutine.
type Queue struct {
	sink Sink
	ch   chan event.Event

	offered   atomic.Uint64 // events offered by shard loops
	dropped   atomic.Uint64 // events shed because the queue was full
	processed atomic.Uint64 // events for which Send returned
	errors    atomic.Uint64 // Sends that returned an error
	inflight  atomic.Int64  // 1 while the worker is inside Send

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewQueue wraps s behind a queue of the given capacity in events.
// Parameter: sink.<name>.queue_capacity. Owner: sink. Units: events; the
// caller supplies the value for its sink.
func NewQueue(s Sink, capacity int) *Queue {
	return &Queue{sink: s, ch: make(chan event.Event, capacity)}
}

// Name is the sink's name.
func (q *Queue) Name() string { return q.sink.Name() }

// Start runs the worker until Stop.
func (q *Queue) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	q.cancel = cancel
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-q.ch:
				q.inflight.Store(1)
				if err := q.sink.Send(ctx, e); err != nil {
					q.errors.Add(1)
				}
				q.processed.Add(1)
				q.inflight.Store(0)
			}
		}
	}()
}

// Stop ends the worker. Queued events are abandoned uncounted; stopping is
// not a shed path.
func (q *Queue) Stop() {
	if q.cancel != nil {
		q.cancel()
		q.wg.Wait()
	}
}

// Offer enqueues e or, if the queue is full, sheds it and counts the drop.
func (q *Queue) Offer(e event.Event) bool {
	q.offered.Add(1)
	select {
	case q.ch <- e:
		return true
	default:
		q.dropped.Add(1)
		return false
	}
}

// Metrics is a queue's self-observation snapshot. Depth, capacity and
// dropped are what the 202 reply carries.
type Metrics struct {
	Depth     int    `json:"depth"`
	Capacity  int    `json:"capacity"`
	Dropped   uint64 `json:"dropped"`
	Offered   uint64 `json:"offered,omitempty"`
	Processed uint64 `json:"processed,omitempty"`
	Errors    uint64 `json:"errors,omitempty"`
	InFlight  int64  `json:"in_flight,omitempty"`
}

// Metrics reads the queue's gauges.
func (q *Queue) Metrics() Metrics {
	return Metrics{
		Depth:     len(q.ch),
		Capacity:  cap(q.ch),
		Dropped:   q.dropped.Load(),
		Offered:   q.offered.Load(),
		Processed: q.processed.Load(),
		Errors:    q.errors.Load(),
		InFlight:  q.inflight.Load(),
	}
}

// Counting is a test sink: it counts what it receives and can sleep per
// event to model a slow destination.
type Counting struct {
	name  string
	delay time.Duration
	n     atomic.Uint64
}

// NewCounting returns a counting sink named name that sleeps delay per Send.
func NewCounting(name string, delay time.Duration) *Counting {
	return &Counting{name: name, delay: delay}
}

func (c *Counting) Name() string { return c.name }

func (c *Counting) Send(ctx context.Context, e event.Event) error {
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.n.Add(1)
	return nil
}

// Count is the number of events Send has completed.
func (c *Counting) Count() uint64 { return c.n.Load() }
