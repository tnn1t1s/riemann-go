package shard

import (
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/index"
)

// DefaultSubscriberCapacity bounds one subscriber's queue.
// Parameter: subscribe.queue_capacity. Owner: transport/http. Units: events
// per subscriber. Default 1000, the same order as the ntfy sink; a lagging
// subscriber gets a lagged frame carrying the shed count. Provisional.
const DefaultSubscriberCapacity = 1000

// Subscription is one subscriber's bounded queue, policy shed-newest. It may
// be attached to several shards; each publishes into the same queue.
type Subscription struct {
	pred   func(event.Event) bool
	ch     chan event.Event
	lagged atomic.Uint64 // events shed since the reader last drained the count
	total  atomic.Uint64 // events shed over the subscription's life
}

// NewSubscription returns a queue of the given capacity delivering events
// for which pred holds.
func NewSubscription(pred func(event.Event) bool, capacity int) *Subscription {
	return &Subscription{pred: pred, ch: make(chan event.Event, capacity)}
}

// offer runs on a shard loop: never blocks.
func (sub *Subscription) offer(e event.Event) {
	select {
	case sub.ch <- e:
	default:
		sub.lagged.Add(1)
		sub.total.Add(1)
	}
}

// Events is the delivery channel.
func (sub *Subscription) Events() <-chan event.Event { return sub.ch }

// Lagged returns and resets the number of events shed since the last call.
func (sub *Subscription) Lagged() uint64 { return sub.lagged.Swap(0) }

// Dropped is the total shed over the subscription's life.
func (sub *Subscription) Dropped() uint64 { return sub.total.Load() }

// Attach registers sub with this shard and, if snapshot is set, returns the
// index entries matching its predicate as of the same instant. Both happen
// in one Do, so no event indexed by this shard falls between them. It
// returns false if the shard has stopped.
func (s *Shard) Attach(sub *Subscription, snapshot bool) ([]event.Event, bool) {
	var snap []event.Event
	ok := s.Do(func() {
		if snapshot {
			s.idx.Each(func(en index.Entry) {
				if sub.pred(en.Event) {
					snap = append(snap, en.Event)
				}
			})
		}
		s.subs = append(s.subs, sub)
		s.subsLen.Store(int64(len(s.subs)))
	})
	return snap, ok
}

// Detach removes sub from this shard.
func (s *Shard) Detach(sub *Subscription) {
	s.Do(func() {
		for i, x := range s.subs {
			if x == sub {
				s.subs = append(s.subs[:i], s.subs[i+1:]...)
				break
			}
		}
		s.subsLen.Store(int64(len(s.subs)))
	})
}
