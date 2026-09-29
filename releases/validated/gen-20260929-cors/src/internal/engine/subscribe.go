package engine

import (
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

// Filter decides whether a subscriber wants an event. now is the time of the
// partition that produced it.
type Filter func(ev *event.Event, now float64) bool

// Subscriber is one subscription to index changes. Its queue is bounded by
// subscribe.queue_capacity; when full the newest event is shed and counted,
// and the subscriber is told how many it missed.
//
// SPEC-GAP: the spec does not say which events a subscription carries.
// Chosen: the changes to the index, which is what `snapshot=true` is a
// snapshot of: every event an index leaf inserts, and every expiry event.
type Subscriber struct {
	filter Filter
	queue  chan *event.Event
	missed atomic.Uint64
	e      *Engine
}

// offer is called under an index slice's lock and never blocks.
func (s *Subscriber) offer(ev *event.Event, now float64) {
	if s.filter != nil && !s.filter(ev, now) {
		return
	}
	select {
	case s.queue <- ev:
	default:
		s.missed.Add(1)
		s.e.subscribeDropped.Add(1)
	}
}

// Events is the subscriber's queue.
func (s *Subscriber) Events() <-chan *event.Event { return s.queue }

// TakeMissed returns the number of events shed since the last call, and
// resets it.
func (s *Subscriber) TakeMissed() uint64 { return s.missed.Swap(0) }

// Subscribe registers a subscriber on every partition's slice. With snapshot
// set it also returns the live entries that pass the filter, each slice taken
// under the same lock as that slice's registration.
func (e *Engine) Subscribe(filter Filter, snapshot bool) (*Subscriber, []*event.Event) {
	sub := &Subscriber{
		filter: filter,
		queue:  make(chan *event.Event, e.params.SubscribeQueueCapacity),
		e:      e,
	}
	var entries []*event.Event
	for _, s := range e.shards {
		now := s.rt.clock.now()
		for _, ev := range s.index.subscribe(sub, snapshot, now) {
			if filter == nil || filter(ev, now) {
				entries = append(entries, ev)
			}
		}
	}
	sortEvents(entries)
	e.subsMu.Lock()
	e.subs[sub] = struct{}{}
	e.subsMu.Unlock()
	return sub, entries
}

// Unsubscribe removes a subscriber.
func (e *Engine) Unsubscribe(sub *Subscriber) {
	for _, s := range e.shards {
		s.index.unsubscribe(sub)
	}
	e.subsMu.Lock()
	delete(e.subs, sub)
	e.subsMu.Unlock()
}
