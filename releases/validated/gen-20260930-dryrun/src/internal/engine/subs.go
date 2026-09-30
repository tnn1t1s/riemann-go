package engine

import (
	"sync"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/exprs"
)

// Subscriber is one SSE client's bounded queue. Policy is shed-newest; the
// drops are counted and reported to the client as a lagged frame.
type Subscriber struct {
	C       chan *event.Event
	dropped atomic.Int64
	prog    *exprs.Program
}

// Dropped returns the cumulative count of events shed for this subscriber.
func (s *Subscriber) Dropped() int64 { return s.dropped.Load() }

type subscribers struct {
	mu       sync.RWMutex
	set      map[*Subscriber]struct{}
	capacity int
	dropped  atomic.Int64
}

func newSubscribers(capacity int) *subscribers {
	return &subscribers{set: map[*Subscriber]struct{}{}, capacity: capacity}
}

// publish offers an event to every subscriber whose query holds. It runs on
// a partition loop and never blocks.
func (s *subscribers) publish(ev *event.Event, now float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.set) == 0 {
		return
	}
	var env map[string]any
	for sub := range s.set {
		if sub.prog != nil {
			if env == nil {
				env = ev.Env(now, nil)
			}
			if !sub.prog.Bool(env) {
				continue
			}
		}
		select {
		case sub.C <- ev:
		default:
			sub.dropped.Add(1)
			s.dropped.Add(1)
		}
	}
}

// Subscribe registers a subscriber and, when snapshot is true, takes the
// index snapshot under the same lock, so no event falls between the two.
func (e *Engine) Subscribe(q string, snapshot bool) (*Subscriber, []*event.Event, error) {
	var prog *exprs.Program
	if q != "" {
		p, err := exprs.CompilePredicate(q, false)
		if err != nil {
			return nil, nil, err
		}
		prog = p
	}
	sub := &Subscriber{C: make(chan *event.Event, e.subs.capacity), prog: prog}
	e.subs.mu.Lock()
	e.subs.set[sub] = struct{}{}
	var snap []*event.Event
	if snapshot {
		now := nowf()
		for _, ev := range e.indexSnapshot() {
			if prog == nil || prog.Bool(ev.Env(now, nil)) {
				snap = append(snap, ev)
			}
		}
	}
	e.subs.mu.Unlock()
	return sub, snap, nil
}

// Unsubscribe removes a subscriber.
func (e *Engine) Unsubscribe(sub *Subscriber) {
	e.subs.mu.Lock()
	delete(e.subs.set, sub)
	e.subs.mu.Unlock()
}
