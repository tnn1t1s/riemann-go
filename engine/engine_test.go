package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/shard"
	"github.com/tnn1t1s/riemann-go/sink"
	"github.com/tnn1t1s/riemann-go/stream"
)

// gate is a test sink whose Send blocks until released, so the worker's
// in-flight gauge can be read while it is exactly 1.
type gate struct {
	name string
	hold chan struct{}
	in   chan struct{}
}

func newGate(name string) *gate {
	return &gate{name: name, hold: make(chan struct{}), in: make(chan struct{}, 1024)}
}
func (g *gate) Name() string { return g.name }
func (g *gate) Send(ctx context.Context, e event.Event) error {
	g.in <- struct{}{}
	select {
	case <-g.hold:
	case <-ctx.Done():
	}
	return nil
}

func now() float64 { return event.Seconds(time.Now()) }

func events(n int) []event.Event {
	out := make([]event.Event, n)
	for i := range out {
		out[i] = event.Event{Host: fmt.Sprintf("h%d", i%7), Service: fmt.Sprint(i), Time: now(), TTL: 60}
	}
	return out
}

// waitFor polls cond for up to a second.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func shardIdentity(t *testing.T, s *shard.Shard, wantInflight int64) {
	t.Helper()
	m := s.Metrics()
	if m.InFlight != wantInflight {
		t.Errorf("in_flight %d want %d", m.InFlight, wantInflight)
	}
	if got := m.Processed + uint64(m.InboxDepth) + uint64(m.InFlight); got != m.Accepted {
		t.Errorf("shard: accepted %d != processed %d + depth %d + in_flight %d", m.Accepted, m.Processed, m.InboxDepth, m.InFlight)
	}
}

func sinkIdentity(t *testing.T, q *sink.Queue, wantInflight int64) {
	t.Helper()
	m := q.Metrics()
	if m.InFlight != wantInflight {
		t.Errorf("sink in_flight %d want %d", m.InFlight, wantInflight)
	}
	if got := m.Processed + m.Dropped + uint64(m.Depth) + uint64(m.InFlight); got != m.Offered {
		t.Errorf("sink: offered %d != processed %d + dropped %d + depth %d + in_flight %d", m.Offered, m.Processed, m.Dropped, m.Depth, m.InFlight)
	}
}

// TestAccountingIdentity is invariant 6 at both gauges: with the loop held
// inside an event and the sink worker held inside Send, the identities
// close exactly with in_flight = 1; after release they close with 0.
func TestAccountingIdentity(t *testing.T) {
	e := New(Config{Shards: 1, Shard: shard.Config{InboxCapacity: 8, RingEvents: 100}}, stream.StdClock{})
	g := newGate("gate")
	q := e.AddSink(g, 4)
	loopHold := make(chan struct{})
	loopIn := make(chan struct{}, 1024)
	e.SetRules(RuleSet{Host: func(s *shard.Shard) stream.Stream {
		// Loop work injected: every event parks here until released.
		work := func(event.Event) { loopIn <- struct{}{}; <-loopHold }
		return stream.Fanout(work, s.IndexSink(), func(ev event.Event) { q.Offer(ev) })
	}})
	e.Start()
	defer e.Stop()

	// Deadline admission: one event is in flight, eight fill the inbox, the
	// tenth stalls past the deadline and the rest are refused.
	const deadline = 20 * time.Millisecond // short so the test is quick; the product default is 200 ms
	accepted, rejected := e.Admit(events(20), deadline)
	if accepted != 9 || rejected != 11 {
		t.Fatalf("accepted %d rejected %d", accepted, rejected)
	}
	<-loopIn
	shardIdentity(t, e.Shards()[0], 1)
	if m := e.Metrics(); m.Ingest.Accepted != 9 || m.Ingest.Rejected != 11 {
		t.Errorf("ingest metrics %+v", m.Ingest)
	}

	// Release the loop: nine events reach the sink queue (capacity 4). The
	// worker holds one, four queue, four are shed.
	close(loopHold)
	<-g.in
	waitFor(t, "sink queue to fill", func() bool { m := q.Metrics(); return m.Offered == 9 })
	shardIdentity(t, e.Shards()[0], 0)
	sinkIdentity(t, q, 1)
	if m := q.Metrics(); m.Dropped != 4 || m.Depth != 4 {
		t.Errorf("sink metrics %+v", m)
	}

	close(g.hold)
	waitFor(t, "sink to drain", func() bool { m := q.Metrics(); return m.Processed == 5 })
	sinkIdentity(t, q, 0)
	if e.Metrics().Global.Accepted != 9 {
		t.Errorf("global shard accepted %d", e.Metrics().Global.Accepted)
	}
}

// TestSlowSinkNeverBlocksLoop: a sink sleeping per event sheds, the loop
// keeps admitting, and the identity still closes at quiescence.
func TestSlowSinkNeverBlocksLoop(t *testing.T) {
	e := New(Config{Shards: 2, Shard: shard.Config{InboxCapacity: 64, RingEvents: 100}}, stream.StdClock{})
	slow := sink.NewCounting("slow", 2*time.Millisecond)
	q := e.AddSink(slow, 8)
	e.SetRules(RuleSet{Host: func(s *shard.Shard) stream.Stream {
		return stream.Fanout(s.IndexSink(), func(ev event.Event) { q.Offer(ev) })
	}})
	e.Start()
	defer e.Stop()
	accepted, rejected := e.Admit(events(500), 200*time.Millisecond)
	if accepted != 500 || rejected != 0 {
		t.Fatalf("accepted %d rejected %d", accepted, rejected)
	}
	waitFor(t, "loops to drain", func() bool {
		var p uint64
		for _, s := range e.Shards() {
			p += s.Metrics().Processed
		}
		return p == 500
	})
	for _, s := range e.Shards() {
		shardIdentity(t, s, 0)
	}
	if m := q.Metrics(); m.Dropped == 0 {
		t.Errorf("slow sink did not shed: %+v", m)
	}
	time.Sleep(30 * time.Millisecond) // let the worker finish the queued ones
	sinkIdentity(t, q, 0)
}

func TestQueryLookupEventsSubscribe(t *testing.T) {
	e := New(Config{Shards: 3, Shard: shard.Config{InboxCapacity: 64, RingEvents: 100}}, stream.StdClock{})
	e.Start()
	defer e.Stop()
	evs := events(10)
	evs[3].State = "critical"
	e.Admit(evs, time.Second)
	all := func(event.Event) bool { return true }
	waitFor(t, "index", func() bool { got, _ := e.Query(all); return len(got) == 10 })

	crit, asOf := e.Query(func(ev event.Event) bool { return ev.State == "critical" })
	if len(crit) != 1 || crit[0].Service != "3" || asOf.Min == 0 || asOf.Max < asOf.Min {
		t.Errorf("query %+v as_of %+v", crit, asOf)
	}
	en, at, ok := e.Lookup("h3", "3")
	if !ok || en.Event.State != "critical" || at == 0 {
		t.Errorf("lookup %+v %v %v", en, at, ok)
	}
	if got := e.Events(all, 0, 4); len(got) != 4 {
		t.Errorf("events limit: %d", len(got))
	}
	if got := e.Events(all, now()+1, 0); len(got) != 0 {
		t.Errorf("events since future: %d", len(got))
	}

	snap, sub := e.Subscribe(all, true, 16)
	if len(snap) != 10 {
		t.Errorf("snapshot %d", len(snap))
	}
	e.Admit([]event.Event{{Host: "h9", Service: "live", Time: now(), TTL: 60}}, time.Second)
	select {
	case ev := <-sub.Events():
		if ev.Service != "live" {
			t.Errorf("subscribed %+v", ev)
		}
	case <-time.After(time.Second):
		t.Error("no live event")
	}
	e.Unsubscribe(sub)
}
