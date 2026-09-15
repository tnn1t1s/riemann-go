package shard

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/stream"
)

func f(v float64) *float64 { return &v }

// ev mirrors Clojure's {:x n :time t}: x rides in Metric, t is seconds from
// the bubble's start.
type ev struct {
	x     int
	t     float64
	state string
}

func (e ev) at(base time.Time) event.Event {
	return event.Event{Host: "h", Service: "s", State: e.state, Metric: f(float64(e.x)),
		Time: event.Seconds(base.Add(time.Duration(e.t * float64(time.Second)))), TTL: 1e9}
}

func metricOf(e event.Event) any {
	if e.Metric == nil {
		return nil
	}
	return *e.Metric
}

func fmtXT(base time.Time, es []event.Event) string {
	var b []string
	for _, e := range es {
		b = append(b, fmt.Sprintf("{x %v t %g}", metricOf(e), e.At().Sub(base).Seconds()))
	}
	return strings.Join(b, " ")
}

func fmtState(es []event.Event) string {
	var b []string
	for _, e := range es {
		b = append(b, e.State)
	}
	return strings.Join(b, " ")
}

type harness struct {
	s    *Shard
	base time.Time
	out  []event.Event
}

func start(mk func(s *Shard, out stream.Stream) stream.Stream) *harness {
	h := &harness{s: New(stream.StdClock{}, Config{InboxCapacity: 16}), base: time.Now()}
	sink := func(e event.Event) { h.out = append(h.out, e) }
	h.s.SetRoot(mk(h.s, sink))
	go h.s.Run()
	return h
}

func (h *harness) read() []event.Event {
	var out []event.Event
	h.s.Do(func() { out = append([]event.Event(nil), h.out...) })
	return out
}

func (h *harness) stop() { h.s.Stop() }

func (h *harness) send(e event.Event) {
	if !h.s.TryOffer(e) {
		panic("inbox full in test")
	}
}

// runStream ports riemann.test/run-stream: advance the clock to each event's
// time (firing due timers), then feed it.
func (h *harness) runStream(in []ev) []event.Event {
	for _, e := range in {
		t := h.base.Add(time.Duration(e.t * float64(time.Second)))
		if d := time.Until(t); d > 0 {
			time.Sleep(d)
		}
		h.send(e.at(h.base))
	}
	return h.read()
}

// runStreamIntervals ports riemann.test/run-stream-intervals: feed an event,
// then advance the clock by its interval; finally feed an expired event
// that is not part of the result.
func (h *harness) runStreamIntervals(in []ev, intervals []float64) []event.Event {
	for i, e := range in {
		h.send(e.at(h.base))
		time.Sleep(time.Duration(intervals[i] * float64(time.Second)))
	}
	out := h.read()
	h.send(event.Event{Host: "h", Service: "s", Time: event.Seconds(time.Now()), State: "expired"})
	return out
}

func TestStable(t *testing.T) {
	cases := []struct {
		name      string
		dt        float64
		in        []ev
		intervals []float64 // nil => run-stream (advance to :time); else run-stream-intervals
		want      []ev
	}{
		{"doesn't emit until dt seconds have passed", 3,
			[]ev{{1, 0, ""}, {1, 1, ""}, {1, 2, ""}}, nil, nil},
		{"constant values are emitted after dt seconds", 3,
			[]ev{{1, 0, ""}, {1, 1, ""}, {1, 3, ""}}, nil,
			[]ev{{1, 0, ""}, {1, 1, ""}, {1, 3, ""}}},
		{"ignores spikes", 3,
			[]ev{{0, 0, ""}, {0, 3, ""}, {1, 4, ""}, {1, 5, ""}, {0, 6, ""}, {0, 9, ""}}, nil,
			[]ev{{0, 0, ""}, {0, 3, ""}, {0, 6, ""}, {0, 9, ""}}},
		{"ignores flapping", 3,
			[]ev{{0, 0, ""}, {0, 10, ""}, {1, 11, ""}, {0, 11, ""}, {1, 12, ""}, {5, 13, ""}, {2, 14, ""}, {2, 17, ""}}, nil,
			[]ev{{0, 0, ""}, {0, 10, ""}, {2, 14, ""}, {2, 17, ""}}},
		{"triggers after dt seconds of stability, even without new events", 10,
			[]ev{{0, 0, ""}, {1, 1, ""}, {2, 11, ""}}, []float64{1, 10, 1},
			[]ev{{1, 1, ""}}},
		{"triggers after dt seconds with new events", 10,
			[]ev{{0, 0, ""}, {0, 1, ""}, {0, 5, ""}, {1, 11, ""}}, []float64{1, 4, 6, 1},
			[]ev{{0, 0, ""}, {0, 1, ""}, {0, 5, ""}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dt := time.Duration(tc.dt * float64(time.Second))
				h := start(func(s *Shard, out stream.Stream) stream.Stream { return stream.Stable(s, dt, metricOf, out) })
				defer h.stop()
				var got []event.Event
				if tc.intervals == nil {
					got = h.runStream(tc.in)
				} else {
					got = h.runStreamIntervals(tc.in, tc.intervals)
				}
				var want []event.Event
				for _, e := range tc.want {
					want = append(want, e.at(h.base))
				}
				if g, w := fmtXT(h.base, got), fmtXT(h.base, want); g != w {
					t.Errorf("got  [%s]\nwant [%s]", g, w)
				}
			})
		})
	}
}

func TestThrottle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := start(func(s *Shard, out stream.Stream) stream.Stream { return stream.Throttle(s, 3, 2*time.Second, out) })
		defer h.stop()
		var in []ev
		for _, st := range []string{"1", "2", "3", "4", "5", "expired", "expired", "expired", "expired"} {
			in = append(in, ev{state: st})
		}
		got := h.runStreamIntervals(in, []float64{0, 0, 1, 1, 1, 0, 0, 2, 100})
		if g, w := fmtState(got), "1 2 3 5 expired expired expired"; g != w {
			t.Errorf("got  [%s]\nwant [%s]", g, w)
		}
	})
}

// TestTimersFireBeforeSameTimestampEvent is invariant 5: a timer due at T
// runs before an event stamped T is dispatched.
func TestTimersFireBeforeSameTimestampEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var order []string
		var h *harness
		h = start(func(s *Shard, out stream.Stream) stream.Stream {
			armed := false
			return func(e event.Event) {
				order = append(order, fmt.Sprintf("event@%g", e.At().Sub(h.base).Seconds()))
				if !armed {
					armed = true
					s.Schedule(e.At().Add(5*time.Second), 0, func(uint64) { order = append(order, "timer@5") })
				}
			}
		})
		defer h.stop()
		h.runStream([]ev{{0, 0, ""}, {0, 5, ""}})
		var got string
		h.s.Do(func() { got = strings.Join(order, " ") })
		if got != "event@0 timer@5 event@5" {
			t.Errorf("order %q", got)
		}
	})
}

// TestIndexExpiry is invariant 2 under a controlled clock: an entry expires
// at time + ttl, the loop wakes for it without any inbox traffic, and the
// synthesised event has only host, service, state and time. A client-sent
// "expired" event passes through unchanged and deletes the entry.
func TestIndexExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := start(func(s *Shard, out stream.Stream) stream.Stream {
			return stream.Fanout(s.IndexSink(), out)
		})
		defer h.stop()
		now := event.Seconds(time.Now())
		h.send(event.Event{Host: "a", Service: "cpu", State: "ok", Metric: f(1), Time: now, TTL: 2, Tags: []string{"x"}})
		h.send(event.Event{Host: "b", Service: "cpu", State: "ok", Metric: f(1), Time: now, TTL: 10})

		time.Sleep(1900 * time.Millisecond)
		if got := h.read(); len(got) != 2 {
			t.Fatalf("before ttl: %d events", len(got))
		}
		var live int
		h.s.Do(func() { live = h.s.Index().Len() })
		if live != 2 {
			t.Fatalf("index len before ttl %d", live)
		}

		time.Sleep(200 * time.Millisecond)
		got := h.read()
		if len(got) != 3 {
			t.Fatalf("after ttl: %d events", len(got))
		}
		x := got[2]
		if x.Host != "a" || x.Service != "cpu" || x.State != event.StateExpired || x.Metric != nil || x.Tags != nil || x.TTL != 0 {
			t.Errorf("synthesised expired event %+v", x)
		}
		if x.Time != now+2 {
			t.Errorf("expired time %v want time+ttl %v", x.Time, now+2)
		}
		h.s.Do(func() {
			if _, ok := h.s.Index().Lookup("a", "cpu"); ok {
				t.Error("expired entry still indexed")
			}
			if h.s.Index().Len() != 1 {
				t.Errorf("index len %d", h.s.Index().Len())
			}
		})
		if h.s.Metrics().Expired != 1 {
			t.Errorf("expired counter %d", h.s.Metrics().Expired)
		}

		// Client-sent expired event: passes through intact, deletes b.
		sent := event.Event{Host: "b", Service: "cpu", State: event.StateExpired, Metric: f(7), Time: now, TTL: 10,
			Tags: []string{"client"}, Description: "clean shutdown"}
		h.send(sent)
		got = h.read()
		if len(got) != 4 {
			t.Fatalf("after client expired: %d events", len(got))
		}
		y := got[3]
		if y.Description != "clean shutdown" || y.Metric == nil || *y.Metric != 7 || len(y.Tags) != 1 {
			t.Errorf("client expired event altered: %+v", y)
		}
		h.s.Do(func() {
			if h.s.Index().Len() != 0 {
				t.Errorf("index len after client expired %d", h.s.Index().Len())
			}
		})
		time.Sleep(20 * time.Second)
		if got := h.read(); len(got) != 4 {
			t.Errorf("deleted entry expired again: %d events", len(got))
		}
	})
}

// TestAttachIsAtomic: an event that reaches the loop after the snapshot
// closure was queued but before it runs must be delivered exactly once. A
// non-atomic implementation (snapshot in one Do, register in another) would
// index it between the two and deliver it to neither.
func TestAttachIsAtomic(t *testing.T) {
	s := New(stream.StdClock{}, Config{InboxCapacity: 16})
	s.SetRoot(s.IndexSink())
	go s.Run()
	defer s.Stop()

	now := event.Seconds(time.Now())
	before := event.Event{Host: "before", Service: "s", Time: now, TTL: 60}
	between := event.Event{Host: "between", Service: "s", Time: now, TTL: 60}
	s.TryOffer(before)

	// Park the loop so the attach closure and the event queue up behind it.
	release := make(chan struct{})
	parked := make(chan struct{})
	go s.Do(func() { close(parked); <-release })
	<-parked

	sub := NewSubscription(func(event.Event) bool { return true }, 16)
	type result struct {
		snap []event.Event
	}
	res := make(chan result, 1)
	go func() {
		snap, _ := s.Attach(sub, true)
		res <- result{snap}
	}()
	for s.InboxDepth() < 1 {
		runtime.Gosched()
	}
	s.TryOffer(between)
	close(release)

	r := <-res
	if len(r.snap) != 1 || r.snap[0].Host != "before" {
		t.Fatalf("snapshot %+v", r.snap)
	}
	select {
	case e := <-sub.Events():
		if e.Host != "between" {
			t.Fatalf("stream delivered %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("event between snapshot and subscribe was lost")
	}
	select {
	case e := <-sub.Events():
		t.Fatalf("duplicate delivery %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscriptionSheds(t *testing.T) {
	s := New(stream.StdClock{}, Config{InboxCapacity: 16})
	s.SetRoot(s.IndexSink())
	go s.Run()
	defer s.Stop()
	sub := NewSubscription(func(event.Event) bool { return true }, 2)
	s.Attach(sub, false)
	for i := 0; i < 5; i++ {
		s.TryOffer(event.Event{Host: "h", Service: fmt.Sprint(i), Time: event.Seconds(time.Now()), TTL: 60})
	}
	s.Do(func() {}) // drain
	if got := sub.Lagged(); got != 3 {
		t.Fatalf("lagged %d want 3", got)
	}
	if sub.Lagged() != 0 || sub.Dropped() != 3 {
		t.Fatal("lagged did not reset or total wrong")
	}
	s.Detach(sub)
	if s.Metrics().Subscribers != 0 {
		t.Fatal("detach left subscriber")
	}
}

func TestRing(t *testing.T) {
	r := NewRing(3, 1<<20)
	for i := 0; i < 5; i++ {
		r.Push(event.Event{Host: fmt.Sprint(i)})
	}
	var hosts []string
	r.Each(func(e event.Event) bool { hosts = append(hosts, e.Host); return true })
	if strings.Join(hosts, ",") != "2,3,4" {
		t.Fatalf("ring by count: %v", hosts)
	}
	big := event.Event{Host: "x", Description: strings.Repeat("d", 1000)}
	r2 := NewRing(100, 2*big.Size()+1)
	for i := 0; i < 5; i++ {
		r2.Push(big)
	}
	if r2.Len() != 2 || r2.Bytes() > 2*big.Size()+1 {
		t.Fatalf("ring by bytes: len %d bytes %d", r2.Len(), r2.Bytes())
	}
	if NewRing(0, 0).Len() != 0 {
		t.Fatal("disabled ring")
	}
}

// TestNoConcurrentTimerAndEvent demonstrates the loop's claim: timers never
// fire concurrently with event processing. busy is plain memory; enter()
// panics on re-entrancy, and the race detector fails the test if the flag
// is ever touched from two goroutines. Two producers send from outside the
// loop; stable and throttle schedule timers; a probe combinator and a probe
// timer both assert busy.
func TestNoConcurrentTimerAndEvent(t *testing.T) {
	run := func(t *testing.T, producers, perProducer int, dt time.Duration, gap time.Duration) {
		var timerFires, events int
		h := start(func(s *Shard, out stream.Stream) stream.Stream {
			probe := func(e event.Event) {
				if !s.busy {
					t.Error("event processed outside the loop")
				}
				events++
				if events%7 == 0 {
					s.Schedule(s.Now().Add(dt/2), 0, func(uint64) {
						if !s.busy {
							t.Error("timer fired outside the loop")
						}
						timerFires++
					})
				}
				out(e)
			}
			return stream.Stable(s, dt, metricOf, stream.Throttle(s, 2, dt, probe))
		})
		defer h.stop()
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perProducer; i++ {
					h.s.Offer(event.Event{Host: "h", Service: "s", Time: event.Seconds(time.Now()), TTL: 1e9,
						Metric: f(float64((i / 40) % 2))}, nil)
					time.Sleep(gap)
				}
			}()
		}
		wg.Wait()
		time.Sleep(4 * dt)
		var fires, seen int
		h.s.Do(func() { fires, seen = timerFires, events })
		if fires == 0 || seen == 0 {
			t.Fatalf("test exercised nothing: timer fires=%d events=%d", fires, seen)
		}
		t.Logf("events through probe=%d bare timer fires=%d", seen, fires)
	}
	t.Run("synctest", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { run(t, 4, 200, 3*time.Second, 500*time.Millisecond) })
	})
	t.Run("real clock", func(t *testing.T) {
		run(t, 4, 100, 3*time.Millisecond, 300*time.Microsecond)
	})
}
