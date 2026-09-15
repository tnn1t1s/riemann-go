package probe3

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// ev builds an event with X and a time offset in seconds from the bubble's
// start, mirroring Clojure's {:x n :time t} with the controlled clock reset to 0.
type ev struct {
	x     int
	t     float64
	state string
}

func (e ev) at(base time.Time) Event {
	return Event{Time: base.Add(time.Duration(e.t * float64(time.Second))), X: e.x, State: e.state}
}

func fmtStable(base time.Time, es []Event) string {
	var b []string
	for _, e := range es {
		b = append(b, fmt.Sprintf("{x %d t %g}", e.X, e.Time.Sub(base).Seconds()))
	}
	return strings.Join(b, " ")
}

func fmtState(es []Event) string {
	var b []string
	for _, e := range es {
		b = append(b, e.State)
	}
	return strings.Join(b, " ")
}

// harness starts a shard whose root is built by mk, collects outputs.
type harness struct {
	s    *Shard
	base time.Time
	out  []Event
	done chan struct{}
}

func start(mk func(s *Shard, out Stream) Stream) *harness {
	h := &harness{s: NewShard(StdClock{}, 16), base: time.Now(), done: make(chan struct{})}
	sink := func(e Event) { h.out = append(h.out, e) }
	h.s.root = mk(h.s, sink)
	go h.s.Run(h.done)
	return h
}

// read returns outputs via the loop goroutine: the read path is serialised
// with writes by the inbox, so no lock and no reliance on synctest.Wait.
func (h *harness) read() []Event {
	var out []Event
	h.s.Do(func() { out = append([]Event(nil), h.out...) })
	return out
}

func (h *harness) stop() { close(h.done) }

// runStream ports riemann.test/run-stream: advance the clock to each event's
// time (firing due timers), then feed it.
func (h *harness) runStream(in []ev) []Event {
	for _, e := range in {
		t := h.base.Add(time.Duration(e.t * float64(time.Second)))
		if d := time.Until(t); d > 0 {
			time.Sleep(d)
		}
		h.s.Send(e.at(h.base))
	}
	return h.read()
}

// runStreamIntervals ports riemann.test/run-stream-intervals: feed an event,
// then advance the clock by its interval; finally feed an expired event
// that is not part of the result.
func (h *harness) runStreamIntervals(in []ev, intervals []float64) []Event {
	for i, e := range in {
		h.s.Send(e.at(h.base))
		time.Sleep(time.Duration(intervals[i] * float64(time.Second)))
	}
	out := h.read()
	h.s.Send(Event{Time: time.Now(), State: "expired"})
	return out
}

func TestStable(t *testing.T) {
	x := func(e Event) int { return e.X }
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
				h := start(func(s *Shard, out Stream) Stream { return Stable(s, dt, x, out) })
				defer h.stop()
				var got []Event
				if tc.intervals == nil {
					got = h.runStream(tc.in)
				} else {
					got = h.runStreamIntervals(tc.in, tc.intervals)
				}
				var want []Event
				for _, e := range tc.want {
					want = append(want, e.at(h.base))
				}
				if g, w := fmtStable(h.base, got), fmtStable(h.base, want); g != w {
					t.Errorf("got  [%s]\nwant [%s]", g, w)
				}
			})
		})
	}
}

func TestThrottle(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		dt        float64
		in        []string
		intervals []float64
		want      []string
	}{
		{"throttle 3 2", 3, 2,
			[]string{"1", "2", "3", "4", "5", "expired", "expired", "expired", "expired"},
			[]float64{0, 0, 1, 1, 1, 0, 0, 2, 100},
			[]string{"1", "2", "3", "5", "expired", "expired", "expired"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dt := time.Duration(tc.dt * float64(time.Second))
				h := start(func(s *Shard, out Stream) Stream { return Throttle(s, tc.n, dt, out) })
				defer h.stop()
				var in []ev
				for _, st := range tc.in {
					in = append(in, ev{state: st})
				}
				got := h.runStreamIntervals(in, tc.intervals)
				if g, w := fmtState(got), strings.Join(tc.want, " "); g != w {
					t.Errorf("got  [%s]\nwant [%s]", g, w)
				}
			})
		})
	}
}

// TestNoConcurrentTimerAndEvent demonstrates the architecture's claim:
// timers never fire concurrently with event processing. The shard's busy
// flag is plain (unsynchronised) memory; enter() panics on re-entrancy, and
// the race detector fails the test if the flag is ever touched from two
// goroutines. Two producers send from outside the loop; stable and throttle
// schedule timers; a probe combinator and a probe timer both assert busy.
func TestNoConcurrentTimerAndEvent(t *testing.T) {
	run := func(t *testing.T, producers, perProducer int, dt time.Duration, gap time.Duration) {
		var timerFires, events int
		h := start(func(s *Shard, out Stream) Stream {
			probe := func(e Event) {
				if !s.busy {
					t.Error("event processed outside the loop")
				}
				events++
				// every 7th event arms an extra bare timer that checks the flag too
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
			x := func(e Event) int { return e.X }
			return Stable(s, dt, x, Throttle(s, 2, dt, probe))
		})
		defer h.stop()
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				for i := 0; i < perProducer; i++ {
					h.s.Send(Event{Time: time.Now(), X: (i / 40) % 2})
					time.Sleep(gap)
				}
			}(p)
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
