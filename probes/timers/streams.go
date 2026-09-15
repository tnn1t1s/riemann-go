package probe3

import "time"

// Stable ports riemann.streams/stable (streams.clj:1936-2030). Passes events
// only once f(event) has been equal across successive events for at least dt.
//
// Difference from the Clojure: instead of "N tasks fighting it out" (one
// once! per value change, each idempotent), one heap entry per value change
// carries a generation; a stale entry is skipped. With monotone event times
// the two are equivalent, because a stale task's flush check (dt <= now -
// first.Time) is always false for a newer buffer. With non-monotone event
// times a stale Clojure task could flush a newer buffer early; the generation
// version cannot. Recorded as a deliberate divergence.
func Stable(s *Shard, dt time.Duration, f func(Event) int, children ...Stream) Stream {
	const unknown = int(^uint(0) >> 1) // sentinel for ::unknown; test values are small
	prev := unknown
	var buffer []Event
	var gen uint64

	emit := func(es []Event) {
		for _, e := range es {
			for _, c := range children {
				c(e)
			}
		}
	}

	timeout := func(g uint64) {
		if g != gen || len(buffer) == 0 {
			return // flushed already, or buffer replaced
		}
		if s.Now().Sub(buffer[0].Time) >= dt {
			out := buffer
			buffer = nil
			emit(out)
		}
	}

	return func(e Event) {
		v := f(e)
		if v == prev {
			if len(buffer) == 0 {
				emit([]Event{e}) // stable: pass immediately
				return
			}
			buffer = append(buffer, e)
			if e.Time.Sub(buffer[0].Time) >= dt {
				out := buffer
				buffer = nil
				emit(out)
			}
			return
		}
		// value changed: start buffering, arm one timer for this generation
		prev = v
		buffer = []Event{e}
		gen++
		s.Schedule(e.Time.Add(dt), gen, timeout)
	}
}

// nextTick ports riemann.time/next-tick: the first instant after now that is
// an exact multiple of dt from anchor. When now is on a boundary, now+dt.
func nextTick(anchor time.Time, dt time.Duration, now time.Time) time.Time {
	return now.Add(dt - now.Sub(anchor)%dt)
}

// Throttle ports riemann.streams/throttle over part-time-simple
// (streams.clj:595-661, 1102-1118): at most n events per fixed dt window
// anchored at construction time; the rest are dropped. The first event of a
// window schedules the window's end tick, which resets the count.
func Throttle(s *Shard, n int, dt time.Duration, children ...Stream) Stream {
	anchor := s.Now()
	sent := 0
	scheduled := false
	return func(e Event) {
		sent++
		if !scheduled {
			scheduled = true
			s.Schedule(nextTick(anchor, dt, s.Now()), 0, func(uint64) {
				sent = 0
				scheduled = false
			})
		}
		if sent <= n {
			for _, c := range children {
				c(e)
			}
		}
	}
}
