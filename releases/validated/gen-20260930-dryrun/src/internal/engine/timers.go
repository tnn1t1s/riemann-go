package engine

import "container/heap"

// timer is one scheduled callback, run by the loop that owns the heap.
type timer struct {
	due float64
	seq uint64
	fn  func()
}

type timerHeap []timer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].due != h[j].due {
		return h[i].due < h[j].due
	}
	return h[i].seq < h[j].seq
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(timer)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[:n-1]
	return t
}

// timers is a loop-owned schedule. SPEC-FREE: the timer mechanism is the
// implementation's; this is a min-heap fired by the owning goroutine.
type timers struct {
	h   timerHeap
	seq uint64
}

func (t *timers) schedule(due float64, fn func()) {
	t.seq++
	heap.Push(&t.h, timer{due: due, seq: t.seq, fn: fn})
}

func (t *timers) next() (float64, bool) {
	if len(t.h) == 0 {
		return 0, false
	}
	return t.h[0].due, true
}

// fireDue runs every timer due at or before now, in due order.
func (t *timers) fireDue(now float64) {
	for len(t.h) > 0 && t.h[0].due <= now {
		tm := heap.Pop(&t.h).(timer)
		tm.fn()
	}
}
