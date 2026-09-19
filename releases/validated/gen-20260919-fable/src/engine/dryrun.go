package engine

import (
	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/rule"
)

// DryFiring is one sink stub delivery.
type DryFiring struct {
	Sink  string       `json:"sink"`
	Event *event.Event `json:"event"`
	Node  string       `json:"node"`
}

// dryHost is the whole world of a dry run: a clock that reads the stamp of
// the event being replayed, its own timer heap, and sink stubs that record.
// Nothing in it reaches a live sink, the live index, or a live counter.
type dryHost struct {
	now     float64
	timers  timerHeap
	firings *[]DryFiring
}

func (h *dryHost) Now() float64               { return h.now }
func (h *dryHost) Arm(due float64, fn func()) { h.timers.arm(due, fn) }
func (h *dryHost) Fire(f rule.Firing) {
	*h.firings = append(*h.firings, DryFiring{Sink: f.Sink, Event: f.Event, Node: f.Node})
}

func (h *dryHost) advance(t float64) {
	for {
		tm, ok := h.timers.popDue(t)
		if !ok {
			break
		}
		if tm.due > h.now {
			h.now = tm.due
		}
		tm.fn()
	}
	if t > h.now {
		h.now = t
	}
}

// DryRun replays the ring through a fresh compilation of the stored rule and
// returns what each sink stub received. It is a function of the rule and the
// ordered ring content alone.
// SPEC-GAP: the dry run's clock is the replayed events' stamps, so a timer
// still pending after the last event does not fire; firing it against the
// live clock would make two runs over the same ring differ.
// SPEC-GAP: a dry run ignores enabled and expires_at, so a rule can be tried
// before it is switched on. A rule that is not global is replayed with one
// instance per partition, as it runs live.
func (e *Engine) DryRun(id string) ([]DryFiring, bool) {
	e.rulesMu.Lock()
	stored, ok := e.rules[id]
	e.rulesMu.Unlock()
	if !ok {
		return nil, false
	}
	// A separate compilation has its own counters, so GET /rules/{id} does
	// not move.
	r, err := rule.Compile(stored.Doc, stored.Version)
	if err != nil {
		return nil, false
	}
	firings := []DryFiring{}
	var gauges rule.Gauges
	hosts := map[int]*dryHost{}
	insts := map[int]*rule.Instance{}
	for _, it := range e.ringItems() {
		part := 0
		if !r.Global() {
			part = e.shardFor(it.ev.Host).id
		}
		h := hosts[part]
		if h == nil {
			h = &dryHost{firings: &firings}
			hosts[part] = h
			in := rule.NewInstance(r, h, e.cfg.Rule, &gauges)
			in.IgnoreLifecycle()
			insts[part] = in
		}
		h.advance(it.ev.Time)
		insts[part].Handle(it.ev)
	}
	return firings, true
}
