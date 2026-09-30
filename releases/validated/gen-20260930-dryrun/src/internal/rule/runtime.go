package rule

import "github.com/tnn1t1s/riemann-go/internal/event"

// Runtime is what a rule instance needs from the loop that hosts it: a clock,
// a timer, and a place to deliver a firing. The live shard loops, the global
// loop and a dry run each provide one.
type Runtime interface {
	// Now is the engine's current time in float seconds. Live loops answer
	// with the wall clock; a dry run answers with the replayed event's time.
	Now() float64
	// Schedule arms fn to run when the clock reaches due. The loop that owns
	// the runtime runs fn; nothing else touches rule state.
	Schedule(due float64, fn func())
	// Emit delivers an event to a sink leaf.
	Emit(rule *Compiled, sink, path string, ev *event.Event, fr Frame)
}

// Frame is the provenance accumulated along one path through a tree.
type Frame struct {
	// PriorState is the state a traversed changed-state node replaced, or nil
	// when the path traversed no changed-state node.
	PriorState *string
	// Events is the coalesce set below a coalesce node, as expression values.
	Events []map[string]any
}

// Fork tracks how many timers are armed under one by-fork so the fork is not
// freed while a release is pending. A nested fork's arming counts toward its
// parents too.
type Fork struct {
	parent *Fork
	armed  int
}

func (f *Fork) arm() {
	for p := f; p != nil; p = p.parent {
		p.armed++
	}
}

func (f *Fork) disarm() {
	for p := f; p != nil; p = p.parent {
		p.armed--
	}
}
