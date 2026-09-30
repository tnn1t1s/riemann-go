package rule

import (
	"strings"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

// Node is one live combinator. Only the goroutine that owns the hosting
// Runtime calls Process, so nodes hold their state without locks.
type Node interface {
	Process(ev *event.Event, fr Frame)
}

// Instance is one loop's live copy of a rule.
type Instance struct {
	C    *Compiled
	rt   Runtime
	root Node
}

// NewInstance instantiates the rule's tree against a runtime.
func NewInstance(c *Compiled, rt Runtime) *Instance {
	return &Instance{C: c, rt: rt, root: instantiate(c.Root, c, rt, nil)}
}

// Offer presents one event to the rule: disabled and expired rules take
// nothing, and only events the match expression admits enter the tree.
func (i *Instance) Offer(ev *event.Event) {
	if !i.C.Enabled {
		return
	}
	now := i.rt.Now()
	if i.C.HasExpiry && now > i.C.ExpiresAt {
		return
	}
	if !i.C.Match.Bool(ev.Env(now, nil)) {
		return
	}
	i.root.Process(ev, Frame{})
}

func instantiate(t *Template, c *Compiled, rt Runtime, fork *Fork) Node {
	switch t.Op {
	case "sink":
		return &sinkNode{t: t, c: c, rt: rt}
	case "where":
		return &whereNode{base: newBase(t, c, rt, fork)}
	case "by":
		return &byNode{t: t, c: c, rt: rt, fork: fork, forks: map[string]*forkEntry{}}
	case "changed-state":
		return &changedStateNode{base: newBase(t, c, rt, fork), prev: t.Initial}
	case "throttle":
		return &throttleNode{base: newBase(t, c, rt, fork)}
	case "splitp":
		n := &splitpNode{t: t, c: c, rt: rt}
		for _, b := range t.Branches {
			n.branches = append(n.branches, instantiate(b, c, rt, fork))
		}
		n.otherwise = instantiate(t.Otherwise, c, rt, fork)
		return n
	case "set":
		return &setNode{base: newBase(t, c, rt, fork)}
	case "coalesce":
		return &coalesceNode{base: newBase(t, c, rt, fork), held: map[string]*event.Event{}}
	case "ddt":
		return &ddtNode{base: newBase(t, c, rt, fork)}
	case "stable":
		return &stableNode{base: newBase(t, c, rt, fork), fork: fork}
	}
	panic("rule: unknown op " + t.Op)
}

type base struct {
	t        *Template
	c        *Compiled
	rt       Runtime
	children []Node
}

func newBase(t *Template, c *Compiled, rt Runtime, fork *Fork) base {
	b := base{t: t, c: c, rt: rt}
	for _, ch := range t.Children {
		b.children = append(b.children, instantiate(ch, c, rt, fork))
	}
	return b
}

// pass delivers ev to every child in array order and counts one pass.
func (b *base) pass(ev *event.Event, fr Frame) {
	b.t.passed.Add(1)
	for _, ch := range b.children {
		ch.Process(ev, fr)
	}
}

func (b *base) discard(n int64) {
	b.t.discarded.Add(n)
}

func (b *base) env(ev *event.Event, fr Frame) map[string]any {
	return ev.Env(b.rt.Now(), fr.Events)
}

// sinkNode is a terminal leaf.
type sinkNode struct {
	t  *Template
	c  *Compiled
	rt Runtime
}

func (n *sinkNode) Process(ev *event.Event, fr Frame) {
	n.t.passed.Add(1)
	n.rt.Emit(n.c, n.t.Sink, n.t.Path, ev, fr)
}

// whereNode: stateless predicate.
type whereNode struct{ base }

func (n *whereNode) Process(ev *event.Event, fr Frame) {
	if n.t.Expr.Bool(n.env(ev, fr)) {
		n.pass(ev, fr)
		return
	}
	// A where that does not match is filtering, not losing: SEMANTICS.md
	// says the event is discarded at this node, and the node's pass counter
	// reports what went through. It is not a drop for I5 purposes.
}

// byNode: one subtree instance per distinct fork key.
type byNode struct {
	t     *Template
	c     *Compiled
	rt    Runtime
	fork  *Fork
	forks map[string]*forkEntry
}

type forkEntry struct {
	fork     *Fork
	children []Node
}

func (n *byNode) key(ev *event.Event) string {
	if len(n.t.Fields) == 1 {
		return ev.FieldKey(n.t.Fields[0])
	}
	parts := make([]string, len(n.t.Fields))
	for i, f := range n.t.Fields {
		parts[i] = ev.FieldKey(f)
	}
	return strings.Join(parts, "\x1f")
}

func (n *byNode) Process(ev *event.Event, fr Frame) {
	key := n.key(ev)
	fe, ok := n.forks[key]
	if !ok {
		// Creation and delivery are one step: the first event for a key is
		// delivered to the fork it creates.
		fe = &forkEntry{fork: &Fork{parent: n.fork}}
		for _, ch := range n.t.Children {
			fe.children = append(fe.children, instantiate(ch, n.c, n.rt, fe.fork))
		}
		n.forks[key] = fe
		n.c.ForksLive.Add(1)
	}
	n.t.passed.Add(1)
	for _, ch := range fe.children {
		ch.Process(ev, fr)
	}
	// Departure from upstream, per SPEC.md: a fork is freed once the key's
	// expired event has passed through it and no timer beneath it is armed.
	// SPEC-GAP: this is only well defined when the fields are host and/or
	// service, since a synthesized expiry event carries nothing else; forks
	// keyed on any other field are never freed (SEMANTICS.md open question 6).
	if n.t.freeable && ev.State == event.ExpiredState && fe.fork.armed == 0 {
		delete(n.forks, key)
		n.c.ForksLive.Add(-1)
		n.c.ForksFreed.Add(1)
	}
}

// changedStateNode: remembers the last state per fork key.
type changedStateNode struct {
	base
	prev string
}

func (n *changedStateNode) Process(ev *event.Event, fr Frame) {
	if ev.State == n.prev {
		return
	}
	prior := n.prev
	n.prev = ev.State
	fr.PriorState = &prior
	n.pass(ev, fr)
}

// throttleNode: at most Limit events per window, window anchored on the first
// event for the key, measured in event time.
type throttleNode struct {
	base
	open      bool
	windowEnd float64
	count     int
}

func (n *throttleNode) Process(ev *event.Event, fr Frame) {
	if !n.open || ev.Time >= n.windowEnd {
		n.open = true
		n.windowEnd = ev.Time + n.t.Window
		n.count = 0
	}
	n.count++
	if n.count <= n.t.Limit {
		n.pass(ev, fr)
		return
	}
	n.discard(1)
}

// splitpNode: first branch whose test holds, else otherwise.
type splitpNode struct {
	t         *Template
	c         *Compiled
	rt        Runtime
	branches  []Node
	otherwise Node
}

func (n *splitpNode) Process(ev *event.Event, fr Frame) {
	n.t.passed.Add(1)
	env := ev.Env(n.rt.Now(), fr.Events)
	for i, test := range n.t.BranchTests {
		if test.Bool(env) {
			n.branches[i].Process(ev, fr)
			return
		}
	}
	n.otherwise.Process(ev, fr)
}

// setNode: every expression reads the incoming event; the result is a copy.
type setNode struct{ base }

func (n *setNode) Process(ev *event.Event, fr Frame) {
	env := n.env(ev, fr)
	out := ev.Clone()
	for name, prog := range n.t.SetFields {
		event.ApplyField(out, name, prog.Value(env))
	}
	n.pass(out, fr)
}

// coalesceNode: latest event per identity, emitted as a set on each arrival.
type coalesceNode struct {
	base
	held map[string]*event.Event
}

func (n *coalesceNode) Process(ev *event.Event, fr Frame) {
	id := ev.Host + "\x1f" + ev.Service
	n.held[id] = ev
	now := n.rt.Now()
	set := make([]map[string]any, 0, len(n.held))
	var gone []string
	for k, e := range n.held {
		set = append(set, e.AsMap())
		if e.State == event.ExpiredState || now > e.Time+e.TTL {
			gone = append(gone, k)
		}
	}
	// An expired identity is emitted once, in this set, and then removed.
	for _, k := range gone {
		delete(n.held, k)
	}
	fr.Events = set
	n.pass(ev, fr)
}

// ddtNode: rate of change of metric between consecutive metric-carrying events.
type ddtNode struct {
	base
	prev *event.Event
}

func (n *ddtNode) Process(ev *event.Event, fr Frame) {
	if ev.Metric == nil {
		return
	}
	prev := n.prev
	n.prev = ev
	if prev == nil {
		return
	}
	dt := ev.Time - prev.Time
	if dt == 0 {
		return
	}
	rate := (*ev.Metric - *prev.Metric) / dt
	out := ev.Clone()
	out.Metric = &rate
	n.pass(out, fr)
}

// stableNode: buffers while the watched field is changing, releases once it
// has held one value for Duration of event time, then passes freely.
type stableNode struct {
	base
	fork     *Fork
	haveLast bool
	last     string
	buffer   []*event.Event
	stable   bool
	gen      uint64
}

func (n *stableNode) Process(ev *event.Event, fr Frame) {
	v := ev.FieldKey(n.t.Field)
	if !n.haveLast || v != n.last {
		// The value changed: whatever was buffered is discarded and counted.
		if len(n.buffer) > 0 {
			n.discard(int64(len(n.buffer)))
		}
		n.haveLast = true
		n.last = v
		n.stable = false
		n.buffer = append(n.buffer[:0], ev)
		n.gen++
		gen := n.gen
		if n.fork != nil {
			n.fork.arm()
		}
		n.rt.Schedule(ev.Time+n.t.Duration, func() {
			if n.fork != nil {
				n.fork.disarm()
			}
			if gen != n.gen || n.stable {
				return
			}
			n.release(fr)
		})
		return
	}
	if n.stable {
		n.pass(ev, fr)
		return
	}
	// Same value, still settling. Bound the buffer, drop-oldest, and count.
	if cap := n.c.opts.StableBufferCapacity; cap > 0 && len(n.buffer) >= cap {
		n.buffer = n.buffer[1:]
		n.discard(1)
	}
	n.buffer = append(n.buffer, ev)
	if len(n.buffer) > 0 && ev.Time-n.buffer[0].Time >= n.t.Duration {
		n.gen++ // any armed timer is now stale
		n.release(fr)
	}
}

func (n *stableNode) release(fr Frame) {
	buf := n.buffer
	n.buffer = nil
	n.stable = true
	for _, e := range buf {
		n.pass(e, fr)
	}
}
