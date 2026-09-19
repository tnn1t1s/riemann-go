package rule

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/exprs"
)

// Firing is one event reaching one sink leaf through one rule.
type Firing struct {
	Sink       string
	Rule       string
	Version    int
	Owner      string
	Event      *event.Event
	PriorState *string // nil when the path traversed no changed-state node
	Node       string  // node path of the sink leaf
}

// SinkStats is one consistent reading of a sink's bounded queue. The five
// counts close SCALE.md's identity exactly because they are read together:
// Accepted = Processed + Dropped + Depth + InFlight.
type SinkStats struct {
	Depth     int64
	Capacity  int64
	Dropped   int64
	Accepted  int64
	Processed int64
	InFlight  int64
}

// Sink is a destination a sink leaf delivers to. Offer never blocks.
type Sink interface {
	Offer(Firing)
	Stats() SinkStats
}

// Host is what an instance needs from whatever executes it: a shard loop, the
// global executor, or a dry run. All calls into one instance, including its
// timers, are serialized by the host.
type Host interface {
	Now() float64
	Fire(Firing)
	// Arm schedules fn at engine time due, to run in the host's own context
	// before any event stamped at or after due is dispatched.
	Arm(due float64, fn func())
}

// Gauges are the process-wide counts the runtime maintains for the by
// fork-freeing heuristic and for the stable buffer, which is a bounded queue
// under INVARIANTS.md I4.
type Gauges struct {
	ForksLive     atomic.Int64
	ForksFreed    atomic.Int64
	StableDepth   atomic.Int64
	StableDropped atomic.Int64 // drop-oldest evictions
}

// Config carries the parameters the rule layer owns.
type Config struct {
	// StableBufferCapacity is stable.buffer_capacity: events per fork key,
	// policy drop-oldest.
	StableBufferCapacity int
}

// scope tracks the armed timers beneath one by fork, so a fork is freed only
// when no timer still references it.
type scope struct {
	parent      *scope
	timers      int
	pendingFree bool
	free        func()
}

func (s *scope) inc() {
	for ; s != nil; s = s.parent {
		s.timers++
	}
}

func (s *scope) dec() {
	for ; s != nil; s = s.parent {
		s.timers--
		if s.timers == 0 && s.pendingFree && s.free != nil {
			s.free()
		}
	}
}

type item struct {
	ev    *event.Event
	prior *string
	set   *exprs.Set // the coalesce emission this event travels with, or nil
}

type fork struct {
	scope    *scope
	children []*state
}

// state is the mutable counterpart of one node, within one fork scope.
type state struct {
	scope    *scope
	children []*state // nil for by, whose children live per fork

	forks map[string]*fork // by

	prior string // changed-state

	winOpen  bool // throttle
	winEnd   float64
	winCount int64

	prev *event.Event // ddt

	held map[[2]string]*event.Event // coalesce

	hasValue bool // stable
	value    string
	settled  bool
	buf      []item
	gen      uint64
	armed    bool
}

// Instance is one live copy of a rule's state.
type Instance struct {
	rule   *Rule
	host   Host
	cfg    Config
	gauges *Gauges
	root   *state
	dead   bool
	dry    bool
}

// IgnoreLifecycle makes the instance run whatever enabled and expires_at say,
// which is what a dry run wants.
func (in *Instance) IgnoreLifecycle() { in.dry = true }

// NewInstance creates fresh state for r. Nothing carries over from any
// previous version.
func NewInstance(r *Rule, h Host, cfg Config, g *Gauges) *Instance {
	in := &Instance{rule: r, host: h, cfg: cfg, gauges: g}
	in.root = in.newState(r.root, nil)
	return in
}

func (in *Instance) newState(n *node, sc *scope) *state {
	st := &state{scope: sc}
	switch n.kind {
	case kindBy:
		st.forks = map[string]*fork{}
		return st
	case kindChangedState:
		st.prior = n.initial
	case kindCoalesce:
		st.held = map[[2]string]*event.Event{}
	}
	st.children = make([]*state, len(n.children))
	for i, c := range n.children {
		st.children[i] = in.newState(c, sc)
	}
	return st
}

// Handle delivers an event to the rule. Only events for which match holds
// enter the stream.
func (in *Instance) Handle(ev *event.Event) {
	if in.dead {
		return
	}
	ok, err := in.rule.match.Bool(ev, in.host.Now(), nil)
	if err != nil || !ok {
		return
	}
	in.run(in.rule.root, in.root, item{ev: ev})
}

// Close retires the instance: its pending timers become no-ops and its share
// of the process-wide gauges is returned.
func (in *Instance) Close() {
	if in.dead {
		return
	}
	in.dead = true
	forks, buffered := in.census(in.root)
	in.gauges.ForksLive.Add(-forks)
	in.gauges.StableDepth.Add(-buffered)
}

func (in *Instance) census(st *state) (forks, buffered int64) {
	buffered = int64(len(st.buf))
	for _, f := range st.forks {
		forks++
		for _, c := range f.children {
			a, b := in.census(c)
			forks, buffered = forks+a, buffered+b
		}
	}
	for _, c := range st.children {
		a, b := in.census(c)
		forks, buffered = forks+a, buffered+b
	}
	return forks, buffered
}

func (in *Instance) pass(n *node, st *state, it item) {
	n.ctr.Passed.Add(1)
	for i, c := range n.children {
		in.run(c, st.children[i], it)
	}
}

func (in *Instance) run(n *node, st *state, it item) {
	ev := it.ev
	switch n.kind {
	case kindSink:
		// A timer can release an event after expires_at has passed; a rule
		// past that time behaves as disabled and never fires.
		if !in.dry && !in.rule.Active(in.host.Now()) {
			n.ctr.Discarded.Add(1)
			return
		}
		n.ctr.Passed.Add(1)
		in.host.Fire(Firing{Sink: n.sink, Rule: in.rule.Doc.ID, Version: in.rule.Version,
			Owner: in.rule.Doc.Owner, Event: ev, PriorState: it.prior, Node: n.path})

	case kindWhere:
		ok, err := n.expr.Bool(ev, in.host.Now(), it.set)
		if err != nil {
			// SPEC-GAP: an expression that fails at evaluation discards the
			// event at this node and counts it as riemann.rule.discarded.
			n.ctr.Discarded.Add(1)
			return
		}
		if ok {
			in.pass(n, st, it)
		}

	case kindBy:
		in.runBy(n, st, it)

	case kindChangedState:
		prior := st.prior
		st.prior = ev.State // advances whether or not the event passes
		if ev.State != prior {
			it.prior = &prior
			in.pass(n, st, it)
		}

	case kindThrottle:
		// The window is half open: an event stamped exactly at its end opens
		// the next one.
		if !st.winOpen || ev.Time >= st.winEnd {
			st.winOpen = true
			st.winEnd = ev.Time + n.window
			st.winCount = 0
		}
		st.winCount++
		if st.winCount <= n.limit {
			in.pass(n, st, it)
		} else {
			n.ctr.Discarded.Add(1)
		}

	case kindSplitp:
		now := in.host.Now()
		taken := len(n.children) - 1 // otherwise
		for k, test := range n.tests {
			if ok, err := test.Bool(ev, now, it.set); err == nil && ok {
				taken = k
				break
			}
		}
		n.ctr.Passed.Add(1)
		in.run(n.children[taken], st.children[taken], it)

	case kindSet:
		out, err := in.rewrite(n, it)
		if err != nil {
			n.ctr.Discarded.Add(1)
			return
		}
		it.ev = out
		in.pass(n, st, it)

	case kindCoalesce:
		in.runCoalesce(n, st, it)

	case kindDdt:
		if !ev.HasMetric {
			return // ignored entirely; it does not become the previous event
		}
		prev := st.prev
		st.prev = ev
		if prev == nil {
			return
		}
		dt := ev.Time - prev.Time
		if dt == 0 {
			return
		}
		rate := (ev.Metric - prev.Metric) / dt
		if math.IsNaN(rate) || math.IsInf(rate, 0) {
			n.ctr.Discarded.Add(1)
			return
		}
		out := ev.Clone()
		out.Metric = rate
		it.ev = out
		in.pass(n, st, it)

	case kindStable:
		in.runStable(n, st, it)
	}
}

func (in *Instance) runBy(n *node, st *state, it item) {
	key := ""
	for _, f := range n.fields {
		key += fieldValue(it.ev, f) + "\x00"
	}
	f := st.forks[key]
	if f == nil {
		f = &fork{scope: &scope{parent: st.scope}}
		f.children = make([]*state, len(n.children))
		for i, c := range n.children {
			f.children[i] = in.newState(c, f.scope)
		}
		created := f
		f.scope.free = func() {
			if st.forks[key] != created {
				return
			}
			delete(st.forks, key)
			freed := int64(1)
			for _, c := range created.children {
				nested, _ := in.census(c)
				freed += nested
			}
			if !in.dead {
				in.gauges.ForksLive.Add(-freed)
			}
			in.gauges.ForksFreed.Add(freed)
		}
		st.forks[key] = f
		in.gauges.ForksLive.Add(1)
	}
	// SPEC-GAP: an event arriving at a fork that is waiting on a timer to be
	// freed cancels the free; the key is in use again.
	f.scope.pendingFree = false
	n.ctr.Passed.Add(1)
	for i, c := range n.children {
		in.run(c, f.children[i], it)
	}
	// The fork is freed once the key's expired event has passed through it and
	// no timer beneath it still references it.
	// SPEC-GAP: "the key's expired event" is any event whose state is expired,
	// synthesized or client-sent, and only a by whose fields are all host or
	// service frees forks, since an expiry event carries no other field
	// (SEMANTICS.md open question 6). A by on host alone frees the host's fork
	// when any one of its services expires.
	if n.freeable && it.ev.State == event.StateExpired {
		if f.scope.timers == 0 {
			f.scope.free()
		} else {
			f.scope.pendingFree = true
		}
	}
}

func (in *Instance) runCoalesce(n *node, st *state, it item) {
	ev := it.ev
	st.held[[2]string{ev.Host, ev.Service}] = ev
	now := in.host.Now()
	all := make([]*event.Event, 0, len(st.held))
	for id, h := range st.held {
		all = append(all, h)
		// An identity leaves the fold after being emitted once as expired.
		if h.State == event.StateExpired || now > h.Time+h.TTL {
			delete(st.held, id)
		}
	}
	// No order within the set is promised; sorting keeps a dry run reproducible.
	sort.Slice(all, func(i, j int) bool {
		if all[i].Host != all[j].Host {
			return all[i].Host < all[j].Host
		}
		return all[i].Service < all[j].Service
	})
	it.set = exprs.NewSet(all)
	in.pass(n, st, it)
}

func (in *Instance) runStable(n *node, st *state, it item) {
	v := fieldValue(it.ev, n.field)
	if !st.hasValue || v != st.value {
		// The value changed: whatever was buffered is discarded, and counted.
		if len(st.buf) > 0 {
			n.ctr.Discarded.Add(int64(len(st.buf)))
			in.gauges.StableDepth.Add(-int64(len(st.buf)))
		}
		st.hasValue, st.value, st.settled = true, v, false
		st.buf = []item{it}
		in.gauges.StableDepth.Add(1)
		// One generation-numbered timer per value change; an older generation
		// is discarded when it fires.
		st.gen++
		gen := st.gen
		if !st.armed {
			st.armed = true
			st.scope.inc()
		}
		in.host.Arm(it.ev.Time+n.duration, func() {
			if in.dead || st.gen != gen || !st.armed {
				return
			}
			st.armed = false
			in.release(n, st)
			st.scope.dec()
		})
		return
	}
	if st.settled {
		in.pass(n, st, it)
		return
	}
	if in.cfg.StableBufferCapacity > 0 && len(st.buf) >= in.cfg.StableBufferCapacity {
		st.buf = st.buf[1:] // drop-oldest
		n.ctr.Discarded.Add(1)
		in.gauges.StableDropped.Add(1)
		in.gauges.StableDepth.Add(-1)
	}
	st.buf = append(st.buf, it)
	in.gauges.StableDepth.Add(1)
	// Inclusive: an event arriving exactly duration_seconds after the first
	// buffered event releases the buffer, itself included.
	if it.ev.Time-st.buf[0].ev.Time >= n.duration {
		st.gen++ // the armed timer is now stale
		if st.armed {
			st.armed = false
			in.release(n, st)
			st.scope.dec()
		} else {
			in.release(n, st)
		}
	}
}

func (in *Instance) release(n *node, st *state) {
	buf := st.buf
	st.buf = nil
	st.settled = true
	in.gauges.StableDepth.Add(-int64(len(buf)))
	for _, b := range buf {
		in.pass(n, st, b) // arrival order, original times
	}
}

// rewrite builds the event a set node passes downstream. Every expression is
// evaluated against the incoming event, never the partially rewritten one.
// SPEC-GAP: an expression evaluating to null, or referencing metric on an
// event without one, sets the field to its absent-field default; for metric
// that is an event with no metric (SEMANTICS.md open question 4). A null time
// keeps the incoming time.
// SPEC-GAP: a result of the wrong type, an empty host or service, or a
// non-finite number discards the event at this node and counts it as
// riemann.rule.discarded (SEMANTICS.md open question 5).
func (in *Instance) rewrite(n *node, it item) (*event.Event, error) {
	now := in.host.Now()
	out := it.ev.Clone()
	for _, f := range n.setFields {
		v, err := f.expr.Eval(it.ev, now, it.set)
		if err == exprs.ErrNoMetric {
			v, err = nil, nil
		}
		if err != nil {
			return nil, err
		}
		if err := assign(out, f.name, v); err != nil {
			return nil, err
		}
	}
	if out.Host == "" || out.Service == "" {
		return nil, fmt.Errorf("set produced an event with no identity")
	}
	return out, nil
}

func assign(out *event.Event, name string, v any) error {
	switch name {
	case "host", "service", "state", "description", "source":
		s := ""
		if v != nil {
			var ok bool
			if s, ok = v.(string); !ok {
				return fmt.Errorf("%s: result is not a string", name)
			}
		}
		switch name {
		case "host":
			out.Host = s
		case "service":
			out.Service = s
		case "state":
			out.State = s
		case "description":
			out.Description = s
		case "source":
			out.Source = s
		}
	case "metric", "time", "ttl":
		if v == nil {
			switch name {
			case "metric":
				out.Metric, out.HasMetric = 0, false
			case "ttl":
				out.TTL = event.DefaultTTL
			}
			return nil
		}
		f, ok := toFloat(v)
		if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("%s: result is not a finite number", name)
		}
		switch name {
		case "metric":
			out.Metric, out.HasMetric = f, true
		case "time":
			out.Time = f
		case "ttl":
			out.TTL = f
		}
	case "tags":
		tags := []string{}
		switch t := v.(type) {
		case nil:
		case []string:
			tags = append(tags, t...)
		case []any:
			for _, e := range t {
				s, ok := e.(string)
				if !ok {
					return fmt.Errorf("tags: result is not an array of strings")
				}
				tags = append(tags, s)
			}
		default:
			return fmt.Errorf("tags: result is not an array of strings")
		}
		out.Tags = tags
	case "attributes":
		attrs := map[string]string{}
		switch t := v.(type) {
		case nil:
		case map[string]string:
			for k, s := range t {
				attrs[k] = s
			}
		case map[string]any:
			for k, e := range t {
				s, ok := e.(string)
				if !ok {
					return fmt.Errorf("attributes: result is not an object of strings")
				}
				attrs[k] = s
			}
		default:
			return fmt.Errorf("attributes: result is not an object of strings")
		}
		out.Attributes = attrs
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int8:
		return float64(t), true
	case int16:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint:
		return float64(t), true
	case uint8:
		return float64(t), true
	case uint16:
		return float64(t), true
	case uint32:
		return float64(t), true
	case uint64:
		return float64(t), true
	}
	return 0, false
}
