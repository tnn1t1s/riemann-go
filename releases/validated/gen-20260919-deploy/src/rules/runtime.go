package rules

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/exprs"
)

// Host is what an instance needs from whoever owns it: a clock, a timer heap
// and the sink leaves. Every call on an instance, and every timer callback,
// runs on the owner's single thread of control, so node state has no lock.
type Host interface {
	// Now is the owner's current time in float seconds.
	Now() float64
	// Schedule runs fn on the owner's thread once the owner's clock reaches
	// at. Timers due at or before T run before an event stamped T is
	// dispatched (SPEC.md property 12).
	Schedule(at float64, fn func())
	// Fire delivers one firing to a sink leaf's destination.
	Fire(f Firing)
}

// Firing is one event reaching one sink leaf through one rule.
type Firing struct {
	Rule       *Compiled
	Sink       string
	Node       string
	Event      *event.Event
	PriorState *string // nil when the path traversed no changed-state
}

// flow is one event on its way down a tree, with what the path has gathered.
type flow struct {
	ev     *event.Event
	now    float64
	prior  *string
	events []map[string]any // non-nil below a coalesce
	env    map[string]any   // built on first use
}

func (f *flow) envMap() map[string]any {
	if f.env == nil {
		f.env = exprs.Env(f.ev, f.now, f.events)
	}
	return f.env
}

type node interface {
	handle(f *flow)
	// release is called when the state this node holds is thrown away (a fork
	// freed, an instance replaced) so aggregate gauges stay truthful.
	release()
}

// Instance is the live state of one rule version on one owner.
type Instance struct {
	Rule   *Compiled
	host   Host
	cs     *CounterSet
	shared *Shared
	root   node
	dead   bool
	// dryRun exercises a rule whether or not it is enabled.
	dryRun bool
}

// NewInstance builds fresh state. cs and shared are the rule's live ones for a
// live instance and throwaways for a dry run.
func (c *Compiled) NewInstance(h Host, cs *CounterSet, shared *Shared, dryRun bool) *Instance {
	in := &Instance{Rule: c, host: h, cs: cs, shared: shared, dryRun: dryRun}
	in.root = in.build(c.root, nil)
	return in
}

// Handle offers one event to the rule.
func (in *Instance) Handle(ev *event.Event) {
	if in.dead {
		return
	}
	now := in.host.Now()
	if !in.dryRun && !in.Rule.Active(now) {
		return
	}
	f := &flow{ev: ev, now: now}
	ok, err := in.Rule.match.Bool(f.envMap())
	if err != nil {
		in.cs.MatchErrors.Add(1)
		return
	}
	if ok {
		in.root.handle(f)
	}
}

// Kill throws the instance's state away. Timers it armed find it dead.
func (in *Instance) Kill() {
	if !in.dead {
		in.dead = true
		in.root.release()
	}
}

func (in *Instance) build(sp *spec, fk *fork) node {
	base := base{in: in, sp: sp, ctr: in.cs.ByPath[sp.path], fk: fk}
	switch sp.op {
	case "by":
		return &byNode{base: base, forks: map[string]*fork{}}
	case "changed-state":
		base.kids = in.buildAll(sp.children, fk)
		return &changedNode{base: base, state: sp.initial}
	case "throttle":
		base.kids = in.buildAll(sp.children, fk)
		return &throttleNode{base: base}
	case "splitp":
		n := &splitpNode{base: base}
		for _, b := range sp.branches {
			n.branches = append(n.branches, in.build(b.child, fk))
		}
		n.otherwise = in.build(sp.otherwise, fk)
		return n
	case "set":
		base.kids = in.buildAll(sp.children, fk)
		return &setNode{base: base}
	case "coalesce":
		base.kids = in.buildAll(sp.children, fk)
		return &coalesceNode{base: base, held: map[[2]string]*event.Event{}}
	case "ddt":
		base.kids = in.buildAll(sp.children, fk)
		return &ddtNode{base: base}
	case "stable":
		base.kids = in.buildAll(sp.children, fk)
		return &stableNode{base: base}
	case "sink":
		return &sinkNode{base: base}
	default: // "where"
		base.kids = in.buildAll(sp.children, fk)
		return &whereNode{base: base}
	}
}

func (in *Instance) buildAll(specs []*spec, fk *fork) []node {
	out := make([]node, len(specs))
	for i, sp := range specs {
		out[i] = in.build(sp, fk)
	}
	return out
}

type base struct {
	in   *Instance
	sp   *spec
	ctr  *NodeCounters
	fk   *fork // nearest enclosing fork, nil outside any `by`
	kids []node
}

// pass delivers to every child in array order and counts one event passed.
func (b *base) pass(f *flow) {
	b.ctr.Passed.Add(1)
	for _, k := range b.kids {
		k.handle(f)
	}
}

func (b *base) release() {
	for _, k := range b.kids {
		k.release()
	}
}

// ---- where ----

type whereNode struct{ base }

func (n *whereNode) handle(f *flow) {
	ok, err := n.sp.pred.Bool(f.envMap())
	if err != nil {
		// An expression that cannot be evaluated on this event (ordering an
		// absent metric, say) does not hold. The event is lost to an error
		// rather than to the predicate, so it is counted.
		n.ctr.Discarded.Add(1)
		return
	}
	if ok {
		n.pass(f)
	}
}

// ---- by ----

// fork is one subtree instance of a `by`, plus what deciding to free it needs.
type fork struct {
	parent      *fork
	owner       *byNode
	key         string
	kids        []node
	timers      int  // armed timers beneath this fork
	expiredSeen bool // the last event through the fork was its expired event
}

func (fk *fork) timerDelta(d int) {
	for p := fk; p != nil; p = p.parent {
		p.timers += d
	}
}

// maybeFree frees every fork on the chain whose expired event has passed and
// which no timer references any more.
func (fk *fork) maybeFree() {
	for p := fk; p != nil; p = p.parent {
		if p.expiredSeen && p.timers == 0 {
			p.owner.free(p)
		}
	}
}

type byNode struct {
	base
	forks map[string]*fork
}

func (n *byNode) handle(f *flow) {
	var kb strings.Builder
	for _, name := range n.sp.fields {
		kb.WriteString(f.ev.FieldKey(name))
		kb.WriteByte(0)
	}
	key := kb.String()
	fk := n.forks[key]
	if fk == nil {
		fk = &fork{parent: n.fk, owner: n, key: key}
		fk.kids = n.in.buildAll(n.sp.children, fk)
		n.forks[key] = fk
		n.ctr.ForksLive.Add(1)
	}
	n.ctr.Passed.Add(1)
	for _, k := range fk.kids {
		k.handle(f)
	}
	// SPEC-GAP: "the key's expired event" is well defined only when the `by`
	// forks on host and service (SEMANTICS open question 6), and the spec
	// does not say whether a client-sent `expired` state counts. Chosen: the
	// rule is applied mechanically. Any event whose state is `expired` marks
	// the fork it landed in, whatever the fields are and whoever produced it,
	// and a later live event for the same key unmarks it. So a `by ["host"]`
	// fork is freed when any one of that host's services expires.
	fk.expiredSeen = f.ev.State == event.StateExpired
	if fk.expiredSeen && fk.timers == 0 {
		n.free(fk)
	}
}

func (n *byNode) free(fk *fork) {
	if n.forks[fk.key] != fk {
		return
	}
	delete(n.forks, fk.key)
	for _, k := range fk.kids {
		k.release()
	}
	n.ctr.ForksLive.Add(-1)
	n.ctr.ForksFreed.Add(1)
}

func (n *byNode) release() {
	for key, fk := range n.forks {
		for _, k := range fk.kids {
			k.release()
		}
		delete(n.forks, key)
		n.ctr.ForksLive.Add(-1)
	}
}

// ---- changed-state ----

type changedNode struct {
	base
	state string
}

func (n *changedNode) handle(f *flow) {
	prev := n.state
	n.state = f.ev.State
	if prev == f.ev.State {
		return
	}
	// SPEC-GAP: a path may traverse two changed-state nodes and the alert has
	// one prior_state. Chosen: the one nearest the sink wins.
	g := *f
	g.prior = &prev
	n.pass(&g)
}

// ---- throttle ----

type throttleNode struct {
	base
	open  bool
	end   float64
	count int64
}

func (n *throttleNode) handle(f *flow) {
	t := f.ev.Time
	// The window is half open, [t0, t0+window), and anchored on the first
	// event for the key, in event time.
	if !n.open || t >= n.end {
		n.open, n.end, n.count = true, t+n.sp.window, 0
	}
	n.count++
	if n.count > n.sp.limit {
		n.ctr.Discarded.Add(1)
		return
	}
	n.pass(f)
}

// ---- splitp ----

type splitpNode struct {
	base
	branches  []node
	otherwise node
}

func (n *splitpNode) handle(f *flow) {
	n.ctr.Passed.Add(1)
	for i, b := range n.sp.branches {
		// SPEC-GAP: the spec does not say what a branch test that fails to
		// evaluate does. Chosen: it does not hold, so an event no test can
		// judge goes to `otherwise` and exactly one subtree still receives it.
		if ok, err := b.pred.Bool(f.envMap()); err == nil && ok {
			n.branches[i].handle(f)
			return
		}
	}
	n.otherwise.handle(f)
}

func (n *splitpNode) release() {
	for _, b := range n.branches {
		b.release()
	}
	n.otherwise.release()
}

// ---- set ----

type setNode struct{ base }

func (n *setNode) handle(f *flow) {
	// Every expression reads the incoming event; the rewritten one is built
	// only after all of them have been evaluated.
	env := f.envMap()
	out := f.ev.Clone()
	for _, sf := range n.sp.setFields {
		v, err := sf.prog.Eval(env)
		if err == nil {
			err = assign(out, sf.name, v, f.now)
		}
		if err != nil {
			n.ctr.Discarded.Add(1)
			return
		}
	}
	// SPEC-GAP: nothing re-validates identity below ingest (SEMANTICS open
	// question 5). Chosen: a `set` that produces an empty host or service
	// discards the event here and counts it, rather than letting an event
	// with no identity reach a sink or the index.
	if out.Host == "" || out.Service == "" {
		n.ctr.Discarded.Add(1)
		return
	}
	n.pass(&flow{ev: out, now: f.now, prior: f.prior, events: f.events})
}

// assign writes one evaluated `set` value into the event.
//
// SPEC-GAP: the spec does not say what a `set` expression evaluating to null
// does (SEMANTICS open question 4). Chosen: null on `metric` produces an event
// with no metric; null on any other field produces that field's schema
// default, with `time` defaulting to the engine's current time as an absent
// `time` defaults to receive time.
func assign(ev *event.Event, field string, v any, now float64) error {
	switch field {
	case "metric", "time", "ttl":
		if v == nil {
			switch field {
			case "metric":
				ev.Metric, ev.HasMetric = 0, false
			case "time":
				ev.Time = now
			case "ttl":
				ev.TTL = event.DefaultTTL
			}
			return nil
		}
		x, ok := exprs.ToFloat(v)
		if !ok {
			return fmt.Errorf("set %s: value is %T, not a number", field, v)
		}
		switch field {
		case "metric":
			ev.Metric, ev.HasMetric = x, true
		case "time":
			ev.Time = x
		case "ttl":
			ev.TTL = x
		}
	case "tags":
		ev.Tags = nil
		if v == nil {
			return nil
		}
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return fmt.Errorf("set tags: value is %T, not an array", v)
		}
		for i := 0; i < rv.Len(); i++ {
			s, ok := rv.Index(i).Interface().(string)
			if !ok {
				return fmt.Errorf("set tags: element %d is not a string", i)
			}
			ev.Tags = append(ev.Tags, s)
		}
	case "attributes":
		ev.Attributes = nil
		if v == nil {
			return nil
		}
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Map {
			return fmt.Errorf("set attributes: value is %T, not an object", v)
		}
		ev.Attributes = make(map[string]string, rv.Len())
		it := rv.MapRange()
		for it.Next() {
			k, kok := it.Key().Interface().(string)
			s, sok := it.Value().Interface().(string)
			if !kok || !sok {
				return fmt.Errorf("set attributes: keys and values must be strings")
			}
			ev.Attributes[k] = s
		}
	default:
		s := ""
		if v != nil {
			var ok bool
			if s, ok = v.(string); !ok {
				return fmt.Errorf("set %s: value is %T, not a string", field, v)
			}
		}
		switch field {
		case "host":
			ev.Host = s
		case "service":
			ev.Service = s
		case "state":
			ev.State = s
		case "description":
			ev.Description = s
		case "source":
			ev.Source = s
		}
	}
	return nil
}

// ---- coalesce ----

type coalesceNode struct {
	base
	held map[[2]string]*event.Event
}

func (n *coalesceNode) handle(f *flow) {
	n.held[[2]string{f.ev.Host, f.ev.Service}] = f.ev
	// The emitted set is ordered by identity. The spec promises no order, and
	// a fixed one keeps a dry run a pure function of its input.
	ids := make([][2]string, 0, len(n.held))
	for id := range n.held {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i][0] != ids[j][0] {
			return ids[i][0] < ids[j][0]
		}
		return ids[i][1] < ids[j][1]
	})
	set := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		ev := n.held[id]
		set = append(set, exprs.EventMap(ev))
		// An expired identity is emitted once, in this set, and not retained.
		// Expiry here is the event's own state, or its time plus ttl against
		// the current time, strictly.
		if ev.State == event.StateExpired || f.now-ev.Time > ev.TTL {
			delete(n.held, id)
		}
	}
	// The child receives the event whose arrival caused the emission; the set
	// reaches expressions through `events`.
	n.pass(&flow{ev: f.ev, now: f.now, prior: f.prior, events: set})
}

// ---- ddt ----

type ddtNode struct {
	base
	has        bool
	prevTime   float64
	prevMetric float64
}

func (n *ddtNode) handle(f *flow) {
	if !f.ev.HasMetric {
		return // ignored entirely; it does not become the previous event
	}
	hadPrev, pt, pm := n.has, n.prevTime, n.prevMetric
	n.has, n.prevTime, n.prevMetric = true, f.ev.Time, f.ev.Metric
	if !hadPrev {
		return
	}
	dt := f.ev.Time - pt
	if dt == 0 {
		return // the rate is undefined; the event still became the previous one
	}
	out := f.ev.Clone()
	out.Metric = (f.ev.Metric - pm) / dt
	n.pass(&flow{ev: out, now: f.now, prior: f.prior, events: f.events})
}

// ---- stable ----

type held struct {
	ev     *event.Event
	prior  *string
	events []map[string]any
}

type stableNode struct {
	base
	hasValue bool
	value    string
	settled  bool    // the buffer has been released and the value has not changed since
	anchor   float64 // time of the event that last changed the value
	buf      []held
	gen      uint64
	armed    bool
}

func (n *stableNode) handle(f *flow) {
	v := f.ev.FieldKey(n.sp.field)
	if !n.hasValue || v != n.value {
		// The value changed: what was waiting is discarded, and counted.
		n.drop(len(n.buf))
		n.buf = n.buf[:0]
		n.hasValue, n.value, n.settled, n.anchor = true, v, false, f.ev.Time
		n.push(f)
		n.gen++
		gen := n.gen
		if !n.armed {
			n.armed = true
			n.fk.timerDelta(1)
		}
		n.in.host.Schedule(n.anchor+n.sp.duration, func() { n.timer(gen) })
		return
	}
	if n.settled {
		n.pass(f)
		return
	}
	n.push(f)
	// Inclusive: an event arriving exactly duration_seconds after the first
	// buffered one releases the buffer, itself included.
	if f.ev.Time-n.anchor >= n.sp.duration {
		n.releaseBuffer()
		n.disarm()
	}
}

func (n *stableNode) push(f *flow) {
	if limit := n.in.shared.StableBufferCapacity; len(n.buf) >= limit {
		// stable.buffer_capacity, policy drop-oldest, each eviction counted.
		over := len(n.buf) - limit + 1
		n.buf = append(n.buf[:0], n.buf[over:]...)
		n.ctr.Discarded.Add(int64(over))
		n.in.shared.StableEvicted.Add(int64(over))
		n.in.shared.StableDepth.Add(-int64(over))
	}
	n.buf = append(n.buf, held{ev: f.ev, prior: f.prior, events: f.events})
	n.in.shared.StableDepth.Add(1)
}

func (n *stableNode) drop(k int) {
	if k > 0 {
		n.ctr.Discarded.Add(int64(k))
		n.in.shared.StableDepth.Add(-int64(k))
	}
}

// releaseBuffer passes the buffer downstream in arrival order. Released events
// keep their original times.
func (n *stableNode) releaseBuffer() {
	buf := n.buf
	n.buf = nil
	n.settled = true
	n.in.shared.StableDepth.Add(-int64(len(buf)))
	now := n.in.host.Now()
	for _, h := range buf {
		n.pass(&flow{ev: h.ev, now: now, prior: h.prior, events: h.events})
	}
}

func (n *stableNode) disarm() {
	if n.armed {
		n.armed = false
		n.gen++ // whatever is still on the heap is stale now
		n.fk.timerDelta(-1)
	}
}

func (n *stableNode) timer(gen uint64) {
	if gen != n.gen || !n.armed {
		return // a stale generation
	}
	if n.in.dead {
		return
	}
	n.armed = false
	n.fk.timerDelta(-1)
	if n.in.dryRun || n.in.Rule.Active(n.in.host.Now()) {
		n.releaseBuffer()
	} else {
		// The rule was disabled by expires_at while events waited here.
		n.drop(len(n.buf))
		n.buf = nil
	}
	n.fk.maybeFree()
}

func (n *stableNode) release() {
	n.in.shared.StableDepth.Add(-int64(len(n.buf)))
	n.buf = nil
	n.gen++
	n.armed = false
	n.base.release()
}

// ---- sink leaf ----

type sinkNode struct{ base }

func (n *sinkNode) handle(f *flow) {
	n.ctr.Passed.Add(1)
	n.in.host.Fire(Firing{Rule: n.in.Rule, Sink: n.sp.sink, Node: n.sp.path, Event: f.ev, PriorState: f.prior})
}
