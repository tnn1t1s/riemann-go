package rule

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/expression"
)

// Firing is one event reaching one sink leaf through one rule.
type Firing struct {
	Sink    string
	Event   *event.Event
	Rule    string
	Version int
	Owner   string
	// PriorState is nil when the path traversed no changed-state node.
	PriorState *string
	Node       string
}

// Host is where an instance runs: a partition, the global partition, or a
// dry run. Every method is called from the goroutine that called Handle or
// that fired a timer, never concurrently for one instance.
type Host interface {
	// Now is the engine's current time in float seconds.
	Now() float64
	// Schedule arranges for fn to run at or after the given time, ordered
	// against events per SPEC.md property 12.
	Schedule(at float64, fn func())
	// Deliver hands a firing to a sink, or to a dry run's stub.
	Deliver(f Firing)
	// StableBufferCapacity is stable.buffer_capacity, in events per fork key.
	StableBufferCapacity() int
	// ForksChanged reports a change in the number of live by-forks and the
	// number freed.
	ForksChanged(liveDelta, freed int)
}

// delivery is an event on its way down a tree, with what it has picked up.
type delivery struct {
	ev     *event.Event
	prior  *string
	events []map[string]any
	env    map[string]any
}

// scope holds the state of every stateful node for one fork key. The root
// scope is the single fork key of a rule with no by.
type scope struct {
	parent *scope
	by     *Shared
	key    string

	states map[*Shared]any
	forks  map[*Shared]map[string]*scope

	// timers counts armed timers at or beneath this scope that have not yet
	// fired. A fork with one is not freed.
	timers int
	// expiredSeen is true when the last event through this fork carried
	// state expired.
	expiredSeen bool
}

func newScope(parent *scope, by *Shared, key string) *scope {
	return &scope{parent: parent, by: by, key: key, states: map[*Shared]any{}}
}

// Instance is the state one partition holds for one rule version.
type Instance struct {
	prog      *Program
	host      Host
	root      *scope
	dead      bool
	liveForks int
}

// NewInstance returns a fresh instance with no state.
func (p *Program) NewInstance(h Host) *Instance {
	return &Instance{prog: p, host: h, root: newScope(nil, nil, "")}
}

// Program returns the compiled rule the instance runs.
func (in *Instance) Program() *Program { return in.prog }

// Kill drops the instance's state. Timers it armed find it dead and do
// nothing.
func (in *Instance) Kill() {
	if in.dead {
		return
	}
	in.dead = true
	in.host.ForksChanged(-in.liveForks, 0)
	in.liveForks = 0
	in.root = nil
}

// Handle offers one event to the rule.
//
// SPEC-GAP: the spec does not say what a `match` that fails at evaluation
// does, for instance `metric > 10` on an event with no metric. Chosen: the
// event does not enter the rule. It is not counted as a discard, because it
// never reached a node.
func (in *Instance) Handle(ev *event.Event) {
	if in.dead {
		return
	}
	d := &delivery{ev: ev}
	ok, err := in.prog.match.Bool(in.env(d))
	if err != nil || !ok {
		return
	}
	in.run(in.prog.root, d, in.root)
}

func (in *Instance) env(d *delivery) map[string]any {
	if d.env == nil {
		d.env = expression.Env(d.ev, in.host.Now(), d.events)
	}
	return d.env
}

func (in *Instance) pass(n *node, d *delivery, sc *scope) {
	n.sh.passed.Add(1)
	for _, child := range n.children {
		in.run(child, d, sc)
	}
}

func (in *Instance) discard(n *node, count int) {
	if count > 0 {
		n.sh.discarded.Add(uint64(count))
	}
}

func (in *Instance) run(n *node, d *delivery, sc *scope) {
	switch n.kind {
	case kindSink:
		n.sh.passed.Add(1)
		in.host.Deliver(Firing{
			Sink:       n.sink,
			Event:      d.ev,
			Rule:       in.prog.Doc.ID,
			Version:    in.prog.Version,
			Owner:      in.prog.Doc.Owner,
			PriorState: d.prior,
			Node:       n.sh.Path,
		})
	case kindWhere:
		in.where(n, d, sc)
	case kindBy:
		in.by(n, d, sc)
	case kindChangedState:
		in.changedState(n, d, sc)
	case kindThrottle:
		in.throttle(n, d, sc)
	case kindSplitp:
		in.splitp(n, d, sc)
	case kindSet:
		in.set(n, d, sc)
	case kindCoalesce:
		in.coalesce(n, d, sc)
	case kindDdt:
		in.ddt(n, d, sc)
	case kindStable:
		in.stable(n, d, sc)
	}
}

// SPEC-GAP: INVARIANTS.md I5 counts every discarded event, SEMANTICS.md says
// a false `where` discards, and SPEC.md's list of what riemann.rule.discarded
// covers leaves `where` and changed-state out. Chosen: an event a `where`
// declines or a changed-state suppresses is the node doing its job and is not
// counted as a discard; the node's passed count at GET /rules/{id} shows it.
// An expression that fails at evaluation is counted, because that event was
// lost rather than filtered.
func (in *Instance) where(n *node, d *delivery, sc *scope) {
	ok, err := n.pred.Bool(in.env(d))
	if err != nil {
		in.discard(n, 1)
		return
	}
	if ok {
		in.pass(n, d, sc)
	}
}

func (in *Instance) by(n *node, d *delivery, sc *scope) {
	var sb strings.Builder
	for _, read := range n.readers {
		v := read(d.ev)
		sb.WriteString(strconv.Itoa(len(v)))
		sb.WriteByte(':')
		sb.WriteString(v)
	}
	key := sb.String()
	if sc.forks == nil {
		sc.forks = map[*Shared]map[string]*scope{}
	}
	table := sc.forks[n.sh]
	if table == nil {
		table = map[string]*scope{}
		sc.forks[n.sh] = table
	}
	fork := table[key]
	if fork == nil {
		fork = newScope(sc, n.sh, key)
		table[key] = fork
		in.liveForks++
		in.host.ForksChanged(1, 0)
	}
	// SPEC-GAP: SEMANTICS.md open question 6, which fork a non-identity by
	// frees. "The key's expired event" is taken to be any event with state
	// expired that reaches the fork, whatever fields the by names and
	// whether the index or a client produced the event. A by on a field an
	// expiry event does not carry is therefore freed only by a client's
	// expired event that carries it.
	fork.expiredSeen = d.ev.State == event.StateExpired
	in.pass(n, d, fork)
	in.maybeFree(fork)
}

// maybeFree frees a fork whose expired event has passed through it and which
// no timer still references.
func (in *Instance) maybeFree(fork *scope) {
	if in.dead || fork.by == nil || !fork.expiredSeen || fork.timers > 0 {
		return
	}
	table := fork.parent.forks[fork.by]
	if table[fork.key] != fork {
		return
	}
	delete(table, fork.key)
	freed := 1 + countForks(fork)
	in.liveForks -= freed
	in.host.ForksChanged(-freed, freed)
}

func countForks(sc *scope) int {
	n := 0
	for _, table := range sc.forks {
		for _, child := range table {
			n += 1 + countForks(child)
		}
	}
	return n
}

type changedState struct{ last string }

func (in *Instance) changedState(n *node, d *delivery, sc *scope) {
	st, _ := sc.states[n.sh].(*changedState)
	if st == nil {
		st = &changedState{last: n.initial}
		sc.states[n.sh] = st
	}
	prior := st.last
	st.last = d.ev.State
	if prior == d.ev.State {
		return
	}
	nd := *d
	nd.prior = &prior
	in.pass(n, &nd, sc)
}

type throttleState struct {
	open  bool
	end   float64
	count uint64
}

func (in *Instance) throttle(n *node, d *delivery, sc *scope) {
	st, _ := sc.states[n.sh].(*throttleState)
	if st == nil {
		st = &throttleState{}
		sc.states[n.sh] = st
	}
	// The window is half open: an event stamped exactly at its end opens
	// the next one.
	if !st.open || d.ev.Time >= st.end {
		st.open = true
		st.end = d.ev.Time + n.window
		st.count = 0
	}
	st.count++
	if st.count > n.limit {
		in.discard(n, 1)
		return
	}
	in.pass(n, d, sc)
}

func (in *Instance) splitp(n *node, d *delivery, sc *scope) {
	env := in.env(d)
	for _, b := range n.branches {
		// A test that fails at evaluation does not hold.
		if ok, err := b.test.Bool(env); err == nil && ok {
			n.sh.passed.Add(1)
			in.run(b.stream, d, sc)
			return
		}
	}
	n.sh.passed.Add(1)
	in.run(n.otherwise, d, sc)
}

func (in *Instance) set(n *node, d *delivery, sc *scope) {
	env := in.env(d)
	out := d.ev.Clone()
	for _, f := range n.sets {
		// Every expression reads the incoming event, never the partially
		// rewritten one.
		v, err := f.prog.Eval(env)
		if err == nil {
			err = assign(out, f.name, v, in.host.Now())
		}
		if err != nil {
			in.discard(n, 1)
			return
		}
	}
	// SPEC-GAP: SEMANTICS.md open question 5, whether set may produce an
	// empty host or service. Chosen: checked at evaluation; the event is
	// discarded and counted, because nothing downstream can name it.
	if out.Host == "" || out.Service == "" {
		in.discard(n, 1)
		return
	}
	in.pass(n, &delivery{ev: out, prior: d.prior, events: d.events}, sc)
}

func toFloat(v any) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	}
	return 0, false
}

func finite(v any) (float64, bool) {
	f, ok := toFloat(v)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// assign writes one set result into the event.
//
// SPEC-GAP: SEMANTICS.md open question 4, what a set expression evaluating
// to null does. Chosen: the field takes the event model's absent-field
// default, which for metric is no metric at all and for time is the engine's
// current time. A result of the wrong type for its field fails the node, and
// the event is discarded and counted.
func assign(e *event.Event, field string, v any, now float64) error {
	str := func(dst *string) error {
		if v == nil {
			*dst = ""
			return nil
		}
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("set %s: result is %T, not a string", field, v)
		}
		*dst = s
		return nil
	}
	switch field {
	case "host":
		return str(&e.Host)
	case "service":
		return str(&e.Service)
	case "state":
		return str(&e.State)
	case "description":
		return str(&e.Description)
	case "source":
		return str(&e.Source)
	case "metric":
		if v == nil {
			e.Metric, e.HasMetric = 0, false
			return nil
		}
		f, ok := finite(v)
		if !ok {
			return fmt.Errorf("set metric: result %v is not a finite number", v)
		}
		e.Metric, e.HasMetric = f, true
	case "time":
		if v == nil {
			e.Time = now
			return nil
		}
		f, ok := finite(v)
		if !ok {
			return fmt.Errorf("set time: result %v is not a finite number", v)
		}
		e.Time = f
	case "ttl":
		if v == nil {
			e.TTL = event.DefaultTTLSeconds
			return nil
		}
		f, ok := finite(v)
		if !ok {
			return fmt.Errorf("set ttl: result %v is not a finite number", v)
		}
		e.TTL = f
	case "tags":
		switch t := v.(type) {
		case nil:
			e.Tags = nil
		case []string:
			e.Tags = append([]string(nil), t...)
		case []any:
			tags := make([]string, 0, len(t))
			for _, item := range t {
				s, ok := item.(string)
				if !ok {
					return fmt.Errorf("set tags: element is %T, not a string", item)
				}
				tags = append(tags, s)
			}
			e.Tags = tags
		default:
			return fmt.Errorf("set tags: result is %T, not an array of strings", v)
		}
	case "attributes":
		switch t := v.(type) {
		case nil:
			e.Attributes = nil
		case map[string]string:
			attrs := make(map[string]string, len(t))
			for k, val := range t {
				attrs[k] = val
			}
			e.Attributes = attrs
		case map[string]any:
			attrs := make(map[string]string, len(t))
			for k, val := range t {
				s, ok := val.(string)
				if !ok {
					return fmt.Errorf("set attributes: value of %q is %T, not a string", k, val)
				}
				attrs[k] = s
			}
			e.Attributes = attrs
		default:
			return fmt.Errorf("set attributes: result is %T, not an object of strings", v)
		}
	}
	return nil
}

type coalesceState struct {
	held map[event.Key]*event.Event
}

func (in *Instance) coalesce(n *node, d *delivery, sc *scope) {
	st, _ := sc.states[n.sh].(*coalesceState)
	if st == nil {
		st = &coalesceState{held: map[event.Key]*event.Event{}}
		sc.states[n.sh] = st
	}
	st.held[d.ev.Key()] = d.ev

	now := in.host.Now()
	set := make([]*event.Event, 0, len(st.held))
	for key, held := range st.held {
		set = append(set, held)
		// An expired identity is emitted once, in this set, and then
		// dropped. The comparison is the index's: live through time + ttl.
		if held.State == event.StateExpired || now > held.Deadline() {
			delete(st.held, key)
		}
	}
	// The spec promises no order within the set. A fixed one keeps a dry
	// run's output a function of its input.
	sort.Slice(set, func(i, j int) bool {
		if set[i].Host != set[j].Host {
			return set[i].Host < set[j].Host
		}
		return set[i].Service < set[j].Service
	})
	events := make([]map[string]any, len(set))
	for i, e := range set {
		events[i] = expression.Fields(e)
	}
	in.pass(n, &delivery{ev: d.ev, prior: d.prior, events: events}, sc)
}

type ddtState struct {
	prev *event.Event
}

func (in *Instance) ddt(n *node, d *delivery, sc *scope) {
	if !d.ev.HasMetric {
		return
	}
	st, _ := sc.states[n.sh].(*ddtState)
	if st == nil {
		sc.states[n.sh] = &ddtState{prev: d.ev}
		return
	}
	prev := st.prev
	st.prev = d.ev
	dt := d.ev.Time - prev.Time
	if dt == 0 {
		return
	}
	out := d.ev.Clone()
	out.Metric = (d.ev.Metric - prev.Metric) / dt
	in.pass(n, &delivery{ev: out, prior: d.prior, events: d.events}, sc)
}

type stableState struct {
	has     bool
	value   string
	settled bool
	// since is the time of the event that changed the watched value.
	since float64
	buf   []*delivery
	// gen numbers value changes. A timer armed for an earlier one is stale.
	gen uint64
}

func (in *Instance) stable(n *node, d *delivery, sc *scope) {
	st, _ := sc.states[n.sh].(*stableState)
	if st == nil {
		st = &stableState{}
		sc.states[n.sh] = st
	}
	value := n.watch(d.ev)
	if !st.has || value != st.value {
		in.discard(n, len(st.buf))
		n.sh.buffered.Add(-int64(len(st.buf)))
		st.has, st.value, st.settled = true, value, false
		st.since = d.ev.Time
		st.buf = append(st.buf[:0:0], in.hold(n, d))
		st.gen++
		in.arm(n, st, sc)
		return
	}
	if st.settled {
		in.pass(n, d, sc)
		return
	}
	// The comparison is inclusive: an event arriving exactly
	// duration_seconds after the value changed releases the buffer and
	// follows it out.
	if d.ev.Time-st.since >= n.duration {
		in.release(n, st, sc)
		in.pass(n, d, sc)
		return
	}
	if capacity := in.host.StableBufferCapacity(); len(st.buf) >= capacity {
		evict := len(st.buf) - capacity + 1
		in.discard(n, evict)
		n.sh.buffered.Add(-int64(evict))
		st.buf = append(st.buf[:0:0], st.buf[evict:]...)
	}
	st.buf = append(st.buf, in.hold(n, d))
}

func (in *Instance) hold(n *node, d *delivery) *delivery {
	n.sh.buffered.Add(1)
	// The environment is rebuilt at release, when `now` has moved.
	return &delivery{ev: d.ev, prior: d.prior, events: d.events}
}

func (in *Instance) release(n *node, st *stableState, sc *scope) {
	buf := st.buf
	st.buf = nil
	st.settled = true
	n.sh.buffered.Add(-int64(len(buf)))
	for _, held := range buf {
		in.pass(n, held, sc)
	}
}

func (in *Instance) arm(n *node, st *stableState, sc *scope) {
	gen := st.gen
	for s := sc; s != nil; s = s.parent {
		s.timers++
	}
	in.host.Schedule(st.since+n.duration, func() {
		if in.dead {
			return
		}
		if st.gen == gen && !st.settled {
			in.release(n, st, sc)
		}
		for s := sc; s != nil; s = s.parent {
			s.timers--
		}
		for s := sc; s != nil; s = s.parent {
			in.maybeFree(s)
		}
	})
}
