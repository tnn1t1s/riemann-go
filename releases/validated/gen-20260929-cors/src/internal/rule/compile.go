package rule

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/expression"
)

// The three sink leaves.
const (
	SinkNtfy   = "ntfy"
	SinkInflux = "influx"
	SinkIndex  = "index"
)

type kind int

const (
	kindWhere kind = iota
	kindBy
	kindChangedState
	kindThrottle
	kindSplitp
	kindSet
	kindCoalesce
	kindDdt
	kindStable
	kindSink
)

// Shared is what every instance of one node has in common: its path and its
// counters. Node state and counters are keyed by it, so a binding spliced in
// at several places is one node with one state per fork key, reported under
// its binding-rooted path.
//
// SPEC-GAP: the spec says a ref "splices the named binding in place" and that
// the spliced nodes "carry their binding-rooted path", without saying whether
// two refs to one binding share state. Chosen: they do, because the path is
// the node's identity and both refs yield the same path.
type Shared struct {
	Path      string
	Discards  bool
	Stable    bool
	passed    atomic.Uint64
	discarded atomic.Uint64
	buffered  atomic.Int64
}

type node struct {
	kind     kind
	sh       *Shared
	children []*node

	pred      *expression.Program         // where
	readers   []func(*event.Event) string // by
	initial   string                      // changed-state
	limit     uint64                      // throttle
	window    float64                     // throttle, seconds of event time
	branches  []branch                    // splitp
	otherwise *node                       // splitp
	sets      []setField                  // set
	duration  float64                     // stable, seconds of event time
	watch     func(*event.Event) string   // stable
	sink      string                      // sink leaf
}

type branch struct {
	test   *expression.Program
	stream *node
}

type setField struct {
	name string
	prog *expression.Program
}

// Program is a compiled rule.
type Program struct {
	Doc     *Document
	Version int

	match       *expression.Program
	root        *node
	shared      []*Shared
	hasCoalesce bool
}

// NodeStat is one node's counters.
type NodeStat struct {
	Path      string
	Passed    uint64
	Discarded uint64
	// Discards is true for a node kind that can discard, whether or not it
	// has yet.
	Discards bool
	// Stable is true for a stable node, and Buffered is the number of events
	// it holds across fork keys.
	Stable   bool
	Buffered int64
}

// Stats returns every node's counters in path order.
func (p *Program) Stats() []NodeStat {
	out := make([]NodeStat, 0, len(p.shared))
	for _, sh := range p.shared {
		out = append(out, NodeStat{
			Path:      sh.Path,
			Passed:    sh.passed.Load(),
			Discarded: sh.discarded.Load(),
			Discards:  sh.Discards,
			Stable:    sh.Stable,
			Buffered:  sh.buffered.Load(),
		})
	}
	return out
}

// Counters maps each node path to the number of events that node has passed
// downstream since this version was installed.
func (p *Program) Counters() map[string]uint64 {
	out := make(map[string]uint64, len(p.shared))
	for _, sh := range p.shared {
		out[sh.Path] = sh.passed.Load()
	}
	return out
}

type compiler struct {
	doc         *Document
	shared      map[string]*Shared
	bindings    map[string]*node
	referenced  map[string]bool
	hasCoalesce bool
}

// Compile compiles a document. An error names the node path or the
// expression that failed.
func Compile(doc *Document, version int) (*Program, error) {
	c := &compiler{
		doc:        doc,
		shared:     map[string]*Shared{},
		bindings:   map[string]*node{},
		referenced: map[string]bool{},
	}
	match, err := expression.Compile(doc.Match, false, true)
	if err != nil {
		return nil, fmt.Errorf("match: %v", err)
	}
	root, err := c.node(doc.stream, "stream", false, nil)
	if err != nil {
		return nil, err
	}
	if c.hasCoalesce && doc.Partition == PartitionHost {
		return nil, fmt.Errorf(`stream: a rule with partition "host" may not contain coalesce, because a per-host partition cannot answer a fleet-wide fold`)
	}
	// A binding nothing references is still compiled, so a broken one is
	// refused at PUT rather than when a later version first refers to it.
	names := make([]string, 0, len(doc.bindings))
	for name := range doc.bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if c.referenced[name] {
			continue
		}
		if _, err := c.binding(name, "bindings/"+name, false, nil); err != nil {
			return nil, err
		}
	}
	p := &Program{Doc: doc, Version: version, match: match, root: root, hasCoalesce: c.hasCoalesce}
	paths := make([]string, 0, len(c.shared))
	for path := range c.shared {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		p.shared = append(p.shared, c.shared[path])
	}
	return p, nil
}

func (c *compiler) share(path string, discards bool) *Shared {
	if sh, ok := c.shared[path]; ok {
		return sh
	}
	sh := &Shared{Path: path, Discards: discards}
	c.shared[path] = sh
	return sh
}

func (c *compiler) binding(name, at string, inCoalesce bool, stack []string) (*node, error) {
	raw, ok := c.doc.bindings[name]
	if !ok {
		return nil, fmt.Errorf("node %s: ref names binding %q, which the rule does not define", at, name)
	}
	for _, s := range stack {
		if s == name {
			return nil, fmt.Errorf("node %s: binding %q refers to itself through %s", at, name, strings.Join(append(stack, name), " -> "))
		}
	}
	memo := name + "\x00" + strconv.FormatBool(inCoalesce)
	if n, ok := c.bindings[memo]; ok {
		return n, nil
	}
	n, err := c.node(raw, "bindings/"+name, inCoalesce, append(stack, name))
	if err != nil {
		return nil, err
	}
	c.bindings[memo] = n
	return n, nil
}

func (c *compiler) node(raw any, path string, inCoalesce bool, stack []string) (*node, error) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("node %s: expected an object", path)
	}
	if v, ok := obj["ref"]; ok {
		name, isString := v.(string)
		if !isString || name == "" {
			return nil, fmt.Errorf("node %s: ref must be a binding name", path)
		}
		c.referenced[name] = true
		return c.binding(name, path, inCoalesce, stack)
	}
	if v, ok := obj["sink"]; ok {
		name, _ := v.(string)
		switch name {
		case SinkNtfy, SinkInflux, SinkIndex:
		default:
			return nil, fmt.Errorf(`node %s: sink is %v; expected "ntfy", "influx" or "index"`, path, v)
		}
		return &node{kind: kindSink, sh: c.share(path, false), sink: name}, nil
	}
	v, ok := obj["op"]
	if !ok {
		return nil, fmt.Errorf(`node %s: expected one of the keys "op", "sink" or "ref"`, path)
	}
	op, _ := v.(string)
	n := &node{}
	var err error
	switch op {
	case "where":
		n.kind = kindWhere
		src, isString := obj["expr"].(string)
		if !isString {
			return nil, fmt.Errorf("node %s: where requires expr, a string", path)
		}
		if n.pred, err = expression.Compile(src, inCoalesce, true); err != nil {
			return nil, fmt.Errorf("node %s: %v", path, err)
		}
	case "by":
		n.kind = kindBy
		fields, isArray := obj["fields"].([]any)
		if !isArray || len(fields) == 0 {
			return nil, fmt.Errorf("node %s: by requires fields, a non-empty array of event field names", path)
		}
		for _, f := range fields {
			name, _ := f.(string)
			reader, err := event.Reader(name)
			if err != nil {
				return nil, fmt.Errorf("node %s: by: %v", path, err)
			}
			n.readers = append(n.readers, reader)
		}
	case "changed-state":
		n.kind = kindChangedState
		// SPEC-GAP: the combinator table lists `initial` without saying
		// whether it may be omitted. Chosen: an omitted `initial` is "",
		// which is what an absent state reads as.
		if v, ok := obj["initial"]; ok && v != nil {
			s, isString := v.(string)
			if !isString {
				return nil, fmt.Errorf("node %s: changed-state initial must be a string", path)
			}
			n.initial = s
		}
	case "throttle":
		n.kind = kindThrottle
		limit, ok := number(obj["limit"])
		if !ok || limit < 0 || limit != math.Trunc(limit) {
			return nil, fmt.Errorf("node %s: throttle requires limit, a non-negative integer", path)
		}
		window, ok := number(obj["window_seconds"])
		if !ok || !(window > 0) {
			return nil, fmt.Errorf("node %s: throttle requires window_seconds, a number greater than zero", path)
		}
		n.limit, n.window = uint64(limit), window
	case "splitp":
		n.kind = kindSplitp
	case "set":
		n.kind = kindSet
		fields, isObject := obj["fields"].(map[string]any)
		if !isObject {
			return nil, fmt.Errorf("node %s: set requires fields, an object mapping a field name to an expression", path)
		}
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if !event.IsField(name) {
				return nil, fmt.Errorf("node %s: set: unknown event field %q", path, name)
			}
			src, isString := fields[name].(string)
			if !isString {
				return nil, fmt.Errorf("node %s: set: field %q must be an expression string", path, name)
			}
			prog, err := expression.Compile(src, inCoalesce, false)
			if err != nil {
				return nil, fmt.Errorf("node %s: set field %q: %v", path, name, err)
			}
			n.sets = append(n.sets, setField{name: name, prog: prog})
		}
	case "coalesce":
		n.kind = kindCoalesce
		c.hasCoalesce = true
	case "ddt":
		n.kind = kindDdt
	case "stable":
		n.kind = kindStable
		duration, ok := number(obj["duration_seconds"])
		if !ok || duration < 0 {
			return nil, fmt.Errorf("node %s: stable requires duration_seconds, a non-negative number", path)
		}
		name, isString := obj["field"].(string)
		if !isString {
			return nil, fmt.Errorf("node %s: stable requires field, an event field name", path)
		}
		if n.watch, err = event.Reader(name); err != nil {
			return nil, fmt.Errorf("node %s: stable: %v", path, err)
		}
		n.duration = duration
	default:
		return nil, fmt.Errorf("node %s: unknown op %v; expected one of where, by, changed-state, throttle, splitp, set, coalesce, ddt, stable", path, v)
	}
	n.sh = c.share(path, n.kind == kindWhere || n.kind == kindThrottle || n.kind == kindSplitp || n.kind == kindSet || n.kind == kindStable)
	n.sh.Stable = n.kind == kindStable

	if n.kind == kindSplitp {
		if err := c.splitp(n, obj, path, inCoalesce, stack); err != nil {
			return nil, err
		}
		return n, nil
	}

	// SPEC-GAP: the spec says each node has a `children` array and does not
	// say whether an absent one is an error. Chosen: absent is empty, a node
	// that passes events to nothing and still counts them.
	if v, ok := obj["children"]; ok && v != nil {
		children, isArray := v.([]any)
		if !isArray {
			return nil, fmt.Errorf("node %s: children must be an array", path)
		}
		below := inCoalesce || n.kind == kindCoalesce
		for k, child := range children {
			cn, err := c.node(child, path+"/"+strconv.Itoa(k), below, stack)
			if err != nil {
				return nil, err
			}
			n.children = append(n.children, cn)
		}
	}
	return n, nil
}

func (c *compiler) splitp(n *node, obj map[string]any, path string, inCoalesce bool, stack []string) error {
	test, isString := obj["test"].(string)
	if !isString {
		return fmt.Errorf("node %s: splitp requires test, an expression string containing {}", path)
	}
	if !strings.Contains(test, "{}") {
		return fmt.Errorf("node %s: splitp test %q contains no {} placeholder", path, test)
	}
	branches, isArray := obj["branches"].([]any)
	if !isArray {
		return fmt.Errorf("node %s: splitp requires branches, an array", path)
	}
	rawOtherwise, ok := obj["otherwise"]
	if !ok || rawOtherwise == nil {
		return fmt.Errorf("node %s: splitp requires otherwise", path)
	}
	for k, rawBranch := range branches {
		at := path + "/branches/" + strconv.Itoa(k)
		entry, isObject := rawBranch.(map[string]any)
		if !isObject {
			return fmt.Errorf("node %s: a branch must be an object with threshold and stream", at)
		}
		threshold, hasThreshold := entry["threshold"]
		stream, hasStream := entry["stream"]
		if !hasThreshold || !hasStream || len(entry) != 2 {
			return fmt.Errorf("node %s: a branch has exactly two keys, threshold and stream", at)
		}
		literal, err := thresholdLiteral(threshold)
		if err != nil {
			return fmt.Errorf("node %s: %v", at, err)
		}
		prog, err := expression.Compile(strings.ReplaceAll(test, "{}", literal), inCoalesce, true)
		if err != nil {
			return fmt.Errorf("node %s: test with threshold substituted: %v", at, err)
		}
		sub, err := c.node(stream, at, inCoalesce, stack)
		if err != nil {
			return err
		}
		n.branches = append(n.branches, branch{test: prog, stream: sub})
	}
	sub, err := c.node(rawOtherwise, path+"/otherwise", inCoalesce, stack)
	if err != nil {
		return err
	}
	n.otherwise = sub
	return nil
}

// thresholdLiteral renders a branch's threshold as expression source.
//
// SPEC-GAP: the spec says the threshold is "substituted" into the test and
// shows only numbers. Chosen: a number is substituted as the text the client
// sent, a string as a quoted string literal, a boolean as true or false.
// Arrays, objects and null are refused.
func thresholdLiteral(v any) (string, error) {
	switch t := v.(type) {
	case json.Number:
		return t.String(), nil
	case float64:
		return event.FormatFloat(t), nil
	case string:
		b, err := marshal(t)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case bool:
		return strconv.FormatBool(t), nil
	}
	return "", fmt.Errorf("threshold must be a number, a string or a boolean")
}
