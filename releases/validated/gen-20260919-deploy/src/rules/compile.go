// Package rules parses and compiles rule documents, and runs the nine stream
// combinators of SEMANTICS.md over events. It is a core package: it knows
// nothing about HTTP, ntfy or InfluxDB, and reaches the outside only through
// the Host interface its owner supplies.
package rules

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/exprs"
)

// Partition values of a rule document.
const (
	PartitionHost        = "host"
	PartitionHostService = "host,service"
	PartitionGlobal      = "global"
)

// Sink leaf names.
const (
	SinkNtfy   = "ntfy"
	SinkInflux = "influx"
	SinkIndex  = "index"
)

// NodeCounters are the per-node counts since a rule version was installed.
type NodeCounters struct {
	Op         string
	Passed     atomic.Int64 // events this node passed downstream
	Discarded  atomic.Int64 // events this node discarded (riemann.rule.discarded)
	ForksLive  atomic.Int64 // `by` only
	ForksFreed atomic.Int64 // `by` only
}

// CounterSet maps a node path to its counters. The live set belongs to the
// installed rule version; a dry run gets a throwaway set so it touches
// nothing live.
type CounterSet struct {
	Paths  []string
	ByPath map[string]*NodeCounters
	// MatchErrors counts events for which the rule's `match` failed to
	// evaluate, for example ordering an absent metric. Such an event does not
	// match; it is not discarded, because it never entered the rule.
	MatchErrors atomic.Int64
}

// Shared holds what every stable node of a process reports into: the buffer
// bound and the aggregate gauges of the `stable.buffer` queue.
type Shared struct {
	StableBufferCapacity int // stable.buffer_capacity, events per fork key
	StableDepth          atomic.Int64
	StableEvicted        atomic.Int64
}

// Compiled is one installed version of a rule.
type Compiled struct {
	ID        string
	Owner     string
	Partition string
	Version   int
	Hash      string
	Enabled   bool
	ExpiresAt *float64
	// Doc is the stored document: the submitted keys with defaults filled in,
	// plus the server-owned version.
	Doc  map[string]any
	Live *CounterSet

	match    *exprs.Program
	root     *spec
	paths    []string
	ops      map[string]string
	bindings map[string]any
}

// Active reports whether the rule fires at time now.
func (c *Compiled) Active(now float64) bool {
	if !c.Enabled {
		return false
	}
	return c.ExpiresAt == nil || now <= *c.ExpiresAt
}

// NewCounterSet returns zeroed counters for every node path of the rule.
func (c *Compiled) NewCounterSet() *CounterSet {
	cs := &CounterSet{Paths: c.paths, ByPath: make(map[string]*NodeCounters, len(c.paths))}
	for _, p := range c.paths {
		cs.ByPath[p] = &NodeCounters{Op: c.ops[p]}
	}
	return cs
}

type branchSpec struct {
	pred  *exprs.Program
	child *spec
}

type setField struct {
	name string
	prog *exprs.Program
}

// spec is one compiled node. It holds no state; instances do.
type spec struct {
	op        string
	path      string
	children  []*spec
	pred      *exprs.Program // where
	fields    []string       // by
	initial   string         // changed-state
	limit     int64          // throttle
	window    float64        // throttle, seconds of event time
	branches  []branchSpec   // splitp
	otherwise *spec          // splitp
	setFields []setField     // set
	duration  float64        // stable, seconds of event time
	field     string         // stable
	sink      string         // sink leaf
}

// Error is a rule that does not compile. It is a 400 at PUT and names the
// node path or the expression that failed.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

func errf(format string, a ...any) error { return &Error{Msg: fmt.Sprintf(format, a...)} }

// ParseDoc decodes one rule document, keeping number literals verbatim.
func ParseDoc(body []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, errf("rule document is not a JSON object: %v", err)
	}
	return doc, nil
}

// Canonical returns the form of a document that is hashed: the
// client-supplied `version` removed and the two defaulted keys made explicit.
//
// SPEC-GAP: the spec says "canonical form" without defining it. Chosen: the
// document as a JSON object with sorted keys, `version` removed, `enabled`
// and `partition` filled with their defaults, number literals kept as
// written. Unknown keys are kept and hashed.
func Canonical(doc map[string]any) (map[string]any, string, error) {
	c := make(map[string]any, len(doc)+2)
	for k, v := range doc {
		if k != "version" {
			c[k] = v
		}
	}
	if _, ok := c["enabled"]; !ok {
		c["enabled"] = true
	}
	if _, ok := c["partition"]; !ok {
		c["partition"] = PartitionHost
	}
	b, err := json.Marshal(c) // encoding/json sorts map keys
	if err != nil {
		return nil, "", errf("rule document cannot be canonicalized: %v", err)
	}
	sum := sha256.Sum256(b)
	return c, hex.EncodeToString(sum[:]), nil
}

// Compile validates a canonical document and compiles every expression in it.
func Compile(doc map[string]any, hash string, version int) (*Compiled, error) {
	c := &Compiled{Version: version, Hash: hash, ops: map[string]string{}}
	var err error
	if c.ID, err = reqString(doc, "id", "rule"); err != nil {
		return nil, err
	}
	if c.Owner, err = reqString(doc, "owner", "rule"); err != nil {
		return nil, err
	}
	c.Partition, _ = doc["partition"].(string)
	switch c.Partition {
	case PartitionHost, PartitionHostService, PartitionGlobal:
	default:
		return nil, errf("rule: partition must be %q, %q or %q", PartitionHost, PartitionHostService, PartitionGlobal)
	}
	enabled, ok := doc["enabled"].(bool)
	if !ok {
		return nil, errf("rule: enabled must be a boolean")
	}
	c.Enabled = enabled
	if raw, present := doc["expires_at"]; present && raw != nil {
		f, ok := number(raw)
		if !ok {
			return nil, errf("rule: expires_at must be a number of seconds since the epoch")
		}
		c.ExpiresAt = &f
	}
	matchSrc, err := reqString(doc, "match", "rule")
	if err != nil {
		return nil, err
	}
	if c.match, err = exprs.CompileBool(matchSrc, false); err != nil {
		return nil, errf("match: %v", err)
	}
	if raw, present := doc["bindings"]; present && raw != nil {
		b, ok := raw.(map[string]any)
		if !ok {
			return nil, errf("bindings: must be an object mapping a name to a sub-tree")
		}
		c.bindings = b
	}
	streamRaw, present := doc["stream"]
	if !present {
		return nil, errf("rule: stream is required")
	}
	cc := &compiler{rule: c, used: map[string]bool{}}
	if c.root, err = cc.node(streamRaw, "stream", false, nil); err != nil {
		return nil, err
	}
	// A binding nothing references is still validated, so a broken sub-tree is
	// refused at PUT rather than on the day a ref is added. It is compiled
	// with `events` in scope because where it will be spliced is unknown.
	names := make([]string, 0, len(c.bindings))
	for name := range c.bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !cc.used[name] {
			if _, err := cc.node(c.bindings[name], "bindings/"+name, true, []string{name}); err != nil {
				return nil, err
			}
		}
	}
	if c.Partition == PartitionHost && cc.sawCoalesce {
		return nil, errf("%s: coalesce needs partition %q or %q; a per-host partition cannot answer a fleet-wide fold",
			cc.coalescePath, PartitionHostService, PartitionGlobal)
	}
	c.paths = cc.paths
	stored := make(map[string]any, len(doc)+1)
	for k, v := range doc {
		stored[k] = v
	}
	stored["version"] = version
	c.Doc = stored
	c.Live = c.NewCounterSet()
	return c, nil
}

type compiler struct {
	rule         *Compiled
	paths        []string
	used         map[string]bool
	sawCoalesce  bool
	coalescePath string
}

func (cc *compiler) record(path, op string) {
	if _, seen := cc.rule.ops[path]; !seen {
		cc.paths = append(cc.paths, path)
	}
	cc.rule.ops[path] = op
}

// node compiles one tree entry. inCoalesce says whether `events` is in scope;
// refStack detects a binding that reaches itself.
func (cc *compiler) node(raw any, path string, inCoalesce bool, refStack []string) (*spec, error) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, errf("%s: a node must be an object", path)
	}
	if refRaw, isRef := obj["ref"]; isRef {
		name, ok := refRaw.(string)
		if !ok {
			return nil, errf("%s: ref must be a binding name", path)
		}
		target, ok := cc.rule.bindings[name]
		if !ok {
			return nil, errf("%s: ref names unknown binding %q", path, name)
		}
		for _, r := range refStack {
			if r == name {
				return nil, errf("%s: binding %q references itself", path, name)
			}
		}
		cc.used[name] = true
		// The ref leaf contributes no path segment: the spliced nodes carry
		// their binding-rooted path.
		return cc.node(target, "bindings/"+name, inCoalesce, append(append([]string(nil), refStack...), name))
	}
	if sinkRaw, isSink := obj["sink"]; isSink {
		name, _ := sinkRaw.(string)
		switch name {
		case SinkNtfy, SinkInflux, SinkIndex:
		default:
			return nil, errf("%s: sink must be %q, %q or %q", path, SinkNtfy, SinkInflux, SinkIndex)
		}
		cc.record(path, "sink:"+name)
		return &spec{op: "sink", path: path, sink: name}, nil
	}
	op, ok := obj["op"].(string)
	if !ok {
		return nil, errf("%s: a node needs op, sink or ref", path)
	}
	sp := &spec{op: op, path: path}
	var err error
	childCoalesce := inCoalesce
	switch op {
	case "where":
		src, err := reqString(obj, "expr", path)
		if err != nil {
			return nil, err
		}
		if sp.pred, err = exprs.CompileBool(src, inCoalesce); err != nil {
			return nil, errf("%s: %v", path, err)
		}
	case "by":
		arr, ok := obj["fields"].([]any)
		if !ok || len(arr) == 0 {
			return nil, errf("%s: by needs fields, a non-empty array of event field names", path)
		}
		for _, f := range arr {
			name, ok := f.(string)
			// SPEC-GAP: the spec says `fields` are "event fields" and
			// SEMANTICS mentions `by ["attributes.run_id"]`. Chosen: the ten
			// field names plus `attributes.<key>`; anything else is a 400.
			if !ok || !event.ValidFieldRef(name) {
				return nil, errf("%s: by field %v is not an event field", path, f)
			}
			sp.fields = append(sp.fields, name)
		}
	case "changed-state":
		// SPEC-GAP: the spec does not say whether `initial` may be omitted.
		// Chosen: optional, defaulting to "", the reading of an absent state.
		if raw, present := obj["initial"]; present {
			s, ok := raw.(string)
			if !ok {
				return nil, errf("%s: changed-state initial must be a string", path)
			}
			sp.initial = s
		}
	case "throttle":
		lim, ok := number(obj["limit"])
		if !ok || lim < 0 || lim != math.Trunc(lim) {
			return nil, errf("%s: throttle needs limit, a non-negative integer", path)
		}
		sp.limit = int64(lim)
		if sp.window, ok = number(obj["window_seconds"]); !ok || !(sp.window > 0) {
			return nil, errf("%s: throttle needs window_seconds, a positive number", path)
		}
	case "splitp":
		return cc.splitp(sp, obj, inCoalesce, refStack)
	case "set":
		fields, ok := obj["fields"].(map[string]any)
		if !ok || len(fields) == 0 {
			return nil, errf("%s: set needs fields, an object mapping a field name to an expression", path)
		}
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if !event.IsField(name) {
				return nil, errf("%s: set field %q is not an event field", path, name)
			}
			src, ok := fields[name].(string)
			if !ok {
				return nil, errf("%s: set field %q must be an expression string", path, name)
			}
			prog, err := exprs.Compile(src, inCoalesce)
			if err != nil {
				return nil, errf("%s: set field %q: %v", path, name, err)
			}
			if err := checkSetKind(name, prog.OutKind()); err != nil {
				return nil, errf("%s: set field %q: expression %q %v", path, name, src, err)
			}
			sp.setFields = append(sp.setFields, setField{name: name, prog: prog})
		}
	case "coalesce":
		childCoalesce = true
		if !cc.sawCoalesce {
			cc.sawCoalesce, cc.coalescePath = true, path
		}
	case "ddt":
	case "stable":
		var ok bool
		if sp.duration, ok = number(obj["duration_seconds"]); !ok || sp.duration < 0 {
			return nil, errf("%s: stable needs duration_seconds, a non-negative number", path)
		}
		if sp.field, err = reqString(obj, "field", path); err != nil {
			return nil, err
		}
		if !event.ValidFieldRef(sp.field) {
			return nil, errf("%s: stable field %q is not an event field", path, sp.field)
		}
	default:
		return nil, errf("%s: unknown op %q", path, op)
	}
	cc.record(path, op)
	// SPEC-GAP: the spec gives every combinator but splitp a `children`
	// array and does not say whether omitting it is an error. Chosen: an
	// absent `children` is an empty one, a dead end that passes nothing on.
	if raw, present := obj["children"]; present && raw != nil {
		arr, ok := raw.([]any)
		if !ok {
			return nil, errf("%s: children must be an array", path)
		}
		for k, childRaw := range arr {
			child, err := cc.node(childRaw, fmt.Sprintf("%s/%d", path, k), childCoalesce, refStack)
			if err != nil {
				return nil, err
			}
			sp.children = append(sp.children, child)
		}
	}
	return sp, nil
}

func (cc *compiler) splitp(sp *spec, obj map[string]any, inCoalesce bool, refStack []string) (*spec, error) {
	path := sp.path
	test, err := reqString(obj, "test", path)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(test, "{}") {
		return nil, errf("%s: splitp test %q has no {} placeholder", path, test)
	}
	otherwiseRaw, present := obj["otherwise"]
	if !present || otherwiseRaw == nil {
		return nil, errf("%s: splitp needs otherwise", path)
	}
	arr, ok := obj["branches"].([]any)
	if !ok {
		return nil, errf("%s: splitp needs branches, an array", path)
	}
	cc.record(path, "splitp")
	for k, raw := range arr {
		bpath := fmt.Sprintf("%s/branches/%d", path, k)
		b, ok := raw.(map[string]any)
		if !ok {
			return nil, errf("%s: a branch must be an object with threshold and stream", bpath)
		}
		thr, present := b["threshold"]
		if !present || thr == nil {
			return nil, errf("%s: branch has no threshold", bpath)
		}
		streamRaw, present := b["stream"]
		if !present {
			return nil, errf("%s: branch has no stream", bpath)
		}
		lit, err := thresholdLiteral(thr)
		if err != nil {
			return nil, errf("%s: %v", bpath, err)
		}
		pred, err := exprs.CompileBool(strings.ReplaceAll(test, "{}", lit), inCoalesce)
		if err != nil {
			return nil, errf("%s: test with threshold %s: %v", bpath, lit, err)
		}
		child, err := cc.node(streamRaw, bpath, inCoalesce, refStack)
		if err != nil {
			return nil, err
		}
		sp.branches = append(sp.branches, branchSpec{pred: pred, child: child})
	}
	if sp.otherwise, err = cc.node(otherwiseRaw, path+"/otherwise", inCoalesce, refStack); err != nil {
		return nil, err
	}
	return sp, nil
}

// thresholdLiteral renders a branch threshold as expression source.
//
// SPEC-GAP: the spec says the threshold is "substituted" into `test` and
// shows only numbers. Chosen: a number substitutes as written, a string as a
// double-quoted literal, a boolean as true or false; anything else is a 400.
func thresholdLiteral(v any) (string, error) {
	switch t := v.(type) {
	case json.Number:
		return t.String(), nil
	case string:
		b, _ := json.Marshal(t)
		return string(b), nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	}
	return "", fmt.Errorf("threshold must be a number, a string or a boolean")
}

// checkSetKind refuses, at PUT, a `set` expression whose static type can
// never fit the field. A type the checker could not pin is checked at
// evaluation instead.
func checkSetKind(field string, k reflect.Kind) error {
	if k == reflect.Interface || k == reflect.Invalid {
		return nil
	}
	numeric := k == reflect.Float64 || k == reflect.Float32 ||
		(k >= reflect.Int && k <= reflect.Uint64)
	switch field {
	case "metric", "time", "ttl":
		if !numeric {
			return fmt.Errorf("does not produce a number")
		}
	case "tags":
		if k != reflect.Slice && k != reflect.Array {
			return fmt.Errorf("does not produce an array")
		}
	case "attributes":
		if k != reflect.Map {
			return fmt.Errorf("does not produce an object")
		}
	default:
		if k != reflect.String {
			return fmt.Errorf("does not produce a string")
		}
	}
	return nil
}

func reqString(obj map[string]any, key, where string) (string, error) {
	s, ok := obj[key].(string)
	if !ok || s == "" {
		return "", errf("%s: %s is required and must be a non-empty string", where, key)
	}
	return s, nil
}

func number(v any) (float64, bool) {
	switch t := v.(type) {
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case float64:
		return t, true
	}
	return 0, false
}
