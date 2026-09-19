package rule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/exprs"
)

// Sink leaf names.
const (
	SinkNtfy   = "ntfy"
	SinkInflux = "influx"
	SinkIndex  = "index"
)

type kind int

const (
	kindSink kind = iota
	kindWhere
	kindBy
	kindChangedState
	kindThrottle
	kindSplitp
	kindSet
	kindCoalesce
	kindDdt
	kindStable
)

// Counter is the per-node-path accounting of one rule version. Passed is what
// GET /rules/{id} reports; Discarded is what riemann.rule.discarded reports.
type Counter struct {
	Passed    atomic.Int64
	Discarded atomic.Int64
}

// node is one compiled combinator or sink leaf. It is immutable; all mutable
// state lives in the instance's state tree.
type node struct {
	kind     kind
	path     string
	ctr      *Counter
	children []*node // for splitp: the branches in order, then otherwise

	expr      *exprs.Program   // where
	fields    []string         // by
	freeable  bool             // by: every field is host or service
	initial   string           // changed-state
	limit     int64            // throttle
	window    float64          // throttle, seconds of event time
	tests     []*exprs.Program // splitp, one per branch
	setFields []setField       // set, sorted by field name
	duration  float64          // stable, seconds of event time
	field     string           // stable
	sink      string           // sink leaf
}

type setField struct {
	name string
	expr *exprs.Program
}

// Rule is one compiled version of a rule document.
type Rule struct {
	Doc     *Doc
	Version int

	match    *exprs.Program
	root     *node
	paths    []string
	counters map[string]*Counter
}

// Active reports whether the rule fires at engine time now.
func (r *Rule) Active(now float64) bool {
	if !r.Doc.Enabled {
		return false
	}
	return r.Doc.ExpiresAt == nil || now < *r.Doc.ExpiresAt
}

// Global reports whether the rule holds one instance for the whole process.
func (r *Rule) Global() bool { return r.Doc.Partition == PartitionGlobal }

// Counters maps each node path in the tree to the events that node passed
// downstream since this version was installed.
func (r *Rule) Counters() map[string]int64 {
	out := make(map[string]int64, len(r.paths))
	for _, p := range r.paths {
		out[p] = r.counters[p].Passed.Load()
	}
	return out
}

// Discards maps each node path that has discarded anything to its count.
func (r *Rule) Discards() map[string]int64 {
	out := map[string]int64{}
	for _, p := range r.paths {
		if n := r.counters[p].Discarded.Load(); n > 0 {
			out[p] = n
		}
	}
	return out
}

type compiler struct {
	doc      *Doc
	rule     *Rule
	refStack []string
	coalesce bool // the tree contains a coalesce
}

// Compile compiles a document. An error names the node path or the expression
// that failed, and is a 400 at PUT.
// SPEC-GAP: a binding no ref reaches is not compiled and has no counters; the
// spec defines paths only for nodes in the rule's tree.
// SPEC-GAP: partition "host,service" runs per partition exactly as "host"
// does, since events route by host alone; only "host" refuses coalesce.
func Compile(doc *Doc, version int) (*Rule, error) {
	r := &Rule{Doc: doc, Version: version, counters: map[string]*Counter{}}
	m, err := exprs.Compile(doc.Match, exprs.Options{AsBool: true})
	if err != nil {
		return nil, fmt.Errorf("match: %v", err)
	}
	r.match = m
	c := &compiler{doc: doc, rule: r}
	if r.root, err = c.node(doc.Stream, "stream", false); err != nil {
		return nil, err
	}
	if c.coalesce && doc.Partition == PartitionHost {
		return nil, fmt.Errorf("stream: partition %q cannot hold a coalesce, which is a fleet-wide fold; declare %q", PartitionHost, PartitionGlobal)
	}
	return r, nil
}

func (c *compiler) counter(path string) *Counter {
	if ctr, ok := c.rule.counters[path]; ok {
		return ctr // a binding spliced twice shares its binding-rooted paths
	}
	ctr := &Counter{}
	c.rule.counters[path] = ctr
	c.rule.paths = append(c.rule.paths, path)
	return ctr
}

func (c *compiler) node(raw json.RawMessage, path string, inCoalesce bool) (*node, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%s: node is not a JSON object", path)
	}
	if r, ok := m["ref"]; ok {
		var name string
		if err := json.Unmarshal(r, &name); err != nil {
			return nil, fmt.Errorf("%s: ref must be a string", path)
		}
		sub, ok := c.doc.Bindings[name]
		if !ok {
			return nil, fmt.Errorf("%s: ref names unknown binding %q", path, name)
		}
		for _, seen := range c.refStack {
			if seen == name {
				return nil, fmt.Errorf("%s: binding %q refers to itself", path, name)
			}
		}
		c.refStack = append(c.refStack, name)
		n, err := c.node(sub, "bindings/"+name, inCoalesce)
		c.refStack = c.refStack[:len(c.refStack)-1]
		return n, err
	}
	if r, ok := m["sink"]; ok {
		var name string
		if err := json.Unmarshal(r, &name); err != nil {
			return nil, fmt.Errorf("%s: sink must be a string", path)
		}
		switch name {
		case SinkNtfy, SinkInflux, SinkIndex:
		default:
			return nil, fmt.Errorf("%s: unknown sink %q", path, name)
		}
		return &node{kind: kindSink, path: path, ctr: c.counter(path), sink: name}, nil
	}
	var op string
	if r, ok := m["op"]; !ok {
		return nil, fmt.Errorf("%s: node has no op, sink or ref", path)
	} else if err := json.Unmarshal(r, &op); err != nil {
		return nil, fmt.Errorf("%s: op must be a string", path)
	}

	n := &node{path: path}
	var err error
	switch op {
	case "where":
		n.kind = kindWhere
		src, err := nodeString(m, "expr", path)
		if err != nil {
			return nil, err
		}
		if n.expr, err = exprs.Compile(src, exprs.Options{AsBool: true, AllowEvents: inCoalesce}); err != nil {
			return nil, fmt.Errorf("%s: %v", path, err)
		}
	case "by":
		n.kind = kindBy
		if err := nodeValue(m, "fields", path, &n.fields); err != nil {
			return nil, err
		}
		if len(n.fields) == 0 {
			return nil, fmt.Errorf("%s: fields must name at least one event field", path)
		}
		n.freeable = true
		for _, f := range n.fields {
			if !validField(f) {
				return nil, fmt.Errorf("%s: %q is not an event field", path, f)
			}
			if f != "host" && f != "service" {
				n.freeable = false
			}
		}
	case "changed-state":
		n.kind = kindChangedState
		// SPEC-GAP: an absent initial reads as "", the default of an absent state.
		if _, ok := m["initial"]; ok {
			if err := nodeValue(m, "initial", path, &n.initial); err != nil {
				return nil, err
			}
		}
	case "throttle":
		n.kind = kindThrottle
		var limit float64
		if err := nodeValue(m, "limit", path, &limit); err != nil {
			return nil, err
		}
		if limit < 0 || limit != float64(int64(limit)) {
			return nil, fmt.Errorf("%s: limit must be a non-negative integer", path)
		}
		n.limit = int64(limit)
		if err := nodeValue(m, "window_seconds", path, &n.window); err != nil {
			return nil, err
		}
		if n.window <= 0 {
			return nil, fmt.Errorf("%s: window_seconds must be greater than zero", path)
		}
	case "splitp":
		n.kind = kindSplitp
		if err := c.splitp(n, m, path, inCoalesce); err != nil {
			return nil, err
		}
	case "set":
		n.kind = kindSet
		var fields map[string]string
		if err := nodeValue(m, "fields", path, &fields); err != nil {
			return nil, err
		}
		for name, src := range fields {
			if !settableField(name) {
				return nil, fmt.Errorf("%s: %q is not an event field", path, name)
			}
			p, err := exprs.Compile(src, exprs.Options{AllowEvents: inCoalesce})
			if err != nil {
				return nil, fmt.Errorf("%s: field %q: %v", path, name, err)
			}
			n.setFields = append(n.setFields, setField{name: name, expr: p})
		}
		sort.Slice(n.setFields, func(i, j int) bool { return n.setFields[i].name < n.setFields[j].name })
	case "coalesce":
		n.kind = kindCoalesce
		c.coalesce = true
		inCoalesce = true
	case "ddt":
		n.kind = kindDdt
	case "stable":
		n.kind = kindStable
		if err := nodeValue(m, "duration_seconds", path, &n.duration); err != nil {
			return nil, err
		}
		if n.duration < 0 {
			return nil, fmt.Errorf("%s: duration_seconds must not be negative", path)
		}
		if n.field, err = nodeString(m, "field", path); err != nil {
			return nil, err
		}
		if !validField(n.field) {
			return nil, fmt.Errorf("%s: %q is not an event field", path, n.field)
		}
	default:
		return nil, fmt.Errorf("%s: unknown op %q", path, op)
	}
	n.ctr = c.counter(path)

	if n.kind != kindSplitp {
		// SPEC-GAP: an absent children array is an empty one; a node with no
		// children compiles and delivers nowhere.
		if r, ok := m["children"]; ok && !bytes.Equal(bytes.TrimSpace(r), []byte("null")) {
			var kids []json.RawMessage
			if err := json.Unmarshal(r, &kids); err != nil {
				return nil, fmt.Errorf("%s: children must be an array", path)
			}
			for k, kid := range kids {
				child, err := c.node(kid, path+"/"+strconv.Itoa(k), inCoalesce)
				if err != nil {
					return nil, err
				}
				n.children = append(n.children, child)
			}
		}
	}
	return n, nil
}

func (c *compiler) splitp(n *node, m map[string]json.RawMessage, path string, inCoalesce bool) error {
	test, err := nodeString(m, "test", path)
	if err != nil {
		return err
	}
	if !strings.Contains(test, "{}") {
		return fmt.Errorf("%s: test %q has no {} placeholder", path, test)
	}
	var branches []map[string]json.RawMessage
	if r, ok := m["branches"]; ok {
		if err := json.Unmarshal(r, &branches); err != nil {
			return fmt.Errorf("%s: branches must be an array of objects", path)
		}
	}
	for k, b := range branches {
		bpath := path + "/branches/" + strconv.Itoa(k)
		th, ok := b["threshold"]
		if !ok {
			return fmt.Errorf("%s: branch has no threshold", bpath)
		}
		lit, err := thresholdLiteral(th)
		if err != nil {
			return fmt.Errorf("%s: %v", bpath, err)
		}
		p, err := exprs.Compile(strings.ReplaceAll(test, "{}", lit), exprs.Options{AsBool: true, AllowEvents: inCoalesce})
		if err != nil {
			return fmt.Errorf("%s: %v", bpath, err)
		}
		sub, ok := b["stream"]
		if !ok {
			return fmt.Errorf("%s: branch has no stream", bpath)
		}
		// The stream key contributes no path segment.
		child, err := c.node(sub, bpath, inCoalesce)
		if err != nil {
			return err
		}
		n.tests = append(n.tests, p)
		n.children = append(n.children, child)
	}
	other, ok := m["otherwise"]
	if !ok || bytes.Equal(bytes.TrimSpace(other), []byte("null")) {
		return fmt.Errorf("%s: splitp requires otherwise", path)
	}
	child, err := c.node(other, path+"/otherwise", inCoalesce)
	if err != nil {
		return err
	}
	n.children = append(n.children, child)
	return nil
}

// thresholdLiteral renders a branch threshold as expression source.
func thresholdLiteral(raw json.RawMessage) (string, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return "", fmt.Errorf("threshold is not valid JSON")
	}
	switch t := v.(type) {
	case json.Number:
		return t.String(), nil
	case string:
		return strconv.Quote(t), nil
	case bool:
		return strconv.FormatBool(t), nil
	}
	return "", fmt.Errorf("threshold must be a number, a string or a boolean")
}

func nodeString(m map[string]json.RawMessage, key, path string) (string, error) {
	var s string
	if err := nodeValue(m, key, path, &s); err != nil {
		return "", err
	}
	return s, nil
}

func nodeValue(m map[string]json.RawMessage, key, path string, into any) error {
	r, ok := m[key]
	if !ok {
		return fmt.Errorf("%s: %s is required", path, key)
	}
	if err := json.Unmarshal(r, into); err != nil {
		return fmt.Errorf("%s: %s has the wrong type", path, key)
	}
	return nil
}

const attrPrefix = "attributes."

func settableField(f string) bool {
	switch f {
	case "host", "service", "state", "metric", "time", "ttl", "tags", "attributes", "description", "source":
		return true
	}
	return false
}

// validField reports whether by and stable can read f.
// SPEC-GAP: besides the ten event fields, "attributes.<key>" reads one
// attribute, the form SEMANTICS.md uses when it discusses by.
func validField(f string) bool {
	return settableField(f) || (strings.HasPrefix(f, attrPrefix) && len(f) > len(attrPrefix))
}

// fieldValue reads a field as the string a fork key or a stable comparison
// uses. An absent field reads as its documented default.
// SPEC-GAP: an absent metric reads as "" here (SEMANTICS.md leaves by
// ["metric"] ill-defined).
func fieldValue(ev *event.Event, f string) string {
	switch f {
	case "host":
		return ev.Host
	case "service":
		return ev.Service
	case "state":
		return ev.State
	case "description":
		return ev.Description
	case "source":
		return ev.Source
	case "metric":
		if !ev.HasMetric {
			return ""
		}
		return strconv.FormatFloat(ev.Metric, 'g', -1, 64)
	case "time":
		return strconv.FormatFloat(ev.Time, 'g', -1, 64)
	case "ttl":
		return strconv.FormatFloat(ev.TTL, 'g', -1, 64)
	case "tags":
		return strings.Join(ev.Tags, "\x1f")
	case "attributes":
		keys := make([]string, 0, len(ev.Attributes))
		for k := range ev.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(k)
			b.WriteByte('\x1f')
			b.WriteString(ev.Attributes[k])
			b.WriteByte('\x1e')
		}
		return b.String()
	}
	return ev.Attributes[strings.TrimPrefix(f, attrPrefix)]
}
