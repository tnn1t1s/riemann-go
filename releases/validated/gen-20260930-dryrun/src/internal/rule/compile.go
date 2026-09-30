// Package rule parses rule documents, compiles them into path-addressed
// templates, and instantiates the combinator tree as stateful nodes.
package rule

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/exprs"
)

// Options carries the parameters the compiler needs from SCALE.md.
type Options struct {
	StableBufferCapacity int // events per fork key, drop-oldest
}

// Compiled is a rule document that compiled. It is shared by every loop's
// instance of the rule; the per-node counters are therefore process-wide.
type Compiled struct {
	ID        string
	Owner     string
	Partition string
	Match     *exprs.Program
	Enabled   bool
	ExpiresAt float64
	HasExpiry bool
	Version   int64
	Hash      string
	// Doc is the normalized document, without version. The engine adds
	// version when it stores the rule.
	Doc map[string]any

	Root        *Template
	HasCoalesce bool

	// paths lists every node path in the tree, in a stable order.
	paths    []string
	passed   map[string]*atomic.Int64 // node path → events passed downstream
	discards map[string]*atomic.Int64 // node path → events discarded

	ForksLive  atomic.Int64
	ForksFreed atomic.Int64

	opts Options
}

// Counters reports events passed downstream per node path.
func (c *Compiled) Counters() map[string]int64 {
	out := make(map[string]int64, len(c.paths))
	for _, p := range c.paths {
		out[p] = c.passed[p].Load()
	}
	return out
}

// Discards reports events discarded per node path, in path order.
func (c *Compiled) Discards() (paths []string, counts []int64) {
	for _, p := range c.paths {
		n := c.discards[p].Load()
		paths = append(paths, p)
		counts = append(counts, n)
	}
	return
}

// Template is one node of a compiled tree. It holds no per-event state.
type Template struct {
	Path string
	Op   string // where, by, changed-state, throttle, splitp, set, coalesce, ddt, stable, sink
	Sink string

	Expr        *exprs.Program            // where
	Fields      []string                  // by
	Initial     string                    // changed-state
	Limit       int                       // throttle
	Window      float64                   // throttle, seconds
	Branches    []*Template               // splitp; each is the branch's stream node
	BranchTests []*exprs.Program          // splitp; test with threshold substituted
	Otherwise   *Template                 // splitp
	SetFields   map[string]*exprs.Program // set
	Duration    float64                   // stable, seconds
	Field       string                    // stable

	Children []*Template

	passed    *atomic.Int64
	discarded *atomic.Int64
	freeable  bool // by: fields are a subset of host and service
}

type compiler struct {
	c        *Compiled
	bindings map[string]json.RawMessage
	compiled map[string]*Template
	inFlight map[string]bool
	seen     map[string]bool
}

// ParseError is a rule that does not compile. Its message names the node
// path or the expression that failed.
type ParseError struct{ Msg string }

func (e *ParseError) Error() string { return e.Msg }

func perr(format string, args ...any) error {
	return &ParseError{Msg: fmt.Sprintf(format, args...)}
}

// Parse decodes and compiles a rule document body. pathID is the id from the
// request path, or empty when the document must carry its own.
func Parse(body []byte, pathID string, opts Options) (*Compiled, error) {
	var doc map[string]any
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, perr("rule body is not a JSON object: %v", err)
	}
	if doc == nil {
		return nil, perr("rule body is not a JSON object")
	}
	return compileDoc(doc, pathID, opts)
}

func compileDoc(doc map[string]any, pathID string, opts Options) (*Compiled, error) {
	c := &Compiled{passed: map[string]*atomic.Int64{}, discards: map[string]*atomic.Int64{}, opts: opts}

	id, _ := doc["id"].(string)
	if id == "" {
		if pathID == "" {
			return nil, perr("rule has no id")
		}
		id = pathID
	}
	if pathID != "" && id != pathID {
		return nil, perr("rule id %q does not match path id %q", id, pathID)
	}
	c.ID = id

	owner, ok := doc["owner"].(string)
	if !ok || owner == "" {
		return nil, perr("rule %s: owner is required", id)
	}
	c.Owner = owner

	c.Partition = "host"
	if p, present := doc["partition"]; present {
		s, ok := p.(string)
		if !ok {
			return nil, perr("rule %s: partition must be a string", id)
		}
		switch s {
		case "host", "host,service", "global":
			c.Partition = s
		default:
			return nil, perr("rule %s: partition %q is not one of host, host,service, global", id, s)
		}
	}

	match, ok := doc["match"].(string)
	if !ok || match == "" {
		return nil, perr("rule %s: match is required", id)
	}
	prog, err := exprs.CompilePredicate(match, false)
	if err != nil {
		return nil, perr("rule %s: match: %v", id, err)
	}
	c.Match = prog

	c.Enabled = true
	if e, present := doc["enabled"]; present {
		b, ok := e.(bool)
		if !ok {
			return nil, perr("rule %s: enabled must be a boolean", id)
		}
		c.Enabled = b
	}
	if x, present := doc["expires_at"]; present && x != nil {
		f, err := toFloat(x)
		if err != nil {
			return nil, perr("rule %s: expires_at must be a number", id)
		}
		c.ExpiresAt = f
		c.HasExpiry = true
	}

	stream, present := doc["stream"]
	if !present || stream == nil {
		return nil, perr("rule %s: stream is required", id)
	}

	cc := &compiler{c: c, bindings: map[string]json.RawMessage{}, compiled: map[string]*Template{}, inFlight: map[string]bool{}, seen: map[string]bool{}}
	if b, present := doc["bindings"]; present && b != nil {
		bm, ok := b.(map[string]any)
		if !ok {
			return nil, perr("rule %s: bindings must be an object", id)
		}
		for name, sub := range bm {
			raw, _ := json.Marshal(sub)
			cc.bindings[name] = raw
		}
	}
	rawStream, _ := json.Marshal(stream)
	root, err := cc.node(rawStream, "stream", false)
	if err != nil {
		return nil, err
	}
	c.Root = root
	// Bindings that nothing references still compile, so a broken one is a
	// 400 rather than a latent failure.
	names := make([]string, 0, len(cc.bindings))
	for n := range cc.bindings {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if _, err := cc.binding(n, false); err != nil {
			return nil, err
		}
	}
	sort.Strings(c.paths)

	if c.HasCoalesce && c.Partition != "global" {
		// SPEC.md refuses `host`; a `host,service` partition cannot answer a
		// fleet-wide fold either. SPEC-GAP: the spec names only `host`; both
		// non-global partitions are refused here for the same reason.
		return nil, perr("rule %s: a rule with partition %q cannot contain coalesce; use partition \"global\"", id, c.Partition)
	}

	// Normalized document: defaults filled, version removed. Its canonical
	// JSON form (sorted keys, compact) is what is hashed.
	norm := make(map[string]any, len(doc))
	for k, v := range doc {
		norm[k] = v
	}
	norm["id"] = id
	norm["partition"] = c.Partition
	norm["enabled"] = c.Enabled
	delete(norm, "version")
	canon, err := json.Marshal(norm)
	if err != nil {
		return nil, perr("rule %s: cannot canonicalize: %v", id, err)
	}
	sum := sha256.Sum256(canon)
	c.Hash = hex.EncodeToString(sum[:])
	c.Doc = norm
	return c, nil
}

func toFloat(v any) (float64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Float64()
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	}
	return 0, fmt.Errorf("not a number")
}

func (cc *compiler) newTemplate(path, op string) *Template {
	t := &Template{Path: path, Op: op, passed: &atomic.Int64{}, discarded: &atomic.Int64{}}
	if !cc.seen[path] {
		cc.seen[path] = true
		cc.c.paths = append(cc.c.paths, path)
	}
	cc.c.passed[path] = t.passed
	cc.c.discards[path] = t.discarded
	return t
}

func (cc *compiler) binding(name string, inCoalesce bool) (*Template, error) {
	if t, ok := cc.compiled[name]; ok {
		return t, nil
	}
	raw, ok := cc.bindings[name]
	if !ok {
		return nil, perr("ref %q names no binding", name)
	}
	if cc.inFlight[name] {
		return nil, perr("bindings/%s: binding references itself", name)
	}
	cc.inFlight[name] = true
	// SPEC-GAP: a binding referenced from inside and outside a coalesce
	// subtree would need `events` in one place and not the other. It is
	// compiled once, with the visibility of its first reference.
	t, err := cc.node(raw, "bindings/"+name, inCoalesce)
	cc.inFlight[name] = false
	if err != nil {
		return nil, err
	}
	cc.compiled[name] = t
	return t, nil
}

func (cc *compiler) node(raw json.RawMessage, path string, inCoalesce bool) (*Template, error) {
	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil || m == nil {
		return nil, perr("%s: node is not a JSON object", path)
	}

	if ref, ok := m["ref"]; ok {
		name, ok := ref.(string)
		if !ok || name == "" {
			return nil, perr("%s: ref must be a binding name", path)
		}
		return cc.binding(name, inCoalesce)
	}

	if s, ok := m["sink"]; ok {
		name, _ := s.(string)
		switch name {
		case "ntfy", "influx", "index":
		default:
			return nil, perr("%s: sink %q is not one of ntfy, influx, index", path, name)
		}
		t := cc.newTemplate(path, "sink")
		t.Sink = name
		return t, nil
	}

	op, _ := m["op"].(string)
	if op == "" {
		return nil, perr("%s: node has no op, sink or ref", path)
	}

	children := func(t *Template) error {
		raw, present := m["children"]
		if !present {
			return perr("%s: %s requires a children array", path, op)
		}
		arr, ok := raw.([]any)
		if !ok {
			return perr("%s: children must be an array", path)
		}
		for k, child := range arr {
			cr, _ := json.Marshal(child)
			ct, err := cc.node(cr, path+"/"+strconv.Itoa(k), inCoalesce || op == "coalesce")
			if err != nil {
				return err
			}
			t.Children = append(t.Children, ct)
		}
		return nil
	}

	switch op {
	case "where":
		src, ok := m["expr"].(string)
		if !ok || src == "" {
			return nil, perr("%s: where requires expr", path)
		}
		prog, err := exprs.CompilePredicate(src, inCoalesce)
		if err != nil {
			return nil, perr("%s: %v", path, err)
		}
		t := cc.newTemplate(path, op)
		t.Expr = prog
		return t, children(t)

	case "by":
		arr, ok := m["fields"].([]any)
		if !ok || len(arr) == 0 {
			return nil, perr("%s: by requires a non-empty fields array", path)
		}
		t := cc.newTemplate(path, op)
		t.freeable = true
		for _, f := range arr {
			name, ok := f.(string)
			if !ok || !event.ValidFieldName(name) {
				return nil, perr("%s: by field %v is not an event field", path, f)
			}
			if name != "host" && name != "service" {
				t.freeable = false
			}
			t.Fields = append(t.Fields, name)
		}
		return t, children(t)

	case "changed-state":
		t := cc.newTemplate(path, op)
		if init, present := m["initial"]; present {
			s, ok := init.(string)
			if !ok {
				return nil, perr("%s: changed-state initial must be a string", path)
			}
			t.Initial = s
		}
		// SPEC-GAP: initial is listed as the node's key without saying whether
		// it is required. An absent initial reads as "", the model's default
		// state.
		return t, children(t)

	case "throttle":
		lim, err := toFloat(m["limit"])
		if err != nil || lim < 1 || lim != float64(int(lim)) {
			return nil, perr("%s: throttle limit must be a positive integer", path)
		}
		win, err := toFloat(m["window_seconds"])
		if err != nil || win <= 0 {
			return nil, perr("%s: throttle window_seconds must be a positive number", path)
		}
		t := cc.newTemplate(path, op)
		t.Limit = int(lim)
		t.Window = win
		return t, children(t)

	case "splitp":
		test, ok := m["test"].(string)
		if !ok || !strings.Contains(test, "{}") {
			return nil, perr("%s: splitp test must be an expression containing {}", path)
		}
		branches, ok := m["branches"].([]any)
		if !ok || len(branches) == 0 {
			return nil, perr("%s: splitp requires a non-empty branches array", path)
		}
		otherwise, present := m["otherwise"]
		if !present || otherwise == nil {
			return nil, perr("%s: splitp requires otherwise", path)
		}
		if _, has := m["children"]; has {
			return nil, perr("%s: splitp takes branches and otherwise, not children", path)
		}
		t := cc.newTemplate(path, op)
		for k, b := range branches {
			bpath := fmt.Sprintf("%s/branches/%d", path, k)
			bm, ok := b.(map[string]any)
			if !ok || len(bm) != 2 {
				return nil, perr("%s: branch must be an object with exactly threshold and stream", bpath)
			}
			thr, hasThr := bm["threshold"]
			sub, hasStream := bm["stream"]
			if !hasThr || !hasStream {
				return nil, perr("%s: branch must carry threshold and stream", bpath)
			}
			lit, err := thresholdLiteral(thr)
			if err != nil {
				return nil, perr("%s: %v", bpath, err)
			}
			prog, err := exprs.CompilePredicate(strings.ReplaceAll(test, "{}", lit), inCoalesce)
			if err != nil {
				return nil, perr("%s: %v", bpath, err)
			}
			sr, _ := json.Marshal(sub)
			st, err := cc.node(sr, bpath, inCoalesce)
			if err != nil {
				return nil, err
			}
			t.BranchTests = append(t.BranchTests, prog)
			t.Branches = append(t.Branches, st)
		}
		or, _ := json.Marshal(otherwise)
		ot, err := cc.node(or, path+"/otherwise", inCoalesce)
		if err != nil {
			return nil, err
		}
		t.Otherwise = ot
		return t, nil

	case "set":
		fm, ok := m["fields"].(map[string]any)
		if !ok || len(fm) == 0 {
			return nil, perr("%s: set requires a non-empty fields object", path)
		}
		t := cc.newTemplate(path, op)
		t.SetFields = map[string]*exprs.Program{}
		for name, v := range fm {
			if !event.ValidFieldName(name) {
				return nil, perr("%s: set field %q is not an event field", path, name)
			}
			src, ok := v.(string)
			if !ok {
				return nil, perr("%s: set field %q must be an expression string", path, name)
			}
			prog, err := exprs.CompileValue(src, inCoalesce)
			if err != nil {
				return nil, perr("%s: field %s: %v", path, name, err)
			}
			t.SetFields[name] = prog
		}
		return t, children(t)

	case "coalesce":
		cc.c.HasCoalesce = true
		t := cc.newTemplate(path, op)
		return t, children(t)

	case "ddt":
		t := cc.newTemplate(path, op)
		return t, children(t)

	case "stable":
		dur, err := toFloat(m["duration_seconds"])
		if err != nil || dur <= 0 {
			return nil, perr("%s: stable duration_seconds must be a positive number", path)
		}
		field, ok := m["field"].(string)
		if !ok || !event.ValidFieldName(field) {
			return nil, perr("%s: stable field must name an event field", path)
		}
		t := cc.newTemplate(path, op)
		t.Duration = dur
		t.Field = field
		return t, children(t)
	}
	return nil, perr("%s: unknown op %q", path, op)
}

// thresholdLiteral renders a branch threshold as an expr-lang literal.
func thresholdLiteral(v any) (string, error) {
	switch x := v.(type) {
	case json.Number:
		return x.String(), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case string:
		return strconv.Quote(x), nil
	case bool:
		return strconv.FormatBool(x), nil
	}
	return "", fmt.Errorf("threshold must be a number, string or boolean")
}
