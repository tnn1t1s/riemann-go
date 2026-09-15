// Package expr compiles predicates and value expressions over events with
// expr-lang/expr. The environment is closed: an unknown identifier is a
// compile error, so a typo in a query is a 400 rather than a silent false.
package expr

import (
	"fmt"
	"sort"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"

	"github.com/tnn1t1s/riemann-go/event"
)

// Env is what an expression sees. Event fields carry their wire names;
// now, expired, events and tagged are engine-supplied.
type Env struct {
	Host        string            `expr:"host"`
	Service     string            `expr:"service"`
	State       string            `expr:"state"`
	Description string            `expr:"description"`
	Metric      float64           `expr:"metric"` // 0 when the event has no metric
	Time        float64           `expr:"time"`
	TTL         float64           `expr:"ttl"`
	Tags        []string          `expr:"tags"`
	Attributes  map[string]string `expr:"attributes"`
	Source      string            `expr:"source"`

	Now     float64 `expr:"now"`     // engine clock, float seconds
	Expired bool    `expr:"expired"` // state == "expired"
	Events  []Env   `expr:"events"`  // only set for batch transforms

	Tagged func(string) bool `expr:"tagged"`
}

// NewEnv builds the environment for one event at engine time now.
func NewEnv(e event.Event, now float64) Env {
	env := Env{
		Host:        e.Host,
		Service:     e.Service,
		State:       e.State,
		Description: e.Description,
		Time:        e.Time,
		TTL:         e.TTL,
		Tags:        e.Tags,
		Attributes:  e.Attributes,
		Source:      e.Source,
		Now:         now,
		Expired:     e.Expired(),
		Tagged:      e.Tagged,
	}
	if e.Metric != nil {
		env.Metric = *e.Metric
	}
	if env.Attributes == nil {
		env.Attributes = emptyAttrs // missing key reads as ""
	}
	return env
}

var emptyAttrs = map[string]string{}

// Predicate is a compiled boolean expression.
type Predicate struct {
	src  string
	prog *vm.Program
}

// CompilePredicate compiles src as a boolean expression.
func CompilePredicate(src string) (*Predicate, error) {
	prog, err := expr.Compile(src, expr.Env(Env{}), expr.AsBool())
	if err != nil {
		return nil, err
	}
	return &Predicate{src: src, prog: prog}, nil
}

// Source returns the expression text.
func (p *Predicate) Source() string { return p.src }

// Eval runs the predicate against e at time now. A runtime error reads as
// false and is returned so the caller can count it.
func (p *Predicate) Eval(e event.Event, now float64) (bool, error) {
	v, err := expr.Run(p.prog, NewEnv(e, now))
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("predicate %q returned %T", p.src, v)
	}
	return b, nil
}

// Reads returns the fields the predicate reads, from its AST.
func (p *Predicate) Reads() []string { return ReadSet(p.prog) }

// Value is a compiled expression of any result type, used by set transforms.
type Value struct {
	src  string
	prog *vm.Program
}

// CompileValue compiles src without constraining its result type.
func CompileValue(src string) (*Value, error) {
	prog, err := expr.Compile(src, expr.Env(Env{}))
	if err != nil {
		return nil, err
	}
	return &Value{src: src, prog: prog}, nil
}

func (v *Value) Source() string { return v.src }

// Eval runs the expression against env.
func (v *Value) Eval(env Env) (any, error) { return expr.Run(v.prog, env) }

func (v *Value) Reads() []string { return ReadSet(v.prog) }

// reads walks an AST and records identifiers and member accesses. A member
// access on an identifier records "base.prop" and drops the bare base; a
// pointer member (`.metric` inside map(events, ...)) records "[].prop".
type reads struct{ fields map[string]bool }

func (r *reads) Visit(n *ast.Node) {
	switch x := (*n).(type) {
	case *ast.IdentifierNode:
		r.fields[x.Value] = true
	case *ast.MemberNode:
		s, ok := x.Property.(*ast.StringNode)
		if !ok {
			return
		}
		switch base := x.Node.(type) {
		case *ast.IdentifierNode:
			r.fields[base.Value+"."+s.Value] = true
			delete(r.fields, base.Value)
		case *ast.PointerNode:
			r.fields["[]."+s.Value] = true
		}
	}
}

// ReadSet returns the sorted set of names a compiled program reads.
func ReadSet(p *vm.Program) []string {
	r := &reads{fields: map[string]bool{}}
	n := p.Node()
	ast.Walk(&n, r)
	out := make([]string, 0, len(r.fields))
	for k := range r.fields {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
