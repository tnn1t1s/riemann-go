// Package exprs compiles and evaluates expr-lang expressions over the closed
// name domain SPEC.md fixes: the ten event fields plus tagged, now, expired
// and, below a coalesce, events.
package exprs

import (
	"errors"
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"

	"github.com/tnn1t1s/riemann-go/event"
)

// View is one element of the events array.
// SPEC-GAP: an element with no metric reads .metric as 0 inside events; the
// spec fixes no reading for an absent metric in the array.
type View struct {
	Host        string            `expr:"host"`
	Service     string            `expr:"service"`
	State       string            `expr:"state"`
	Metric      float64           `expr:"metric"`
	Time        float64           `expr:"time"`
	TTL         float64           `expr:"ttl"`
	Tags        []string          `expr:"tags"`
	Attributes  map[string]string `expr:"attributes"`
	Description string            `expr:"description"`
	Source      string            `expr:"source"`
}

// Set is the events array of one coalesce emission, built once and shared by
// every expression evaluated below that emission.
type Set struct {
	views []View
}

// NewSet builds the events array from a coalesce's held set.
func NewSet(evs []*event.Event) *Set {
	s := &Set{views: make([]View, len(evs))}
	for i, e := range evs {
		s.views[i] = view(e)
	}
	return s
}

func view(e *event.Event) View {
	return View{Host: e.Host, Service: e.Service, State: e.State, Metric: e.Metric, Time: e.Time, TTL: e.TTL,
		Tags: e.Tags, Attributes: e.Attributes, Description: e.Description, Source: e.Source}
}

// env is the whole name domain. The world is closed: a name not declared here
// is a compile error.
type env struct {
	Host        string            `expr:"host"`
	Service     string            `expr:"service"`
	State       string            `expr:"state"`
	Metric      float64           `expr:"metric"`
	Time        float64           `expr:"time"`
	TTL         float64           `expr:"ttl"`
	Tags        []string          `expr:"tags"`
	Attributes  map[string]string `expr:"attributes"`
	Description string            `expr:"description"`
	Source      string            `expr:"source"`

	Tagged  func(string) bool `expr:"tagged"`
	Now     float64           `expr:"now"`
	Expired bool              `expr:"expired"`
	Events  []View            `expr:"events"`
}

// Program is one compiled expression.
type Program struct {
	src        string
	prog       *vm.Program
	refsMetric bool
}

// Options selects the compile mode.
type Options struct {
	AsBool      bool // the expression must be boolean; anything else fails to compile
	AllowEvents bool // true only inside a coalesce subtree
}

type identVisitor struct {
	metric, events bool
}

func (v *identVisitor) Visit(node *ast.Node) {
	if id, ok := (*node).(*ast.IdentifierNode); ok {
		switch id.Value {
		case "metric":
			v.metric = true
		case "events":
			v.events = true
		}
	}
}

// Compile compiles src once. Evaluation is per event.
func Compile(src string, o Options) (*Program, error) {
	v := &identVisitor{}
	opts := []expr.Option{expr.Env(env{}), expr.Patch(v)}
	if o.AsBool {
		opts = append(opts, expr.AsBool())
	}
	prog, err := expr.Compile(src, opts...)
	if err != nil {
		return nil, fmt.Errorf("expression %q: %v", src, err)
	}
	if v.events && !o.AllowEvents {
		return nil, fmt.Errorf("expression %q: events is defined only inside a coalesce subtree", src)
	}
	return &Program{src: src, prog: prog, refsMetric: v.metric}, nil
}

// Source returns the expression text.
func (p *Program) Source() string { return p.src }

// ErrNoMetric reports an expression that references metric evaluated against
// an event that carries none.
var ErrNoMetric = errors.New("expression references metric and the event carries none")

// Eval evaluates the expression against ev. set is nil outside a coalesce.
func (p *Program) Eval(ev *event.Event, now float64, set *Set) (any, error) {
	if p.refsMetric && !ev.HasMetric {
		return nil, ErrNoMetric
	}
	e := env{Host: ev.Host, Service: ev.Service, State: ev.State, Metric: ev.Metric, Time: ev.Time, TTL: ev.TTL,
		Tags: ev.Tags, Attributes: ev.Attributes, Description: ev.Description, Source: ev.Source,
		Now: now, Expired: ev.Expired}
	if e.Tags == nil {
		e.Tags = []string{}
	}
	if e.Attributes == nil {
		e.Attributes = map[string]string{}
	}
	tags := ev.Tags
	e.Tagged = func(name string) bool {
		for _, t := range tags {
			if t == name {
				return true
			}
		}
		return false
	}
	if set != nil {
		e.Events = set.views
	}
	return expr.Run(p.prog, e)
}

// Bool evaluates a predicate.
// SPEC-GAP: a predicate that references metric does not hold for an event with
// no metric (SEMANTICS.md open question 1). It is a non-match, not an error.
func (p *Program) Bool(ev *event.Event, now float64, set *Set) (bool, error) {
	v, err := p.Eval(ev, now, set)
	if err == ErrNoMetric {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expression %q: result is not boolean", p.src)
	}
	return b, nil
}
