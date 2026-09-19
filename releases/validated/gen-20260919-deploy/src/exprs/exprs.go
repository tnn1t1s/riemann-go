// Package exprs compiles and evaluates expr-lang expressions against the
// closed name domain SPEC.md fixes: the ten event fields plus tagged, now,
// expired and, inside a coalesce subtree, events.
package exprs

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/tnn1t1s/riemann-go/event"
)

// Program is one compiled expression.
type Program struct {
	Source string
	prog   *vm.Program
}

// typeEnv is the compile-time environment. Its values carry types only.
//
// SPEC-GAP: the spec does not fix how an expression reads `metric` on an event
// that carries none (SEMANTICS open question 1). Chosen: `metric` is typed as
// a float for compilation and reads as nil at evaluation when absent. So
// `metric == nil` tests absence, and arithmetic or ordering on an absent
// metric is an evaluation error, which a predicate treats as "does not hold".
func typeEnv(withEvents bool) map[string]any {
	env := map[string]any{
		"host":        "",
		"service":     "",
		"state":       "",
		"metric":      float64(0),
		"time":        float64(0),
		"ttl":         float64(0),
		"tags":        []string{},
		"attributes":  map[string]string{},
		"description": "",
		"source":      "",
		"tagged":      func(string) bool { return false },
		"now":         float64(0),
		"expired":     false,
	}
	if withEvents {
		env["events"] = []map[string]any{}
	}
	return env
}

// CompileBool compiles a predicate. An expression whose type is not boolean is
// a compile error, not a truthiness coercion.
func CompileBool(src string, withEvents bool) (*Program, error) {
	return compile(src, withEvents, expr.AsBool())
}

// Compile compiles a value expression, as a `set` field uses.
func Compile(src string, withEvents bool) (*Program, error) {
	return compile(src, withEvents)
}

func compile(src string, withEvents bool, opts ...expr.Option) (*Program, error) {
	if strings.TrimSpace(src) == "" {
		return nil, fmt.Errorf("expression is empty")
	}
	all := append([]expr.Option{expr.Env(typeEnv(withEvents))}, opts...)
	p, err := expr.Compile(src, all...)
	if err != nil {
		return nil, fmt.Errorf("expression %q: %s", src, firstLine(err.Error()))
	}
	return &Program{Source: src, prog: p}, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// OutKind is the statically known kind of the expression's value, or
// reflect.Interface when the checker could not pin it.
func (p *Program) OutKind() reflect.Kind {
	t := p.prog.Node().Type()
	if t == nil {
		return reflect.Interface
	}
	return t.Kind()
}

// Env builds the evaluation environment for one event. events is nil outside
// a coalesce subtree.
func Env(ev *event.Event, now float64, events []map[string]any) map[string]any {
	env := EventMap(ev)
	tags := ev.Tags
	env["tagged"] = func(name string) bool {
		for _, t := range tags {
			if t == name {
				return true
			}
		}
		return false
	}
	env["now"] = now
	env["expired"] = ev.Expired
	if events != nil {
		env["events"] = events
	}
	return env
}

// EventMap renders the ten event fields as expression values. It is also the
// element type of `events`.
//
// SPEC-GAP: the spec does not say how an element of `events` reads a missing
// metric. Chosen: nil, the same as the top-level name, so
// `sum(map(events, .metric))` errors when any held event has no metric and
// `filter(events, .metric != nil)` is the way to skip those.
func EventMap(ev *event.Event) map[string]any {
	var metric any
	if ev.HasMetric {
		metric = ev.Metric
	}
	tags := ev.Tags
	if tags == nil {
		tags = []string{}
	}
	attrs := ev.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}
	return map[string]any{
		"host":        ev.Host,
		"service":     ev.Service,
		"state":       ev.State,
		"metric":      metric,
		"time":        ev.Time,
		"ttl":         ev.TTL,
		"tags":        tags,
		"attributes":  attrs,
		"description": ev.Description,
		"source":      ev.Source,
	}
}

// Bool evaluates a predicate.
func (p *Program) Bool(env map[string]any) (bool, error) {
	v, err := expr.Run(p.prog, env)
	if err != nil {
		return false, fmt.Errorf("expression %q: %s", p.Source, firstLine(err.Error()))
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expression %q: value is %T, not a boolean", p.Source, v)
	}
	return b, nil
}

// Eval evaluates a value expression.
func (p *Program) Eval(env map[string]any) (any, error) {
	v, err := expr.Run(p.prog, env)
	if err != nil {
		return nil, fmt.Errorf("expression %q: %s", p.Source, firstLine(err.Error()))
	}
	return v, nil
}

// ToFloat converts an expression value to a float.
func ToFloat(v any) (float64, bool) {
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
