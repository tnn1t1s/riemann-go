// Package expression compiles and evaluates expr-lang expressions against an
// event. The name domain is closed: the ten event fields, plus `tagged`,
// `now` and `expired`, plus `events` inside a coalesce subtree. It is the one
// expression language; rules and the read surface's q= both use it.
package expression

import (
	"errors"
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/tnn1t1s/riemann-go/internal/event"
)

// Program is one compiled expression.
type Program struct {
	Source string
	prog   *vm.Program
}

// typeEnv declares the names and their types for the compiler. Values are
// samples; only their types matter.
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

// Compile compiles source. withEvents admits the `events` name, which exists
// only inside a coalesce subtree. wantBool requires a boolean result at
// compile time, so a projection written where a predicate belongs is refused
// rather than coerced.
func Compile(source string, withEvents, wantBool bool) (*Program, error) {
	if strings.TrimSpace(source) == "" {
		return nil, errors.New("expression is empty")
	}
	opts := []expr.Option{
		expr.Env(typeEnv(withEvents)),
		// expr-lang ships a builtin now() returning a time.Time. The spec
		// defines `now` as the engine's time in float seconds and says the
		// engine supplies nothing else, so the builtin is removed.
		expr.DisableBuiltin("now"),
	}
	if wantBool {
		opts = append(opts, expr.AsBool())
	}
	prog, err := expr.Compile(source, opts...)
	if err != nil {
		return nil, fmt.Errorf("expression %q: %s", source, firstLine(err.Error()))
	}
	return &Program{Source: source, prog: prog}, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// Eval runs the program against an environment built by Env.
func (p *Program) Eval(env map[string]any) (any, error) {
	out, err := expr.Run(p.prog, env)
	if err != nil {
		return nil, fmt.Errorf("expression %q: %s", p.Source, firstLine(err.Error()))
	}
	return out, nil
}

// Bool runs a predicate.
func (p *Program) Bool(env map[string]any) (bool, error) {
	out, err := p.Eval(env)
	if err != nil {
		return false, err
	}
	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("expression %q: result is %T, not a boolean", p.Source, out)
	}
	return b, nil
}

// Env builds the evaluation environment for one event. events is nil outside
// a coalesce subtree.
//
// SPEC-GAP: SEMANTICS.md open question 1, what `metric` reads as on an event
// that carries none. Chosen: nil. `metric == nil` is therefore the presence
// test, and arithmetic or ordering against an absent metric is an evaluation
// error, which a predicate treats as not holding.
func Env(e *event.Event, now float64, events []map[string]any) map[string]any {
	env := Fields(e)
	tags := e.Tags
	env["tagged"] = func(name string) bool {
		for _, t := range tags {
			if t == name {
				return true
			}
		}
		return false
	}
	env["now"] = now
	env["expired"] = e.Expired
	if events != nil {
		env["events"] = events
	}
	return env
}

// Fields returns the ten event fields as a map, which is both the base of an
// evaluation environment and one element of `events`.
func Fields(e *event.Event) map[string]any {
	m := make(map[string]any, 14)
	m["host"] = e.Host
	m["service"] = e.Service
	m["state"] = e.State
	if e.HasMetric {
		m["metric"] = e.Metric
	} else {
		m["metric"] = nil
	}
	m["time"] = e.Time
	m["ttl"] = e.TTL
	if e.Tags != nil {
		m["tags"] = e.Tags
	} else {
		m["tags"] = []string{}
	}
	if e.Attributes != nil {
		m["attributes"] = e.Attributes
	} else {
		m["attributes"] = map[string]string{}
	}
	m["description"] = e.Description
	m["source"] = e.Source
	return m
}
