// Package exprs wraps expr-lang for the rule language: a closed name set,
// boolean-typed predicates rejected at compile time when they are not
// boolean, and evaluation that reads an absent metric as nil.
package exprs

import (
	"fmt"
	"reflect"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// Program is a compiled expression.
type Program struct {
	Source string
	prog   *vm.Program
}

// typeEnv fixes the closed world of names and their static types. metric is
// typed float64 so comparisons and arithmetic type-check, while at evaluation
// it may be nil; see Bool and Value for what that does.
func typeEnv(withEvents bool) map[string]any {
	env := map[string]any{
		"host":        "",
		"service":     "",
		"state":       "",
		"metric":      0.0,
		"time":        0.0,
		"ttl":         0.0,
		"tags":        []string{},
		"attributes":  map[string]string{},
		"description": "",
		"source":      "",
		"tagged":      func(string) bool { return false },
		"now":         0.0,
		"expired":     false,
	}
	if withEvents {
		env["events"] = []map[string]any{}
	}
	return env
}

// CompilePredicate compiles an expression that must be of boolean type. A
// projection such as `metric`, or an expression whose static type expr cannot
// fix, is an error rather than a truthiness coercion.
func CompilePredicate(src string, withEvents bool) (*Program, error) {
	prog, err := expr.Compile(src, expr.Env(typeEnv(withEvents)), expr.AsBool())
	if err != nil {
		return nil, fmt.Errorf("expression %q: %v", src, err)
	}
	node := prog.Node()
	if node == nil || node.Type() == nil || node.Type().Kind() != reflect.Bool {
		return nil, fmt.Errorf("expression %q is not of boolean type", src)
	}
	return &Program{Source: src, prog: prog}, nil
}

// CompileValue compiles an expression of any type, for set fields.
func CompileValue(src string, withEvents bool) (*Program, error) {
	prog, err := expr.Compile(src, expr.Env(typeEnv(withEvents)))
	if err != nil {
		return nil, fmt.Errorf("expression %q: %v", src, err)
	}
	return &Program{Source: src, prog: prog}, nil
}

// Bool evaluates a predicate. An evaluation error, which is what comparing an
// absent metric against a number produces, reads as false: SPEC.md says
// `metric > 5` on a metric-less event is false rather than an error.
func (p *Program) Bool(env map[string]any) bool {
	out, err := expr.Run(p.prog, env)
	if err != nil {
		return false
	}
	b, ok := out.(bool)
	return ok && b
}

// Value evaluates an expression. An evaluation error reads as nil, so
// arithmetic on an absent metric yields nil, per SPEC.md's event model.
func (p *Program) Value(env map[string]any) any {
	out, err := expr.Run(p.prog, env)
	if err != nil {
		return nil
	}
	return out
}
