// Probe 2: can expr-lang/expr express every predicate and transform in
// today's riemann rules (rules.json, riemann.config.j2, agent-obs.clj) and
// the forms the retired query grammar supports?
package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
)

// Event is the environment an expression sees. `now` (float seconds) and
// `expired` are engine-supplied, not event fields. `events` is only set
// for batch transforms (coalesce -> sum).
type Event struct {
	Host        string            `expr:"host"`
	Service     string            `expr:"service"`
	State       string            `expr:"state"`
	Description string            `expr:"description"`
	Metric      float64           `expr:"metric"`
	Time        float64           `expr:"time"`
	TTL         float64           `expr:"ttl"`
	Tags        []string          `expr:"tags"`
	Attributes  map[string]string `expr:"attributes"`

	Now     float64 `expr:"now"`
	Expired bool    `expr:"expired"`
	Events  []Event `expr:"events"`

	Tagged func(string) bool `expr:"tagged"`
}

func mk(e Event) Event {
	e.Tagged = func(name string) bool {
		for _, t := range e.Tags {
			if t == name {
				return true
			}
		}
		return false
	}
	return e
}

// ---------------------------------------------------------------- events

const now = 1_757_900_000.0

func events() []Event {
	f := func(h, s, st, d string, m, t float64, tags []string, attrs map[string]string) Event {
		return mk(Event{Host: h, Service: s, State: st, Description: d, Metric: m, Time: t, TTL: 60, Tags: tags, Attributes: attrs, Now: now})
	}
	es := []Event{
		f("ghost", "cpu", "ok", "", 50, now-5, []string{"synthetic"}, nil),                                            // 0
		f("ghost", "cpu", "ok", "", 80, now-5, []string{"synthetic"}, nil),                                            // 1
		f("ghost", "cpu", "ok", "", 95, now-5, []string{"synthetic"}, nil),                                            // 2
		f("mercy", "ntfy.listen.up", "ok", "", 1, now-5, nil, nil),                                                    // 3
		f("mercy", "ntfy.listen.connected", "warning", "reconnect", 0, now-5, nil, nil),                               // 4
		f("ghost", "agent.session", "ok", "", 12, now-30, []string{"agent-obs"}, map[string]string{"run_id": "r-42"}), // 5
		f("ghost", "agent.session", "expired", "", 12, now-700, nil, nil),                                             // 6 reaper-expired crash
		f("ghost", "agent.session", "expired", "completed", 12, now-700, nil, nil),                                    // 7 clean shutdown
		f("ghost", "agent.tokens.out", "ok", "", 1234, now-10, []string{"agent-obs"}, nil),                            // 8
		f("ghost", "agent.tokens.out", "ok", "", 0, now-10, []string{"agent-obs"}, nil),                               // 9
		f("ghost", "agent.cost", "ok", "", 7.5, now-10, []string{"agent-obs"}, nil),                                   // 10
		f("ghost", "agent.cost", "ok", "", 25, now-10, []string{"agent-obs"}, nil),                                    // 11
		f("ghost", "agent.subagents", "ok", "", 9, now-10, []string{"agent-obs"}, nil),                                // 12
		f("ghost", "agent.result", "ok", "done", 1, now-10, []string{"agent-obs"}, map[string]string{"paws": "4"}),    // 13
		f("mercy", "tick.heartbeat", "ok", "", 1, now-1, []string{"health-loop"}, nil),                                // 14
		f("api 1", "web", "ok", "", 1.2, now-1, nil, nil),                                                             // 15
		f("foo19", "web", "no", "", 0.5, now-1, nil, nil),                                                             // 16
		f("fooo42", "web", "ok", "hey", 1e11, now-1, nil, nil),                                                        // 17
		f("", "", "", "", 0, 0, nil, nil),                                                                             // 18 empty
		f("ghost", "agent.session", "ok", "", 12, now-300, []string{"agent-obs"}, nil),                                // 19 stale stable release
	}
	es[6].Expired = true
	es[7].Expired = true
	return es
}

// ------------------------------------------------------------- predicates

type pred struct {
	src    string
	from   string           // rules.json / config.j2 / query_test
	intend func(Event) bool // Clojure semantics, hand-written
}

func has(tags []string, n string) bool {
	for _, t := range tags {
		if t == n {
			return true
		}
	}
	return false
}

func preds() []pred {
	return []pred{
		{`true`, "rules.json raw", func(e Event) bool { return true }},
		{`tagged("synthetic")`, "rules.json", func(e Event) bool { return has(e.Tags, "synthetic") }},
		{`service == "ntfy.listen.up"`, "rules.json", func(e Event) bool { return e.Service == "ntfy.listen.up" }},
		{`service == "ntfy.listen.connected"`, "rules.json", func(e Event) bool { return e.Service == "ntfy.listen.connected" }},
		{`tagged("agent-obs")`, "rules.json", func(e Event) bool { return has(e.Tags, "agent-obs") }},
		{`expired and service == "agent.session" and description != "completed"`, "agent-obs.clj crash",
			func(e Event) bool { return e.Expired && e.Service == "agent.session" && e.Description != "completed" }},
		{`service == "agent.session"`, "rules.json", func(e Event) bool { return e.Service == "agent.session" }},
		{`service == "agent.tokens.out"`, "rules.json", func(e Event) bool { return e.Service == "agent.tokens.out" }},
		{`service == "agent.cost"`, "rules.json", func(e Event) bool { return e.Service == "agent.cost" }},
		{`service == "agent.subagents"`, "rules.json", func(e Event) bool { return e.Service == "agent.subagents" }},
		{`service == "agent.result"`, "rules.json", func(e Event) bool { return e.Service == "agent.result" }},
		{`service == "tick.heartbeat" and tagged("health-loop")`, "dashboard",
			func(e Event) bool { return e.Service == "tick.heartbeat" && has(e.Tags, "health-loop") }},
		// splitp tests (splitp < metric 90 ...) become explicit comparisons
		{`metric > 90`, "config.j2 splitp crit", func(e Event) bool { return e.Metric > 90 }},
		{`metric > 75`, "config.j2 splitp warn", func(e Event) bool { return e.Metric > 75 }},
		{`metric > 20.0`, "atlas cost-crit", func(e Event) bool { return e.Metric > 20 }},
		{`metric > 8`, "atlas fanout-warn", func(e Event) bool { return e.Metric > 8 }},
		// query_test.clj forms
		{`state == "foo"`, "query_test equal", func(e Event) bool { return e.State == "foo" }},
		{`state != "ok"`, "query_test not-equal", func(e Event) bool { return e.State != "ok" }},
		{`host matches "^foo?[1-9]+$"`, "query_test regexp ~=", func(e Event) bool {
			// foo?[1-9]+ with re-matches (anchored)
			if strings.HasPrefix(e.Host, "fo") {
				rest := strings.TrimPrefix(strings.TrimPrefix(e.Host, "fo"), "o")
				return rest != "" && strings.Trim(rest, "123456789") == ""
			}
			return false
		}},
		{`host matches "^api .*$"`, "query_test wildcard =~ \"api %\" (LIKE, translated)",
			func(e Event) bool { return strings.HasPrefix(e.Host, "api ") }},
		{`metric > 1e10`, "query_test inequality", func(e Event) bool { return e.Metric > 1e10 }},
		{`metric >= -1`, "query_test inequality", func(e Event) bool { return e.Metric >= -1 }},
		{`metric < 1.2e2`, "query_test inequality", func(e Event) bool { return e.Metric < 120 }},
		{`metric <= 1`, "query_test inequality", func(e Event) bool { return e.Metric <= 1 }},
		{`tagged("cat")`, "query_test tagged", func(e Event) bool { return has(e.Tags, "cat") }},
		{`description != ""`, "query_test null (description != nil; string field has no nil)",
			func(e Event) bool { return e.Description != "" }},
		{`not ("run_id" in attributes)`, "query_test null (missing key; typed map returns zero value, not nil, so == nil is wrong)",
			func(e Event) bool { _, ok := e.Attributes["run_id"]; return !ok }},
		{`not ((host == "ghost" or host == "mercy") and service == "cpu")`, "query_test bool",
			func(e Event) bool { return !((e.Host == "ghost" || e.Host == "mercy") && e.Service == "cpu") }},
		{`attributes.paws == "4" and tagged("agent-obs")`, "query_test custom-fields (attribute access)",
			func(e Event) bool { return e.Attributes["paws"] == "4" && has(e.Tags, "agent-obs") }},
		{`host matches "^api .*" and state == "ok" and metric > 0`, "query_test fast",
			func(e Event) bool { return strings.HasPrefix(e.Host, "api ") && e.State == "ok" && e.Metric > 0 }},
		{`not (host == "ghost") or host == "mercy" and host == "x"`, "query_test precedence (expr not binds tighter than ==; parens required)",
			func(e Event) bool { return e.Host != "ghost" || (e.Host == "mercy" && e.Host == "x") }},
	}
}

// ------------------------------------------------------------- transforms

// A `set` transform: one expression per field. Each compiles independently.
type setT struct {
	name   string
	guard  string            // optional; nil/false result drops the event (smap*)
	fields map[string]string // field -> expression
	batch  bool
	intend func(Event) (Event, bool)
}

func transforms() []setT {
	return []setT{
		{name: "assoc (agent.crashed)", fields: map[string]string{`service`: `"agent.crashed"`, `state`: `"critical"`},
			intend: func(e Event) (Event, bool) { e.Service = "agent.crashed"; e.State = "critical"; return e, true }},
		{name: "scale-assoc (agent.burn)", fields: map[string]string{`service`: `"agent.burn"`, `metric`: `metric * 60.0`},
			intend: func(e Event) (Event, bool) { e.Service = "agent.burn"; e.Metric = e.Metric * 60; return e, true }},
		{name: "fresh-assoc (agent.stalled)", guard: `now - time < 120`,
			fields: map[string]string{`service`: `"agent.stalled"`, `state`: `"warning"`},
			intend: func(e Event) (Event, bool) {
				if now-e.Time < 120 {
					e.Service = "agent.stalled"
					e.State = "warning"
					return e, true
				}
				return e, false
			}},
		{name: "sum over batch (fleet.burn)", batch: true,
			fields: map[string]string{`metric`: `sum(map(events, .metric))`, `host`: `"fleet"`, `service`: `"fleet.burn"`},
			intend: func(e Event) (Event, bool) {
				var s float64
				for _, x := range e.Events {
					s += x.Metric
				}
				e.Metric = s
				e.Host = "fleet"
				e.Service = "fleet.burn"
				return e, true
			}},
	}
}

func applySet(t setT, progs map[string]*vm.Program, guard *vm.Program, e Event) (Event, bool, error) {
	if guard != nil {
		v, err := expr.Run(guard, e)
		if err != nil {
			return e, false, err
		}
		if b, _ := v.(bool); !b {
			return e, false, nil
		}
	}
	out := e
	for f, p := range progs {
		v, err := expr.Run(p, e)
		if err != nil {
			return e, false, err
		}
		switch f {
		case "service":
			out.Service = v.(string)
		case "state":
			out.State = v.(string)
		case "host":
			out.Host = v.(string)
		case "metric":
			out.Metric = v.(float64)
		}
	}
	return out, true, nil
}

// ------------------------------------------------------------- AST walk

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
		case *ast.PointerNode: // `.metric` inside map(events, ...)
			r.fields["[]."+s.Value] = true
		}
	}
}

func readSet(p *vm.Program) []string {
	r := &reads{fields: map[string]bool{}}
	n := p.Node()
	ast.Walk(&n, r)
	var out []string
	for k := range r.fields {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ------------------------------------------------------------------- main

func main() {
	es := events()
	env := mk(Event{})
	fmt.Printf("go %s, expr v1.17.8, %d events\n\n", "1.26.3", len(es))

	// predicates
	fmt.Println("== predicates (T=true, .=false per event 0..19; ok = matches Clojure intent on all 20)")
	fmt.Printf("%-72s %-22s %s\n", "expr", "results", "ok")
	fails := 0
	for _, p := range preds() {
		prog, err := expr.Compile(p.src, expr.Env(env), expr.AsBool())
		if err != nil {
			fmt.Printf("%-72s COMPILE ERROR: %v\n", p.src, err)
			fails++
			continue
		}
		var sb strings.Builder
		ok := true
		for _, e := range es {
			v, err := expr.Run(prog, e)
			if err != nil {
				fmt.Printf("%-72s RUN ERROR: %v\n", p.src, err)
				ok = false
				break
			}
			got := v.(bool)
			if got {
				sb.WriteByte('T')
			} else {
				sb.WriteByte('.')
			}
			if got != p.intend(e) {
				ok = false
			}
		}
		if !ok {
			fails++
		}
		fmt.Printf("%-72s %-22s %v   [%s]\n", p.src, sb.String(), ok, p.from)
	}

	// transforms
	fmt.Println("\n== transforms (set maps)")
	for _, t := range transforms() {
		progs := map[string]*vm.Program{}
		for f, src := range t.fields {
			p, err := expr.Compile(src, expr.Env(env))
			if err != nil {
				fmt.Printf("  %s: field %s COMPILE ERROR: %v\n", t.name, f, err)
				fails++
				continue
			}
			progs[f] = p
		}
		var guard *vm.Program
		if t.guard != "" {
			var err error
			guard, err = expr.Compile(t.guard, expr.Env(env), expr.AsBool())
			if err != nil {
				fmt.Printf("  %s: guard COMPILE ERROR: %v\n", t.name, err)
				fails++
				continue
			}
		}
		inputs := es
		if t.batch {
			b := mk(Event{Host: "ghost", Service: "agent.burn", Time: now, Now: now, Events: []Event{es[8], es[9], es[10]}})
			inputs = []Event{b}
		}
		ok := true
		var shown []string
		for i, e := range inputs {
			got, kept, err := applySet(t, progs, guard, e)
			if err != nil {
				fmt.Printf("  %s: RUN ERROR on event %d: %v\n", t.name, i, err)
				ok = false
				break
			}
			want, wkept := t.intend(e)
			if kept != wkept || (kept && (got.Host != want.Host || got.Service != want.Service || got.State != want.State || got.Metric != want.Metric)) {
				ok = false
			}
			if i == 5 || i == 8 || i == 19 || t.batch {
				if kept {
					shown = append(shown, fmt.Sprintf("ev%d->{%s %s %s %g}", i, got.Host, got.Service, got.State, got.Metric))
				} else {
					shown = append(shown, fmt.Sprintf("ev%d->dropped", i))
				}
			}
		}
		if !ok {
			fails++
		}
		fmt.Printf("  %-32s ok=%v  %s\n", t.name, ok, strings.Join(shown, "  "))
	}

	// timing
	fmt.Println("\n== timing (compile: 200 iterations each; eval: 200k iterations over the 20 events)")
	fmt.Printf("%-72s %12s %12s\n", "expr", "compile", "eval/op")
	for _, src := range []string{
		`tagged("synthetic")`,
		`service == "ntfy.listen.up"`,
		`expired and service == "agent.session" and description != "completed"`,
		`host matches "^api .*" and state == "ok" and metric > 0`,
		`metric * 60.0`,
		`sum(map(events, .metric))`,
	} {
		t0 := time.Now()
		var prog *vm.Program
		for i := 0; i < 200; i++ {
			prog, _ = expr.Compile(src, expr.Env(env))
		}
		ct := time.Since(t0) / 200
		batch := mk(Event{Events: []Event{es[8], es[9], es[10]}, Now: now})
		t0 = time.Now()
		const n = 200_000
		for i := 0; i < n; i++ {
			e := es[i%len(es)]
			if strings.Contains(src, "events") {
				e = batch
			}
			if _, err := expr.Run(prog, e); err != nil {
				panic(err)
			}
		}
		et := time.Since(t0) / n
		fmt.Printf("%-72s %12v %12v\n", src, ct, et)
	}

	// AST walk
	fmt.Println("\n== static read-set via ast.Walk")
	for _, src := range []string{
		`expired and service == "agent.session" and description != "completed"`,
		`attributes.paws == "4" and tagged("agent-obs") and now - time < 120`,
		`sum(map(events, .metric))`,
	} {
		p, err := expr.Compile(src, expr.Env(env))
		if err != nil {
			panic(err)
		}
		fmt.Printf("  %-72s reads %v\n", src, readSet(p))
	}

	// unexpected-behaviour checks
	fmt.Println("\n== edge checks")
	for _, src := range []string{
		`attributes.run_id == nil`, // typed map: missing key is "" not nil
		`not host == "ghost"`,      // grammar precedence differs
		`description != nil`,       // grammar allows; string field
		`time == nil`,              // grammar allows; float field
		`paws == 4`,                // grammar allows any NAME; struct env
		`host =~ "%s."`,            // grammar LIKE operator, not expr syntax
		`metric ?? 0`,              // nil-coalesce on non-nil float
		`service = "x"`,            // single = (grammar) vs == (expr)
	} {
		p, err := expr.Compile(src, expr.Env(env))
		if err != nil {
			fmt.Printf("  %-28s compile error: %s\n", src, strings.SplitN(err.Error(), "\n", 2)[0])
			continue
		}
		v, err := expr.Run(p, es[17])
		fmt.Printf("  %-28s -> %v (err=%v)\n", src, v, err)
	}

	if fails > 0 {
		fmt.Printf("\nFAILURES: %d\n", fails)
		os.Exit(1)
	}
	fmt.Println("\nall predicates and transforms match intended semantics")
}
