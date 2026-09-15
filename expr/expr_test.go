package expr

import (
	"strings"
	"testing"

	"github.com/tnn1t1s/riemann-go/event"
)

func f(v float64) *float64 { return &v }

func TestPredicates(t *testing.T) {
	const now = 1_757_900_000.0
	e := event.Event{Host: "ghost", Service: "agent.cost", State: "ok", Metric: f(7.5),
		Time: now - 10, TTL: 60, Tags: []string{"agent-obs"}, Attributes: map[string]string{"run_id": "r-42"}}
	exp := event.Event{Host: "ghost", Service: "agent.session", State: "expired", Time: now - 700}
	cases := []struct {
		src  string
		e    event.Event
		want bool
	}{
		{`tagged("agent-obs") && service == "agent.cost"`, e, true},
		{`tagged("nope")`, e, false},
		{`metric > 5 and state == "ok"`, e, true},
		{`not (host == "ghost")`, e, false},
		{`service matches "^agent\\."`, e, true},
		{`"run_id" in attributes`, e, true},
		{`"missing" in attributes`, e, false},
		{`attributes.missing == ""`, e, true},
		{`now - time < 120`, e, true},
		{`expired`, e, false},
		{`expired and service == "agent.session"`, exp, true},
		{`"k" in attributes`, exp, false}, // nil attributes read as an empty map
		{`metric == 0`, exp, true},        // absent metric reads as 0
	}
	for _, tc := range cases {
		p, err := CompilePredicate(tc.src)
		if err != nil {
			t.Fatalf("%s: %v", tc.src, err)
		}
		got, err := p.Eval(tc.e, now)
		if err != nil || got != tc.want {
			t.Errorf("%s: got %v err %v want %v", tc.src, got, err, tc.want)
		}
	}
}

func TestUnknownIdentifierIsCompileError(t *testing.T) {
	_, err := CompilePredicate(`paws == 4`)
	if err == nil || !strings.Contains(err.Error(), "paws") {
		t.Fatalf("want compile error naming paws, got %v", err)
	}
	if _, err := CompilePredicate(`metric * 2`); err == nil {
		t.Fatal("non-boolean predicate compiled")
	}
}

func TestReadSet(t *testing.T) {
	p, err := CompilePredicate(`attributes.paws == "4" and tagged("agent-obs") and now - time < 120`)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(p.Reads(), ",")
	if got != "attributes.paws,now,tagged,time" {
		t.Errorf("read set %s", got)
	}
	v, err := CompileValue(`sum(map(events, .metric))`)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(v.Reads(), ","); got != "[].metric,events" {
		t.Errorf("read set %s", got)
	}
}

func TestValue(t *testing.T) {
	v, err := CompileValue(`metric * 60.0`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := v.Eval(NewEnv(event.Event{Metric: f(2)}, 0))
	if err != nil || out.(float64) != 120 {
		t.Errorf("got %v %v", out, err)
	}
}
