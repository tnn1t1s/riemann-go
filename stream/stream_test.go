package stream

import (
	"testing"

	"github.com/tnn1t1s/riemann-go/event"
)

func ev(host, state string) event.Event { return event.Event{Host: host, Service: "s", State: state} }

func TestWhereByChangedStateSet(t *testing.T) {
	var c Collector
	root := Where(func(e event.Event) bool { return e.Host != "skip" },
		By(func(e event.Event) string { return e.Host }, func() Stream {
			return ChangedState("ok", Set(func(e event.Event) event.Event { e.Service = "changed"; return e }, c.Stream()))
		}))
	for _, e := range []event.Event{ev("a", "ok"), ev("a", "warning"), ev("a", "warning"), ev("b", "critical"), ev("skip", "critical"), ev("a", "ok")} {
		root(e)
	}
	want := []string{"a/warning", "b/critical", "a/ok"}
	if len(c.Events) != len(want) {
		t.Fatalf("got %d events want %d", len(c.Events), len(want))
	}
	for i, e := range c.Events {
		if got := e.Host + "/" + e.State; got != want[i] || e.Service != "changed" {
			t.Errorf("event %d: %+v", i, e)
		}
	}
}
