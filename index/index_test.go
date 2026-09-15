package index

import (
	"testing"

	"github.com/tnn1t1s/riemann-go/event"
)

func ev(host, service, state string, t, ttl float64) event.Event {
	return event.Event{Host: host, Service: service, State: state, Time: t, TTL: ttl}
}

func TestInsertLookupExpire(t *testing.T) {
	ix := New(DefaultMaxEntries)
	ix.Insert(ev("a", "cpu", "ok", 100, 10))
	ix.Insert(ev("a", "cpu", "warning", 105, 10)) // re-insert: first item is stale
	ix.Insert(ev("b", "cpu", "ok", 100, 5))

	en, ok := ix.Lookup("a", "cpu")
	if !ok || en.Event.State != "warning" || en.LastTransition != 105 {
		t.Fatalf("lookup %+v %v", en, ok)
	}
	if ix.Len() != 2 || ix.HeapLen() != 3 {
		t.Fatalf("len %d heap %d", ix.Len(), ix.HeapLen())
	}

	if got := ix.Expire(104); len(got) != 0 {
		t.Fatalf("expired early: %v", got)
	}
	got := ix.Expire(110) // b at 105 due; a's stale item at 110 is skipped, live one at 115 is not
	if len(got) != 1 || got[0].Host != "b" || got[0].State != event.StateExpired || got[0].Time != 110 {
		t.Fatalf("expire at 110: %+v", got)
	}
	if ix.Len() != 1 {
		t.Fatalf("len after expire %d", ix.Len())
	}
	got = ix.Expire(115)
	if len(got) != 1 || got[0].Host != "a" || got[0].Service != "cpu" || got[0].Metric != nil || got[0].Tags != nil {
		t.Fatalf("expire at 115: %+v", got)
	}
	if ix.Len() != 0 || ix.HeapLen() != 0 || ix.Expired() != 2 {
		t.Fatalf("final len %d heap %d expired %d", ix.Len(), ix.HeapLen(), ix.Expired())
	}
}

func TestExpiredEventDeletes(t *testing.T) {
	ix := New(DefaultMaxEntries)
	ix.Insert(ev("a", "cpu", "ok", 100, 10))
	ix.Insert(ev("a", "cpu", "expired", 101, 10))
	if _, ok := ix.Lookup("a", "cpu"); ok {
		t.Fatal("expired insert did not delete")
	}
	if got := ix.Expire(200); len(got) != 0 {
		t.Fatalf("deleted key expired again: %v", got)
	}
}

func TestCardinalityCap(t *testing.T) {
	ix := New(2)
	ix.Insert(ev("a", "s", "", 0, 1))
	ix.Insert(ev("b", "s", "", 0, 1))
	if ix.Insert(ev("c", "s", "", 0, 1)) {
		t.Fatal("third key admitted over cap")
	}
	if !ix.Insert(ev("a", "s", "x", 1, 1)) {
		t.Fatal("update of existing key refused")
	}
	if ix.Rejected() != 1 || ix.Len() != 2 {
		t.Fatalf("rejected %d len %d", ix.Rejected(), ix.Len())
	}
}
