package http

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tnn1t1s/riemann-go/engine"
	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/shard"
	"github.com/tnn1t1s/riemann-go/stream"
)

func newServer(t *testing.T) (*httptest.Server, *engine.Engine) {
	t.Helper()
	eng := engine.New(engine.Config{Shards: 2, Shard: shard.Config{InboxCapacity: 64, RingEvents: 1000}}, stream.StdClock{})
	eng.Start()
	srv := httptest.NewServer(New(eng).Handler()) // httptest binds 127.0.0.1
	t.Cleanup(func() { srv.Close(); eng.Stop() })
	return srv, eng
}

func post(t *testing.T, url, body string) (int, map[string]any, http.Header) {
	t.Helper()
	resp, err := http.Post(url+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, resp.Header
}

// withQ appends q=expr, escaped.
func withQ(base, expr string) string { return base + "?q=" + url.QueryEscape(expr) }

func get(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestEndToEnd(t *testing.T) {
	srv, _ := newServer(t)

	code, body, _ := post(t, srv.URL, `[{"host":"ghost","service":"cpu","state":"ok","metric":0.5,"tags":["health-loop"]},
		{"host":"mercy","service":"cpu","state":"critical","metric":0.9}]`)
	if code != http.StatusAccepted || body["accepted"].(float64) != 2 {
		t.Fatalf("post: %d %v", code, body)
	}
	if _, ok := body["sinks"].(map[string]any); !ok {
		t.Fatalf("202 body lacks sinks: %v", body)
	}
	code, body, _ = post(t, srv.URL, `{"host":"ghost","service":"mem"}`)
	if code != http.StatusAccepted || body["accepted"].(float64) != 1 {
		t.Fatalf("single object: %d %v", code, body)
	}

	code, body, _ = post(t, srv.URL, `{"service":"nohost"}`)
	if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "missing host") {
		t.Fatalf("missing host: %d %v", code, body)
	}
	code, _, _ = post(t, srv.URL, "["+strings.Repeat(`{"host":"h","service":"s"},`, MaxBatchEvents)+`{"host":"h","service":"s"}]`)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over max batch: %d", code)
	}
	code, _, _ = post(t, srv.URL, `[{"host":"h","service":"s","description":"`+strings.Repeat("x", MaxBatchBytes)+`"}]`)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over max bytes: %d", code)
	}

	// Index query: the loops are asynchronous, so poll briefly.
	var idx map[string]any
	for i := 0; i < 100; i++ {
		_, idx = get(t, withQ(srv.URL+"/index", `tagged("health-loop")`))
		if len(idx["events"].([]any)) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	evs := idx["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["host"] != "ghost" {
		t.Fatalf("index query: %v", idx)
	}
	asOf := idx["as_of"].(map[string]any)
	if asOf["min"].(float64) == 0 || asOf["max"].(float64) < asOf["min"].(float64) {
		t.Fatalf("as_of: %v", asOf)
	}
	code, body = get(t, srv.URL+"/index?q=paws+==+4")
	if code != http.StatusBadRequest || !strings.Contains(body["error"].(string), "paws") {
		t.Fatalf("unknown identifier: %d %v", code, body)
	}
	_, idx = get(t, withQ(srv.URL+"/index", `metric > 0.7`))
	if evs := idx["events"].([]any); len(evs) != 1 || evs[0].(map[string]any)["host"] != "mercy" {
		t.Fatalf("metric query: %v", idx)
	}
	_, idx = get(t, srv.URL+"/index")
	if len(idx["events"].([]any)) != 3 {
		t.Fatalf("all: %v", idx)
	}

	code, body = get(t, srv.URL+"/index/mercy/cpu")
	if code != http.StatusOK || body["event"].(map[string]any)["state"] != "critical" || body["last_transition"] == nil {
		t.Fatalf("lookup: %d %v", code, body)
	}
	if code, _ = get(t, srv.URL+"/index/nobody/cpu"); code != http.StatusNotFound {
		t.Fatalf("lookup missing: %d", code)
	}

	_, body = get(t, withQ(srv.URL+"/events", `service == "cpu"`)+"&limit=1")
	if evs := body["events"].([]any); len(evs) != 1 {
		t.Fatalf("events limit: %v", body)
	}
	_, body = get(t, srv.URL+"/events?since=0")
	if evs := body["events"].([]any); len(evs) != 3 {
		t.Fatalf("events: %v", body)
	}
	if code, _ = get(t, srv.URL+"/events?limit=x"); code != http.StatusBadRequest {
		t.Fatalf("bad limit: %d", code)
	}

	_, body = get(t, srv.URL+"/metrics")
	if body["ingest"].(map[string]any)["accepted"].(float64) != 3 || len(body["shards"].([]any)) != 2 {
		t.Fatalf("metrics: %v", body)
	}
}

// sse reads frames from an event-stream body.
type sseFrame struct {
	kind string
	data string
}

func readSSE(r io.Reader, out chan<- sseFrame) {
	sc := bufio.NewScanner(r)
	var f sseFrame
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			f.kind = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			f.data = strings.TrimPrefix(line, "data: ")
		case line == "":
			out <- f
			f = sseFrame{}
		}
	}
	close(out)
}

func TestSubscribe(t *testing.T) {
	srv, _ := newServer(t)
	post(t, srv.URL, `{"host":"ghost","service":"cpu","state":"ok"}`)
	for i := 0; i < 100; i++ {
		if _, idx := get(t, srv.URL+"/index"); len(idx["events"].([]any)) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	resp, err := http.Get(withQ(srv.URL+"/subscribe", `service == "cpu"`) + "&snapshot=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	frames := make(chan sseFrame, 16)
	go readSSE(resp.Body, frames)
	next := func() sseFrame {
		select {
		case f := <-frames:
			return f
		case <-time.After(2 * time.Second):
			t.Fatal("no frame")
			return sseFrame{}
		}
	}
	if f := next(); f.kind != "snapshot" || !strings.Contains(f.data, `"host":"ghost"`) {
		t.Fatalf("snapshot frame %+v", f)
	}
	if f := next(); f.kind != "snapshot_end" || f.data != `{"count":1}` {
		t.Fatalf("snapshot_end frame %+v", f)
	}
	post(t, srv.URL, `[{"host":"mercy","service":"cpu","state":"warning"},{"host":"mercy","service":"mem"}]`)
	f := next()
	if f.kind != "event" {
		t.Fatalf("live frame %+v", f)
	}
	var e event.Event
	if err := json.Unmarshal([]byte(f.data), &e); err != nil || e.Host != "mercy" || e.State != "warning" {
		t.Fatalf("live event %+v %v", e, err)
	}
	select {
	case f := <-frames:
		t.Fatalf("unexpected frame for non-matching event: %+v", f)
	case <-time.After(50 * time.Millisecond):
	}
	if code, _ := get(t, withQ(srv.URL+"/subscribe", "nope")); code != http.StatusBadRequest {
		t.Fatalf("bad q: %d", code)
	}
}

func TestAdmissionDeadline429(t *testing.T) {
	eng := engine.New(engine.Config{Shards: 1, Shard: shard.Config{InboxCapacity: 4, RingEvents: 10}}, stream.StdClock{})
	hold := make(chan struct{})
	eng.SetRules(engine.RuleSet{Host: func(s *shard.Shard) stream.Stream {
		return func(event.Event) { <-hold }
	}})
	eng.Start()
	srv := httptest.NewServer(New(eng).Handler())
	t.Cleanup(func() { srv.Close(); close(hold); eng.Stop() })

	body := "[" + strings.Repeat(`{"host":"h","service":"s"},`, 9) + `{"host":"h","service":"s"}]`
	code, out, hdr := post(t, srv.URL, body)
	if code != http.StatusTooManyRequests || hdr.Get("Retry-After") != "1" {
		t.Fatalf("code %d retry-after %q", code, hdr.Get("Retry-After"))
	}
	// One event in flight, four in the inbox, five refused.
	if out["accepted"].(float64) != 5 || out["rejected"].(float64) != 5 {
		t.Fatalf("429 body %v", out)
	}
}
