// Package httpapi is the HTTP transport: the normative surface of SPEC.md on
// net/http and the stdlib router. It is an adapter; it imports the core and no
// other adapter.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/engine"
	"github.com/tnn1t1s/riemann-go/event"
)

// maxRuleBytes bounds the body of PUT /rules/{id}. One mebibyte, the same
// order as ingest.max_batch_bytes: a rule document is a few kilobytes, and the
// bound exists only so a client cannot make the server buffer without limit.
const maxRuleBytes = 1 << 20

// defaultEventsLimit is how many events GET /events returns when `limit` is
// absent.
//
// SPEC-GAP: the spec names `limit` and gives it no default. Chosen: 1000, the
// order of one ingest batch, so an unqualified read of a full ring does not
// serialize a hundred thousand events.
const defaultEventsLimit = 1000

// readHeaderTimeout bounds how long a connection may take to send its request
// headers, so an idle socket cannot hold one of the in-flight slots forever.
const readHeaderTimeout = 10 * time.Second

// API serves the surface over one engine.
type API struct {
	eng            *engine.Engine
	inflight       atomic.Int64
	refusedBatches atomic.Int64 // batches answered 400 or 413
}

// New returns the transport and registers its gauges with the engine's
// snapshot.
func New(eng *engine.Engine) *API {
	a := &API{eng: eng}
	eng.AddSamples(func() []engine.Sample {
		return []engine.Sample{
			{Service: "riemann.ingest.inflight_requests", Value: float64(a.inflight.Load())},
			{Service: "riemann.ingest.refused_batches", Value: float64(a.refusedBatches.Load())},
		}
	})
	return a
}

// Server returns an http.Server for the surface. It sets no write timeout,
// because /subscribe is a stream.
func (a *API) Server() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", a.postEvents)
	mux.HandleFunc("GET /events", a.getEvents)
	mux.HandleFunc("GET /index", a.getIndex)
	mux.HandleFunc("GET /index/{host}/{service...}", a.getIndexEntry)
	mux.HandleFunc("GET /subscribe", a.subscribe)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("GET /rules", a.listRules)
	mux.HandleFunc("PUT /rules/{id}", a.putRule)
	mux.HandleFunc("GET /rules/{id}", a.getRule)
	mux.HandleFunc("DELETE /rules/{id}", a.deleteRule)
	mux.HandleFunc("POST /rules/{id}/dryrun", a.dryRun)
	return &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout}
}

// Limit wraps a listener so at most ingest.max_inflight_requests connections
// are being served at once. Past that, Accept is not called and new
// connections wait in the kernel accept queue instead of allocating here.
//
// SPEC-GAP: SCALE.md bounds "in-flight requests" and describes the overflow
// as waiting in the kernel accept queue, which only a bound on accepted
// connections can produce. Chosen: the bound is applied to connections, so an
// open /subscribe stream or an idle keep-alive connection holds a slot.
func (a *API) Limit(l net.Listener) net.Listener {
	return &limitListener{Listener: l, slots: make(chan struct{}, a.eng.Params.IngestMaxInflightRequests), api: a}
}

type limitListener struct {
	net.Listener
	slots chan struct{}
	api   *API
}

func (l *limitListener) Accept() (net.Conn, error) {
	l.slots <- struct{}{}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	l.api.inflight.Add(1)
	return &limitConn{Conn: c, release: func() { <-l.slots; l.api.inflight.Add(-1) }}, nil
}

type limitConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ---- ingest ----

type queueView struct {
	Depth    int   `json:"depth"`
	Capacity int   `json:"capacity"`
	Dropped  int64 `json:"dropped"`
}

func (a *API) postEvents(w http.ResponseWriter, r *http.Request) {
	p := a.eng.Params
	if r.ContentLength > int64(p.IngestMaxBatchBytes) {
		a.refusedBatches.Add(1)
		writeError(w, http.StatusRequestEntityTooLarge, "batch exceeds ingest.max_batch_bytes")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(p.IngestMaxBatchBytes)+1))
	if err != nil {
		a.refusedBatches.Add(1)
		writeError(w, http.StatusBadRequest, "body could not be read: "+err.Error())
		return
	}
	if len(body) > p.IngestMaxBatchBytes {
		a.refusedBatches.Add(1)
		writeError(w, http.StatusRequestEntityTooLarge, "batch exceeds ingest.max_batch_bytes")
		return
	}
	evs, err := event.DecodeBatch(body, engine.Now(), p.IngestMaxBatchEvents)
	if err != nil {
		a.refusedBatches.Add(1)
		status := http.StatusBadRequest
		if errors.Is(err, event.ErrTooMany) {
			status = http.StatusRequestEntityTooLarge
		}
		writeError(w, status, err.Error())
		return
	}
	accepted := a.eng.Admit(evs)
	if accepted < len(evs) {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]int{"accepted": accepted, "rejected": len(evs) - accepted})
		return
	}
	sinks := map[string]queueView{}
	for name, st := range a.eng.SinkStats() {
		sinks[name] = queueView{Depth: st.Depth, Capacity: st.Capacity, Dropped: st.Dropped}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted, "sinks": sinks})
}

// ---- reads ----

func (a *API) getEvents(w http.ResponseWriter, r *http.Request) {
	q, err := engine.CompileQuery(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "q: "+err.Error())
		return
	}
	var since *float64
	if raw := r.URL.Query().Get("since"); raw != "" {
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be float seconds since the epoch")
			return
		}
		since = &f
	}
	limit := defaultEventsLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": nonNil(a.eng.RecentEvents(q, since, limit))})
}

func nonNil(evs []*event.Event) []*event.Event {
	if evs == nil {
		return []*event.Event{}
	}
	return evs
}

func (a *API) getIndex(w http.ResponseWriter, r *http.Request) {
	q, err := engine.CompileQuery(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "q: "+err.Error())
		return
	}
	entries, asOf := a.eng.QueryIndex(q)
	writeJSON(w, http.StatusOK, map[string]any{
		"as_of":   map[string]float64{"min": asOf.Min, "max": asOf.Max},
		"entries": nonNil(entries),
	})
}

func (a *API) getIndexEntry(w http.ResponseWriter, r *http.Request) {
	ev := a.eng.Lookup(r.PathValue("host"), r.PathValue("service"))
	if ev == nil {
		writeError(w, http.StatusNotFound, "no live index entry for that host and service")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// subscribe streams Server-Sent Events.
//
// SPEC-GAP: the spec fixes `event: lagged` "carrying the count it missed" and
// nothing else about the frames. Chosen: every event, snapshot entry or live,
// is an unnamed frame whose data is the event object, so a plain EventSource
// `onmessage` sees all of them; a lagged frame's data is {"count":n}.
func (a *API) subscribe(w http.ResponseWriter, r *http.Request) {
	q, err := engine.CompileQuery(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "q: "+err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported on this connection")
		return
	}
	sub, snap := a.eng.Subscribe(q, r.URL.Query().Get("snapshot") == "true")
	defer a.eng.Unsubscribe(sub)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	writeEvent := func(ev *event.Event) error {
		b, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(w, "data: %s\n\n", b)
		return err
	}
	for _, ev := range snap {
		if writeEvent(ev) != nil {
			return
		}
	}
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.C:
			if n := sub.TakeMissed(); n > 0 {
				if _, err := fmt.Fprintf(w, "event: lagged\ndata: {\"count\":%d}\n\n", n); err != nil {
					return
				}
			}
			if writeEvent(ev) != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// metrics renders the engine's snapshot as Prometheus text.
//
// SPEC-GAP: the spec requires the depth, capacity and dropped counter of every
// bounded queue at GET /metrics and names no metric. Chosen: each
// self-observation service name with every character outside [a-zA-Z0-9_]
// replaced by an underscore, so `riemann.sink.ntfy.dropped` is
// `riemann_sink_ntfy_dropped`, with the reading's attributes as labels. One
// naming scheme then covers both surfaces.
func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	samples := a.eng.Snapshot()
	byName := map[string][]engine.Sample{}
	var names []string
	for _, s := range samples {
		name := metricName(s.Service)
		if _, seen := byName[name]; !seen {
			names = append(names, name)
		}
		byName[name] = append(byName[name], s)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "# TYPE %s gauge\n", name)
		for _, s := range byName[name] {
			b.WriteString(name)
			if len(s.Attrs) > 0 {
				keys := make([]string, 0, len(s.Attrs))
				for k := range s.Attrs {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				b.WriteByte('{')
				for i, k := range keys {
					if i > 0 {
						b.WriteByte(',')
					}
					fmt.Fprintf(&b, "%s=%s", metricName(k), strconv.Quote(s.Attrs[k]))
				}
				b.WriteByte('}')
			}
			b.WriteByte(' ')
			b.WriteString(strconv.FormatFloat(s.Value, 'g', -1, 64))
			b.WriteByte('\n')
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

func metricName(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, s)
}

// ---- rules ----

func (a *API) putRule(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRuleBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "body could not be read: "+err.Error())
		return
	}
	if len(body) > maxRuleBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "rule document exceeds one mebibyte")
		return
	}
	c, created, err := a.eng.Store.Put(r.PathValue("id"), body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, c.Doc)
}

func (a *API) listRules(w http.ResponseWriter, r *http.Request) {
	docs := []map[string]any{}
	for _, c := range a.eng.Store.Current().Rules {
		docs = append(docs, c.Doc)
	}
	writeJSON(w, http.StatusOK, docs)
}

func (a *API) getRule(w http.ResponseWriter, r *http.Request) {
	c := a.eng.Store.Get(r.PathValue("id"))
	if c == nil {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	doc := make(map[string]any, len(c.Doc)+1)
	for k, v := range c.Doc {
		doc[k] = v
	}
	counters := make(map[string]int64, len(c.Live.Paths))
	for _, path := range c.Live.Paths {
		counters[path] = c.Live.ByPath[path].Passed.Load()
	}
	doc["counters"] = counters
	writeJSON(w, http.StatusOK, doc)
}

func (a *API) deleteRule(w http.ResponseWriter, r *http.Request) {
	if !a.eng.Store.Delete(r.PathValue("id")) {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// dryRun replays the ring through the stored rule against sink stubs.
//
// SPEC-GAP: the spec says each firing carries "the sink name, the event as the
// stub received it, and the node path" without naming the keys. Chosen:
// `sink`, `event` and `node`.
func (a *API) dryRun(w http.ResponseWriter, r *http.Request) {
	c := a.eng.Store.Get(r.PathValue("id"))
	if c == nil {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	type firing struct {
		Sink  string       `json:"sink"`
		Event *event.Event `json:"event"`
		Node  string       `json:"node"`
	}
	out := []firing{}
	for _, f := range a.eng.DryRun(c) {
		out = append(out, firing{Sink: f.Sink, Event: f.Event, Node: f.Node})
	}
	writeJSON(w, http.StatusOK, map[string]any{"firings": out})
}
