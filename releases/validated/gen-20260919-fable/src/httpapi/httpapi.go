// Package httpapi is the HTTP transport: ingest, the read surface, the rule
// lifecycle and SSE subscriptions, on net/http and the stdlib router. Paths,
// verbs, field names and status codes are SPEC.md's and nothing is added.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/engine"
	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/exprs"
)

// Config carries the parameters SCALE.md assigns to the HTTP transport.
type Config struct {
	MaxInflightRequests    int           // ingest.max_inflight_requests, requests
	MaxBatchEvents         int           // ingest.max_batch_events, events
	MaxBatchBytes          int64         // ingest.max_batch_bytes, bytes
	AdmissionDeadline      time.Duration // ingest.admission_deadline
	SubscribeQueueCapacity int           // subscribe.queue_capacity, events per subscriber
}

// API serves the HTTP surface over one engine.
type API struct {
	cfg Config
	eng *engine.Engine

	rejectedBatches atomic.Int64 // bodies refused with 413

	subsMu      sync.Mutex
	subscribers map[*subscriber]struct{}
	subDropped  atomic.Int64
}

// New builds the API and registers the subscriber queues with the engine's
// snapshot, so they answer at /metrics and in self-observation like any other
// bounded queue.
func New(cfg Config, eng *engine.Engine) *API {
	a := &API{cfg: cfg, eng: eng, subscribers: map[*subscriber]struct{}{}}
	eng.RegisterQueue("subscribe", func() engine.QueueStat {
		a.subsMu.Lock()
		defer a.subsMu.Unlock()
		var depth int64
		for s := range a.subscribers {
			depth += int64(len(s.ch))
		}
		// Depth sums every subscriber's queue; capacity is per subscriber.
		return engine.QueueStat{Depth: depth, Capacity: int64(cfg.SubscribeQueueCapacity), Dropped: a.subDropped.Load()}
	})
	return a
}

// Register mounts the surface on mux.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /events", a.postEvents)
	mux.HandleFunc("GET /events", a.getEvents)
	mux.HandleFunc("GET /index", a.getIndex)
	mux.HandleFunc("GET /index/{host}/{service...}", a.getIndexEntry)
	mux.HandleFunc("GET /subscribe", a.subscribe)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("PUT /rules/{id}", a.putRule)
	mux.HandleFunc("GET /rules", a.getRules)
	mux.HandleFunc("GET /rules/{id}", a.getRule)
	mux.HandleFunc("DELETE /rules/{id}", a.deleteRule)
	mux.HandleFunc("POST /rules/{id}/dryrun", a.dryRun)
}

// Listen binds addr behind a limit of ingest.max_inflight_requests open
// connections. Past the limit the listener stops accepting, so new
// connections wait in the kernel accept queue instead of allocating here.
func (a *API) Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &limitListener{Listener: ln, slots: make(chan struct{}, a.cfg.MaxInflightRequests)}, nil
}

// limitListener's slots channel is a counting semaphore, not a queue of
// events: its capacity is the bound and nothing is ever shed from it.
type limitListener struct {
	net.Listener
	slots chan struct{}
}

func (l *limitListener) Accept() (net.Conn, error) {
	l.slots <- struct{}{}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &limitConn{Conn: c, release: func() { <-l.slots }}, nil
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
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ---- ingest ----

type queueView struct {
	Depth    int64 `json:"depth"`
	Capacity int64 `json:"capacity"`
	Dropped  int64 `json:"dropped"`
}

func (a *API) postEvents(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > a.cfg.MaxBatchBytes {
		a.rejectedBatches.Add(1)
		writeError(w, http.StatusRequestEntityTooLarge, "batch exceeds ingest.max_batch_bytes")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, a.cfg.MaxBatchBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			a.rejectedBatches.Add(1)
			writeError(w, http.StatusRequestEntityTooLarge, "batch exceeds ingest.max_batch_bytes")
			return
		}
		writeError(w, http.StatusBadRequest, "could not read the body")
		return
	}
	raws, err := event.SplitBatch(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(raws) > a.cfg.MaxBatchEvents {
		a.rejectedBatches.Add(1)
		writeError(w, http.StatusRequestEntityTooLarge, "batch exceeds ingest.max_batch_events")
		return
	}
	// An event with no time is stamped with the receive time, read from the
	// engine's clock so it is comparable with every other time the engine holds.
	now := a.eng.Now()
	events := make([]*event.Event, len(raws))
	for i, raw := range raws {
		// Validation covers the whole batch before anything is admitted.
		if events[i], err = event.Parse(raw, now); err != nil {
			writeError(w, http.StatusBadRequest, "event "+strconv.Itoa(i)+": "+err.Error())
			return
		}
	}
	accepted, rejected := a.eng.Admit(events, a.cfg.AdmissionDeadline)
	if rejected > 0 {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]int{"accepted": accepted, "rejected": rejected})
		return
	}
	sinks := map[string]queueView{}
	for name, st := range a.eng.SinkStats() {
		sinks[name] = queueView{Depth: st.Depth, Capacity: st.Capacity, Dropped: st.Dropped}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted, "sinks": sinks})
}

// ---- reads ----

// filter compiles the q parameter, the same expression language rules use.
// SPEC-GAP: an absent or empty q selects everything.
func filter(q string) (func(*event.Event, float64) bool, error) {
	if q == "" {
		return func(*event.Event, float64) bool { return true }, nil
	}
	p, err := exprs.Compile(q, exprs.Options{AsBool: true})
	if err != nil {
		return nil, err
	}
	return func(ev *event.Event, now float64) bool {
		ok, err := p.Bool(ev, now, nil)
		return err == nil && ok
	}, nil
}

func (a *API) getIndex(w http.ResponseWriter, r *http.Request) {
	keep, err := filter(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	entries, lo, hi := a.eng.IndexQuery(keep)
	writeJSON(w, http.StatusOK, map[string]any{"as_of": map[string]float64{"min": lo, "max": hi}, "entries": entries})
}

func (a *API) getIndexEntry(w http.ResponseWriter, r *http.Request) {
	ev, ok := a.eng.IndexGet(r.PathValue("host"), r.PathValue("service"))
	if !ok {
		writeError(w, http.StatusNotFound, "no live entry for that identity")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

// defaultEventsLimit is how many ring events GET /events returns when limit is
// absent. Default: large enough to show a scenario's worth of traffic, small
// enough that a bare GET against a full ring does not serialize 100000
// events. Nothing has measured a caller's need.
const defaultEventsLimit = 1000

// getEvents reads the ring.
// SPEC-GAP: since is float seconds since the epoch and keeps events whose time
// is at or after it; limit keeps the most recent events after filtering.
func (a *API) getEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	keep, err := filter(query.Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit := defaultEventsLimit
	if s := query.Get("limit"); s != "" {
		if limit, err = strconv.Atoi(s); err != nil || limit < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
	}
	if s := query.Get("since"); s != "" {
		since, err := strconv.ParseFloat(s, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be float seconds since the epoch")
			return
		}
		inner := keep
		keep = func(ev *event.Event, now float64) bool { return ev.Time >= since && inner(ev, now) }
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": a.eng.Events(keep, limit)})
}

// ---- rules ----

func (a *API) putRule(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, a.cfg.MaxBatchBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read the body")
		return
	}
	doc, created, err := a.eng.PutRule(r.PathValue("id"), body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, doc)
}

func (a *API) getRules(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.eng.Rules())
}

func (a *API) getRule(w http.ResponseWriter, r *http.Request) {
	doc, ok := a.eng.Rule(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func (a *API) deleteRule(w http.ResponseWriter, r *http.Request) {
	if !a.eng.DeleteRule(r.PathValue("id")) {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) dryRun(w http.ResponseWriter, r *http.Request) {
	firings, ok := a.eng.DryRun(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"firings": firings})
}

// RejectedBatches counts bodies refused with 413.
func (a *API) RejectedBatches() int64 { return a.rejectedBatches.Load() }
