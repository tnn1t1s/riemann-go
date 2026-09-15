// Package http is the HTTP/1.1 JSON transport: ingest, index reads, ring
// reads, SSE subscribe and metrics. It imports the engine and core; the
// listen address lives in cmd.
package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/tnn1t1s/riemann-go/engine"
	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/expr"
	"github.com/tnn1t1s/riemann-go/shard"
)

// MaxInflightRequests bounds parked ingest handlers.
// Parameter: ingest.max_inflight_requests. Owner: transport/http. Units:
// requests. Default 256, provisional; beyond it new requests wait for a slot.
const MaxInflightRequests = 256

// MaxBatchEvents caps events per POST.
// Parameter: ingest.max_batch_events. Owner: transport/http. Units: events.
// Default 1000, one atlas fan-out burst; larger batches get 413. Provisional.
const MaxBatchEvents = 1000

// MaxBatchBytes caps the request body independent of event count.
// Parameter: ingest.max_batch_bytes. Owner: transport/http. Units: bytes.
// Default 1 MiB, provisional; larger bodies get 413.
const MaxBatchBytes = 1 << 20

// AdmissionDeadline is how long one request's offers may wait on full
// inboxes before the reply is 429 for the remainder.
// Parameter: ingest.admission_deadline. Owner: transport/http. Units:
// milliseconds. Default 200: shorter than any HTTP client's default timeout,
// longer than an estimated 50 ms rule burst. Provisional until the
// milestone 3 flood.
const AdmissionDeadline = 200 * time.Millisecond

// RetryAfterSeconds is the Retry-After header on a 429.
// Default 1 s; emitters do not honour it, it is advisory for humans and curl.
const RetryAfterSeconds = 1

// DefaultEventsLimit is the limit on GET /events when none is given.
// Default 1000, chosen to fit one riemann-surface page; not a product
// parameter, a display default.
const DefaultEventsLimit = 1000

// Server serves the API for one engine.
type Server struct {
	eng   *engine.Engine
	slots chan struct{} // MaxInflightRequests semaphore
}

// New builds a server over eng.
func New(eng *engine.Engine) *Server {
	return &Server{eng: eng, slots: make(chan struct{}, MaxInflightRequests)}
}

// Handler is the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.postEvents)
	mux.HandleFunc("GET /index", s.getIndex)
	mux.HandleFunc("GET /index/{host}/{service}", s.getIndexEntry)
	mux.HandleFunc("GET /events", s.getEvents)
	mux.HandleFunc("GET /subscribe", s.subscribe)
	mux.HandleFunc("GET /metrics", s.metrics)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) now() float64 { return event.Seconds(s.eng.Clock().Now()) }

// matcher compiles q into a predicate over events at the engine's clock.
// An empty q matches everything. A runtime error reads as false.
func (s *Server) matcher(q string) (func(event.Event) bool, error) {
	if q == "" {
		return func(event.Event) bool { return true }, nil
	}
	p, err := expr.CompilePredicate(q)
	if err != nil {
		return nil, err
	}
	return func(e event.Event) bool {
		ok, _ := p.Eval(e, s.now())
		return ok
	}, nil
}

func (s *Server) postEvents(w http.ResponseWriter, r *http.Request) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-r.Context().Done():
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBatchBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body over %d bytes", MaxBatchBytes))
		return
	}
	events, err := event.DecodeBatch(body, event.Defaults{Time: s.now(), TTL: s.eng.DefaultTTL()})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(events) > MaxBatchEvents {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("batch of %d over %d events", len(events), MaxBatchEvents))
		return
	}
	accepted, rejected := s.eng.Admit(events, AdmissionDeadline)
	if rejected > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(RetryAfterSeconds))
		writeJSON(w, http.StatusTooManyRequests, map[string]int{"accepted": accepted, "rejected": rejected})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted, "sinks": s.eng.SinkMetrics()})
}

func (s *Server) getIndex(w http.ResponseWriter, r *http.Request) {
	pred, err := s.matcher(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	events, asOf := s.eng.Query(pred)
	if events == nil {
		events = []event.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"as_of": asOf, "events": events})
}

func (s *Server) getIndexEntry(w http.ResponseWriter, r *http.Request) {
	en, at, ok := s.eng.Lookup(r.PathValue("host"), r.PathValue("service"))
	if !ok {
		writeError(w, http.StatusNotFound, "not indexed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"as_of": at, "event": en.Event, "last_transition": en.LastTransition})
}

func (s *Server) getEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pred, err := s.matcher(q.Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var since float64
	if v := q.Get("since"); v != "" {
		if since, err = strconv.ParseFloat(v, 64); err != nil {
			writeError(w, http.StatusBadRequest, "since: "+err.Error())
			return
		}
	}
	limit := DefaultEventsLimit
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit <= 0 {
			writeError(w, http.StatusBadRequest, "limit: positive integer required")
			return
		}
	}
	events := s.eng.Events(pred, since, limit)
	if events == nil {
		events = []event.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

// subscribe streams matching events as SSE. Frames are "event: event" with
// the event JSON, "event: snapshot" for the initial index entries when
// snapshot=true, and "event: lagged" with {"count": n} after a shed.
func (s *Server) subscribe(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pred, err := s.matcher(q.Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	snapshot := q.Get("snapshot") == "true"
	snap, sub := s.eng.Subscribe(pred, snapshot, shard.DefaultSubscriberCapacity)
	defer s.eng.Unsubscribe(sub)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	frame := func(kind string, v any) bool {
		b, _ := json.Marshal(v)
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for _, e := range snap {
		if !frame("snapshot", e) {
			return
		}
	}
	if snapshot && !frame("snapshot_end", map[string]int{"count": len(snap)}) {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-sub.Events():
			if !frame("event", e) {
				return
			}
			if n := sub.Lagged(); n > 0 && !frame("lagged", map[string]uint64{"count": n}) {
				return
			}
		}
	}
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.eng.Metrics())
}
