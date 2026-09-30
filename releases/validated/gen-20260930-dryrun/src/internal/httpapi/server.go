// Package httpapi is the HTTP adapter: the normative surface from SPEC.md
// over net/http and the standard library router.
package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

// Server serves the surface for one engine.
type Server struct {
	E *engine.Engine
}

// Handler returns the routed handler with CORS applied to every response.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.postEvents)
	mux.HandleFunc("GET /index", s.getIndex)
	mux.HandleFunc("GET /index/{host}/{service}", s.getIndexEntry)
	mux.HandleFunc("GET /events", s.getEvents)
	mux.HandleFunc("GET /subscribe", s.subscribe)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("PUT /rules/{id}", s.putRule)
	mux.HandleFunc("GET /rules", s.getRules)
	mux.HandleFunc("GET /rules/{id}", s.getRule)
	mux.HandleFunc("DELETE /rules/{id}", s.deleteRule)
	mux.HandleFunc("POST /rules/{id}/dryrun", s.dryRun)
	return cors(mux)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization, Last-Event-ID")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
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

// --- ingest ---

func (s *Server) postEvents(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	p := s.E.P
	r.Body = http.MaxBytesReader(w, r.Body, p.IngestMaxBatchBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("batch exceeds ingest.max_batch_bytes (%d)", p.IngestMaxBatchBytes))
			return
		}
		writeError(w, http.StatusBadRequest, "cannot read body: "+err.Error())
		return
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		writeError(w, http.StatusBadRequest, "empty body")
		return
	}
	var raws []json.RawMessage
	switch trimmed[0] {
	case '[':
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			writeError(w, http.StatusBadRequest, "body is not a JSON array of events: "+err.Error())
			return
		}
	case '{':
		raws = []json.RawMessage{trimmed}
	default:
		writeError(w, http.StatusBadRequest, "body must be a JSON array or a JSON object")
		return
	}
	if len(raws) > p.IngestMaxBatchEvents {
		s.E.IngestRejected.Add(int64(len(raws)))
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("batch of %d exceeds ingest.max_batch_events (%d)", len(raws), p.IngestMaxBatchEvents))
		return
	}
	recv := float64(start.UnixNano()) / 1e9
	evs := make([]*event.Event, 0, len(raws))
	for i, raw := range raws {
		ev, err := event.Decode(raw, recv)
		if err != nil {
			s.E.IngestRejected.Add(int64(len(raws)))
			writeError(w, http.StatusBadRequest, fmt.Sprintf("event %d: %v", i, err))
			return
		}
		evs = append(evs, ev)
	}
	accepted, rejected := s.E.Admit(evs, start.Add(p.IngestAdmissionDeadline))
	if rejected > 0 {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]int{"accepted": accepted, "rejected": rejected})
		return
	}
	sinks := map[string]map[string]int64{}
	for _, st := range s.E.Stats().Sinks {
		sinks[st.Name] = map[string]int64{"depth": int64(st.Depth), "capacity": int64(st.Capacity), "dropped": st.Dropped}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted, "sinks": sinks})
}

// --- reads ---

func (s *Server) getIndex(w http.ResponseWriter, r *http.Request) {
	entries, asOf, err := s.E.IndexQuery(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"as_of": asOf, "entries": entries})
}

func (s *Server) getIndexEntry(w http.ResponseWriter, r *http.Request) {
	ev, ok := s.E.IndexLookup(r.PathValue("host"), r.PathValue("service"))
	if !ok {
		writeError(w, http.StatusNotFound, "no live entry for that identity")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *Server) getEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	since := 0.0
	if v := q.Get("since"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be float seconds")
			return
		}
		since = f
	}
	limit := 1000
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		limit = n
	}
	if limit <= 0 {
		writeError(w, http.StatusBadRequest, "limit must be greater than 0")
		return
	}
	evs, err := s.E.RecentEvents(q.Get("q"), since, limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if evs == nil {
		evs = []*event.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs})
}

func (s *Server) subscribe(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	snapshot := q.Get("snapshot") == "true"
	sub, snap, err := s.E.Subscribe(q.Get("q"), snapshot)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer s.E.Unsubscribe(sub)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// SPEC-GAP: the SSE frame names are not pinned. Snapshot entries arrive
	// as `event: snapshot`, live events as `event: event`, and shed events as
	// `event: lagged` with the count missed, which SPEC.md does name.
	frame := func(name string, v any) bool {
		raw, err := json.Marshal(v)
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, raw); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for _, ev := range snap {
		if !frame("snapshot", ev) {
			return
		}
	}
	var reported int64
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-sub.C:
			if d := sub.Dropped(); d > reported {
				if !frame("lagged", map[string]int64{"missed": d - reported}) {
					return
				}
				reported = d
			}
			if !frame("event", ev) {
				return
			}
		}
	}
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- rules ---

func (s *Server) putRule(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read body: "+err.Error())
		return
	}
	doc, created, err := s.E.PutRule(body, r.PathValue("id"))
	if err != nil {
		var pe *rule.ParseError
		if errors.As(err, &pe) {
			writeError(w, http.StatusBadRequest, pe.Msg)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if created {
		writeJSON(w, http.StatusCreated, doc)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func (s *Server) getRules(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.E.Rules())
}

func (s *Server) getRule(w http.ResponseWriter, r *http.Request) {
	doc, counters, err := s.E.Rule(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	out := make(map[string]any, len(doc)+1)
	for k, v := range doc {
		out[k] = v
	}
	out["counters"] = counters
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteRule(w http.ResponseWriter, r *http.Request) {
	if err := s.E.DeleteRule(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, "unknown rule")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) dryRun(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read body: "+err.Error())
		return
	}
	firings, err := s.E.DryRun(body, r.PathValue("id"))
	if err != nil {
		var pe *rule.ParseError
		if errors.As(err, &pe) {
			writeError(w, http.StatusBadRequest, pe.Msg)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"firings": firings})
}

// --- metrics ---

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	st := s.E.Stats()
	var b strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&b, format, args...); b.WriteByte('\n') }
	line("# TYPE riemann_ingest_inflight_requests gauge")
	line("riemann_ingest_inflight_requests %d", st.InflightRequests)
	line("riemann_ingest_max_inflight_requests %d", st.MaxInflightRequests)
	line("# TYPE riemann_ingest_accepted_total counter")
	line("riemann_ingest_accepted_total %d", st.IngestAccepted)
	line("riemann_ingest_rejected_total %d", st.IngestRejected)
	for _, sh := range st.Shards {
		l := fmt.Sprintf(`{shard="%d"}`, sh.ID)
		line("riemann_shard_inbox_depth%s %d", l, sh.InboxDepth)
		line("riemann_shard_inbox_capacity%s %d", l, sh.InboxCapacity)
		line("riemann_shard_inbox_stalled_total%s %d", l, sh.Stalled)
		line("riemann_shard_inbox_rejected_total%s %d", l, sh.Rejected)
		line("riemann_shard_processed_total%s %d", l, sh.Processed)
		line("riemann_shard_loop_lag_seconds%s %g", l, sh.LoopLag)
		line("riemann_shard_in_flight%s %d", l, sh.InFlight)
		line("riemann_shard_index_entries%s %d", l, sh.IndexEntries)
		line("riemann_shard_index_capacity%s %d", l, s.E.P.IndexMaxEntriesPerShard)
		line("riemann_shard_index_rejected_total%s %d", l, sh.IndexRejected)
		line("riemann_shard_ring_events%s %d", l, sh.RingEvents)
		line("riemann_shard_ring_bytes%s %d", l, sh.RingBytes)
	}
	line("riemann_global_inbox_depth %d", st.Global.Depth)
	line("riemann_global_inbox_capacity %d", st.Global.Capacity)
	line("riemann_global_inbox_dropped_total %d", st.Global.Dropped)
	line("riemann_global_processed_total %d", st.Global.Processed)
	for _, sk := range st.Sinks {
		l := fmt.Sprintf(`{sink="%s"}`, sk.Name)
		line("riemann_sink_queue_depth%s %d", l, sk.Depth)
		line("riemann_sink_queue_capacity%s %d", l, sk.Capacity)
		line("riemann_sink_dropped_total%s %d", l, sk.Dropped)
		line("riemann_sink_failed_total%s %d", l, sk.Failed)
		line("riemann_sink_accepted_total%s %d", l, sk.Accepted)
		line("riemann_sink_processed_total%s %d", l, sk.Processed)
		line("riemann_sink_in_flight%s %d", l, sk.InFlight)
	}
	line("riemann_subscribers %d", st.Subscribers)
	line("riemann_subscribe_queue_capacity %d", st.SubscribeCapacity)
	line("riemann_subscribe_dropped_total %d", st.SubscribeDropped)
	line("riemann_stable_buffer_capacity %d", st.StableCapacity)
	for _, rs := range st.Rules {
		for i, p := range rs.Paths {
			line(`riemann_rule_discarded_total{rule="%s",version="%d",node="%s"} %d`, rs.ID, rs.Version, p, rs.Discards[i])
		}
		line(`riemann_rule_forks_live{rule="%s",version="%d"} %d`, rs.ID, rs.Version, rs.ForksLive)
		line(`riemann_rule_forks_freed_total{rule="%s",version="%d"} %d`, rs.ID, rs.Version, rs.ForksFreed)
	}
	line("riemann_accounting_residual %d", st.Residual)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// LimitListener bounds concurrent connections at ingest.max_inflight_requests.
// Beyond it new connections wait in the kernel accept queue rather than
// allocating a handler goroutine.
type LimitListener struct {
	net.Listener
	sem   chan struct{}
	gauge func(delta int64)
}

// NewLimitListener wraps l.
func NewLimitListener(l net.Listener, max int, gauge func(delta int64)) *LimitListener {
	return &LimitListener{Listener: l, sem: make(chan struct{}, max), gauge: gauge}
}

// Accept waits for a slot, then accepts.
func (l *LimitListener) Accept() (net.Conn, error) {
	l.sem <- struct{}{}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	l.gauge(1)
	return &limitConn{Conn: c, release: func() {
		<-l.sem
		l.gauge(-1)
	}}, nil
}

type limitConn struct {
	net.Conn
	release func()
	closed  bool
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	if !c.closed {
		c.closed = true
		c.release()
	}
	return err
}
