package httpapi

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
	"github.com/tnn1t1s/riemann-go/internal/event"
)

func wallNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

type queueBody struct {
	Depth    int    `json:"depth"`
	Capacity int    `json:"capacity"`
	Dropped  uint64 `json:"dropped"`
}

type acceptedBody struct {
	Accepted int                  `json:"accepted"`
	Sinks    map[string]queueBody `json:"sinks"`
}

type rejectedBody struct {
	Accepted int `json:"accepted"`
	Rejected int `json:"rejected"`
}

func (s *Server) postEvents(w http.ResponseWriter, r *http.Request) {
	params := s.eng.Params()
	limit := int64(params.IngestMaxBatchBytes)
	if r.ContentLength > limit {
		s.rejectedBatches.Add(1)
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body of %d bytes exceeds ingest.max_batch_bytes of %d", r.ContentLength, limit))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		s.rejectedBatches.Add(1)
		writeError(w, http.StatusBadRequest, "body could not be read: "+err.Error())
		return
	}
	if int64(len(body)) > limit {
		s.rejectedBatches.Add(1)
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body exceeds ingest.max_batch_bytes of %d", limit))
		return
	}
	// An event that names no time takes the receive time, read here from
	// the wall clock.
	events, err := event.DecodeBatch(body, wallNow(), params.IngestMaxBatchEvents)
	if err != nil {
		s.rejectedBatches.Add(1)
		status := http.StatusBadRequest
		if errors.Is(err, event.ErrBatchTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeError(w, status, err.Error())
		return
	}

	accepted := s.eng.Admit(events, params.IngestAdmissionDeadline)
	if accepted < len(events) {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, rejectedBody{Accepted: accepted, Rejected: len(events) - accepted})
		return
	}
	snap := s.eng.Snapshot()
	sinks := make(map[string]queueBody, len(snap.Sinks))
	for name, st := range snap.Sinks {
		sinks[name] = queueBody{Depth: st.Depth, Capacity: st.Capacity, Dropped: st.Dropped}
	}
	writeJSON(w, http.StatusAccepted, acceptedBody{Accepted: accepted, Sinks: sinks})
}

type asOfBody struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type indexBody struct {
	AsOf    asOfBody       `json:"as_of"`
	Entries []*event.Event `json:"entries"`
}

func (s *Server) getIndex(w http.ResponseWriter, r *http.Request) {
	f, err := filter(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	entries, lo, hi := s.eng.IndexQuery(f)
	writeJSON(w, http.StatusOK, indexBody{AsOf: asOfBody{Min: lo, Max: hi}, Entries: entries})
}

func (s *Server) getIndexEntry(w http.ResponseWriter, r *http.Request) {
	ev := s.eng.IndexGet(r.PathValue("host"), r.PathValue("service"))
	if ev == nil {
		writeError(w, http.StatusNotFound, "no live entry for that host and service")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

// defaultEventsLimit is how many events GET /events returns when the request
// names no limit, in events. A default: enough to fill a dashboard pane, and
// small against a ring of 100000 events per partition, which would otherwise
// be one response.
const defaultEventsLimit = 1000

type eventsBody struct {
	Events []*event.Event `json:"events"`
}

// SPEC-GAP: the spec names `since` and `limit` and defines neither. Chosen:
// `since` is float seconds since epoch and keeps events whose `time` is at or
// after it, inclusive so that a poller passing the last time it saw cannot
// miss an event sharing that time; `limit` keeps the most recent that many
// of the events selected, in processing order, and defaults to
// defaultEventsLimit.
func (s *Server) getEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	f, err := filter(query.Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var since *float64
	if raw := query.Get("since"); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("since %q is not a number of seconds since epoch", raw))
			return
		}
		since = &v
	}
	limit := defaultEventsLimit
	if raw := query.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("limit %q is not an integer of 1 or greater", raw))
			return
		}
		limit = v
	}
	writeJSON(w, http.StatusOK, eventsBody{Events: s.eng.Recent(f, since, limit)})
}

// SPEC-GAP: the spec does not give the body of a healthy answer. Chosen: the
// JSON object {"status":"ok"}.
func (s *Server) getHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeFrame(w io.Writer, name string, data []byte) error {
	if name != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", name); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "data: %s\n\n", data)
	return err
}

// SPEC-GAP: the spec names one SSE frame, `event: lagged`, and says it
// carries the count. It does not name the frames that carry events or give
// any frame's data. Chosen: a snapshot entry is a frame named `snapshot`, a
// live event is an unnamed frame so a browser's onmessage receives it, and
// both carry one event object as JSON; `lagged` carries {"count":n}. With
// `snapshot=true` the snapshot frames come first, in identity order.
func (s *Server) getSubscribe(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	f, err := filter(query.Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	snapshot := false
	if raw := query.Get("snapshot"); raw != "" {
		if snapshot, err = strconv.ParseBool(raw); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("snapshot %q is not true or false", raw))
			return
		}
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "connection does not support streaming")
		return
	}

	sub, entries := s.eng.Subscribe(f, snapshot)
	defer s.eng.Unsubscribe(sub)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	send := func(name string, v any) bool {
		data, err := marshal(v)
		if err != nil {
			return true
		}
		return writeFrame(w, name, data) == nil
	}
	for _, ev := range entries {
		if !send("snapshot", ev) {
			return
		}
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.Events():
			if !send("", ev) {
				return
			}
			if missed := sub.TakeMissed(); missed > 0 {
				if !send("lagged", map[string]uint64{"count": missed}) {
					return
				}
			}
			flusher.Flush()
		}
	}
}

func (s *Server) putRule(w http.ResponseWriter, r *http.Request) {
	// A rule document shares the ingest body cap: it is the one bound on a
	// request body SCALE.md names.
	limit := int64(s.eng.Params().IngestMaxBatchBytes)
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "body could not be read: "+err.Error())
		return
	}
	if int64(len(body)) > limit {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body exceeds ingest.max_batch_bytes of %d", limit))
		return
	}
	stored, created, err := s.eng.PutRule(r.PathValue("id"), body)
	if errors.Is(err, engine.ErrInvalidRule) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, stored)
}

func (s *Server) getRules(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.eng.Rules())
}

func (s *Server) getRule(w http.ResponseWriter, r *http.Request) {
	stored, counters, ok := s.eng.Rule(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "no rule with that id")
		return
	}
	stored["counters"] = counters
	writeJSON(w, http.StatusOK, stored)
}

func (s *Server) deleteRule(w http.ResponseWriter, r *http.Request) {
	if !s.eng.DeleteRule(r.PathValue("id")) {
		writeError(w, http.StatusNotFound, "no rule with that id")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type firingBody struct {
	Sink  string       `json:"sink"`
	Event *event.Event `json:"event"`
	Node  string       `json:"node"`
}

type dryRunBody struct {
	Firings []firingBody `json:"firings"`
}

// SPEC-GAP: the spec says each firing carries "the sink name, the event as
// the stub received it, and the node path" and names no keys. Chosen:
// `sink`, `event` and `node`. The request body is not read; the spec gives
// the dry run no parameters.
func (s *Server) postDryRun(w http.ResponseWriter, r *http.Request) {
	firings, ok := s.eng.DryRun(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "no rule with that id")
		return
	}
	out := dryRunBody{Firings: make([]firingBody, 0, len(firings))}
	for _, f := range firings {
		out.Firings = append(out.Firings, firingBody{Sink: f.Sink, Event: f.Event, Node: f.Node})
	}
	writeJSON(w, http.StatusOK, out)
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// SPEC-GAP: the spec requires Prometheus text format carrying each queue's
// depth, capacity and dropped counter, and names no metric. Chosen: the
// self-observation service name with its dots replaced by underscores, and
// the event's attributes as labels, so `riemann.sink.ntfy.dropped` is
// `riemann_sink_ntfy_dropped`.
func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request) {
	readings := append(s.eng.Snapshot().Readings(), s.Readings()...)
	var sb strings.Builder
	typed := map[string]bool{}
	for _, rd := range readings {
		name := strings.ReplaceAll(rd.Service, ".", "_")
		if !typed[name] {
			typed[name] = true
			kind := "gauge"
			if rd.Counter {
				kind = "counter"
			}
			fmt.Fprintf(&sb, "# TYPE %s %s\n", name, kind)
		}
		sb.WriteString(name)
		if len(rd.Attributes) > 0 {
			keys := make([]string, 0, len(rd.Attributes))
			for k := range rd.Attributes {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			sb.WriteByte('{')
			for i, k := range keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `%s="%s"`, k, labelEscaper.Replace(rd.Attributes[k]))
			}
			sb.WriteByte('}')
		}
		sb.WriteByte(' ')
		sb.WriteString(strconv.FormatFloat(rd.Value, 'g', -1, 64))
		sb.WriteByte('\n')
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, sb.String())
}
