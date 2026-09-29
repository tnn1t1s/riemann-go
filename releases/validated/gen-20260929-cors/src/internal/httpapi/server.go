// Package httpapi is the HTTP transport: ingest, the read surface, the rule
// endpoints, health and metrics. It is an adapter. It imports the core and no
// other adapter; what it reports about the sinks it reads through the engine.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/expression"
)

// readHeaderTimeout bounds how long a connection may take to send its request
// headers, in seconds of wall time. A heuristic: a connection holds one of
// ingest.max_inflight_requests slots from accept, so one that never sends a
// request must not hold it forever.
const readHeaderTimeout = 10 * time.Second

// idleTimeout bounds how long a kept-alive connection may sit between
// requests, in seconds of wall time. A heuristic, for the same reason: an
// idle connection holds a slot a waiting one could use.
const idleTimeout = 60 * time.Second

// Server is the HTTP surface over one engine.
type Server struct {
	eng  *engine.Engine
	http *http.Server

	ctx    context.Context
	cancel context.CancelFunc

	inflight        atomic.Int64
	rejectedBatches atomic.Uint64
}

// New builds the server. It binds nothing.
func New(eng *engine.Engine) *Server {
	s := &Server{eng: eng}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.postEvents)
	mux.HandleFunc("GET /events", s.getEvents)
	mux.HandleFunc("GET /index", s.getIndex)
	mux.HandleFunc("GET /index/{host}/{service...}", s.getIndexEntry)
	mux.HandleFunc("GET /subscribe", s.getSubscribe)
	mux.HandleFunc("GET /healthz", s.getHealthz)
	mux.HandleFunc("GET /metrics", s.getMetrics)
	mux.HandleFunc("PUT /rules/{id}", s.putRule)
	mux.HandleFunc("GET /rules", s.getRules)
	mux.HandleFunc("GET /rules/{id}", s.getRule)
	mux.HandleFunc("DELETE /rules/{id}", s.deleteRule)
	mux.HandleFunc("POST /rules/{id}/dryrun", s.postDryRun)
	s.http = &http.Server{
		Handler:           s.cors(mux),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		BaseContext:       func(net.Listener) context.Context { return s.ctx },
	}
	return s
}

// Handler returns the full surface, CORS included.
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Serve answers requests on ln until Shutdown.
func (s *Server) Serve(ln net.Listener) error { return s.http.Serve(ln) }

// Shutdown ends open subscriptions and stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.cancel()
	return s.http.Shutdown(ctx)
}

// The methods and request headers the surface accepts, as a preflight
// reports them.
//
// SPEC-GAP: the spec requires the preflight's two headers to cover "what the
// surface accepts" and lists neither. Chosen: the four verbs the surface
// uses plus OPTIONS, and the request headers a browser sends to it:
// Content-Type for the JSON bodies, and Accept, Cache-Control and
// Last-Event-ID, which an EventSource sets.
const (
	allowMethods = "GET, POST, PUT, DELETE, OPTIONS"
	allowHeaders = "Content-Type, Accept, Cache-Control, Last-Event-ID"
)

// cors puts Access-Control-Allow-Origin on every response, whatever its
// status, and answers a preflight to any path with 204.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.inflight.Add(1)
		defer s.inflight.Add(-1)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", allowMethods)
			w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Readings reports what only the transport knows.
//
// SPEC-GAP: SCALE.md gives the HTTP server an "in-flight gauge" and the
// request body a "rejected count" without naming either. Chosen:
// `riemann.ingest.inflight_requests`, and `riemann.ingest.rejected_batches`
// for batches refused with 400 or 413. `riemann.ingest.rejected` counts
// events refused with 429, the only refusals whose event count is always
// known.
func (s *Server) Readings() []engine.Reading {
	return []engine.Reading{
		{Service: "riemann.ingest.inflight_requests", Value: float64(s.inflight.Load())},
		{Service: "riemann.ingest.max_inflight_requests", Value: float64(s.eng.Params().IngestMaxInflightRequests)},
		{Service: "riemann.ingest.rejected_batches", Value: float64(s.rejectedBatches.Load()), Counter: true},
	}
}

// marshal encodes v as one line of JSON with no trailing newline and no HTML
// escaping.
func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := marshal(v)
	if err != nil {
		http.Error(w, `{"error":"response could not be encoded"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(append(body, '\n'))
}

// SPEC-GAP: the spec fixes the status of each error and not its body.
// Chosen: a JSON object with one key, `error`, holding the message.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// filter compiles a q= parameter. An absent or empty q selects everything.
//
// SPEC-GAP: the spec does not say what an absent `q` selects. Chosen:
// everything, so `GET /index` with no parameter lists the index.
func filter(q string) (engine.Filter, error) {
	if q == "" {
		return nil, nil
	}
	prog, err := expression.Compile(q, false, true)
	if err != nil {
		return nil, err
	}
	return func(ev *event.Event, now float64) bool {
		// A predicate that fails at evaluation does not hold.
		ok, err := prog.Bool(expression.Env(ev, now, nil))
		return err == nil && ok
	}, nil
}

// limitListener holds at most n connections open. Past that it stops
// accepting, so further connections wait in the kernel's accept queue rather
// than allocating in the process.
type limitListener struct {
	net.Listener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

// Listen binds addr and bounds open connections at maxConns, which is
// ingest.max_inflight_requests.
//
// SPEC-GAP: SCALE.md bounds "in-flight requests" by making new connections
// wait in the accept queue, which bounds connections, not requests. Chosen:
// the bound is on open connections, since that is the only place the kernel
// queue can be made to hold the excess. A long-lived subscription holds one
// slot for as long as it is open.
func Listen(addr string, maxConns int) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &limitListener{Listener: ln, slots: make(chan struct{}, maxConns), done: make(chan struct{})}, nil
}

func (l *limitListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &limitConn{Conn: conn, release: func() { <-l.slots }}, nil
}

func (l *limitListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.Listener.Close()
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
