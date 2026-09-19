// Package ntfy is the adapter behind {"sink":"ntfy"}: a bounded shed-newest
// queue and one worker that publishes each firing to the ntfy server.
package ntfy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/boundedq"
	"github.com/tnn1t1s/riemann-go/rule"
)

// requestTimeout bounds one publish, in seconds of wall time, so a server
// that accepts a connection and never answers cannot hold the worker, and
// through it the queue, forever. Default; chosen to sit well above a healthy
// publish and has not been measured against the fleet's server.
const requestTimeout = 10 * time.Second

// Config is everything the sink needs. There are no defaults for a URL or a
// topic; the caller has already refused to start without them.
type Config struct {
	URL           string // --ntfy-url, the server root
	Topic         string // --ntfy-topic
	QueueCapacity int    // sink.ntfy.queue_capacity, events
}

// Sink implements rule.Sink.
type Sink struct {
	cfg    Config
	q      *boundedq.Queue[rule.Firing]
	client *http.Client
}

// New builds the sink. Nothing talks to the server until Start.
func New(cfg Config) *Sink {
	return &Sink{cfg: cfg, q: boundedq.New[rule.Firing](cfg.QueueCapacity), client: &http.Client{Timeout: requestTimeout}}
}

// Start launches the worker.
func (s *Sink) Start() { go s.work() }

// Offer never blocks; a full queue sheds the firing and counts it.
func (s *Sink) Offer(f rule.Firing) { s.q.Offer(f) }

// Stats reads the queue.
func (s *Sink) Stats() rule.SinkStats { return rule.SinkStats(s.q.Stats()) }

func (s *Sink) work() {
	for {
		f := s.q.Take(1)[0]
		s.q.Done(1, s.publish(f))
	}
}

// provenance is the object embedded in message. Field order is the contract.
type provenance struct {
	Rule       string   `json:"rule"`
	Version    int      `json:"version"`
	Owner      string   `json:"owner"`
	Host       string   `json:"host"`
	Service    string   `json:"service"`
	State      string   `json:"state"`
	Metric     *float64 `json:"metric"`
	PriorState *string  `json:"prior_state"`
	Node       string   `json:"node"`
}

type publishBody struct {
	Topic    string   `json:"topic"`
	Title    string   `json:"title"`
	Message  string   `json:"message"`
	Priority int      `json:"priority"`
	Tags     []string `json:"tags"`
}

// priority is the mapping the fleet's Clojure sink uses.
func priority(state string) int {
	switch state {
	case "ok", "info":
		return 2
	case "warning":
		return 4
	case "error", "critical", "emergency":
		return 5
	}
	return 3
}

func compactJSON(v any) (string, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// Body renders the publish for one firing.
// SPEC-GAP: the human line writes an absent metric as "null", matching the
// provenance object; the spec fixes only "(<metric>)".
func Body(topic string, f rule.Firing) ([]byte, error) {
	ev := f.Event
	p := provenance{Rule: f.Rule, Version: f.Version, Owner: f.Owner, Host: ev.Host, Service: ev.Service,
		State: ev.State, PriorState: f.PriorState, Node: f.Node}
	metric := "null"
	if ev.HasMetric {
		m := ev.Metric
		p.Metric = &m
		text, err := compactJSON(m)
		if err != nil {
			return nil, err
		}
		metric = text
	}
	prov, err := compactJSON(p)
	if err != nil {
		return nil, err
	}
	body := publishBody{
		Topic:    topic,
		Title:    fmt.Sprintf("%s %s %s", ev.Host, ev.Service, ev.State),
		Message:  fmt.Sprintf("%s %s is %s (%s)\nriemann-go: %s", ev.Host, ev.Service, ev.State, metric, prov),
		Priority: priority(ev.State),
		Tags:     []string{"rule:" + f.Rule, "owner:" + f.Owner},
	}
	return json.Marshal(body)
}

// publish makes one attempt and reports whether the server observed it.
// SPEC-GAP: delivery is at most once with no retry, since a retry after an
// ambiguous failure could produce a second ntfy_post for one firing. A request
// that got any HTTP response counts as processed; one that got none counts as
// dropped, which keeps the accounting identity exact either way.
func (s *Sink) publish(f rule.Firing) bool {
	body, err := Body(s.cfg.Topic, f)
	if err != nil {
		log.Printf("ntfy: encode: %v", err)
		return false
	}
	req, err := http.NewRequest(http.MethodPost, s.cfg.URL, bytes.NewReader(body))
	if err != nil {
		log.Printf("ntfy: request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("ntfy: publish: %v", err)
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("ntfy: publish: status %d", resp.StatusCode)
	}
	return true
}
