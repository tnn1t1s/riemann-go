// Package ntfy is the adapter that publishes firings to an ntfy topic. It
// imports the core and no other adapter.
package ntfy

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

// requestTimeout bounds one publish, in seconds of wall time. It is a
// heuristic: long enough for a slow ntfy server to answer, short enough that
// a dead one costs the queue one timeout per firing rather than a stalled
// worker. Nothing has measured the fleet's ntfy latency.
const requestTimeout = 10 * time.Second

// Sink publishes each firing as one POST to the ntfy root.
//
// SPEC-GAP: the spec does not say what the sink does when a publish fails.
// Chosen: no retry. A publish the server answered, with any status, counts as
// processed, since the far end observed it; one that got no answer counts as
// dropped.
type Sink struct {
	url    string
	topic  string
	queue  *engine.SinkQueue
	client *http.Client
	stop   chan struct{}
	wg     sync.WaitGroup
}

// New returns a sink posting to url for topic, behind a queue of the given
// capacity. Nothing is sent until Start.
func New(url, topic string, capacity int) *Sink {
	return &Sink{
		url:    url,
		topic:  topic,
		queue:  engine.NewSinkQueue(capacity),
		client: &http.Client{Timeout: requestTimeout},
		stop:   make(chan struct{}),
	}
}

func (s *Sink) Offer(f rule.Firing) { s.queue.Offer(f) }

func (s *Sink) Stats() engine.SinkStats { return s.queue.Stats() }

// Start launches the worker.
func (s *Sink) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			batch := s.queue.Take(1, s.stop)
			if batch == nil {
				return
			}
			s.queue.Done(1, s.publish(batch[0]))
		}
	}()
}

// Stop ends the worker after the publish in hand.
func (s *Sink) Stop() {
	close(s.stop)
	s.wg.Wait()
}

func (s *Sink) publish(f rule.Firing) bool {
	body, err := Body(s.topic, f)
	if err != nil {
		log.Printf("ntfy: encoding firing of rule %s: %v", f.Rule, err)
		return false
	}
	req, err := http.NewRequest(http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		log.Printf("ntfy: building request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("ntfy: publish failed: %v", err)
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("ntfy: publish answered %s", resp.Status)
	}
	return true
}

// priority maps a state to an ntfy priority, as the fleet's Clojure sink does.
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

// emoji is the ntfy short code the fleet's Clojure sink attaches per state.
func emoji(state string) string {
	switch state {
	case "ok":
		return "white_check_mark"
	case "warning":
		return "warning"
	case "error", "critical", "emergency":
		return "rotating_light"
	case "expired":
		return "hourglass"
	}
	return ""
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// provenance writes the compact object that follows "riemann-go: ", with its
// nine keys in the order the spec fixes.
func provenance(f rule.Firing) ([]byte, error) {
	var metric any
	if f.Event.HasMetric {
		metric = f.Event.Metric
	}
	var prior any
	if f.PriorState != nil {
		prior = *f.PriorState
	}
	fields := []struct {
		key   string
		value any
	}{
		{"rule", f.Rule},
		{"version", f.Version},
		{"owner", f.Owner},
		{"host", f.Event.Host},
		{"service", f.Event.Service},
		{"state", f.Event.State},
		{"metric", metric},
		{"prior_state", prior},
		{"node", f.Node},
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, field := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := encode(field.key)
		if err != nil {
			return nil, err
		}
		value, err := encode(field.value)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(value)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

type envelope struct {
	Topic    string   `json:"topic"`
	Title    string   `json:"title"`
	Message  string   `json:"message"`
	Priority int      `json:"priority"`
	Tags     []string `json:"tags"`
}

// Body renders the request body for one firing.
func Body(topic string, f rule.Firing) ([]byte, error) {
	ev := f.Event
	prov, err := provenance(f)
	if err != nil {
		return nil, err
	}
	// SPEC-GAP: the human line is "<host> <service> is <state> (<metric>)"
	// and the spec does not say what <metric> is for an event with none.
	// Chosen: nothing between the parentheses, as the fleet's Clojure sink
	// renders a nil metric.
	metric := ""
	if ev.HasMetric {
		metric = event.FormatFloat(ev.Metric)
	}
	// SPEC-GAP: the spec requires two tags and allows more without naming
	// them. Chosen: the two required tags, then the state's emoji short code
	// when the state has one, then the event's own tags.
	tags := []string{"rule:" + f.Rule, "owner:" + f.Owner}
	if code := emoji(ev.State); code != "" {
		tags = append(tags, code)
	}
	tags = append(tags, ev.Tags...)
	return encode(envelope{
		Topic:    topic,
		Title:    ev.Host + " " + ev.Service + " " + ev.State,
		Message:  ev.Host + " " + ev.Service + " is " + ev.State + " (" + metric + ")\nriemann-go: " + string(prov),
		Priority: priority(ev.State),
		Tags:     tags,
	})
}
