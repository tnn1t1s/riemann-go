// Package ntfy is the adapter behind a {"sink":"ntfy"} leaf. It owns the
// alert shape SPEC.md pins and one bounded queue.
package ntfy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/rules"
	"github.com/tnn1t1s/riemann-go/sinkqueue"
)

// deliveryTimeout bounds one publish, so a sink that accepts a connection and
// never answers cannot hold the worker, and with it the queue, forever. Ten
// seconds is a default with no measurement behind it; the observation that
// would revise it is the fleet ntfy server's publish latency under load.
const deliveryTimeout = 10 * time.Second

// Sink publishes one alert per firing.
type Sink struct {
	url    string
	topic  string
	queue  *sinkqueue.Queue[rules.Firing]
	client *http.Client
}

// New returns a sink publishing to the root of baseURL on topic, behind a
// queue of the given capacity (sink.ntfy.queue_capacity).
func New(baseURL, topic string, capacity int) *Sink {
	return &Sink{
		url:    strings.TrimRight(baseURL, "/") + "/",
		topic:  topic,
		queue:  sinkqueue.New[rules.Firing](capacity),
		client: &http.Client{Timeout: deliveryTimeout},
	}
}

// Offer enqueues a firing or sheds it; it never blocks.
func (s *Sink) Offer(f rules.Firing) { s.queue.Offer(f) }

// Stats reads the queue.
func (s *Sink) Stats() sinkqueue.Stats { return s.queue.Stats() }

// Run publishes until ctx is done. Alerts do not batch: one POST per firing.
func (s *Sink) Run(ctx context.Context) {
	s.queue.Run(ctx, 1, func(ctx context.Context, batch []rules.Firing) error {
		err := s.publish(ctx, batch[0])
		if err != nil && ctx.Err() == nil {
			// A debugging artifact only; the loss itself is in `dropped`.
			log.Printf("ntfy sink: publish failed, firing dropped: %v", err)
		}
		return err
	})
}

// provenance is the object embedded in the message. Field order is the
// contract's key order.
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

// Body renders the publish body for one firing.
func Body(topic string, f rules.Firing) ([]byte, error) {
	ev := f.Event
	p := provenance{Rule: f.Rule.ID, Version: f.Rule.Version, Owner: f.Rule.Owner,
		Host: ev.Host, Service: ev.Service, State: ev.State, PriorState: f.PriorState, Node: f.Node}
	// SPEC-GAP: the human line is "<host> <service> is <state> (<metric>)" and
	// the spec does not say what stands in for an absent metric there. Chosen:
	// the word null, matching the provenance object beneath it.
	human := "null"
	if ev.HasMetric && !math.IsNaN(ev.Metric) && !math.IsInf(ev.Metric, 0) {
		m := ev.Metric
		p.Metric = &m
		human = strconv.FormatFloat(m, 'f', -1, 64)
	}
	prov, err := marshalCompact(p)
	if err != nil {
		return nil, err
	}
	return marshalCompact(publishBody{
		Topic:    topic,
		Title:    ev.Host + " " + ev.Service + " " + ev.State,
		Message:  fmt.Sprintf("%s %s is %s (%s)\nriemann-go: %s", ev.Host, ev.Service, ev.State, human, prov),
		Priority: Priority(ev.State),
		Tags:     []string{"rule:" + f.Rule.ID, "owner:" + f.Rule.Owner},
	})
}

func marshalCompact(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}

// Priority maps a state to an ntfy priority, the mapping the fleet's Clojure
// sink uses.
func Priority(state string) int {
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

func (s *Sink) publish(ctx context.Context, f rules.Firing) error {
	body, err := Body(s.topic, f)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("ntfy replied %s", resp.Status)
	}
	return nil
}
