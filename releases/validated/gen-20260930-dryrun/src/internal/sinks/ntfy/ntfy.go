// Package ntfy is the adapter that turns a firing into one ntfy publish with
// the alert shape SPEC.md pins.
package ntfy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
)

// Client publishes to one topic on one ntfy server.
type Client struct {
	URL   string
	Topic string
	HTTP  *http.Client
}

// New returns a client for the given base URL and topic.
func New(url, topic string) *Client {
	return &Client{URL: strings.TrimRight(url, "/"), Topic: topic, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

// Priority maps an event state to an ntfy priority, per SPEC.md.
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

// Body renders the publish body. Keys are exactly topic, title, message,
// priority and tags; provenance rides inside message as a compact JSON
// object with keys in the order SPEC.md fixes.
func (c *Client) Body(f engine.Firing) ([]byte, error) {
	ev := f.Event
	title := fmt.Sprintf("%s %s %s", ev.Host, ev.Service, ev.State)
	human := fmt.Sprintf("%s %s is %s", ev.Host, ev.Service, ev.State)
	if ev.Metric != nil {
		human += " (" + strconv.FormatFloat(*ev.Metric, 'g', -1, 64) + ")"
	}
	var prov bytes.Buffer
	prov.WriteString(`{"rule":`)
	writeJSON(&prov, f.Rule)
	prov.WriteString(`,"version":`)
	writeJSON(&prov, f.Version)
	prov.WriteString(`,"owner":`)
	writeJSON(&prov, f.Owner)
	prov.WriteString(`,"host":`)
	writeJSON(&prov, ev.Host)
	prov.WriteString(`,"service":`)
	writeJSON(&prov, ev.Service)
	prov.WriteString(`,"state":`)
	writeJSON(&prov, ev.State)
	prov.WriteString(`,"metric":`)
	if ev.Metric != nil {
		writeJSON(&prov, *ev.Metric)
	} else {
		prov.WriteString("null")
	}
	prov.WriteString(`,"prior_state":`)
	if f.PriorState != nil {
		writeJSON(&prov, *f.PriorState)
	} else {
		prov.WriteString("null")
	}
	prov.WriteString(`,"node":`)
	writeJSON(&prov, f.Node)
	prov.WriteString("}")

	body := struct {
		Topic    string   `json:"topic"`
		Title    string   `json:"title"`
		Message  string   `json:"message"`
		Priority int      `json:"priority"`
		Tags     []string `json:"tags"`
	}{
		Topic:    c.Topic,
		Title:    title,
		Message:  human + "\nriemann-go: " + prov.String(),
		Priority: Priority(ev.State),
		Tags:     []string{"rule:" + f.Rule, "owner:" + f.Owner},
	}
	return json.Marshal(body)
}

func writeJSON(b *bytes.Buffer, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		b.WriteString("null")
		return
	}
	b.Write(raw)
}

// Deliver publishes each firing as one POST to the server root. It is the
// sink queue's delivery function; the queue drains ntfy one firing at a time.
func (c *Client) Deliver(fs []engine.Firing) error {
	for _, f := range fs {
		body, err := c.Body(f)
		if err != nil {
			return err
		}
		req, err := http.NewRequest(http.MethodPost, c.URL+"/", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("ntfy: %s", resp.Status)
		}
	}
	return nil
}
