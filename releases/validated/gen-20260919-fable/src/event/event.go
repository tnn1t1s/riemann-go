// Package event holds the event model: one observation about one identity at
// one time, its JSON wire form, and the validation ingest applies.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// DefaultTTL is the lease, in seconds, of an ingested event that carries no
// ttl. SPEC.md classifies it as a default carried from the fleet's Clojure
// configuration.
const DefaultTTL = 60.0

// StateExpired is the state an index expiry event carries.
const StateExpired = "expired"

// Event is immutable once admitted. A combinator that changes a field makes a
// copy with Clone.
type Event struct {
	Host        string
	Service     string
	State       string
	Metric      float64
	HasMetric   bool // an absent metric is distinct from 0
	Time        float64
	TTL         float64
	Tags        []string
	Attributes  map[string]string
	Description string
	Source      string

	// Expired is true when the index synthesized this event. It is engine
	// state, not wire content.
	Expired bool
}

// Clone returns a shallow copy. Tags and Attributes are shared until replaced.
func (e *Event) Clone() *Event {
	c := *e
	return &c
}

// Size estimates the bytes an event holds, for the ring's byte bound.
func (e *Event) Size() int64 {
	// eventOverheadBytes approximates the struct, its slice and map headers.
	const eventOverheadBytes = 160
	n := eventOverheadBytes + len(e.Host) + len(e.Service) + len(e.State) + len(e.Description) + len(e.Source)
	for _, t := range e.Tags {
		n += len(t) + 16
	}
	for k, v := range e.Attributes {
		n += len(k) + len(v) + 32
	}
	return int64(n)
}

type wire struct {
	Host        string            `json:"host"`
	Service     string            `json:"service"`
	State       string            `json:"state"`
	Metric      *float64          `json:"metric,omitempty"`
	Time        float64           `json:"time"`
	TTL         float64           `json:"ttl"`
	Tags        []string          `json:"tags"`
	Attributes  map[string]string `json:"attributes"`
	Description string            `json:"description"`
	Source      string            `json:"source"`
}

// MarshalJSON writes every field except metric, which is absent when unset.
// SPEC-GAP: the spec does not say whether defaulted fields are written on the
// read surface; every field but metric is always present.
func (e *Event) MarshalJSON() ([]byte, error) {
	w := wire{Host: e.Host, Service: e.Service, State: e.State, Time: e.Time, TTL: e.TTL,
		Tags: e.Tags, Attributes: e.Attributes, Description: e.Description, Source: e.Source}
	if e.HasMetric && !math.IsNaN(e.Metric) && !math.IsInf(e.Metric, 0) {
		m := e.Metric
		w.Metric = &m
	}
	if w.Tags == nil {
		w.Tags = []string{}
	}
	if w.Attributes == nil {
		w.Attributes = map[string]string{}
	}
	return json.Marshal(w)
}

// SplitBatch separates a POST /events body into raw event objects. The body is
// an array of events, or one event object, which is a batch of one.
func SplitBatch(body []byte) ([]json.RawMessage, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, errors.New("empty body")
	}
	switch trimmed[0] {
	case '[':
		var raws []json.RawMessage
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, fmt.Errorf("body is not a JSON array of events: %v", err)
		}
		return raws, nil
	case '{':
		if !json.Valid(trimmed) {
			return nil, errors.New("body is not valid JSON")
		}
		return []json.RawMessage{json.RawMessage(trimmed)}, nil
	}
	return nil, errors.New("body must be a JSON array of events or one event object")
}

// Parse validates one event object. now stamps an event that carries no time.
// SPEC-GAP: keys outside the event model are ignored rather than rejected.
// SPEC-GAP: a field of the wrong JSON type is a validation failure; the spec
// names only a missing host or service.
// SPEC-GAP: a zero or negative ttl is accepted (SEMANTICS.md open question 7).
func Parse(raw json.RawMessage, now float64) (*Event, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, errors.New("event is not a JSON object")
	}
	e := &Event{Time: now, TTL: DefaultTTL}
	var err error
	if e.Host, err = str(m, "host"); err != nil {
		return nil, err
	}
	if e.Service, err = str(m, "service"); err != nil {
		return nil, err
	}
	if e.Host == "" {
		return nil, errors.New("event has no host")
	}
	if e.Service == "" {
		return nil, errors.New("event has no service")
	}
	if e.State, err = str(m, "state"); err != nil {
		return nil, err
	}
	if e.Description, err = str(m, "description"); err != nil {
		return nil, err
	}
	if e.Source, err = str(m, "source"); err != nil {
		return nil, err
	}
	if v, ok, err := num(m, "metric"); err != nil {
		return nil, err
	} else if ok {
		e.Metric, e.HasMetric = v, true
	}
	if v, ok, err := num(m, "time"); err != nil {
		return nil, err
	} else if ok {
		e.Time = v
	}
	if v, ok, err := num(m, "ttl"); err != nil {
		return nil, err
	} else if ok {
		e.TTL = v
	}
	if r, ok := present(m, "tags"); ok {
		if err := json.Unmarshal(r, &e.Tags); err != nil {
			return nil, errors.New(`field "tags" must be an array of strings`)
		}
	}
	if r, ok := present(m, "attributes"); ok {
		if err := json.Unmarshal(r, &e.Attributes); err != nil {
			return nil, errors.New(`field "attributes" must be an object of string to string`)
		}
	}
	return e, nil
}

func present(m map[string]json.RawMessage, key string) (json.RawMessage, bool) {
	r, ok := m[key]
	if !ok || bytes.Equal(bytes.TrimSpace(r), []byte("null")) {
		return nil, false
	}
	return r, true
}

func str(m map[string]json.RawMessage, key string) (string, error) {
	r, ok := present(m, key)
	if !ok {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(r, &s); err != nil {
		return "", fmt.Errorf("field %q must be a string", key)
	}
	return s, nil
}

func num(m map[string]json.RawMessage, key string) (float64, bool, error) {
	r, ok := present(m, key)
	if !ok {
		return 0, false, nil
	}
	var f float64
	if err := json.Unmarshal(r, &f); err != nil {
		return 0, false, fmt.Errorf("field %q must be a number", key)
	}
	return f, true, nil
}
