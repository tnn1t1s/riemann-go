// Package event holds the event model of SPEC.md and its JSON wire form.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// DefaultTTL is the ttl, in seconds, an ingested event gets when it carries
// none. A default carried from the fleet's Clojure configuration (SPEC.md,
// event model).
const DefaultTTL = 60.0

// StateExpired is the state of an expiry event.
const StateExpired = "expired"

// Event is one observation about one identity at one time. An Event is
// immutable once it has been handed to the engine; combinators that rewrite
// build a copy.
type Event struct {
	Host        string
	Service     string
	State       string
	Metric      float64
	HasMetric   bool
	Time        float64
	TTL         float64
	Tags        []string
	Attributes  map[string]string
	Description string
	Source      string

	// Expired is true when the event was synthesized by index expiry rather
	// than ingested. It backs the `expired` expression name and is not on the
	// wire.
	Expired bool
}

// Fields are the ten event field names an expression, a `by`, a `stable` or a
// `set` may name.
var Fields = []string{"host", "service", "state", "metric", "time", "ttl", "tags", "attributes", "description", "source"}

// IsField reports whether name is one of the ten event fields.
func IsField(name string) bool {
	for _, f := range Fields {
		if f == name {
			return true
		}
	}
	return false
}

// Clone returns a copy that shares no mutable storage with e.
func (e *Event) Clone() *Event {
	c := *e
	if e.Tags != nil {
		c.Tags = append([]string(nil), e.Tags...)
	}
	if e.Attributes != nil {
		c.Attributes = make(map[string]string, len(e.Attributes))
		for k, v := range e.Attributes {
			c.Attributes[k] = v
		}
	}
	return &c
}

// SizeBytes estimates the memory an event holds, for the ring's byte bound.
func (e *Event) SizeBytes() int {
	// eventFixedBytes is an estimate of the struct, slice and map headers.
	const eventFixedBytes = 160
	n := eventFixedBytes + len(e.Host) + len(e.Service) + len(e.State) + len(e.Description) + len(e.Source)
	for _, t := range e.Tags {
		n += len(t) + 16
	}
	for k, v := range e.Attributes {
		n += len(k) + len(v) + 32
	}
	return n
}

// FieldKey renders one field of the event as a string usable as part of a
// fork key or as a `stable` comparison value. name is an event field or
// `attributes.<key>`.
func (e *Event) FieldKey(name string) string {
	switch name {
	case "host":
		return e.Host
	case "service":
		return e.Service
	case "state":
		return e.State
	case "description":
		return e.Description
	case "source":
		return e.Source
	case "metric":
		// SPEC-GAP: an absent metric has no default, so `by ["metric"]` and
		// `stable` on `metric` are ill-defined (SEMANTICS open question).
		// Chosen: every metric-less event shares one key distinct from any number.
		if !e.HasMetric {
			return "\x00absent"
		}
		return strconv.FormatFloat(e.Metric, 'g', -1, 64)
	case "time":
		return strconv.FormatFloat(e.Time, 'g', -1, 64)
	case "ttl":
		return strconv.FormatFloat(e.TTL, 'g', -1, 64)
	case "tags":
		return strings.Join(e.Tags, "\x1f")
	case "attributes":
		keys := make([]string, 0, len(e.Attributes))
		for k := range e.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(k)
			b.WriteByte('\x1f')
			b.WriteString(e.Attributes[k])
			b.WriteByte('\x1e')
		}
		return b.String()
	}
	if k, ok := strings.CutPrefix(name, "attributes."); ok {
		return e.Attributes[k]
	}
	return ""
}

// ValidFieldRef reports whether name can be passed to FieldKey.
func ValidFieldRef(name string) bool {
	if IsField(name) {
		return true
	}
	k, ok := strings.CutPrefix(name, "attributes.")
	return ok && k != ""
}

// MarshalJSON writes the wire form. `metric` is absent when the event carries
// none; every other field is always present with its value.
func (e *Event) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	writeKV := func(k string, v any, first bool) {
		if !first {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		vb, err := json.Marshal(v)
		if err != nil {
			vb = []byte("null")
		}
		b.Write(vb)
	}
	writeKV("host", e.Host, true)
	writeKV("service", e.Service, false)
	writeKV("state", e.State, false)
	if e.HasMetric {
		writeKV("metric", jsonFloat(e.Metric), false)
	}
	writeKV("time", jsonFloat(e.Time), false)
	writeKV("ttl", jsonFloat(e.TTL), false)
	tags := e.Tags
	if tags == nil {
		tags = []string{}
	}
	writeKV("tags", tags, false)
	attrs := e.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}
	writeKV("attributes", attrs, false)
	writeKV("description", e.Description, false)
	writeKV("source", e.Source, false)
	b.WriteByte('}')
	return b.Bytes(), nil
}

// jsonFloat marshals a float that JSON cannot carry (NaN, Inf) as null
// instead of failing the whole document.
type jsonFloat float64

func (f jsonFloat) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(float64(f))
	if err != nil {
		return []byte("null"), nil
	}
	return b, nil
}

// wire is the decode target; pointers distinguish absent from zero.
type wire struct {
	Host        *string           `json:"host"`
	Service     *string           `json:"service"`
	State       *string           `json:"state"`
	Metric      *float64          `json:"metric"`
	Time        *float64          `json:"time"`
	TTL         *float64          `json:"ttl"`
	Tags        []string          `json:"tags"`
	Attributes  map[string]string `json:"attributes"`
	Description *string           `json:"description"`
	Source      *string           `json:"source"`
}

// ErrNoIdentity marks an event with no host or no service.
var ErrNoIdentity = errors.New("event has no host or no service")

// DecodeBatch parses the body of POST /events: a JSON array of event objects,
// or one event object, which is a batch of one. receiveTime fills an absent
// `time`. maxEvents bounds the array; exceeding it returns ErrTooMany before
// any event is validated.
func DecodeBatch(body []byte, receiveTime float64, maxEvents int) ([]*Event, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, errors.New("empty body")
	}
	var raws []json.RawMessage
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, fmt.Errorf("body is not a JSON array of events: %w", err)
		}
	} else {
		raws = []json.RawMessage{trimmed}
	}
	if len(raws) > maxEvents {
		return nil, ErrTooMany
	}
	out := make([]*Event, 0, len(raws))
	for i, raw := range raws {
		ev, err := decodeOne(raw, receiveTime)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		out = append(out, ev)
	}
	return out, nil
}

// ErrTooMany marks a batch over ingest.max_batch_events.
var ErrTooMany = errors.New("batch exceeds ingest.max_batch_events")

func decodeOne(raw json.RawMessage, receiveTime float64) (*Event, error) {
	var w wire
	// SPEC-GAP: the spec does not say whether an unknown event key is an
	// error. Chosen: ignored, so an emitter may carry extra keys.
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, fmt.Errorf("not an event object: %w", err)
	}
	// SPEC-GAP: the spec says an event with "no host" is a 400 and does not
	// say whether an empty string counts. Chosen: empty is the same as absent,
	// because an empty identity is unusable everywhere downstream.
	if w.Host == nil || *w.Host == "" || w.Service == nil || *w.Service == "" {
		return nil, ErrNoIdentity
	}
	ev := &Event{Host: *w.Host, Service: *w.Service, Time: receiveTime, TTL: DefaultTTL,
		Tags: w.Tags, Attributes: w.Attributes}
	if w.State != nil {
		ev.State = *w.State
	}
	if w.Metric != nil {
		ev.Metric, ev.HasMetric = *w.Metric, true
	}
	if w.Time != nil {
		ev.Time = *w.Time
	}
	if w.TTL != nil {
		// SPEC-GAP: a zero or negative ttl is neither rejected nor clamped
		// (SEMANTICS open question 7). Chosen: accepted as sent, as upstream does.
		ev.TTL = *w.TTL
	}
	if w.Description != nil {
		ev.Description = *w.Description
	}
	if w.Source != nil {
		ev.Source = *w.Source
	}
	return ev, nil
}
