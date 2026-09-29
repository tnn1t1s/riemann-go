// Package event holds the event model: one observation about one identity at
// one time, its JSON form, and the field readers the combinators share.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// DefaultTTLSeconds is the ttl an ingested event takes when it names none.
// SPEC.md classifies it as a default carried from the fleet's configuration.
const DefaultTTLSeconds = 60.0

// StateExpired is the state an expiry event carries.
const StateExpired = "expired"

// Event is immutable once built. A node that rewrites one builds a copy.
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

	// Expired is true when index expiry synthesized the event. It is not part
	// of the wire form; expressions read it as `expired`.
	Expired bool
}

// Key is an identity.
type Key struct {
	Host    string
	Service string
}

func (e *Event) Key() Key { return Key{Host: e.Host, Service: e.Service} }

// Deadline is the last instant at which an indexed copy of e is live.
func (e *Event) Deadline() float64 { return e.Time + e.TTL }

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

// eventOverheadBytes is the fixed part of an event's size estimate: the
// struct itself plus slice and map headers. It is a heuristic used only to
// apply shard.ring_bytes, not a measurement of heap use.
const eventOverheadBytes = 160

// Size estimates the bytes an event holds, for the ring's byte bound.
func (e *Event) Size() int {
	n := eventOverheadBytes + len(e.Host) + len(e.Service) + len(e.State) + len(e.Description) + len(e.Source)
	for _, t := range e.Tags {
		n += len(t) + 16
	}
	for k, v := range e.Attributes {
		n += len(k) + len(v) + 32
	}
	return n
}

// wire is the JSON form.
//
// SPEC-GAP: the spec says an absent metric is absent on output but does not
// say whether fields holding their default are written. Chosen: every field
// except metric is always written, so a reader sees one shape; metric is
// omitted when the event carries none.
type wire struct {
	Host        *string           `json:"host"`
	Service     *string           `json:"service"`
	State       *string           `json:"state"`
	Metric      *float64          `json:"metric,omitempty"`
	Time        *float64          `json:"time"`
	TTL         *float64          `json:"ttl"`
	Tags        []string          `json:"tags"`
	Attributes  map[string]string `json:"attributes"`
	Description *string           `json:"description"`
	Source      *string           `json:"source"`
}

// MarshalJSON writes the event object the read surface returns.
func (e *Event) MarshalJSON() ([]byte, error) {
	w := wire{
		Host:        &e.Host,
		Service:     &e.Service,
		State:       &e.State,
		Time:        &e.Time,
		TTL:         &e.TTL,
		Tags:        e.Tags,
		Attributes:  e.Attributes,
		Description: &e.Description,
		Source:      &e.Source,
	}
	if e.HasMetric {
		w.Metric = &e.Metric
	}
	if w.Tags == nil {
		w.Tags = []string{}
	}
	if w.Attributes == nil {
		w.Attributes = map[string]string{}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(w); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// ErrIdentity reports an event with no host or no service.
var ErrIdentity = errors.New("event has no host or no service")

func fromWire(w *wire, receiveTime float64) (*Event, error) {
	// SPEC-GAP: the spec rejects an event with "no host"; it does not say
	// whether an empty string counts. Chosen: it does, because SEMANTICS.md
	// describes an empty host as an event with no usable identity that ingest
	// rejects at the door.
	if w.Host == nil || *w.Host == "" {
		return nil, fmt.Errorf("%w: host is required", ErrIdentity)
	}
	if w.Service == nil || *w.Service == "" {
		return nil, fmt.Errorf("%w: service is required", ErrIdentity)
	}
	e := &Event{
		Host:       *w.Host,
		Service:    *w.Service,
		Time:       receiveTime,
		TTL:        DefaultTTLSeconds,
		Tags:       w.Tags,
		Attributes: w.Attributes,
	}
	if w.State != nil {
		e.State = *w.State
	}
	if w.Metric != nil {
		e.Metric, e.HasMetric = *w.Metric, true
	}
	if w.Time != nil {
		e.Time = *w.Time
	}
	// SPEC-GAP: SEMANTICS.md open question 7, whether a zero or negative ttl
	// is accepted at ingest. Chosen: accepted unchanged, as upstream does.
	if w.TTL != nil {
		e.TTL = *w.TTL
	}
	if w.Description != nil {
		e.Description = *w.Description
	}
	if w.Source != nil {
		e.Source = *w.Source
	}
	return e, nil
}

// ErrBatchTooLarge reports a batch holding more events than the caller allows.
var ErrBatchTooLarge = errors.New("batch holds too many events")

// DecodeBatch reads the body of POST /events: a JSON array of event objects,
// or one event object, which is a batch of one. receiveTime fills an absent
// time. A batch of more than maxEvents is ErrBatchTooLarge, decided before
// any event in it is validated.
//
// SPEC-GAP: the spec names 400 for a missing host or service and is silent on
// a body that is not JSON, or whose fields have the wrong type. Chosen: both
// are errors the caller answers with 400. Keys the event model does not name
// are ignored.
func DecodeBatch(body []byte, receiveTime float64, maxEvents int) ([]*Event, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, errors.New("empty body: expected a JSON array of events or one event object")
	}
	var wires []wire
	switch trimmed[0] {
	case '[':
		if err := json.Unmarshal(trimmed, &wires); err != nil {
			return nil, fmt.Errorf("malformed batch: %v", err)
		}
	case '{':
		var one wire
		if err := json.Unmarshal(trimmed, &one); err != nil {
			return nil, fmt.Errorf("malformed event: %v", err)
		}
		wires = []wire{one}
	default:
		return nil, errors.New("expected a JSON array of events or one event object")
	}
	if len(wires) > maxEvents {
		return nil, fmt.Errorf("%w: %d events, limit %d", ErrBatchTooLarge, len(wires), maxEvents)
	}
	out := make([]*Event, 0, len(wires))
	for i := range wires {
		e, err := fromWire(&wires[i], receiveTime)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// FieldNames lists the ten event fields, which are also the top-level names
// an expression may read.
var FieldNames = []string{"host", "service", "state", "metric", "time", "ttl", "tags", "attributes", "description", "source"}

// IsField reports whether name is one of the ten event fields.
func IsField(name string) bool {
	for _, f := range FieldNames {
		if f == name {
			return true
		}
	}
	return false
}

// FormatFloat renders a number the way the JSON encoder does.
func FormatFloat(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	b, err := json.Marshal(f)
	if err != nil {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return string(b)
}

// absentMetricKey is what a field reader returns for an event with no metric.
// It begins with a byte no formatted number contains.
const absentMetricKey = "\x00absent"

// Reader returns a function reading the named field as a string, for forming
// a fork key or comparing a watched value.
//
// SPEC-GAP: the spec calls `fields` and `field` event field names, while
// SEMANTICS.md writes `by ["attributes.run_id"]`. Chosen: the ten field names
// plus `attributes.<key>` for one attribute. SEMANTICS.md open question on
// `by ["metric"]`: an absent metric reads as a value distinct from every
// number, so metric-less events share one fork.
func Reader(name string) (func(*Event) string, error) {
	if key, ok := strings.CutPrefix(name, "attributes."); ok && key != "" {
		return func(e *Event) string { return e.Attributes[key] }, nil
	}
	switch name {
	case "host":
		return func(e *Event) string { return e.Host }, nil
	case "service":
		return func(e *Event) string { return e.Service }, nil
	case "state":
		return func(e *Event) string { return e.State }, nil
	case "description":
		return func(e *Event) string { return e.Description }, nil
	case "source":
		return func(e *Event) string { return e.Source }, nil
	case "metric":
		return func(e *Event) string {
			if !e.HasMetric {
				return absentMetricKey
			}
			return FormatFloat(e.Metric)
		}, nil
	case "time":
		return func(e *Event) string { return FormatFloat(e.Time) }, nil
	case "ttl":
		return func(e *Event) string { return FormatFloat(e.TTL) }, nil
	case "tags":
		return func(e *Event) string {
			b, _ := json.Marshal(e.Tags)
			return string(b)
		}, nil
	case "attributes":
		return func(e *Event) string {
			keys := make([]string, 0, len(e.Attributes))
			for k := range e.Attributes {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var sb strings.Builder
			for _, k := range keys {
				sb.WriteString(strconv.Quote(k))
				sb.WriteByte('=')
				sb.WriteString(strconv.Quote(e.Attributes[k]))
				sb.WriteByte(',')
			}
			return sb.String()
		}, nil
	}
	return nil, fmt.Errorf("unknown event field %q", name)
}
