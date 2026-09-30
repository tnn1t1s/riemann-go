// Package event is the event model from SPEC.md: the ten wire fields, their
// defaults, JSON encoding with an absent metric kept absent, and the
// expression environment an event presents to expr-lang.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Event is one observation about one identity at one time.
type Event struct {
	Host        string
	Service     string
	State       string
	Metric      *float64 // nil means absent, which is distinct from zero
	Time        float64  // seconds since epoch
	TTL         float64  // seconds
	Tags        []string
	Attributes  map[string]string
	Description string
	Source      string

	// Expired is true when the event was synthesized by index expiry rather
	// than ingested. It is not part of the wire schema and is never encoded.
	Expired bool
}

// DefaultTTL is the model default for an ingested event with no ttl (seconds).
const DefaultTTL = 60.0

// ExpiredState is the state carried by a synthesized expiry event.
const ExpiredState = "expired"

var (
	ErrMissingHost    = errors.New("event has no host")
	ErrMissingService = errors.New("event has no service")
)

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

// Decode parses one event object, applying the schema defaults. receiveTime
// is the default for a missing time.
func Decode(raw []byte, receiveTime float64) (*Event, error) {
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, fmt.Errorf("malformed event: %v", err)
	}
	if w.Host == nil || *w.Host == "" {
		return nil, ErrMissingHost
	}
	if w.Service == nil || *w.Service == "" {
		return nil, ErrMissingService
	}
	ev := &Event{Host: *w.Host, Service: *w.Service, Metric: w.Metric, Time: receiveTime, TTL: DefaultTTL}
	if w.State != nil {
		ev.State = *w.State
	}
	if w.Time != nil {
		ev.Time = *w.Time
	}
	if w.TTL != nil {
		ev.TTL = *w.TTL
	}
	if w.Description != nil {
		ev.Description = *w.Description
	}
	if w.Source != nil {
		ev.Source = *w.Source
	}
	ev.Tags = w.Tags
	if ev.Tags == nil {
		ev.Tags = []string{}
	}
	ev.Attributes = w.Attributes
	if ev.Attributes == nil {
		ev.Attributes = map[string]string{}
	}
	return ev, nil
}

type encoded struct {
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

// MarshalJSON encodes the ten wire fields. metric is omitted when absent.
func (e *Event) MarshalJSON() ([]byte, error) {
	tags := e.Tags
	if tags == nil {
		tags = []string{}
	}
	attrs := e.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}
	return json.Marshal(encoded{
		Host: e.Host, Service: e.Service, State: e.State, Metric: e.Metric,
		Time: e.Time, TTL: e.TTL, Tags: tags, Attributes: attrs,
		Description: e.Description, Source: e.Source,
	})
}

// Clone returns a copy whose tags and attributes are independent of the
// original, so a downstream rewrite never alters what a sibling path sees.
func (e *Event) Clone() *Event {
	c := *e
	if e.Metric != nil {
		m := *e.Metric
		c.Metric = &m
	}
	c.Tags = append([]string(nil), e.Tags...)
	if c.Tags == nil {
		c.Tags = []string{}
	}
	c.Attributes = make(map[string]string, len(e.Attributes))
	for k, v := range e.Attributes {
		c.Attributes[k] = v
	}
	return &c
}

// Size is an estimate of the event's memory footprint in bytes, used by the
// ring's byte bound. SPEC-FREE: the ring's representation is the
// implementation's; this counts string payloads plus a fixed overhead.
func (e *Event) Size() int64 {
	n := int64(96) + int64(len(e.Host)+len(e.Service)+len(e.State)+len(e.Description)+len(e.Source))
	for _, t := range e.Tags {
		n += int64(len(t)) + 16
	}
	for k, v := range e.Attributes {
		n += int64(len(k)+len(v)) + 32
	}
	return n
}

// Tagged reports whether name is in the event's tags.
func (e *Event) Tagged(name string) bool {
	for _, t := range e.Tags {
		if t == name {
			return true
		}
	}
	return false
}

// FieldNames are the ten event fields an expression, a by, a set or a stable
// may name. attributes.<key> is additionally accepted by by, set and stable.
var FieldNames = map[string]bool{
	"host": true, "service": true, "state": true, "metric": true, "time": true,
	"ttl": true, "tags": true, "attributes": true, "description": true, "source": true,
}

// ValidFieldName reports whether name is a field a combinator may reference.
func ValidFieldName(name string) bool {
	if FieldNames[name] {
		return true
	}
	return strings.HasPrefix(name, "attributes.") && len(name) > len("attributes.")
}

// FieldKey renders the named field as a string suitable for a fork key or a
// stability comparison. An absent metric renders as a value no number can
// equal, so metric-less events share one fork rather than each taking its own.
func (e *Event) FieldKey(name string) string {
	switch name {
	case "host":
		return e.Host
	case "service":
		return e.Service
	case "state":
		return e.State
	case "metric":
		if e.Metric == nil {
			return "\x00nil"
		}
		return strconv.FormatFloat(*e.Metric, 'g', -1, 64)
	case "time":
		return strconv.FormatFloat(e.Time, 'g', -1, 64)
	case "ttl":
		return strconv.FormatFloat(e.TTL, 'g', -1, 64)
	case "tags":
		return strings.Join(e.Tags, "\x1e")
	case "attributes":
		keys := make([]string, 0, len(e.Attributes))
		for k := range e.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(e.Attributes[k])
			b.WriteByte('\x1e')
		}
		return b.String()
	case "description":
		return e.Description
	case "source":
		return e.Source
	}
	if strings.HasPrefix(name, "attributes.") {
		return e.Attributes[name[len("attributes."):]]
	}
	return ""
}

// Env builds the expression environment for this event. now is the engine's
// clock in float seconds. events, when non-nil, is the coalesce set.
func (e *Event) Env(now float64, events []map[string]any) map[string]any {
	var metric any
	if e.Metric != nil {
		metric = *e.Metric
	}
	tags := e.Tags
	if tags == nil {
		tags = []string{}
	}
	attrs := e.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}
	env := map[string]any{
		"host":        e.Host,
		"service":     e.Service,
		"state":       e.State,
		"metric":      metric,
		"time":        e.Time,
		"ttl":         e.TTL,
		"tags":        tags,
		"attributes":  attrs,
		"description": e.Description,
		"source":      e.Source,
		"tagged":      e.Tagged,
		"now":         now,
		"expired":     e.Expired,
	}
	if events != nil {
		env["events"] = events
	}
	return env
}

// AsMap renders the event as a generic map, which is the element type of the
// coalesce `events` array in expressions.
func (e *Event) AsMap() map[string]any {
	var metric any
	if e.Metric != nil {
		metric = *e.Metric
	}
	return map[string]any{
		"host": e.Host, "service": e.Service, "state": e.State, "metric": metric,
		"time": e.Time, "ttl": e.TTL, "tags": e.Tags, "attributes": e.Attributes,
		"description": e.Description, "source": e.Source, "expired": e.Expired,
	}
}

// ApplyField returns a copy of the event with one field replaced by a value an
// expression produced. Unknown names are ignored; the rule compiler rejects
// them before this runs.
func ApplyField(dst *Event, name string, v any) {
	str := func(v any) string {
		switch x := v.(type) {
		case nil:
			return ""
		case string:
			return x
		default:
			// SPEC-GAP: the spec does not say what a non-string result assigned
			// to a string field becomes. It is rendered with %v.
			return fmt.Sprint(x)
		}
	}
	num := func(v any) (float64, bool) {
		switch x := v.(type) {
		case float64:
			return x, true
		case float32:
			return float64(x), true
		case int:
			return float64(x), true
		case int64:
			return float64(x), true
		case int32:
			return float64(x), true
		case uint:
			return float64(x), true
		case uint64:
			return float64(x), true
		}
		return 0, false
	}
	switch name {
	case "host":
		dst.Host = str(v)
	case "service":
		dst.Service = str(v)
	case "state":
		dst.State = str(v)
	case "description":
		dst.Description = str(v)
	case "source":
		dst.Source = str(v)
	case "metric":
		if f, ok := num(v); ok {
			dst.Metric = &f
		} else {
			// nil, or a non-numeric result, produces an event with no metric.
			dst.Metric = nil
		}
	case "time":
		if f, ok := num(v); ok {
			dst.Time = f
		}
	case "ttl":
		if f, ok := num(v); ok {
			dst.TTL = f
		}
	case "tags":
		switch x := v.(type) {
		case []string:
			dst.Tags = append([]string{}, x...)
		case []any:
			out := make([]string, 0, len(x))
			for _, t := range x {
				out = append(out, str(t))
			}
			dst.Tags = out
		case nil:
			dst.Tags = []string{}
		}
	case "attributes":
		switch x := v.(type) {
		case map[string]string:
			out := make(map[string]string, len(x))
			for k, s := range x {
				out[k] = s
			}
			dst.Attributes = out
		case map[string]any:
			out := make(map[string]string, len(x))
			for k, s := range x {
				out[k] = str(s)
			}
			dst.Attributes = out
		case nil:
			dst.Attributes = map[string]string{}
		}
	default:
		if strings.HasPrefix(name, "attributes.") {
			k := name[len("attributes."):]
			if v == nil {
				delete(dst.Attributes, k)
			} else {
				dst.Attributes[k] = str(v)
			}
		}
	}
}
