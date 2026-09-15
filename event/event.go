// Package event holds the Event type, its JSON codec, and the canonical
// (host, service) key. It imports only the standard library.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultTTL is the ttl assigned to an event that arrives without one.
// Parameter: event.default_ttl. Owner: engine. Units: seconds. Default 60,
// carried from the fleet's Riemann config and upstream index.clj default-ttl;
// provisional until a rule is observed to depend on expiry timing.
const DefaultTTL = 60.0

// StateExpired is the state of an event produced by index expiry.
const StateExpired = "expired"

// Event is the unit of the stream. Time and TTL are float seconds; Metric is
// a pointer so that an absent metric stays distinct from 0.
type Event struct {
	Host        string            `json:"host"`
	Service     string            `json:"service"`
	State       string            `json:"state,omitempty"`
	Metric      *float64          `json:"metric,omitempty"`
	Time        float64           `json:"time"`
	TTL         float64           `json:"ttl"`
	Tags        []string          `json:"tags,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Description string            `json:"description,omitempty"`
	Source      string            `json:"source,omitempty"`
}

// Key is the index identity of an event.
type Key struct {
	Host    string
	Service string
}

func (e Event) Key() Key { return Key{e.Host, e.Service} }

// Tagged reports whether the event carries tag name.
func (e Event) Tagged(name string) bool {
	for _, t := range e.Tags {
		if t == name {
			return true
		}
	}
	return false
}

// Expired reports whether the event's state is "expired", whether the reaper
// synthesised it or a client sent it.
func (e Event) Expired() bool { return e.State == StateExpired }

// At converts the event's float time to a time.Time.
func (e Event) At() time.Time { return FromSeconds(e.Time) }

// Seconds converts a time.Time to float seconds since the epoch.
func Seconds(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// FromSeconds converts float seconds since the epoch to a time.Time.
func FromSeconds(s float64) time.Time { return time.Unix(0, int64(s*1e9)) }

// Size is an estimate of the event's resident bytes, used to bound the ring.
// It counts string payloads plus a fixed overhead for the struct, slice and
// map headers; it is not an allocation measurement.
func (e Event) Size() int {
	const structOverhead = 160 // bytes; approximate sizeof(Event) plus headers
	n := structOverhead + len(e.Host) + len(e.Service) + len(e.State) + len(e.Description) + len(e.Source)
	for _, t := range e.Tags {
		n += len(t) + 16
	}
	for k, v := range e.Attributes {
		n += len(k) + len(v) + 32
	}
	return n
}

// Errors returned by Decode.
var (
	ErrMissingHost    = errors.New("missing host")
	ErrMissingService = errors.New("missing service")
)

// Defaults supplies the values filled in for absent optional fields.
type Defaults struct {
	Time float64 // receive time, float seconds
	TTL  float64 // event.default_ttl
}

// wire is the JSON shape on input: pointers distinguish absent from zero.
type wire struct {
	Host        *string           `json:"host"`
	Service     *string           `json:"service"`
	State       string            `json:"state"`
	Metric      *float64          `json:"metric"`
	Time        *float64          `json:"time"`
	TTL         *float64          `json:"ttl"`
	Tags        []string          `json:"tags"`
	Attributes  map[string]string `json:"attributes"`
	Description string            `json:"description"`
	Source      string            `json:"source"`
}

// DecodeBatch parses a JSON array of events or a single object as a batch of
// one, fills defaults, and validates. It returns the first validation error
// with the offending index; nothing is partially returned.
func DecodeBatch(body []byte, d Defaults) ([]Event, error) {
	var raw json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	var ws []wire
	if len(raw) > 0 && raw[0] == '[' {
		if err := json.Unmarshal(raw, &ws); err != nil {
			return nil, err
		}
	} else {
		var one wire
		if err := json.Unmarshal(raw, &one); err != nil {
			return nil, err
		}
		ws = []wire{one}
	}
	out := make([]Event, 0, len(ws))
	for i, w := range ws {
		e, err := w.event(d)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		out = append(out, e)
	}
	return out, nil
}

func (w wire) event(d Defaults) (Event, error) {
	if w.Host == nil || *w.Host == "" {
		return Event{}, ErrMissingHost
	}
	if w.Service == nil || *w.Service == "" {
		return Event{}, ErrMissingService
	}
	e := Event{
		Host:        *w.Host,
		Service:     *w.Service,
		State:       w.State,
		Metric:      w.Metric,
		Time:        d.Time,
		TTL:         d.TTL,
		Tags:        w.Tags,
		Attributes:  w.Attributes,
		Description: w.Description,
		Source:      w.Source,
	}
	if w.Time != nil {
		e.Time = *w.Time
	}
	if w.TTL != nil {
		e.TTL = *w.TTL
	}
	return e, nil
}
