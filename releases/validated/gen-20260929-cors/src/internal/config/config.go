// Package config holds the parameters SCALE.md names, their defaults, and the
// parser behind the repeatable --set flag. It is a core package: it imports
// the standard library only.
package config

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Params is every parameter named in SCALE.md. Each default below is the
// value SCALE.md's parameter table gives, and SCALE.md classifies all of them
// as provisional defaults.
type Params struct {
	// IngestMaxInflightRequests bounds connections the HTTP server holds
	// open, in requests. Beyond it new connections wait in the kernel accept
	// queue.
	IngestMaxInflightRequests int
	// IngestMaxBatchEvents is the largest batch POST /events admits, in events.
	IngestMaxBatchEvents int
	// IngestMaxBatchBytes is the largest request body POST /events reads, in bytes.
	IngestMaxBatchBytes int
	// IngestAdmissionDeadline is how long one POST /events may wait on full
	// inboxes before it answers 429.
	IngestAdmissionDeadline time.Duration
	// ShardInboxCapacity is each partition inbox's capacity, in events.
	ShardInboxCapacity int
	// SinkNtfyQueueCapacity is the ntfy sink queue's capacity, in events.
	SinkNtfyQueueCapacity int
	// SinkInfluxQueueCapacity is the influx sink queue's capacity, in events.
	SinkInfluxQueueCapacity int
	// SubscribeQueueCapacity is each SSE subscriber's queue capacity, in events.
	SubscribeQueueCapacity int
	// StableBufferCapacity is a stable node's buffer capacity per fork key, in events.
	StableBufferCapacity int
	// IndexMaxEntriesPerShard bounds each partition's index slice, in entries.
	IndexMaxEntriesPerShard int
	// ShardRingEvents bounds each partition's ring, in events.
	ShardRingEvents int
	// ShardRingBytes bounds each partition's ring, in bytes.
	ShardRingBytes int
	// EngineShards is the number of partitions.
	EngineShards int
	// SelfInterval is the self-observation sampling period.
	SelfInterval time.Duration
}

// Defaults returns SCALE.md's parameter table.
func Defaults() Params {
	return Params{
		IngestMaxInflightRequests: 256,
		IngestMaxBatchEvents:      1000,
		IngestMaxBatchBytes:       1 << 20,
		IngestAdmissionDeadline:   200 * time.Millisecond,
		ShardInboxCapacity:        4096,
		SinkNtfyQueueCapacity:     1000,
		SinkInfluxQueueCapacity:   10000,
		SubscribeQueueCapacity:    1000,
		StableBufferCapacity:      1024,
		IndexMaxEntriesPerShard:   1000000,
		ShardRingEvents:           100000,
		ShardRingBytes:            64 << 20,
		EngineShards:              runtime.GOMAXPROCS(0),
		SelfInterval:              10 * time.Second,
	}
}

type setter func(p *Params, value string) error

func positiveInt(dst func(p *Params) *int) setter {
	return func(p *Params, value string) error {
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("value %q is not an integer", value)
		}
		if n < 1 {
			return fmt.Errorf("value %d must be 1 or greater", n)
		}
		*dst(p) = n
		return nil
	}
}

func positiveDuration(unit time.Duration, dst func(p *Params) *time.Duration) setter {
	return func(p *Params, value string) error {
		f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("value %q is not a number", value)
		}
		d := time.Duration(f * float64(unit))
		if d <= 0 {
			return fmt.Errorf("value %q must be greater than zero", value)
		}
		*dst(p) = d
		return nil
	}
}

// SPEC-GAP: SCALE.md gives each parameter a unit but not a value grammar for
// --set. Chosen: counts and byte sizes are base-10 integers of 1 or greater
// with no unit suffix; ingest.admission_deadline is a number of milliseconds
// and self.interval a number of seconds, both allowing a fraction.
var setters = map[string]setter{
	"ingest.max_inflight_requests": positiveInt(func(p *Params) *int { return &p.IngestMaxInflightRequests }),
	"ingest.max_batch_events":      positiveInt(func(p *Params) *int { return &p.IngestMaxBatchEvents }),
	"ingest.max_batch_bytes":       positiveInt(func(p *Params) *int { return &p.IngestMaxBatchBytes }),
	"ingest.admission_deadline":    positiveDuration(time.Millisecond, func(p *Params) *time.Duration { return &p.IngestAdmissionDeadline }),
	"shard.inbox_capacity":         positiveInt(func(p *Params) *int { return &p.ShardInboxCapacity }),
	"sink.ntfy.queue_capacity":     positiveInt(func(p *Params) *int { return &p.SinkNtfyQueueCapacity }),
	"sink.influx.queue_capacity":   positiveInt(func(p *Params) *int { return &p.SinkInfluxQueueCapacity }),
	"subscribe.queue_capacity":     positiveInt(func(p *Params) *int { return &p.SubscribeQueueCapacity }),
	"stable.buffer_capacity":       positiveInt(func(p *Params) *int { return &p.StableBufferCapacity }),
	"index.max_entries_per_shard":  positiveInt(func(p *Params) *int { return &p.IndexMaxEntriesPerShard }),
	"shard.ring_events":            positiveInt(func(p *Params) *int { return &p.ShardRingEvents }),
	"shard.ring_bytes":             positiveInt(func(p *Params) *int { return &p.ShardRingBytes }),
	"engine.shards":                positiveInt(func(p *Params) *int { return &p.EngineShards }),
	"self.interval":                positiveDuration(time.Second, func(p *Params) *time.Duration { return &p.SelfInterval }),
}

// Names returns every parameter name --set recognises, sorted.
func Names() []string {
	out := make([]string, 0, len(setters))
	for name := range setters {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Set applies one "name=value" assignment. An unrecognised name is an error
// that names it.
func (p *Params) Set(assignment string) (name string, err error) {
	name, value, ok := strings.Cut(assignment, "=")
	if !ok {
		return "", fmt.Errorf("--set %q: expected name=value", assignment)
	}
	name = strings.TrimSpace(name)
	set, known := setters[name]
	if !known {
		return name, fmt.Errorf("--set: unrecognised parameter %q", name)
	}
	if err := set(p, value); err != nil {
		return name, fmt.Errorf("--set %s: %v", name, err)
	}
	return name, nil
}
