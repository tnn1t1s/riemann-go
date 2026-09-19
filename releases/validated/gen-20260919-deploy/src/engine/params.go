// Package engine owns the partitions: their inboxes, timers, index slices and
// rings, the global rule executor, admission, subscriptions, dry run and the
// self-observation snapshot. It is a core package and imports no adapter.
package engine

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"time"
)

// Params are the parameters of SCALE.md. Each default is SCALE.md's, and is
// provisional there; the reason for each value is in that document's table.
type Params struct {
	IngestMaxInflightRequests int           // ingest.max_inflight_requests, requests
	IngestMaxBatchEvents      int           // ingest.max_batch_events, events
	IngestMaxBatchBytes       int           // ingest.max_batch_bytes, bytes
	IngestAdmissionDeadline   time.Duration // ingest.admission_deadline, set in milliseconds
	ShardInboxCapacity        int           // shard.inbox_capacity, events
	SinkNtfyQueueCapacity     int           // sink.ntfy.queue_capacity, events
	SinkInfluxQueueCapacity   int           // sink.influx.queue_capacity, events
	SubscribeQueueCapacity    int           // subscribe.queue_capacity, events per subscriber
	StableBufferCapacity      int           // stable.buffer_capacity, events per fork key
	IndexMaxEntriesPerShard   int           // index.max_entries_per_shard, entries
	ShardRingEvents           int           // shard.ring_events, events
	ShardRingBytes            int           // shard.ring_bytes, bytes
	EngineShards              int           // engine.shards, partitions
	SelfInterval              time.Duration // self.interval, set in seconds
}

// DefaultParams returns SCALE.md's defaults.
func DefaultParams() Params {
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

// ParamNames lists every name --set accepts.
func ParamNames() []string {
	names := make([]string, 0, len(setters))
	for n := range setters {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

var setters = map[string]func(*Params, string) error{
	"ingest.max_inflight_requests": intSetter(func(p *Params) *int { return &p.IngestMaxInflightRequests }),
	"ingest.max_batch_events":      intSetter(func(p *Params) *int { return &p.IngestMaxBatchEvents }),
	"ingest.max_batch_bytes":       intSetter(func(p *Params) *int { return &p.IngestMaxBatchBytes }),
	"ingest.admission_deadline": func(p *Params, v string) error {
		ms, err := positiveFloat(v)
		if err != nil {
			return err
		}
		p.IngestAdmissionDeadline = time.Duration(ms * float64(time.Millisecond))
		return nil
	},
	"shard.inbox_capacity":        intSetter(func(p *Params) *int { return &p.ShardInboxCapacity }),
	"sink.ntfy.queue_capacity":    intSetter(func(p *Params) *int { return &p.SinkNtfyQueueCapacity }),
	"sink.influx.queue_capacity":  intSetter(func(p *Params) *int { return &p.SinkInfluxQueueCapacity }),
	"subscribe.queue_capacity":    intSetter(func(p *Params) *int { return &p.SubscribeQueueCapacity }),
	"stable.buffer_capacity":      intSetter(func(p *Params) *int { return &p.StableBufferCapacity }),
	"index.max_entries_per_shard": intSetter(func(p *Params) *int { return &p.IndexMaxEntriesPerShard }),
	"shard.ring_events":           intSetter(func(p *Params) *int { return &p.ShardRingEvents }),
	"shard.ring_bytes":            intSetter(func(p *Params) *int { return &p.ShardRingBytes }),
	"engine.shards":               intSetter(func(p *Params) *int { return &p.EngineShards }),
	"self.interval": func(p *Params, v string) error {
		sec, err := positiveFloat(v)
		if err != nil {
			return err
		}
		p.SelfInterval = time.Duration(sec * float64(time.Second))
		return nil
	},
}

// Set applies one --set name=value. An unrecognised name is an error naming it.
//
// SPEC-GAP: the spec does not give the value syntax of --set. Chosen: a plain
// number in the units SCALE.md's table states for the parameter (bytes as an
// integer count, the deadline in milliseconds, the interval in seconds), and
// every value must be positive.
func (p *Params) Set(name, value string) error {
	set, ok := setters[name]
	if !ok {
		return fmt.Errorf("--set: unrecognised parameter %q", name)
	}
	if err := set(p, value); err != nil {
		return fmt.Errorf("--set %s: %v", name, err)
	}
	return nil
}

func intSetter(field func(*Params) *int) func(*Params, string) error {
	return func(p *Params, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("value %q is not an integer", v)
		}
		if n < 1 {
			return fmt.Errorf("value %d must be 1 or greater", n)
		}
		*field(p) = n
		return nil
	}
}

func positiveFloat(v string) (float64, error) {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("value %q is not a number", v)
	}
	if !(f > 0) {
		return 0, fmt.Errorf("value %v must be greater than zero", f)
	}
	return f, nil
}
