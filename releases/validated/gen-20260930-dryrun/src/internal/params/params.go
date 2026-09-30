// Package params holds every named parameter from SCALE.md, with its default
// and the parser behind the --set flag. A name the binary does not know is an
// error here, which main turns into a fatal startup failure.
package params

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Params is the full set of SCALE.md parameters. Field comments carry the
// unit; defaults and their reasons are in SCALE.md and are not repeated here.
type Params struct {
	IngestMaxInflightRequests int           // requests
	IngestMaxBatchEvents      int           // events per body
	IngestMaxBatchBytes       int64         // bytes per body
	IngestAdmissionDeadline   time.Duration // wall time an ingest request may wait on inboxes
	ShardInboxCapacity        int           // events per partition inbox
	SinkNtfyQueueCapacity     int           // events
	SinkInfluxQueueCapacity   int           // events
	SubscribeQueueCapacity    int           // events per subscriber
	StableBufferCapacity      int           // events per fork key
	IndexMaxEntriesPerShard   int           // entries
	ShardRingEvents           int           // events per partition ring
	ShardRingBytes            int64         // bytes per partition ring
	Shards                    int           // partitions; --shards overrides
	SelfInterval              time.Duration // self-observation sampling period
}

// Defaults returns the SCALE.md defaults.
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
		Shards:                    runtime.GOMAXPROCS(0),
		SelfInterval:              10 * time.Second,
	}
}

// Set applies one name=value pair from --set. The name must be one SCALE.md
// lists; anything else is an error naming the unknown parameter.
func (p *Params) Set(pair string) error {
	name, value, ok := strings.Cut(pair, "=")
	if !ok {
		return fmt.Errorf("--set %q: expected name=value", pair)
	}
	name = strings.TrimSpace(name)
	value = strings.TrimSpace(value)
	posInt := func(dst *int) error {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			return fmt.Errorf("--set %s: %q is not a positive integer", name, value)
		}
		*dst = n
		return nil
	}
	posInt64 := func(dst *int64) error {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 1 {
			return fmt.Errorf("--set %s: %q is not a positive integer", name, value)
		}
		*dst = n
		return nil
	}
	switch name {
	case "ingest.max_inflight_requests":
		return posInt(&p.IngestMaxInflightRequests)
	case "ingest.max_batch_events":
		return posInt(&p.IngestMaxBatchEvents)
	case "ingest.max_batch_bytes":
		return posInt64(&p.IngestMaxBatchBytes)
	case "ingest.admission_deadline":
		// SCALE.md gives this parameter in milliseconds.
		ms, err := strconv.ParseFloat(value, 64)
		if err != nil || ms <= 0 {
			return fmt.Errorf("--set %s: %q is not a positive number of milliseconds", name, value)
		}
		p.IngestAdmissionDeadline = time.Duration(ms * float64(time.Millisecond))
		return nil
	case "shard.inbox_capacity":
		return posInt(&p.ShardInboxCapacity)
	case "sink.ntfy.queue_capacity":
		return posInt(&p.SinkNtfyQueueCapacity)
	case "sink.influx.queue_capacity":
		return posInt(&p.SinkInfluxQueueCapacity)
	case "subscribe.queue_capacity":
		return posInt(&p.SubscribeQueueCapacity)
	case "stable.buffer_capacity":
		return posInt(&p.StableBufferCapacity)
	case "index.max_entries_per_shard":
		return posInt(&p.IndexMaxEntriesPerShard)
	case "shard.ring_events":
		return posInt(&p.ShardRingEvents)
	case "shard.ring_bytes":
		return posInt64(&p.ShardRingBytes)
	case "engine.shards":
		// SPEC-GAP: SCALE.md lists engine.shards as a parameter and SPEC.md says
		// --shards sets it. Both are accepted; an explicit --shards wins.
		return posInt(&p.Shards)
	case "self.interval":
		// SCALE.md gives this parameter in seconds.
		s, err := strconv.ParseFloat(value, 64)
		if err != nil || s <= 0 {
			return fmt.Errorf("--set %s: %q is not a positive number of seconds", name, value)
		}
		p.SelfInterval = time.Duration(s * float64(time.Second))
		return nil
	}
	return fmt.Errorf("--set %s: unknown parameter; see SCALE.md for the names", name)
}
