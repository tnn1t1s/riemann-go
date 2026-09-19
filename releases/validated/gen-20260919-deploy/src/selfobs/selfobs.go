// Package selfobs is the self-observation sampler. It lives at the edge: it
// reads the core's snapshot on an interval and admits the readings through
// the same admission path POST /events uses, so they are ordinary events that
// rules match, sinks receive and, given a rule with an index leaf, the index
// holds.
package selfobs

import (
	"context"
	"os"
	"time"

	"github.com/tnn1t1s/riemann-go/engine"
	"github.com/tnn1t1s/riemann-go/event"
)

// ttlIntervals is the ttl of a self-observation event in sampling intervals:
// two, carried from upstream's instrumentation rule, so one missed sample
// does not expire a gauge.
const ttlIntervals = 2

// Run samples until ctx is done.
//
// SPEC-GAP: the spec fixes the service names, the `riemann` tag and the ttl
// of a self-observation event, and leaves its `host` and `state` open.
// Chosen: `host` is the machine's hostname, or "riemannd" when the OS will not
// give one (an identity label, not a destination); `state` is "ok". Because
// identity is (host, service), readings that share a service, such as the
// per-partition riemann.shard.* set, share an identity too and are told apart
// only by their attributes; a rule that indexes them keeps the last one.
func Run(ctx context.Context, eng *engine.Engine) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "riemannd"
	}
	interval := eng.Params.SelfInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := engine.Now()
		samples := eng.Snapshot()
		evs := make([]*event.Event, 0, len(samples))
		for _, s := range samples {
			evs = append(evs, &event.Event{
				Host: host, Service: s.Service, State: "ok",
				Metric: s.Value, HasMetric: true,
				Time: now, TTL: ttlIntervals * interval.Seconds(),
				Tags: []string{"riemann"}, Attributes: s.Attrs,
			})
		}
		// Admitted like any other batch: bounded by ingest.max_batch_events
		// and subject to the admission deadline, with refusals counted.
		for len(evs) > 0 {
			n := min(len(evs), eng.Params.IngestMaxBatchEvents)
			eng.Admit(evs[:n])
			evs = evs[n:]
		}
	}
}
