// Package selfobs samples the engine's snapshot and admits it as ordinary
// events tagged riemann, through the same admission path as POST /events, so
// a rule can alert on riemann.sink.ntfy.dropped > 0 with no new mechanism.
// It lives at the edge; the core only exposes the snapshot.
package selfobs

import (
	"strconv"
	"time"

	"github.com/tnn1t1s/riemann-go/engine"
	"github.com/tnn1t1s/riemann-go/event"
)

// Config carries the parameters the process owns for self-observation.
type Config struct {
	// Interval is self.interval. Each event's ttl is twice the interval.
	Interval time.Duration
	// Host is the host every self-observation event carries.
	// SPEC-GAP: the spec fixes the service names and not the host; the caller
	// passes the machine's hostname.
	Host string
	// AdmissionDeadline is ingest.admission_deadline: self-observation is
	// admitted like any other batch and is rejected, and counted, the same way.
	AdmissionDeadline time.Duration
}

// Run samples forever.
func Run(cfg Config, eng *engine.Engine) {
	tick := time.NewTicker(cfg.Interval)
	defer tick.Stop()
	for range tick.C {
		eng.Admit(Events(cfg, eng.Snapshot(), eng.Now()), cfg.AdmissionDeadline)
	}
}

// Events renders one snapshot.
// SPEC-GAP: per-partition and per-node series share one (host, service)
// identity and differ only in attributes, as the spec's service names require,
// so an index leaf keeps the last one written for each service.
// SPEC-GAP: per-node firing counts are emitted as riemann.rule.passed, a name
// the spec does not fix. riemann.rule.discarded is emitted only for a node
// that has discarded something.
func Events(cfg Config, snap engine.Snapshot, now float64) []*event.Event {
	ttl := 2 * cfg.Interval.Seconds()
	var out []*event.Event
	emit := func(service string, metric float64, attrs map[string]string) {
		out = append(out, &event.Event{Host: cfg.Host, Service: service, State: "ok", Metric: metric, HasMetric: true,
			Time: now, TTL: ttl, Tags: []string{"riemann"}, Attributes: attrs})
	}
	for _, q := range snap.Queues {
		var attrs map[string]string
		if q.Shard >= 0 {
			attrs = map[string]string{"shard": strconv.Itoa(q.Shard)}
		}
		emit("riemann."+q.Name+".depth", float64(q.Depth), attrs)
		emit("riemann."+q.Name+".capacity", float64(q.Capacity), attrs)
		emit("riemann."+q.Name+".dropped", float64(q.Dropped), attrs)
	}
	for _, s := range snap.Shards {
		attrs := map[string]string{"shard": strconv.Itoa(s.ID)}
		emit("riemann.shard.loop_lag", s.LoopLag, attrs)
		emit("riemann.shard.index_entries", float64(s.IndexEntries), attrs)
		emit("riemann.shard.in_flight", float64(s.InFlight), attrs)
	}
	emit("riemann.ingest.accepted", float64(snap.IngestAccepted), nil)
	emit("riemann.ingest.rejected", float64(snap.IngestRejected), nil)
	emit("riemann.index.rejected", float64(snap.IndexRejected), nil)
	emit("riemann.by.forks_live", float64(snap.ForksLive), nil)
	emit("riemann.by.forks_freed", float64(snap.ForksFreed), nil)
	emit("riemann.accounting.residual", float64(snap.Residual), nil)
	for _, n := range snap.Nodes {
		attrs := map[string]string{"rule": n.Rule, "version": strconv.Itoa(n.Version), "node": n.Node}
		emit("riemann.rule.passed", float64(n.Passed), attrs)
		if n.Discarded > 0 {
			emit("riemann.rule.discarded", float64(n.Discarded), attrs)
		}
	}
	return out
}
