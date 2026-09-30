// Package selfobs samples the engine's snapshot and admits it as ordinary
// events tagged riemann, through the same admission path as ingest, so rules
// can alert on the process's own counters with no new mechanism.
package selfobs

import (
	"os"
	"strconv"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
	"github.com/tnn1t1s/riemann-go/internal/event"
)

// Sampler emits the self-observation events on an interval.
type Sampler struct {
	E        *engine.Engine
	Interval time.Duration
	Host     string
}

// New returns a sampler. SPEC-GAP: the host on a self-observation event is
// not pinned; it is the process's hostname, or "riemann-go" if that fails.
func New(e *engine.Engine, interval time.Duration) *Sampler {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "riemann-go"
	}
	return &Sampler{E: e, Interval: interval, Host: host}
}

// Run samples until the process exits.
func (s *Sampler) Run() {
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for range t.C {
		s.Sample()
	}
}

// Sample takes one snapshot and admits it.
func (s *Sampler) Sample() {
	st := s.E.Stats()
	now := float64(time.Now().UnixNano()) / 1e9
	ttl := 2 * s.Interval.Seconds()
	var evs []*event.Event
	add := func(service string, metric float64, attrs map[string]string) {
		m := metric
		if attrs == nil {
			attrs = map[string]string{}
		}
		evs = append(evs, &event.Event{
			Host: s.Host, Service: service, State: "ok", Metric: &m, Time: now, TTL: ttl,
			Tags: []string{"riemann"}, Attributes: attrs,
		})
	}
	for _, sk := range st.Sinks {
		add("riemann.sink."+sk.Name+".depth", float64(sk.Depth), nil)
		add("riemann.sink."+sk.Name+".capacity", float64(sk.Capacity), nil)
		add("riemann.sink."+sk.Name+".dropped", float64(sk.Dropped), nil)
	}
	for _, sh := range st.Shards {
		a := map[string]string{"shard": strconv.Itoa(sh.ID)}
		add("riemann.shard.inbox.depth", float64(sh.InboxDepth), a)
		add("riemann.shard.inbox.capacity", float64(sh.InboxCapacity), a)
		add("riemann.shard.inbox.dropped", float64(sh.Rejected), a)
		add("riemann.shard.loop_lag", sh.LoopLag, a)
		add("riemann.shard.index_entries", float64(sh.IndexEntries), a)
		add("riemann.shard.in_flight", float64(sh.InFlight), a)
		add("riemann.index.rejected", float64(sh.IndexRejected), a)
	}
	add("riemann.global.inbox.depth", float64(st.Global.Depth), nil)
	add("riemann.global.inbox.capacity", float64(st.Global.Capacity), nil)
	add("riemann.global.inbox.dropped", float64(st.Global.Dropped), nil)
	add("riemann.subscribe.depth", float64(st.Subscribers), nil)
	add("riemann.subscribe.capacity", float64(st.SubscribeCapacity), nil)
	add("riemann.subscribe.dropped", float64(st.SubscribeDropped), nil)
	add("riemann.ingest.accepted", float64(st.IngestAccepted), nil)
	add("riemann.ingest.rejected", float64(st.IngestRejected), nil)
	for _, rs := range st.Rules {
		for i, p := range rs.Paths {
			add("riemann.rule.discarded", float64(rs.Discards[i]), map[string]string{
				"rule": rs.ID, "version": strconv.FormatInt(rs.Version, 10), "node": p,
			})
		}
		// SPEC-GAP: the two fork counters SPEC.md requires have no pinned
		// service names. They are riemann.rule.forks_live and
		// riemann.rule.forks_freed, attributed to the rule.
		ra := map[string]string{"rule": rs.ID, "version": strconv.FormatInt(rs.Version, 10)}
		add("riemann.rule.forks_live", float64(rs.ForksLive), ra)
		add("riemann.rule.forks_freed", float64(rs.ForksFreed), ra)
	}
	add("riemann.accounting.residual", float64(st.Residual), nil)
	s.E.Admit(evs, time.Now().Add(s.E.P.IngestAdmissionDeadline))
}
