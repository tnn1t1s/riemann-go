// Package selfobs is the adapter that samples the engine's snapshot and
// admits it as events. It imports the core and no other adapter; readings
// another adapter owns reach it as a function value.
package selfobs

import (
	"sync"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
	"github.com/tnn1t1s/riemann-go/internal/event"
)

// Tag is the tag every self-observation event carries.
const Tag = "riemann"

// ttlIntervals is a self-observation event's ttl, in sampling intervals.
// SPEC.md classifies it as carried from upstream's instrumentation rule.
const ttlIntervals = 2

// stateOK is the state every self-observation event carries.
//
// SPEC-GAP: the spec gives these events a service, a metric, a tag and a ttl,
// and no state. Chosen: "ok" on every one, so the state never changes and a
// rule alerts on the metric, as the spec's own example does.
const stateOK = "ok"

// Sampler emits the process's readings as events every interval.
type Sampler struct {
	eng      *engine.Engine
	host     string
	interval time.Duration
	deadline time.Duration
	extra    func() []engine.Reading
	stop     chan struct{}
	wg       sync.WaitGroup
}

// New returns a sampler over eng. host is the host every event carries.
// extra returns readings the engine's snapshot does not hold; it may be nil.
//
// SPEC-GAP: the spec does not say which host a self-observation event
// carries. Chosen: the caller passes the machine's hostname, as upstream's
// instrumentation service does. Readings that share a service, one per shard
// or one per rule node, therefore share an identity and differ in their
// attributes; an index leaf keeps the last of them admitted, and a rule that
// needs each one forks on the attribute.
func New(eng *engine.Engine, host string, extra func() []engine.Reading) *Sampler {
	params := eng.Params()
	return &Sampler{
		eng:      eng,
		host:     host,
		interval: params.SelfInterval,
		deadline: params.IngestAdmissionDeadline,
		extra:    extra,
		stop:     make(chan struct{}),
	}
}

// Start launches the sampling loop. The first sample is taken one interval
// after the call.
func (s *Sampler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case now := <-ticker.C:
				s.sample(now)
			}
		}
	}()
}

// Stop ends the loop and waits for it.
func (s *Sampler) Stop() {
	close(s.stop)
	s.wg.Wait()
}

// sample admits one event per reading through the same admission path a
// POST /events body takes, so a full inbox rejects these as it would any
// other event and counts them in riemann.ingest.rejected.
func (s *Sampler) sample(at time.Time) {
	readings := s.eng.Snapshot().Readings()
	if s.extra != nil {
		readings = append(readings, s.extra()...)
	}
	now := float64(at.UnixNano()) / 1e9
	ttl := ttlIntervals * s.interval.Seconds()
	events := make([]*event.Event, 0, len(readings))
	for _, r := range readings {
		if r.PerNode && r.Value == 0 {
			continue
		}
		ev := &event.Event{
			Host:      s.host,
			Service:   r.Service,
			State:     stateOK,
			Metric:    r.Value,
			HasMetric: true,
			Time:      now,
			TTL:       ttl,
			Tags:      []string{Tag},
		}
		if len(r.Attributes) > 0 {
			ev.Attributes = make(map[string]string, len(r.Attributes))
			for k, v := range r.Attributes {
				ev.Attributes[k] = v
			}
		}
		events = append(events, ev)
	}
	s.eng.Admit(events, s.deadline)
}
