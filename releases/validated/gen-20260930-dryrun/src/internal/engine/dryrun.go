package engine

import (
	"sort"

	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

// DryFiring is one sink-stub delivery during a dry run.
type DryFiring struct {
	Sink       string       `json:"sink"`
	Event      *event.Event `json:"event"`
	Node       string       `json:"node"`
	PriorState *string      `json:"prior_state"`
}

// dryRuntime replays under a clock driven by the replayed events' own
// timestamps and delivers to stubs. It touches no live sink and no index.
type dryRuntime struct {
	now     float64
	tm      timers
	firings []DryFiring
}

func (d *dryRuntime) Now() float64                     { return d.now }
func (d *dryRuntime) Schedule(due float64, fn func()) { d.tm.schedule(due, fn) }
func (d *dryRuntime) Emit(c *rule.Compiled, sink, path string, ev *event.Event, fr rule.Frame) {
	d.firings = append(d.firings, DryFiring{Sink: sink, Event: ev, Node: path, PriorState: fr.PriorState})
}

// DryRun compiles body as a rule and replays the ring through it. The rule
// need not be installed. SPEC-FREE: events replay in processing order, and a
// partition-local rule is instantiated once per partition so the replay
// forks state the way the live loops would.
func (e *Engine) DryRun(body []byte, id string) ([]DryFiring, error) {
	c, err := rule.Parse(body, id, e.ruleOptions())
	if err != nil {
		return nil, err
	}
	var all []RingEntry
	for _, s := range e.shards {
		all = append(all, s.ring.snapshot()...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Seq < all[j].Seq })

	rt := &dryRuntime{firings: []DryFiring{}}
	var instances []*rule.Instance
	if c.Partition == "global" {
		instances = []*rule.Instance{rule.NewInstance(c, rt)}
	} else {
		for range e.shards {
			instances = append(instances, rule.NewInstance(c, rt))
		}
	}
	for _, re := range all {
		ev := re.Event
		rt.now = ev.Time
		// Property 12 under the replay clock: timers due at or before this
		// event's time fire before it is dispatched. A timer due after the
		// last event never fires, because nothing advances the clock past it.
		rt.tm.fireDue(ev.Time)
		if len(instances) == 1 {
			instances[0].Offer(ev)
		} else {
			instances[e.shardFor(ev.Host)].Offer(ev)
		}
	}
	// SPEC-GAP: the dry run's index stub records inserts but does not
	// synthesize expiry, so a rule that depends on its own index leaf's
	// expiry sees expiry events only if they were in the ring already.
	return rt.firings, nil
}
