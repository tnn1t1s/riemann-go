// Package promtext is the GET /metrics adapter. It renders the engine's
// snapshot in Prometheus text format by hand; it imports no telemetry SDK.
//
// SPEC-GAP: the spec requires depth, capacity and dropped for every bounded
// queue but does not name the metrics. A self-observation service name maps to
// a metric name by replacing dots with underscores, so riemann.sink.ntfy.dropped
// is riemann_sink_ntfy_dropped; per-partition series carry a shard label.
package promtext

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/tnn1t1s/riemann-go/engine"
)

// Extra is a counter an adapter outside the engine owns.
type Extra struct {
	Name string
	Read func() int64
}

// Register mounts GET /metrics on mux.
func Register(mux *http.ServeMux, eng *engine.Engine, extras []Extra) {
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Write([]byte(Render(eng.Snapshot(), extras)))
	})
}

type series struct {
	labels string
	value  float64
}

type family struct {
	kind   string
	series []series
}

// Render writes the snapshot as Prometheus text.
func Render(snap engine.Snapshot, extras []Extra) string {
	fams := map[string]*family{}
	add := func(name, kind, labels string, v float64) {
		f := fams[name]
		if f == nil {
			f = &family{kind: kind}
			fams[name] = f
		}
		f.series = append(f.series, series{labels, v})
	}
	for _, q := range snap.Queues {
		base := "riemann_" + strings.ReplaceAll(q.Name, ".", "_")
		labels := ""
		if q.Shard >= 0 {
			labels = `{shard="` + strconv.Itoa(q.Shard) + `"}`
		}
		add(base+"_depth", "gauge", labels, float64(q.Depth))
		add(base+"_capacity", "gauge", labels, float64(q.Capacity))
		add(base+"_dropped", "counter", labels, float64(q.Dropped))
	}
	for name, st := range snap.Sinks {
		base := "riemann_sink_" + name
		add(base+"_accepted", "counter", "", float64(st.Accepted))
		add(base+"_processed", "counter", "", float64(st.Processed))
		add(base+"_in_flight", "gauge", "", float64(st.InFlight))
	}
	for _, s := range snap.Shards {
		labels := `{shard="` + strconv.Itoa(s.ID) + `"}`
		add("riemann_shard_loop_lag", "gauge", labels, s.LoopLag)
		add("riemann_shard_index_entries", "gauge", labels, float64(s.IndexEntries))
		add("riemann_shard_in_flight", "gauge", labels, float64(s.InFlight))
		add("riemann_shard_processed", "counter", labels, float64(s.Processed))
		add("riemann_shard_inbox_stalled_offers", "counter", labels, float64(s.Stalled))
	}
	add("riemann_ingest_accepted", "counter", "", float64(snap.IngestAccepted))
	add("riemann_ingest_rejected", "counter", "", float64(snap.IngestRejected))
	add("riemann_index_rejected", "counter", "", float64(snap.IndexRejected))
	add("riemann_accounting_residual", "gauge", "", float64(snap.Residual))
	add("riemann_by_forks_live", "gauge", "", float64(snap.ForksLive))
	add("riemann_by_forks_freed", "counter", "", float64(snap.ForksFreed))
	for _, n := range snap.Nodes {
		labels := fmt.Sprintf(`{rule=%q,version="%d",node=%q}`, n.Rule, n.Version, n.Node)
		add("riemann_rule_passed", "counter", labels, float64(n.Passed))
		add("riemann_rule_discarded", "counter", labels, float64(n.Discarded))
	}
	for _, x := range extras {
		add(x.Name, "counter", "", float64(x.Read()))
	}

	names := make([]string, 0, len(fams))
	for name := range fams {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		f := fams[name]
		fmt.Fprintf(&b, "# TYPE %s %s\n", name, f.kind)
		for _, s := range f.series {
			fmt.Fprintf(&b, "%s%s %s\n", name, s.labels, strconv.FormatFloat(s.value, 'g', -1, 64))
		}
	}
	return b.String()
}
