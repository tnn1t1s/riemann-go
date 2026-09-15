// Probe 1: single-threaded shard loop throughput.
//
// One goroutine reads events from a bounded channel, updates a map index
// keyed by (host, service) with a min-heap of expiry times carrying entry
// generations, pops due expiries between events, and pushes every event
// (live or expired) through a closure-style pipeline:
//
//	where(service == "cpu") -> by(host) -> changed-state(init "ok") -> counting sink
//
// Workload: 1,000,000 synthetic events, 1,000 hosts, services cpu/mem
// alternating, ttl 10 s, synthetic clock advancing 1 ms per event.
// Host h emits only in epochs (1 s windows) where epoch % (1 + h%16) == 0;
// otherwise its slot is filled by hot host h%64, so hosts with period > 10 s
// expire during the run. State cycles ok/warning/critical per epoch.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"sync/atomic"
	"time"
)

type Event struct {
	Host       string
	Service    string
	State      string
	Metric     float64
	Time       float64
	TTL        float64
	Tags       []string
	Attributes map[string]string
}

type Stream func(Event)

// ---- combinators, Clojure style: a stream is a function calling its children.

func where(pred func(Event) bool, children ...Stream) Stream {
	return func(e Event) {
		if pred(e) {
			for _, c := range children {
				c(e)
			}
		}
	}
}

func by(key func(Event) string, mk func() Stream) Stream {
	forks := map[string]Stream{}
	return func(e Event) {
		k := key(e)
		s, ok := forks[k]
		if !ok {
			s = mk()
			forks[k] = s
		}
		s(e)
	}
}

func changedState(init string, children ...Stream) Stream {
	prev := init
	return func(e Event) {
		if e.State != prev {
			prev = e.State
			for _, c := range children {
				c(e)
			}
		}
	}
}

func countingSink(n *int64) Stream {
	return func(Event) { *n++ }
}

// ---- index with generation-carrying expiry heap.

type key struct{ host, service string }

type entry struct {
	ev  Event
	gen uint64
}

type heapItem struct {
	at  float64
	k   key
	gen uint64
}

type expiryHeap []heapItem

func (h *expiryHeap) push(it heapItem) {
	*h = append(*h, it)
	i := len(*h) - 1
	for i > 0 {
		p := (i - 1) / 2
		if (*h)[p].at <= (*h)[i].at {
			break
		}
		(*h)[p], (*h)[i] = (*h)[i], (*h)[p]
		i = p
	}
}

func (h *expiryHeap) pop() heapItem {
	old := *h
	top := old[0]
	n := len(old) - 1
	old[0] = old[n]
	*h = old[:n]
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		m := i
		if l < n && old[l].at < old[m].at {
			m = l
		}
		if r < n && old[r].at < old[m].at {
			m = r
		}
		if m == i {
			break
		}
		old[i], old[m] = old[m], old[i]
		i = m
	}
	return top
}

type shard struct {
	index   map[key]*entry
	heap    expiryHeap
	gen     uint64
	clock   float64
	expired int64
	root    Stream
}

func (s *shard) insert(e Event) {
	k := key{e.Host, e.Service}
	s.gen++
	en, ok := s.index[k]
	if !ok {
		en = &entry{}
		s.index[k] = en
	}
	en.ev = e
	en.gen = s.gen
	s.heap.push(heapItem{at: e.Time + e.TTL, k: k, gen: s.gen})
}

func (s *shard) reap() {
	for len(s.heap) > 0 && s.heap[0].at <= s.clock {
		it := s.heap.pop()
		en, ok := s.index[it.k]
		if !ok || en.gen != it.gen {
			continue // stale generation
		}
		delete(s.index, it.k)
		s.expired++
		s.root(Event{Host: it.k.host, Service: it.k.service, State: "expired", Time: s.clock})
	}
}

func (s *shard) run(in <-chan Event) {
	for e := range in {
		s.clock = e.Time
		s.reap()
		s.insert(e)
		s.root(e)
	}
}

// ---- workload.

const (
	nEvents  = 1_000_000
	nHosts   = 1000
	ttl      = 10.0
	tickSec  = 0.001
	chanSize = 1024
)

func generate() []Event {
	hosts := make([]string, nHosts)
	for i := range hosts {
		hosts[i] = "host-" + strconv.Itoa(i)
	}
	states := []string{"ok", "warning", "critical"}
	evs := make([]Event, nEvents)
	for i := range evs {
		epoch := i / nHosts
		h := i % nHosts
		if epoch%(1+h%16) != 0 {
			h = h % 64
		}
		svc := "cpu"
		if i%2 == 1 {
			svc = "mem"
		}
		evs[i] = Event{
			Host:       hosts[h],
			Service:    svc,
			State:      states[epoch%3],
			Metric:     float64(i%100) / 100,
			Time:       float64(i) * tickSec,
			TTL:        ttl,
			Tags:       []string{"probe", "synthetic"},
			Attributes: map[string]string{"dc": "local", "env": "probe"},
		}
	}
	return evs
}

func heapObjectsBytes() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

type result struct {
	wall            time.Duration
	allocsPerEvent  float64
	baselineHeap    uint64
	peakHeap        uint64
	expired         int64
	fired           int64
	indexSize       int
	heapLen         int
}

func runOnce(evs []Event, withPipeline bool) result {
	var fired int64
	sh := &shard{index: map[key]*entry{}}
	if withPipeline {
		sh.root = where(func(e Event) bool { return e.Service == "cpu" },
			by(func(e Event) string { return e.Host }, func() Stream {
				return changedState("ok", countingSink(&fired))
			}))
	} else {
		sh.root = func(Event) {}
	}

	runtime.GC()
	baseline := heapObjectsBytes()
	var peak atomic.Uint64
	peak.Store(baseline)
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if v := heapObjectsBytes(); v > peak.Load() {
					peak.Store(v)
				}
			}
		}
	}()

	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	in := make(chan Event, chanSize)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		sh.run(in)
		close(done)
	}()
	for i := range evs {
		in <- evs[i]
	}
	close(in)
	<-done
	wall := time.Since(start)
	runtime.ReadMemStats(&ms1)
	close(stop)
	<-sampled

	return result{
		wall:           wall,
		allocsPerEvent: float64(ms1.Mallocs-ms0.Mallocs) / nEvents,
		baselineHeap:   baseline,
		peakHeap:       peak.Load(),
		expired:        sh.expired,
		fired:          fired,
		indexSize:      len(sh.index),
		heapLen:        len(sh.heap),
	}
}

func main() {
	runs := flag.Int("runs", 3, "runs per mode")
	flag.Parse()
	debug.SetGCPercent(100)

	t0 := time.Now()
	evs := generate()
	fmt.Fprintf(os.Stderr, "generated %d events in %v; GOMAXPROCS=%d\n", len(evs), time.Since(t0), runtime.GOMAXPROCS(0))

	fmt.Printf("%-9s %-4s %-10s %-12s %-8s %-10s %-10s %-9s %-8s %-8s\n",
		"mode", "run", "wall", "events/s", "allocs", "baseMB", "peakMB", "expired", "fired", "index")
	for _, mode := range []struct {
		name string
		pipe bool
	}{{"pipeline", true}, {"empty", false}} {
		for r := 1; r <= *runs; r++ {
			res := runOnce(evs, mode.pipe)
			fmt.Printf("%-9s %-4d %-10v %-12.0f %-8.3f %-10.1f %-10.1f %-9d %-8d %-8d\n",
				mode.name, r, res.wall.Round(time.Millisecond),
				nEvents/res.wall.Seconds(), res.allocsPerEvent,
				float64(res.baselineHeap)/1e6, float64(res.peakHeap)/1e6,
				res.expired, res.fired, res.indexSize)
		}
	}
}
