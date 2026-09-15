// Probe 4: admission under burst. Minimal single-shard ingest server.
//
// POST /events  JSON array (or one object) of events -> offered to the shard
//
//	inbox with a per-request admission deadline.
//
// GET  /metrics JSON gauges and counters.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
)

// Parameter: shard.inbox_capacity. Owner: shard. Units: events.
// Default 4096 (provisional; architecture doc "Backpressure, end to end").
// Bounds memory per shard and sets how deep a burst can go before offers stall.
const shardInboxCapacity = 4096

// Parameter: ingest.admission_deadline. Owner: ingest handler. Units: duration.
// Default 200 ms (provisional). Maximum time one request's offers may block on
// a full inbox before the handler replies 429 for the remainder of the batch.
// The deadline is per request, not per event: it starts at the first stalled
// offer and covers all remaining offers in the batch.
const ingestAdmissionDeadline = 200 * time.Millisecond

// Parameter: sink.probe.queue_capacity. Owner: sink. Units: events.
// Default 1000 (carried over from rules.json ntfy sink in the architecture doc).
// Policy on full: shed newest, count it.
const sinkQueueCapacity = 1000

// Parameter: probe-only. Simulated per-event sink latency. Units: duration.
// 50 ms models a slow HTTP sink; 0 is the no-stall path.
var sinkSleep = flag.Duration("sink-sleep", 50*time.Millisecond, "simulated per-event sink latency")

// Parameter: probe-only. Simulated per-event shard-loop cost (rules, index).
// Units: duration. 0 means the loop is as fast as the channel. Needed because a
// shed-newest sink never blocks the loop, so only loop cost can fill the inbox.
var shardWork = flag.Duration("shard-work", 0, "simulated per-event shard loop cost")
var addr = flag.String("addr", "127.0.0.1:18085", "listen address")

type Event struct {
	Host        string            `json:"host"`
	Service     string            `json:"service"`
	State       string            `json:"state,omitempty"`
	Metric      *float64          `json:"metric,omitempty"`
	Time        *float64          `json:"time,omitempty"`
	TTL         *float64          `json:"ttl,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Description string            `json:"description,omitempty"`
}

type metrics struct {
	inboxMaxDepth  atomic.Int64 // max len(inbox) seen at any offer
	stalledOffers  atomic.Int64 // offers that did not succeed immediately
	rejectedEvents atomic.Int64 // events refused after the deadline
	acceptedEvents atomic.Int64 // events admitted to the inbox
	requests202    atomic.Int64
	requests429    atomic.Int64
	shardProcessed atomic.Int64 // events the shard loop took from the inbox
	sinkDropped    atomic.Int64 // events shed at the sink queue
	sinkProcessed  atomic.Int64 // events the sink worker finished
	shardInflight  atomic.Int64 // 0 or 1: event held by the loop mid-work
	sinkInflight   atomic.Int64 // 0 or 1: event held by the worker mid-sink
}

func main() {
	flag.Parse()
	m := &metrics{}
	inbox := make(chan Event, shardInboxCapacity)
	sinkQ := make(chan Event, sinkQueueCapacity)

	// Shard loop: one goroutine, never blocks on the sink.
	go func() {
		for ev := range inbox {
			m.shardInflight.Store(1)
			if *shardWork > 0 {
				time.Sleep(*shardWork)
			}
			m.shardProcessed.Add(1)
			m.shardInflight.Store(0)
			select {
			case sinkQ <- ev:
			default:
				m.sinkDropped.Add(1)
			}
		}
	}()
	// Sink worker: slow consumer.
	go func() {
		for range sinkQ {
			m.sinkInflight.Store(1)
			if *sinkSleep > 0 {
				time.Sleep(*sinkSleep)
			}
			m.sinkProcessed.Add(1)
			m.sinkInflight.Store(0)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		dec := json.NewDecoder(r.Body)
		var events []Event
		// Accept either an array or a single object.
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
			return
		}
		if len(raw) > 0 && raw[0] == '[' {
			if err := json.Unmarshal(raw, &events); err != nil {
				http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
				return
			}
		} else {
			var one Event
			if err := json.Unmarshal(raw, &one); err != nil {
				http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
				return
			}
			events = []Event{one}
		}

		accepted := 0
		var deadline <-chan time.Time // armed at the first stalled offer
		var timer *time.Timer
		for i, ev := range events {
			if d := int64(len(inbox)); d > m.inboxMaxDepth.Load() {
				m.inboxMaxDepth.Store(d)
			}
			select {
			case inbox <- ev:
				accepted++
				continue
			default:
			}
			m.stalledOffers.Add(1)
			if deadline == nil {
				timer = time.NewTimer(ingestAdmissionDeadline)
				deadline = timer.C
			}
			select {
			case inbox <- ev:
				accepted++
			case <-deadline:
				rejected := len(events) - i
				m.acceptedEvents.Add(int64(accepted))
				m.rejectedEvents.Add(int64(rejected))
				m.requests429.Add(1)
				w.Header().Set("Retry-After", "1")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprintf(w, `{"accepted":%d,"rejected":%d}`+"\n", accepted, rejected)
				return
			}
		}
		if timer != nil {
			timer.Stop()
		}
		m.acceptedEvents.Add(int64(accepted))
		m.requests202.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"accepted":%d}`+"\n", accepted)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"inbox_depth":           len(inbox),
			"inbox_capacity":        shardInboxCapacity,
			"inbox_max_depth":       m.inboxMaxDepth.Load(),
			"stalled_offers":        m.stalledOffers.Load(),
			"accepted_events":       m.acceptedEvents.Load(),
			"rejected_events":       m.rejectedEvents.Load(),
			"requests_202":          m.requests202.Load(),
			"requests_429":          m.requests429.Load(),
			"shard_processed":       m.shardProcessed.Load(),
			"sink_depth":            len(sinkQ),
			"sink_capacity":         sinkQueueCapacity,
			"sink_dropped":          m.sinkDropped.Load(),
			"sink_processed":        m.sinkProcessed.Load(),
			"shard_inflight":        m.shardInflight.Load(),
			"sink_inflight":         m.sinkInflight.Load(),
			"sink_sleep_ms":         sinkSleep.Milliseconds(),
			"shard_work_us":         shardWork.Microseconds(),
			"admission_deadline_ms": ingestAdmissionDeadline.Milliseconds(),
		})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		srv.Close()
	}()
	log.Printf("listening on %s sink-sleep=%s shard-work=%s", *addr, *sinkSleep, *shardWork)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
