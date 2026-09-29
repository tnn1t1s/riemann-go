// Package influx is the adapter that writes firings to an InfluxDB v2 bucket
// as line protocol. It imports the core and no other adapter.
package influx

import (
	"bytes"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
	"github.com/tnn1t1s/riemann-go/internal/event"
	"github.com/tnn1t1s/riemann-go/internal/rule"
)

// requestTimeout bounds one write, in seconds of wall time. A heuristic, for
// the reason the ntfy sink's is: nothing has measured the fleet's InfluxDB
// latency.
const requestTimeout = 10 * time.Second

// maxBatchLines bounds the lines in one write, in lines. A heuristic: it
// caps what one lost write loses at 500 points, and at the flood floor's
// 1,000 events per second it is half a second of firings. Nothing has
// measured a write size against a real InfluxDB.
//
// SPEC-GAP: the spec says a firing is "appended to a batch" and names no
// batch size and no flush interval. Chosen: the worker writes whatever the
// queue holds when it is free, up to maxBatchLines, and never waits to fill
// a batch. An idle sink therefore writes one line per request and a busy one
// batches.
const maxBatchLines = 500

// Sink batches firings into line protocol writes.
//
// SPEC-GAP: as for ntfy, the spec does not say what a failed write does.
// Chosen: no retry; a write the server answered counts as processed and one
// that got no answer counts every line in it as dropped.
type Sink struct {
	endpoint string
	token    string
	queue    *engine.SinkQueue
	client   *http.Client
	stop     chan struct{}
	wg       sync.WaitGroup
}

// New returns a sink writing to the bucket at base, behind a queue of the
// given capacity. token is empty when writes carry no credential.
func New(base, org, bucket, token string, capacity int) *Sink {
	query := url.Values{}
	query.Set("org", org)
	query.Set("bucket", bucket)
	query.Set("precision", "ns")
	return &Sink{
		endpoint: strings.TrimRight(base, "/") + "/api/v2/write?" + query.Encode(),
		token:    token,
		queue:    engine.NewSinkQueue(capacity),
		client:   &http.Client{Timeout: requestTimeout},
		stop:     make(chan struct{}),
	}
}

// Offer enqueues a firing. An event with no metric is not written and counts
// as dropped, because a point with no field is not a point.
func (s *Sink) Offer(f rule.Firing) {
	if !f.Event.HasMetric || math.IsNaN(f.Event.Metric) || math.IsInf(f.Event.Metric, 0) {
		s.queue.Refuse()
		return
	}
	s.queue.Offer(f)
}

func (s *Sink) Stats() engine.SinkStats { return s.queue.Stats() }

// Start launches the worker.
func (s *Sink) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			batch := s.queue.Take(maxBatchLines, s.stop)
			if batch == nil {
				return
			}
			s.queue.Done(len(batch), s.write(batch))
		}
	}()
}

// Stop ends the worker after the write in hand.
func (s *Sink) Stop() {
	close(s.stop)
	s.wg.Wait()
}

func (s *Sink) write(batch []rule.Firing) bool {
	var body bytes.Buffer
	for _, f := range batch {
		body.WriteString(Line(f.Event))
		body.WriteByte('\n')
	}
	req, err := http.NewRequest(http.MethodPost, s.endpoint, &body)
	if err != nil {
		log.Printf("influx: building request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if s.token != "" {
		req.Header.Set("Authorization", "Token "+s.token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("influx: write of %d lines failed: %v", len(batch), err)
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("influx: write of %d lines answered %s", len(batch), resp.Status)
	}
	return true
}

var (
	measurementEscaper = strings.NewReplacer(`\`, `\\`, ",", `\,`, " ", `\ `, "\n", `\n`)
	tagEscaper         = strings.NewReplacer(`\`, `\\`, ",", `\,`, "=", `\=`, " ", `\ `, "\n", `\n`)
)

// Line renders one event as one line of line protocol:
//
//	<service>,host=<host>[,state=<state>][,<attr-key>=<attr-value>]... metric=<metric> <time_ns>
//
// SPEC-GAP: the spec does not order the attribute tags, and line protocol
// has no empty tag value. Chosen: attributes follow host and state in key
// order, an attribute with an empty key or value is left out, and an
// attribute named host or state is left out so the line carries no duplicate
// tag key.
func Line(ev *event.Event) string {
	var sb strings.Builder
	sb.WriteString(measurementEscaper.Replace(ev.Service))
	sb.WriteString(",host=")
	sb.WriteString(tagEscaper.Replace(ev.Host))
	if ev.State != "" {
		sb.WriteString(",state=")
		sb.WriteString(tagEscaper.Replace(ev.State))
	}
	keys := make([]string, 0, len(ev.Attributes))
	for k, v := range ev.Attributes {
		if k == "" || v == "" || k == "host" || k == "state" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sb.WriteByte(',')
		sb.WriteString(tagEscaper.Replace(k))
		sb.WriteByte('=')
		sb.WriteString(tagEscaper.Replace(ev.Attributes[k]))
	}
	sb.WriteString(" metric=")
	sb.WriteString(strconv.FormatFloat(ev.Metric, 'f', -1, 64))
	sb.WriteByte(' ')
	sb.WriteString(strconv.FormatInt(int64(math.Round(ev.Time*1e9)), 10))
	return sb.String()
}
