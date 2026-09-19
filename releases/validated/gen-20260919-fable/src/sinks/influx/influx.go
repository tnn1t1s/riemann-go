// Package influx is the adapter behind {"sink":"influx"}: a bounded
// shed-newest queue and one worker that writes line protocol to InfluxDB v2.
package influx

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/boundedq"
	"github.com/tnn1t1s/riemann-go/rule"
)

// requestTimeout bounds one write, in seconds of wall time, so a server that
// never answers cannot hold the worker forever. Default, not measured.
const requestTimeout = 10 * time.Second

// maxBatchLines bounds the lines in one write request. The worker batches
// whatever is queued when it becomes free, so under light load a batch is one
// line and under a burst it grows to this bound. Default: 500 lines keeps a
// request near 50 KB at an estimated 100 bytes a line; nothing has measured
// the receiving server's preference.
const maxBatchLines = 500

// Config is everything the sink needs; none of it has a default.
type Config struct {
	URL           string // --influx-url, the server root
	Org           string // --influx-org
	Bucket        string // --influx-bucket
	QueueCapacity int    // sink.influx.queue_capacity, events
}

// Sink implements rule.Sink. The queue holds rendered lines.
type Sink struct {
	writeURL string
	q        *boundedq.Queue[string]
	client   *http.Client
}

// New builds the sink. Nothing talks to the server until Start.
func New(cfg Config) *Sink {
	v := url.Values{}
	v.Set("org", cfg.Org)
	v.Set("bucket", cfg.Bucket)
	v.Set("precision", "ns")
	return &Sink{writeURL: strings.TrimRight(cfg.URL, "/") + "/api/v2/write?" + v.Encode(),
		q: boundedq.New[string](cfg.QueueCapacity), client: &http.Client{Timeout: requestTimeout}}
}

// Start launches the worker.
func (s *Sink) Start() { go s.work() }

// Offer never blocks. An event with no metric is not written and counts as
// dropped, because a point with no field is not a point.
func (s *Sink) Offer(f rule.Firing) {
	line, ok := Line(f)
	if !ok {
		s.q.Refuse()
		return
	}
	s.q.Offer(line)
}

// Stats reads the queue.
func (s *Sink) Stats() rule.SinkStats { return rule.SinkStats(s.q.Stats()) }

var (
	measurementEscaper = strings.NewReplacer(`\`, `\\`, `,`, `\,`, ` `, `\ `, "\n", `\n`)
	tagEscaper         = strings.NewReplacer(`\`, `\\`, `,`, `\,`, `=`, `\=`, ` `, `\ `, "\n", `\n`)
)

// Line renders one firing as
// <service>,host=<host>[,state=<state>][,<attr-key>=<attr-value>]... metric=<metric> <time_ns>
// SPEC-GAP: attribute tags are written in key order, and an attribute whose
// key or value is empty is left out, since line protocol has no empty tag.
func Line(f rule.Firing) (string, bool) {
	ev := f.Event
	if !ev.HasMetric {
		return "", false
	}
	var b strings.Builder
	b.WriteString(measurementEscaper.Replace(ev.Service))
	b.WriteString(",host=")
	b.WriteString(tagEscaper.Replace(ev.Host))
	if ev.State != "" {
		b.WriteString(",state=")
		b.WriteString(tagEscaper.Replace(ev.State))
	}
	keys := make([]string, 0, len(ev.Attributes))
	for k, v := range ev.Attributes {
		if k != "" && v != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteByte(',')
		b.WriteString(tagEscaper.Replace(k))
		b.WriteByte('=')
		b.WriteString(tagEscaper.Replace(ev.Attributes[k]))
	}
	b.WriteString(" metric=")
	b.WriteString(strconv.FormatFloat(ev.Metric, 'f', -1, 64))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(int64(ev.Time*1e9), 10))
	return b.String(), true
}

func (s *Sink) work() {
	for {
		lines := s.q.Take(maxBatchLines)
		s.q.Done(len(lines), s.write(lines))
	}
}

// write makes one attempt. As with ntfy there is no retry, and a request that
// got any HTTP response counts as processed.
func (s *Sink) write(lines []string) bool {
	req, err := http.NewRequest(http.MethodPost, s.writeURL, strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		log.Printf("influx: request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("influx: write: %v", err)
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("influx: write: status %d", resp.StatusCode)
	}
	return true
}
