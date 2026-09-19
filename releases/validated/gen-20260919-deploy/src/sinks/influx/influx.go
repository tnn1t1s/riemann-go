// Package influx is the adapter behind a {"sink":"influx"} leaf. It owns the
// line-protocol shape SPEC.md pins and one bounded queue.
package influx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/event"
	"github.com/tnn1t1s/riemann-go/rules"
	"github.com/tnn1t1s/riemann-go/sinkqueue"
)

// deliveryTimeout bounds one write request, for the same reason the ntfy
// adapter bounds a publish. A default with no measurement behind it.
const deliveryTimeout = 10 * time.Second

// maxBatchLines is the most lines one write carries: 5000, the batch size
// InfluxDB's own write documentation recommends. The worker sends whatever is
// queued up to that many and never waits to fill a batch, so an idle system
// writes each point as it arrives.
const maxBatchLines = 5000

// Sink writes one line per firing.
type Sink struct {
	writeURL string
	token    string
	queue    *sinkqueue.Queue[rules.Firing]
	client   *http.Client
}

// New returns a sink writing to org and bucket at baseURL, behind a queue of
// the given capacity (sink.influx.queue_capacity). token is empty when no
// credential was configured, and then no Authorization header is sent.
func New(baseURL, org, bucket, token string, capacity int) *Sink {
	q := url.Values{}
	q.Set("org", org)
	q.Set("bucket", bucket)
	q.Set("precision", "ns")
	return &Sink{
		writeURL: strings.TrimRight(baseURL, "/") + "/api/v2/write?" + q.Encode(),
		token:    token,
		queue:    sinkqueue.New[rules.Firing](capacity),
		client:   &http.Client{Timeout: deliveryTimeout},
	}
}

// Offer enqueues a firing or sheds it; it never blocks. An event with no
// metric is not written and counts as dropped, because a point with no field
// is not a point.
func (s *Sink) Offer(f rules.Firing) {
	if !writable(f.Event) {
		s.queue.Refuse()
		return
	}
	s.queue.Offer(f)
}

// SPEC-GAP: line protocol cannot carry NaN or an infinity as a float field,
// and the spec speaks only of an absent metric. Chosen: such a metric is
// treated as absent here, refused and counted.
func writable(ev *event.Event) bool {
	return ev.HasMetric && !math.IsNaN(ev.Metric) && !math.IsInf(ev.Metric, 0)
}

// Stats reads the queue.
func (s *Sink) Stats() sinkqueue.Stats { return s.queue.Stats() }

// Run writes until ctx is done.
func (s *Sink) Run(ctx context.Context) {
	s.queue.Run(ctx, maxBatchLines, func(ctx context.Context, batch []rules.Firing) error {
		err := s.write(ctx, batch)
		if err != nil && ctx.Err() == nil {
			// A debugging artifact only; the loss itself is in `dropped`.
			log.Printf("influx sink: write failed, %d points dropped: %v", len(batch), err)
		}
		return err
	})
}

func (s *Sink) write(ctx context.Context, batch []rules.Firing) error {
	var body bytes.Buffer
	for _, f := range batch {
		body.WriteString(Line(f.Event))
		body.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.writeURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if s.token != "" {
		req.Header.Set("Authorization", "Token "+s.token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("influx replied %s", resp.Status)
	}
	return nil
}

var (
	measurementEscaper = strings.NewReplacer(`\`, `\\`, ",", `\,`, " ", `\ `, "\n", " ")
	tagEscaper         = strings.NewReplacer(`\`, `\\`, ",", `\,`, "=", `\=`, " ", `\ `, "\n", " ")
)

// Line renders one event as
//
//	<service>,host=<host>[,state=<state>][,<attr-key>=<attr-value>]... metric=<metric> <time_ns>
//
// SPEC-GAP: the spec does not say what happens to an attribute line protocol
// cannot carry as a tag. Chosen: an attribute with an empty key or an empty
// value is left out, since line protocol has no empty tag, and an attribute
// named `host` or `state` is left out in favour of the event's own field,
// since a repeated tag key fails the whole write. Attributes are written in
// key order.
func Line(ev *event.Event) string {
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
		if k == "" || v == "" || k == "host" || k == "state" {
			continue
		}
		keys = append(keys, k)
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
	b.WriteString(strconv.FormatInt(int64(math.Round(ev.Time*1e9)), 10))
	return b.String()
}
