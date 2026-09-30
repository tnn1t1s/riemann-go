// Package influx is the adapter that turns firings into InfluxDB v2 line
// protocol and writes them in batches.
package influx

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tnn1t1s/riemann-go/internal/engine"
)

// Client writes to one org and bucket.
type Client struct {
	URL    string
	Org    string
	Bucket string
	Token  string // empty means no Authorization header
	HTTP   *http.Client
}

// New returns a client. token may be empty.
func New(baseURL, org, bucket, token string) *Client {
	return &Client{URL: strings.TrimRight(baseURL, "/"), Org: org, Bucket: bucket, Token: token, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

var (
	measurementEsc = strings.NewReplacer(",", `\,`, " ", `\ `)
	tagEsc         = strings.NewReplacer(",", `\,`, "=", `\=`, " ", `\ `)
)

// Line renders one firing as a line of line protocol, or "" when the event
// carries no metric. The engine refuses metric-less events before they reach
// the queue, so the empty case is defensive.
func Line(f engine.Firing) string {
	ev := f.Event
	if ev.Metric == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(measurementEsc.Replace(ev.Service))
	b.WriteString(",host=")
	b.WriteString(tagEsc.Replace(ev.Host))
	if ev.State != "" {
		b.WriteString(",state=")
		b.WriteString(tagEsc.Replace(ev.State))
	}
	keys := make([]string, 0, len(ev.Attributes))
	for k := range ev.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := ev.Attributes[k]
		if k == "" || v == "" || k == "host" || k == "state" {
			continue
		}
		b.WriteString(",")
		b.WriteString(tagEsc.Replace(k))
		b.WriteString("=")
		b.WriteString(tagEsc.Replace(v))
	}
	b.WriteString(" metric=")
	b.WriteString(strconv.FormatFloat(*ev.Metric, 'g', -1, 64))
	b.WriteString(" ")
	b.WriteString(strconv.FormatInt(int64(math.Round(ev.Time*1e9)), 10))
	return b.String()
}

// Deliver writes a batch as one POST to /api/v2/write.
func (c *Client) Deliver(fs []engine.Firing) error {
	var body bytes.Buffer
	for _, f := range fs {
		line := Line(f)
		if line == "" {
			continue
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	if body.Len() == 0 {
		return nil
	}
	q := url.Values{}
	q.Set("org", c.Org)
	q.Set("bucket", c.Bucket)
	q.Set("precision", "ns")
	req, err := http.NewRequest(http.MethodPost, c.URL+"/api/v2/write?"+q.Encode(), &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.Token != "" {
		req.Header.Set("Authorization", "Token "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("influx: %s", resp.Status)
	}
	return nil
}
