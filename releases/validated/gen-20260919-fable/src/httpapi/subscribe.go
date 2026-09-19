package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/tnn1t1s/riemann-go/event"
)

// subscriber is one SSE client. ch is its bounded queue: capacity
// subscribe.queue_capacity, policy shed-newest, dropped counted in lagged
// (reported to the client) and in the API's subDropped (reported at /metrics).
type subscriber struct {
	ch     chan *event.Event
	lagged atomic.Int64
}

// subscribe streams Server-Sent Events.
// SPEC-GAP: the frame format is not pinned. A snapshot entry is sent as
// "event: snapshot", a live event as an unnamed frame, and both carry one
// event object as data. A subscriber that fell behind receives
// "event: lagged" with data {"count":n}, n being the events it missed since
// the last such frame.
func (a *API) subscribe(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported on this connection")
		return
	}
	keep, err := filter(r.URL.Query().Get("q"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	sub := &subscriber{ch: make(chan *event.Event, a.cfg.SubscribeQueueCapacity)}
	a.subsMu.Lock()
	a.subscribers[sub] = struct{}{}
	a.subsMu.Unlock()

	// Called on a partition loop: it filters and offers, and never blocks.
	deliver := func(ev *event.Event) {
		if !keep(ev, a.eng.Now()) {
			return
		}
		select {
		case sub.ch <- ev:
		default:
			sub.lagged.Add(1)
			a.subDropped.Add(1)
		}
	}
	snapshot, cancel := a.eng.Subscribe(deliver, r.URL.Query().Get("snapshot") == "true")
	defer func() {
		cancel()
		a.subsMu.Lock()
		delete(a.subscribers, sub)
		a.subsMu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": subscribed\n\n")
	now := a.eng.Now()
	for _, ev := range snapshot {
		if keep(ev, now) {
			writeFrame(w, "snapshot", ev)
		}
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.ch:
			if n := sub.lagged.Swap(0); n > 0 {
				writeFrame(w, "lagged", map[string]int64{"count": n})
			}
			writeFrame(w, "", ev)
			flusher.Flush()
		}
	}
}

func writeFrame(w http.ResponseWriter, name string, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	if name != "" {
		fmt.Fprintf(w, "event: %s\n", name)
	}
	fmt.Fprintf(w, "data: %s\n\n", data)
}
