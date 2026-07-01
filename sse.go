package dashboard

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// sseKeepAlive is how often an idle SSE connection emits a comment line so
// proxies and clients keep the stream open.
const sseKeepAlive = 15 * time.Second

// broker is a small subscribe/broadcast hub. Producers publish State snapshots
// and every live subscriber receives a copy on its own buffered channel. A slow
// subscriber that cannot keep up drops snapshots rather than blocking the
// broadcaster. broker is safe for concurrent use.
type broker struct {
	mu   sync.Mutex
	seq  int
	subs map[int]chan State
}

func newBroker() *broker {
	return &broker{subs: make(map[int]chan State)}
}

// subscribe registers a new subscriber, seeds it with the given initial
// snapshot, and returns its channel plus an idempotent cancel func that
// unregisters and closes the channel.
func (b *broker) subscribe(initial State) (<-chan State, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.seq
	b.seq++
	ch := make(chan State, 16)
	ch <- initial // buffered, never blocks
	b.subs[id] = ch
	return ch, func() { b.unsubscribe(id) }
}

func (b *broker) unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(ch)
	}
}

// publish broadcasts a snapshot to every current subscriber, dropping the
// snapshot for any subscriber whose buffer is full.
func (b *broker) publish(st State) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- st:
		default:
			// Subscriber is behind; drop rather than block the broadcaster.
		}
	}
}

// handleEvents serves GET /events?run=<id> as a Server-Sent Events stream. It
// subscribes to the DataSource for the requested run and writes each State
// snapshot as a `data: <json>\n\n` frame, flushing after every write. A
// periodic keep-alive comment holds the connection open, and the subscription
// is cleaned up when the client disconnects.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	run := r.URL.Query().Get("run")
	ch, cancel, err := s.ds.Subscribe(r.Context(), run)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer cancel()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepAlive := time.NewTicker(sseKeepAlive)
	defer keepAlive.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAlive.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case st, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(st)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
