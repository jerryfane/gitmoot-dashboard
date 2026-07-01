package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// readFirstSSEData reads the response body line by line and returns the JSON
// payload of the first `data:` frame.
func readFirstSSEData(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	deadline := time.After(5 * time.Second)
	lines := make(chan string, 1)
	errs := make(chan error, 1)
	go func() {
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				errs <- err
				return
			}
			if strings.HasPrefix(line, "data:") {
				lines <- strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				return
			}
		}
	}()
	select {
	case l := <-lines:
		return l
	case err := <-errs:
		t.Fatalf("reading SSE stream: %v", err)
	case <-deadline:
		t.Fatal("timed out waiting for first SSE data frame")
	}
	return ""
}

func TestEventsStreamsInitialState(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events?run="+fakeRunID, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	data := readFirstSSEData(t, bufio.NewReader(resp.Body))
	var st State
	if err := json.Unmarshal([]byte(data), &st); err != nil {
		t.Fatalf("unmarshal SSE data %q: %v", data, err)
	}
	if st.RunID != fakeRunID {
		t.Fatalf("streamed RunID = %q, want %q", st.RunID, fakeRunID)
	}
	if len(st.Nodes) == 0 {
		t.Fatalf("streamed state has no nodes")
	}
	// Disconnect; the server-side subscription must be cleaned up.
	cancel()
}

func TestEventsUnknownRunReturns404(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events?run=does-not-exist")
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestBrokerBroadcast exercises the hub directly: every subscriber receives a
// published snapshot, and unsubscribing closes the channel.
func TestBrokerBroadcast(t *testing.T) {
	b := newBroker()
	initial := State{RunID: "r0"}
	ch, cancel := b.subscribe(initial)

	if got := <-ch; got.RunID != "r0" {
		t.Fatalf("initial snapshot RunID = %q, want r0", got.RunID)
	}

	b.publish(State{RunID: "r1"})
	select {
	case got := <-ch:
		if got.RunID != "r1" {
			t.Fatalf("published RunID = %q, want r1", got.RunID)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive published snapshot")
	}

	cancel()
	if _, open := <-ch; open {
		t.Fatal("channel should be closed after cancel")
	}

	// publish after all subscribers are gone must not panic.
	b.publish(State{RunID: "r2"})
}

// TestBrokerDropsWhenFull verifies a slow subscriber does not block the
// broadcaster: publishing far more than the buffer capacity returns promptly.
func TestBrokerDropsWhenFull(t *testing.T) {
	b := newBroker()
	_, cancel := b.subscribe(State{RunID: "seed"})
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			b.publish(State{RunID: "x"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a full subscriber buffer")
	}
}
