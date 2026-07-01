package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeIndex(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	if !strings.Contains(string(body[:n]), "gitmoot dashboard") {
		t.Fatalf("index body missing placeholder marker: %q", string(body[:n]))
	}
}

func TestServeSPAFallback(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/some/client/route")
	if err != nil {
		t.Fatalf("GET /some/client/route: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SPA fallback status = %d, want 200", resp.StatusCode)
	}
}

// TestStateRouteRegistered asserts the /api/state route is wired into the mux
// and served by the JSON handler (not the static SPA fallback, which would
// return an HTML index).
func TestStateRouteRegistered(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/state")
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/state status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("GET /api/state Content-Type = %q, want application/json", ct)
	}
}
