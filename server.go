package dashboard

import (
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// Serve returns an http.Handler serving the read-only dashboard: the embedded
// static UI (with SPA fallback to index.html) plus the JSON API (handleRuns/
// handleState/handleJob in api.go) and the SSE stream (handleEvents in sse.go).
func Serve(ds DataSource) http.Handler {
	s := &server{ds: ds}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/runs", s.handleRuns)
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/job/{id}", s.handleJob)
	mux.HandleFunc("GET /api/graph", s.handleGraph)
	mux.HandleFunc("GET /events", s.handleEvents)

	// Everything else is served from the embedded static assets, with an SPA
	// fallback to index.html for unknown paths.
	mux.Handle("/", s.staticHandler())

	return mux
}

type server struct {
	ds DataSource
}

// The JSON API handlers (handleRuns/handleState/handleJob) live in api.go and
// the SSE handler (handleEvents) lives in sse.go.

// staticHandler serves the embedded web/dist assets. Requests that do not map
// to an existing file fall back to index.html so the client-side router can
// take over.
func (s *server) staticHandler() http.Handler {
	dist := webDistFS()
	fileServer := http.FileServer(http.FS(dist))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upath := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if upath == "" {
			upath = "index.html"
		}
		if f, err := dist.Open(upath); err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}
		serveIndex(w, r, dist)
	})
}

// serveIndex writes web/dist/index.html for SPA fallback.
func serveIndex(w http.ResponseWriter, r *http.Request, dist fs.FS) {
	data, err := fs.ReadFile(dist, "index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
