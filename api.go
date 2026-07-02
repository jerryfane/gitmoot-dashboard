package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"
)

// writeJSON encodes v as indented JSON with the given status code. Encoding
// failures are reported as 500s (best-effort, since headers may already be set).
func writeJSON(w http.ResponseWriter, code int, v any) {
	buf, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	buf = append(buf, '\n')
	w.Write(buf)
}

// statusForError maps a DataSource error to an HTTP status code: not-found
// sentinels become 404, everything else 500.
func statusForError(err error) int {
	if errors.Is(err, ErrRunNotFound) || errors.Is(err, ErrJobNotFound) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

// handleRuns serves GET /api/runs -> []RunSummary.
func (s *server) handleRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.ds.Runs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if runs == nil {
		runs = []RunSummary{}
	}
	writeJSON(w, http.StatusOK, runs)
}

// handleState serves GET /api/state?run=<id> -> State. An empty run resolves to
// the active/most-recent run.
func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	run := r.URL.Query().Get("run")
	state, err := s.ds.State(r.Context(), run)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// handleJob serves GET /api/job/{id} -> Node.
func (s *server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing job id", http.StatusBadRequest)
		return
	}
	node, err := s.ds.Job(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	writeJSON(w, http.StatusOK, node)
}

// handleGraph serves GET /api/graph -> Graph, the whole-history galaxy view.
// An optional ?repo= scopes the graph to a single repository.
func (s *server) handleGraph(w http.ResponseWriter, r *http.Request) {
	g, err := s.ds.Graph(r.Context(), r.URL.Query().Get("repo"))
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	writeJSON(w, http.StatusOK, g)
}
