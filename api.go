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
	if errors.Is(err, ErrRunNotFound) || errors.Is(err, ErrJobNotFound) || errors.Is(err, ErrAgentNotFound) {
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

// handleJobs serves GET /api/jobs -> []JobSummary, every job across all runs
// (the client filters). Mirrors handleRuns.
func (s *server) handleJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.ds.Jobs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if jobs == nil {
		jobs = []JobSummary{}
	}
	writeJSON(w, http.StatusOK, jobs)
}

// handleAgents serves GET /api/agents -> []AgentSummary. Mirrors handleRuns.
func (s *server) handleAgents(w http.ResponseWriter, r *http.Request) {
	agents, err := s.ds.Agents(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if agents == nil {
		agents = []AgentSummary{}
	}
	writeJSON(w, http.StatusOK, agents)
}

// handleAgent serves GET /api/agent/{name} -> AgentDetail, the click-through
// detail for a single agent. Unknown names map to 404 (mirrors handleJob).
func (s *server) handleAgent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "missing agent name", http.StatusBadRequest)
		return
	}
	detail, err := s.ds.Agent(r.Context(), name)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if detail.Versions == nil {
		detail.Versions = []TemplateVersionInfo{}
	}
	writeJSON(w, http.StatusOK, detail)
}

// handleCharts serves GET /api/charts?days=N -> Charts. days accepts only 0
// (all history), 7, 30 or 90; any missing/invalid/other value defaults to 30.
// Mirrors handleRuns.
func (s *server) handleCharts(w http.ResponseWriter, r *http.Request) {
	days := 30
	switch r.URL.Query().Get("days") {
	case "0":
		days = 0
	case "7":
		days = 7
	case "30":
		days = 30
	case "90":
		days = 90
	}
	charts, err := s.ds.Charts(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if charts.Days == nil {
		charts.Days = []ChartDay{}
	}
	if charts.Agents == nil {
		charts.Agents = []ChartAgent{}
	}
	if charts.Repos == nil {
		charts.Repos = []ChartRepo{}
	}
	writeJSON(w, http.StatusOK, charts)
}

// handleHealth serves GET /api/health -> Health. Mirrors handleRuns.
func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	h, err := s.ds.Health(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if h.Locks == nil {
		h.Locks = []HealthLock{}
	}
	if h.ResourceLocks == nil {
		h.ResourceLocks = []HealthResourceLock{}
	}
	if h.Stuck == nil {
		h.Stuck = []HealthStuckJob{}
	}
	if h.RecentFailures == nil {
		h.RecentFailures = []HealthFailure{}
	}
	writeJSON(w, http.StatusOK, h)
}

// handleLearningSkills serves GET /api/learning/skills -> Skills, the SkillOpt
// evolution overview. Mirrors handleRuns; every list is coerced non-nil so the
// client always sees JSON arrays.
func (s *server) handleLearningSkills(w http.ResponseWriter, r *http.Request) {
	skills, err := s.ds.Skills(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if skills.Templates == nil {
		skills.Templates = []SkillTemplate{}
	}
	for i := range skills.Templates {
		if skills.Templates[i].Versions == nil {
			skills.Templates[i].Versions = []SkillVersion{}
		}
		if skills.Templates[i].Pending == nil {
			skills.Templates[i].Pending = []SkillCandidate{}
		}
	}
	writeJSON(w, http.StatusOK, skills)
}

// handleLearningKnowledge serves GET /api/learning/knowledge -> Knowledge, the
// memory brain graph. Mirrors handleRuns; every list is coerced non-nil so the
// client always sees JSON arrays.
func (s *server) handleLearningKnowledge(w http.ResponseWriter, r *http.Request) {
	k, err := s.ds.Knowledge(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	if k.Agents == nil {
		k.Agents = []KnowledgeAgent{}
	}
	if k.Facts == nil {
		k.Facts = []KnowledgeFact{}
	}
	if k.Edges == nil {
		k.Edges = []KnowledgeEdge{}
	}
	writeJSON(w, http.StatusOK, k)
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
