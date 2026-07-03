package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleRuns(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/runs")
	if err != nil {
		t.Fatalf("GET /api/runs: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var runs []RunSummary
	if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("len(runs) = %d, want 1", len(runs))
	}
	if runs[0].RunID != fakeRunID {
		t.Fatalf("runs[0].RunID = %q, want %q", runs[0].RunID, fakeRunID)
	}
	if runs[0].Title == "" || runs[0].State == "" {
		t.Fatalf("run summary missing title/state: %+v", runs[0])
	}
}

func TestHandleJobs(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/jobs")
	if err != nil {
		t.Fatalf("GET /api/jobs: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var jobs []JobSummary
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatalf("len(jobs) = 0, want > 0")
	}
	// Sorted Updated desc.
	for i := 1; i < len(jobs); i++ {
		if jobs[i-1].Updated < jobs[i].Updated {
			t.Fatalf("jobs not sorted Updated desc: [%d].Updated=%d < [%d].Updated=%d",
				i-1, jobs[i-1].Updated, i, jobs[i].Updated)
		}
	}
	// Sanity on content: each job carries identity + state, and belongs to the run.
	for _, j := range jobs {
		if j.ID == "" || j.State == "" {
			t.Fatalf("job missing id/state: %+v", j)
		}
		if j.Run != fakeRunID {
			t.Fatalf("job.Run = %q, want %q", j.Run, fakeRunID)
		}
	}
}

func TestHandleAgents(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/agents")
	if err != nil {
		t.Fatalf("GET /api/agents: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var agents []AgentSummary
	if err := json.NewDecoder(resp.Body).Decode(&agents); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(agents) == 0 {
		t.Fatalf("len(agents) = 0, want > 0")
	}

	var ephemeral, registered int
	for _, a := range agents {
		if a.Name == "" || a.Runtime == "" {
			t.Fatalf("agent missing name/runtime: %+v", a)
		}
		if a.Ephemeral {
			ephemeral++
		} else {
			registered++
		}
	}
	if ephemeral != 1 {
		t.Fatalf("ephemeral rollup rows = %d, want exactly 1", ephemeral)
	}
	if registered == 0 {
		t.Fatalf("registered agents = 0, want > 0")
	}
}

func TestHandleState(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/state?run=" + fakeRunID)
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var st State
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if st.RunID != fakeRunID {
		t.Fatalf("RunID = %q, want %q", st.RunID, fakeRunID)
	}
	if len(st.Nodes) != 6 {
		t.Fatalf("len(Nodes) = %d, want 6 (coordinator + 3 implement + integrate + synthesize)", len(st.Nodes))
	}

	// Verify the graph shape: root, three parallel children, an integrate node
	// depending on all three, and a synthesize continuation depending on it.
	byID := map[string]Node{}
	for _, n := range st.Nodes {
		byID[n.ID] = n
	}
	root, ok := byID["job-1"]
	if !ok || root.ParentID != "" || root.Depth != 0 {
		t.Fatalf("root node malformed: %+v", root)
	}
	for _, id := range []string{"job-2", "job-3", "job-4"} {
		n := byID[id]
		if n.ParentID != "job-1" || n.Depth != 1 {
			t.Fatalf("child %s malformed: %+v", id, n)
		}
	}
	integ := byID["job-5"]
	if len(integ.Deps) != 3 {
		t.Fatalf("integrate node deps = %v, want 3", integ.Deps)
	}
	synth := byID["job-6"]
	if len(synth.Deps) != 1 || synth.Deps[0] != "job-5" {
		t.Fatalf("synthesize node deps = %v, want [job-5]", synth.Deps)
	}
}

func TestHandleStateEmptyRunReturnsActive(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/state")
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var st State
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if st.RunID != fakeRunID {
		t.Fatalf("empty run should resolve to active run; got %q", st.RunID)
	}
}

func TestHandleStateNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/state?run=does-not-exist")
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleJob(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/job/job-2")
	if err != nil {
		t.Fatalf("GET /api/job/job-2: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var node Node
	if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if node.ID != "job-2" {
		t.Fatalf("node.ID = %q, want job-2", node.ID)
	}
	if node.Events == nil {
		t.Fatalf("node.Events should be non-nil")
	}
}

func TestHandleJobNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/job/nope")
	if err != nil {
		t.Fatalf("GET /api/job/nope: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestStateResponseIsIndented(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/state")
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "\n  ") {
		t.Fatalf("expected indented JSON, got: %q", string(buf[:n]))
	}
}
