package dashboard

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

	// The MemoryEnabled chip mirrors the agent's config memory switch. The seeded
	// fake feed spans both branches: at least one enrolled row and one not, so the
	// serialized boolean is exercised in both directions.
	byName := map[string]AgentSummary{}
	for _, a := range agents {
		byName[a.Name] = a
	}
	if !byName[fakeTemplatedAgent].MemoryEnabled {
		t.Fatalf("expected %q to carry MemoryEnabled=true (memory chip)", fakeTemplatedAgent)
	}
	if byName["ci-runner"].MemoryEnabled {
		t.Fatalf("expected ci-runner MemoryEnabled=false (not enrolled)")
	}
	// A config with memory OFF must still surface MemoryEnabled=false.
	if byName["implementer"].MemoryEnabled {
		t.Fatalf("expected implementer MemoryEnabled=false (config memory off)")
	}
}

func TestHandleAgent(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// The one seeded agent that carries a template + version history.
	var detail AgentDetail
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/agent/"+fakeTemplatedAgent), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if detail.Name != fakeTemplatedAgent {
		t.Fatalf("detail.Name = %q, want %q", detail.Name, fakeTemplatedAgent)
	}
	if detail.Runtime == "" {
		t.Fatalf("detail missing embedded summary (runtime): %+v", detail)
	}
	if detail.Template == nil {
		t.Fatalf("expected a template for %q", fakeTemplatedAgent)
	}
	if detail.Template.ID == "" {
		t.Fatalf("template missing id: %+v", detail.Template)
	}
	// The seeded templated agent carries a config section (memory on) and a
	// non-empty memory pool, so the detail's config/memory fields are exercised.
	if detail.Config == nil {
		t.Fatalf("expected a config section for %q", fakeTemplatedAgent)
	}
	if !detail.Config.Memory {
		t.Fatalf("expected config.memory=true for %q: %+v", fakeTemplatedAgent, detail.Config)
	}
	if !detail.MemoryEnabled {
		t.Fatalf("expected MemoryEnabled=true on the detail's embedded summary")
	}
	// Pool knobs are populated (parse-time defaults are folded in server-side).
	if detail.Config.MaxBackground == 0 || detail.Config.IdleTimeout == "" || detail.Config.JobTimeout == "" {
		t.Fatalf("expected config pool knobs populated: %+v", detail.Config)
	}
	if detail.MemoryFacts <= 0 || detail.MemoryObservations <= 0 {
		t.Fatalf("expected non-zero memory counts: facts=%d observations=%d", detail.MemoryFacts, detail.MemoryObservations)
	}
	// The per-agent detail carries the template's full prompt body (multi-line).
	if !strings.Contains(detail.Template.Content, "\n") || !strings.Contains(detail.Template.Content, "Researcher agent") {
		t.Fatalf("template content missing/not the multi-line prompt body: %q", detail.Template.Content)
	}
	if len(detail.Versions) != 3 {
		t.Fatalf("len(Versions) = %d, want 3", len(detail.Versions))
	}
	// Newest first, exactly one Current marker, states cover promoted/canary/
	// pending, the canary version carries a sample, and every version carries its
	// own full prompt body — distinct per version so the content viewer is exercised.
	var current, canarySample int
	states := map[string]bool{}
	seenContent := map[string]bool{}
	for i, v := range detail.Versions {
		if v.ID == "" || v.State == "" {
			t.Fatalf("version missing id/state: %+v", v)
		}
		if v.Content == "" {
			t.Fatalf("version %s missing content body: %+v", v.ID, v)
		}
		if seenContent[v.Content] {
			t.Fatalf("versions share identical content (expected distinct per version): %+v", detail.Versions)
		}
		seenContent[v.Content] = true
		states[v.State] = true
		if i > 0 && detail.Versions[i-1].Number < v.Number {
			t.Fatalf("versions not newest-first: %+v", detail.Versions)
		}
		if v.Current {
			current++
		}
		if v.State == "canary" && v.CanarySample > 0 {
			canarySample++
		}
	}
	if current != 1 {
		t.Fatalf("Current markers = %d, want exactly 1", current)
	}
	if canarySample == 0 {
		t.Fatalf("expected a canary version with a CanarySample: %+v", detail.Versions)
	}
	for _, want := range []string{"promoted", "canary", "pending"} {
		if !states[want] {
			t.Fatalf("versions missing state %q: %+v", want, detail.Versions)
		}
	}
}

func TestHandleAgentNoTemplate(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// An agent without a template: Template omitted, Versions still non-nil.
	var detail AgentDetail
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/agent/ci-runner"), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if detail.Name != "ci-runner" {
		t.Fatalf("detail.Name = %q, want ci-runner", detail.Name)
	}
	if detail.Template != nil {
		t.Fatalf("expected nil template for ci-runner: %+v", detail.Template)
	}
	if detail.Versions == nil {
		t.Fatalf("Versions must be non-nil even without a template")
	}
	if len(detail.Versions) != 0 {
		t.Fatalf("len(Versions) = %d, want 0 for a template-less agent", len(detail.Versions))
	}
	// ci-runner has neither a config section nor an enrolled memory pool, so its
	// detail omits Config entirely and reports zero memory counts.
	if detail.Config != nil {
		t.Fatalf("expected nil config for ci-runner (no config section): %+v", detail.Config)
	}
	if detail.MemoryEnabled {
		t.Fatalf("expected MemoryEnabled=false for ci-runner")
	}
	if detail.MemoryFacts != 0 || detail.MemoryObservations != 0 {
		t.Fatalf("expected zero memory counts for ci-runner: facts=%d observations=%d", detail.MemoryFacts, detail.MemoryObservations)
	}
}

// TestHandleAgentMemoryWithConfig covers the branch where a memory-enrolled agent
// (MemoryEnabled chip + a non-empty pool) carries its [agents.<name>] config
// section. This mirrors the live webDataSource, which sets MemoryEnabled and
// Config together in one comma-ok block, so memory-on always implies a non-nil
// config — the fake feed must not present a memory-on/no-config state the real
// backend can never emit.
func TestHandleAgentMemoryWithConfig(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var detail AgentDetail
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/agent/reviewer-kimi"), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !detail.MemoryEnabled {
		t.Fatalf("expected reviewer-kimi MemoryEnabled=true")
	}
	if detail.Config == nil {
		t.Fatalf("expected non-nil config for reviewer-kimi (enrolled agents always carry a config section)")
	}
	if !detail.Config.Memory {
		t.Fatalf("expected reviewer-kimi config memory on: %+v", detail.Config)
	}
	if detail.MemoryFacts <= 0 || detail.MemoryObservations <= 0 {
		t.Fatalf("expected non-zero memory counts: facts=%d observations=%d", detail.MemoryFacts, detail.MemoryObservations)
	}
}

func TestHandleAgentNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/agent/does-not-exist")
	if err != nil {
		t.Fatalf("GET /api/agent/does-not-exist: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleAgentDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// fakeTemplatedAgent does not appear in the seeded run, so its detail (summary
	// + template + versions) is byte-stable across calls.
	url := srv.URL + "/api/agent/" + fakeTemplatedAgent
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("agent detail not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
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

// getRaw fetches url and returns the 200 response body, failing the test on any
// error or non-200 status.
func getRaw(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status = %d, want 200", url, resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("GET %s: Content-Type = %q, want application/json", url, ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("GET %s: read body: %v", url, err)
	}
	return body
}

func TestHandleCharts(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var charts Charts
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/charts"), &charts); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Missing days defaults to a 30-day window.
	if len(charts.Days) != 30 {
		t.Fatalf("default window len(Days) = %d, want 30", len(charts.Days))
	}
	assertContinuousDays(t, charts.Days)
	if charts.Days == nil || charts.Agents == nil || charts.Repos == nil {
		t.Fatalf("charts slices must be non-nil: %+v", charts)
	}
	if len(charts.Agents) == 0 || len(charts.Repos) == 0 {
		t.Fatalf("expected non-empty agents/repos breakdowns: %+v", charts)
	}
	if len(charts.Agents) > 12 || len(charts.Repos) > 12 {
		t.Fatalf("agents/repos capped at 12: got %d/%d", len(charts.Agents), len(charts.Repos))
	}
	// Agents sorted by Jobs desc, name tie-break.
	for i := 1; i < len(charts.Agents); i++ {
		a, b := charts.Agents[i-1], charts.Agents[i]
		if a.Jobs < b.Jobs || (a.Jobs == b.Jobs && a.Name > b.Name) {
			t.Fatalf("agents not sorted Jobs desc/name asc: %+v then %+v", a, b)
		}
	}
	if charts.Totals.Jobs == 0 || charts.Totals.ActiveAgents == 0 {
		t.Fatalf("totals should be populated: %+v", charts.Totals)
	}
}

func TestHandleChartsDaysValidation(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	cases := []struct {
		days    string
		wantLen int // 0 == just assert continuity (all-history length is data-derived)
	}{
		{"7", 7},
		{"30", 30},
		{"90", 90},
		{"", 30},    // missing -> 30
		{"abc", 30}, // invalid -> 30
		{"5", 30},   // unaccepted value -> 30
		{"-1", 30},  // unaccepted value -> 30
		{"0", 0},    // all history
	}
	for _, tc := range cases {
		var charts Charts
		if err := json.Unmarshal(getRaw(t, srv.URL+"/api/charts?days="+tc.days), &charts); err != nil {
			t.Fatalf("days=%q decode: %v", tc.days, err)
		}
		if tc.wantLen != 0 && len(charts.Days) != tc.wantLen {
			t.Fatalf("days=%q: len(Days) = %d, want %d", tc.days, len(charts.Days), tc.wantLen)
		}
		if len(charts.Days) == 0 {
			t.Fatalf("days=%q: Days must not be empty", tc.days)
		}
		assertContinuousDays(t, charts.Days)
	}
}

func TestHandleChartsDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	for _, days := range []string{"", "7", "30", "90", "0"} {
		url := srv.URL + "/api/charts?days=" + days
		if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
			t.Fatalf("days=%q: charts not byte-stable across calls\nfirst:  %s\nsecond: %s", days, a, b)
		}
	}
}

func TestHandleHealth(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var h Health
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/health"), &h); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !h.Daemon.Running {
		t.Fatalf("expected daemon running: %+v", h.Daemon)
	}
	if h.Daemon.Version == "" {
		t.Fatalf("expected daemon version: %+v", h.Daemon)
	}
	// The update check is present and exercises the "update available" badge.
	if h.Update == nil {
		t.Fatalf("expected an update check in health")
	}
	if !h.Update.UpdateAvailable {
		t.Fatalf("expected update available: %+v", h.Update)
	}
	if h.Update.Current == "" || h.Update.Latest == "" {
		t.Fatalf("update missing current/latest: %+v", h.Update)
	}
	if h.Update.Current != h.Daemon.Version {
		t.Fatalf("update.Current %q != daemon.Version %q", h.Update.Current, h.Daemon.Version)
	}
	if u := h.Update.ReleaseURL; u != "" && !strings.HasPrefix(u, "https://") && !strings.HasPrefix(u, "http://") {
		t.Fatalf("update release url not http(s): %q", u)
	}
	if h.Locks == nil || h.ResourceLocks == nil || h.Stuck == nil || h.RecentFailures == nil {
		t.Fatalf("health slices must be non-nil: %+v", h)
	}
	if len(h.Stuck) == 0 {
		t.Fatalf("expected at least one stuck job")
	}
	if len(h.Locks) == 0 {
		t.Fatalf("expected at least one branch lock")
	}
	// Locks oldest first.
	for i := 1; i < len(h.Locks); i++ {
		if h.Locks[i-1].AcquiredAt > h.Locks[i].AcquiredAt {
			t.Fatalf("locks not oldest-first: %+v", h.Locks)
		}
	}
	// Stuck oldest first.
	for i := 1; i < len(h.Stuck); i++ {
		if h.Stuck[i-1].Since > h.Stuck[i].Since {
			t.Fatalf("stuck not oldest-first: %+v", h.Stuck)
		}
	}
	// Recent failures newest first, capped at 10.
	if len(h.RecentFailures) > 10 {
		t.Fatalf("recent failures = %d, want <= 10", len(h.RecentFailures))
	}
	for i := 1; i < len(h.RecentFailures); i++ {
		if h.RecentFailures[i-1].At < h.RecentFailures[i].At {
			t.Fatalf("recent failures not newest-first: %+v", h.RecentFailures)
		}
	}
}

func TestHandleHealthDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/health"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("health not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

// assertContinuousDays checks Days is oldest->newest with no gaps (each date is
// exactly one UTC day after the previous).
func assertContinuousDays(t *testing.T, days []ChartDay) {
	t.Helper()
	if len(days) == 0 {
		t.Fatalf("Days is empty")
	}
	var prev time.Time
	for i, d := range days {
		cur, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			t.Fatalf("Days[%d].Date = %q not YYYY-MM-DD: %v", i, d.Date, err)
		}
		if i > 0 {
			if diff := cur.Sub(prev); diff != 24*time.Hour {
				t.Fatalf("Days not continuous at %d: %s -> %s (%v)", i, days[i-1].Date, d.Date, diff)
			}
		}
		prev = cur
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
