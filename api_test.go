package dashboard

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type noWorkflowDataSource struct{ DataSource }
type noChangeCursorDataSource struct{ DataSource }

type testChangeCursorDataSource struct {
	DataSource
	mu     sync.Mutex
	cursor string
	calls  int
}

func (d *testChangeCursorDataSource) ChangeCursor(context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	return d.cursor, nil
}

func (d *testChangeCursorDataSource) setCursor(cursor string) {
	d.mu.Lock()
	d.cursor = cursor
	d.mu.Unlock()
}

func (d *testChangeCursorDataSource) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func newTestChangeServer(ds *testChangeCursorDataSource, poll, heartbeat time.Duration, cap int) (*server, *httptest.Server) {
	s := &server{ds: ds, changes: newChangeWatcher(ds, poll, cap), changeHeartbeat: heartbeat}
	return s, httptest.NewServer(http.HandlerFunc(s.handleChangeEvents))
}

func readChangeStreamLine(t *testing.T, reader *bufio.Reader, timeout time.Duration) string {
	t.Helper()
	lines := make(chan string, 1)
	errs := make(chan error, 1)
	go func() {
		line, err := reader.ReadString('\n')
		if err != nil {
			errs <- err
			return
		}
		lines <- strings.TrimRight(line, "\r\n")
	}()
	select {
	case line := <-lines:
		return line
	case err := <-errs:
		t.Fatalf("read change stream: %v", err)
	case <-time.After(timeout):
		t.Fatal("timed out reading change stream")
	}
	return ""
}

func waitForChangeStream(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for change stream condition")
}

type recordingWorkflowDataSource struct {
	DataSource
	query WorkflowQuery
}

func (d *recordingWorkflowDataSource) Workflows(_ context.Context) ([]WorkflowIndexEntry, error) {
	return []WorkflowIndexEntry{}, nil
}

func (d *recordingWorkflowDataSource) Workflow(_ context.Context, label string, q WorkflowQuery) (WorkflowView, error) {
	d.query = q
	return WorkflowView{Summary: WorkflowSummary{Label: label}}, nil
}

func TestHandleChangeEventsUnsupported(t *testing.T) {
	ds := noChangeCursorDataSource{DataSource: NewFakeDataSource()}
	srv := httptest.NewServer(Serve(ds))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatalf("GET /api/events: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleChangeEventsHeadersHeartbeatAndNoReplay(t *testing.T) {
	ds := &testChangeCursorDataSource{cursor: "baseline"}
	s, srv := newTestChangeServer(ds, 5*time.Millisecond, 15*time.Millisecond, changeClientCap)
	defer srv.Close()
	if ds.callCount() != 0 || s.changes.watching() {
		t.Fatal("watcher must remain lazy before the first client")
	}

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Last-Event-ID", "stale-cursor")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET change stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q, want no-cache", got)
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no", got)
	}
	if line := readChangeStreamLine(t, bufio.NewReader(resp.Body), time.Second); line != ": keep-alive" {
		t.Fatalf("first frame line = %q, want heartbeat (no cursor replay)", line)
	}
	if s.changes.clientCount() != 1 || !s.changes.watching() {
		t.Fatalf("watcher state clients=%d watching=%t", s.changes.clientCount(), s.changes.watching())
	}
	if ds.callCount() < 1 {
		t.Fatal("first client did not establish a cursor baseline")
	}
	cancel()
	resp.Body.Close()
	waitForChangeStream(t, time.Second, func() bool { return s.changes.clientCount() == 0 && !s.changes.watching() })
}

func TestHandleChangeEventsPingsOnCursorMovement(t *testing.T) {
	ds := &testChangeCursorDataSource{cursor: "1.2"}
	s, srv := newTestChangeServer(ds, 5*time.Millisecond, time.Second, changeClientCap)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET change stream: %v", err)
	}
	reader := bufio.NewReader(resp.Body)
	ds.setCursor("3.4")
	if line := readChangeStreamLine(t, reader, time.Second); line != "event: changed" {
		t.Fatalf("event line = %q", line)
	}
	if line := readChangeStreamLine(t, reader, time.Second); line != "data: 3.4" {
		t.Fatalf("data line = %q", line)
	}
	if line := readChangeStreamLine(t, reader, time.Second); line != "" {
		t.Fatalf("frame terminator = %q", line)
	}
	cancel()
	resp.Body.Close()
	waitForChangeStream(t, time.Second, func() bool { return s.changes.clientCount() == 0 })
}

func TestHandleChangeEventsRegistryCapAndCleanup(t *testing.T) {
	ds := &testChangeCursorDataSource{cursor: "baseline"}
	s, srv := newTestChangeServer(ds, time.Second, time.Second, 2)
	defer srv.Close()

	open := func() (context.CancelFunc, *http.Response) {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("open change stream: %v", err)
		}
		return cancel, resp
	}
	cancelA, respA := open()
	cancelB, respB := open()
	if s.changes.clientCount() != 2 {
		t.Fatalf("clients = %d, want 2", s.changes.clientCount())
	}
	if ds.callCount() != 1 {
		t.Fatalf("baseline calls = %d, want one shared watcher", ds.callCount())
	}
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET capped change stream: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("capped status = %d, want 503", resp.StatusCode)
	}
	resp.Body.Close()

	cancelA()
	respA.Body.Close()
	waitForChangeStream(t, time.Second, func() bool { return s.changes.clientCount() == 1 })
	cancelC, respC := open()
	if respC.StatusCode != http.StatusOK || s.changes.clientCount() != 2 {
		t.Fatalf("replacement client status=%d clients=%d", respC.StatusCode, s.changes.clientCount())
	}
	cancelB()
	cancelC()
	respB.Body.Close()
	respC.Body.Close()
	waitForChangeStream(t, time.Second, func() bool { return s.changes.clientCount() == 0 && !s.changes.watching() })
	if changeClientCap != 32 {
		t.Fatalf("production change client cap = %d, want 32", changeClientCap)
	}
}

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

func TestHandleWorkflowUnsupportedDataSource(t *testing.T) {
	ds := noWorkflowDataSource{DataSource: NewFakeDataSource()}
	srv := httptest.NewServer(Serve(ds))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/workflow/demo-panel")
	if err != nil {
		t.Fatalf("GET unsupported workflow: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "unsupported") {
		t.Fatalf("body = %q, want clear unsupported message", body)
	}
}

func TestHandleWorkflowsUnsupportedDataSource(t *testing.T) {
	ds := noWorkflowDataSource{DataSource: NewFakeDataSource()}
	srv := httptest.NewServer(Serve(ds))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/workflows")
	if err != nil {
		t.Fatalf("GET unsupported workflows: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleOverviewAndTasksUnsupportedDataSource(t *testing.T) {
	ds := noWorkflowDataSource{DataSource: NewFakeDataSource()}
	srv := httptest.NewServer(Serve(ds))
	defer srv.Close()

	for _, path := range []string{"/api/overview", "/api/tasks"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want 404", path, resp.StatusCode)
		}
	}
}

func TestFakeOverviewContractOrderingAndDeterminism(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	raw1 := getRaw(t, srv.URL+"/api/overview")
	raw2 := getRaw(t, srv.URL+"/api/overview")
	if !bytes.Equal(raw1, raw2) {
		t.Fatalf("overview changed across identical reads\nfirst=%s\nsecond=%s", raw1, raw2)
	}
	var overview Overview
	if err := json.Unmarshal(raw1, &overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if len(overview.NeedsYou) < 4 || overview.NeedsYou[0].Kind != "stalled_workflow" || overview.NeedsYou[0].Label == "" || overview.NeedsYou[0].Pane == "" || overview.NeedsYou[0].SessionID == "" || overview.NeedsYou[0].LastNote == "" {
		t.Fatalf("needs_you stalled-first contract = %+v", overview.NeedsYou)
	}
	ciSeen := map[string]bool{}
	for _, item := range overview.NeedsYou {
		if item.Kind == "pr_awaiting_merge" {
			ciSeen[item.CI] = true
		}
		if item.Title == "" || item.Link == "" {
			t.Fatalf("incomplete needs_you item = %+v", item)
		}
	}
	for _, ci := range []string{"green", "red", "pending"} {
		if !ciSeen[ci] {
			t.Fatalf("needs_you fixture missing CI %q: %+v", ci, overview.NeedsYou)
		}
	}
	if len(overview.Activity.Workflows) != 2 || overview.Activity.Workflows[0].Running < overview.Activity.Workflows[1].Running || overview.Activity.Workflows[0].Namespace == "" || overview.Activity.Workflows[0].Campaign == "" || overview.Activity.Workflows[0].Agents == nil || overview.Activity.Queued == 0 || overview.Activity.UnattendedNote == "" {
		t.Fatalf("activity contract = %+v", overview.Activity)
	}
	if len(overview.Today.PerHour) != 24 || len(overview.Today.Notable) != 5 || overview.Today.Completed == 0 || overview.Today.TokensIn == 0 || overview.Today.TokensOut == 0 {
		t.Fatalf("today contract = %+v", overview.Today)
	}
	for i := 1; i < len(overview.Today.Notable); i++ {
		if overview.Today.Notable[i-1].AgeS > overview.Today.Notable[i].AgeS {
			t.Fatalf("notable rows not newest-first: %+v", overview.Today.Notable)
		}
	}
	if len(overview.Scheduled) < 3 || len(overview.Fleet) < 4 || !overview.Fleet[0].Running {
		t.Fatalf("scheduled/fleet contract scheduled=%+v fleet=%+v", overview.Scheduled, overview.Fleet)
	}
	for _, field := range []string{"\"needs_you\"", "\"session_id\"", "\"per_hour\"", "\"next_in_s\"", "\"jobs_today\""} {
		if !bytes.Contains(raw1, []byte(field)) {
			t.Fatalf("overview wire payload missing %s: %s", field, raw1)
		}
	}
}

func TestFakeTasksContractOrderingAndDeterminism(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	raw1 := getRaw(t, srv.URL+"/api/tasks")
	raw2 := getRaw(t, srv.URL+"/api/tasks")
	if !bytes.Equal(raw1, raw2) {
		t.Fatalf("tasks changed across identical reads\nfirst=%s\nsecond=%s", raw1, raw2)
	}
	var tasks []TaskSummary
	if err := json.Unmarshal(raw1, &tasks); err != nil {
		t.Fatalf("decode tasks: %v", err)
	}
	if len(tasks) != 53 {
		t.Fatalf("tasks = %d, want 53", len(tasks))
	}
	wantCounts := map[string]int{"planned": 23, "implementing": 11, "pr_open": 10, "blocked": 5, "merged": 4}
	gotCounts := map[string]int{}
	lastRank := -1
	lastUpdated := int64(0)
	for i, task := range tasks {
		gotCounts[task.State]++
		if task.ID == "" || task.Title == "" || task.Repo == "" || task.UpdatedAt == 0 || task.AgeS <= 0 {
			t.Fatalf("incomplete task[%d] = %+v", i, task)
		}
		rank := taskStateRank(task.State)
		if rank < lastRank {
			t.Fatalf("task state order regressed at %d: %+v", i, tasks)
		}
		if rank == lastRank && task.UpdatedAt > lastUpdated {
			t.Fatalf("task updated order regressed at %d: %+v", i, tasks)
		}
		lastRank, lastUpdated = rank, task.UpdatedAt
		switch task.State {
		case "pr_open":
			if task.PRNumber == 0 || task.CI == "" {
				t.Fatalf("PR task missing PR/CI = %+v", task)
			}
		case "blocked":
			if task.BlockedReason == "" {
				t.Fatalf("blocked task missing reason = %+v", task)
			}
		case "merged":
			if task.AgeS > 7*24*60*60 {
				t.Fatalf("merged task older than 7d = %+v", task)
			}
		}
	}
	for state, want := range wantCounts {
		if gotCounts[state] != want {
			t.Fatalf("task count %s = %d, want %d", state, gotCounts[state], want)
		}
	}
	for _, field := range []string{"\"pr_number\"", "\"blocked_reason\"", "\"updated_at\"", "\"age_s\""} {
		if !bytes.Contains(raw1, []byte(field)) {
			t.Fatalf("tasks wire payload missing %s: %s", field, raw1)
		}
	}
}

func TestHandleWorkflowNotFound(t *testing.T) {
	t.Setenv("FAKEFEED_WORKFLOWS", "1")
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/workflow/unknown")
	if err != nil {
		t.Fatalf("GET unknown workflow: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleWorkflowSlashLabelRoundTrip(t *testing.T) {
	t.Setenv("FAKEFEED_WORKFLOWS", "1")
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/workflow/fable%2Fdashboard-redesign?maxRuns=50&maxNotes=200")
	if err != nil {
		t.Fatalf("GET namespaced workflow: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}
	var view WorkflowView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode namespaced workflow: %v", err)
	}
	if view.Summary.Label != fakeWorkflow || view.State != "active" || len(view.Runs) != 16 || len(view.Notes) != 33 {
		t.Fatalf("namespaced workflow = %+v", view)
	}
}

func TestHandleWorkflowClampsCursorsAndLimits(t *testing.T) {
	ds := &recordingWorkflowDataSource{DataSource: NewFakeDataSource()}
	srv := httptest.NewServer(Serve(ds))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/workflow/panel?runCursor=run-c&noteCursor=note-c&maxRuns=999&maxNotes=-4")
	if err != nil {
		t.Fatalf("GET workflow: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ds.query.RunCursor != "run-c" || ds.query.NoteCursor != "note-c" {
		t.Fatalf("cursors = %+v", ds.query)
	}
	if ds.query.MaxRuns != workflowMaxRuns || ds.query.MaxNotes != workflowMaxNotes {
		t.Fatalf("limits = (%d,%d), want (%d,%d)", ds.query.MaxRuns, ds.query.MaxNotes, workflowMaxRuns, workflowMaxNotes)
	}
}

func TestFakeWorkflowContractAndPagination(t *testing.T) {
	t.Setenv("FAKEFEED_WORKFLOWS", "1")
	ds := NewFakeDataSource()

	state, err := ds.State(context.Background(), fakeRunID)
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if state.Workflow != fakeWorkflow {
		t.Fatalf("State.Workflow = %q, want %q", state.Workflow, fakeWorkflow)
	}
	graph, err := ds.Graph(context.Background(), "")
	if err != nil {
		t.Fatalf("Graph: %v", err)
	}
	var hub *GraphNode
	workflowLinks := 0
	for i := range graph.Nodes {
		if graph.Nodes[i].ID == "workflow::"+fakeWorkflow {
			hub = &graph.Nodes[i]
		}
	}
	for _, link := range graph.Links {
		if link.Kind == "workflow" {
			workflowLinks++
		}
	}
	if hub == nil || hub.Type != "workflow" || hub.JobCount != 7 || hub.NoteCount != 33 || workflowLinks != 7 {
		t.Fatalf("workflow galaxy fixture hub=%+v links=%d", hub, workflowLinks)
	}

	first, err := ds.Workflow(context.Background(), fakeWorkflow, WorkflowQuery{MaxRuns: 1, MaxNotes: 2})
	if err != nil {
		t.Fatalf("Workflow first page: %v", err)
	}
	if first.State != "active" || first.Summary.Summary == "" || first.Coordinator.Pane != "fable" || first.Coordinator.SessionID == "" || first.WorkDir != "/root/gitmoot" {
		t.Fatalf("detail lifecycle/coordinator = %+v", first)
	}
	if len(first.Runs) != 1 || len(first.Runs[0].Nodes) != 3 || len(first.Runs[0].Children) != 2 {
		t.Fatalf("first run page = %+v", first.Runs)
	}
	if first.Runs[0].Agent == "" || first.Runs[0].Repo == "" || first.Runs[0].ElapsedS == 0 || first.Runs[0].Children[1].State != "queued" {
		t.Fatalf("run mission-log shape = %+v", first.Runs[0])
	}
	if len(first.Notes) != 2 || first.NextRunCursor == "" || first.NextNoteCursor == "" || !first.Truncated {
		t.Fatalf("first cursors/notes = %+v", first)
	}
	if !strings.Contains(first.Notes[1].Body, "<script>") {
		t.Fatalf("hostile escaping fixture missing: %+v", first.Notes)
	}
	second, err := ds.Workflow(context.Background(), fakeWorkflow, WorkflowQuery{RunCursor: first.NextRunCursor, NoteCursor: first.NextNoteCursor, MaxRuns: 1, MaxNotes: 2})
	if err != nil {
		t.Fatalf("Workflow second page: %v", err)
	}
	if len(second.Runs) != 1 || second.Runs[0].State != "failed" || len(second.Notes) != 2 || !second.Truncated {
		t.Fatalf("second page = %+v", second)
	}
	all, err := ds.Workflow(context.Background(), fakeWorkflow, WorkflowQuery{MaxRuns: workflowMaxRuns, MaxNotes: workflowMaxNotes})
	if err != nil {
		t.Fatalf("Workflow all: %v", err)
	}
	if len(all.Runs) != 16 || len(all.Notes) != 33 || all.Truncated {
		t.Fatalf("full mission log = %+v", all)
	}
	batch, err := ds.Workflow(context.Background(), fakeWorkflow, WorkflowQuery{MaxRuns: 4, MaxNotes: 8})
	if err != nil {
		t.Fatalf("Workflow scroll batch: %v", err)
	}
	if len(batch.Runs) != 4 || len(batch.Notes) != 8 || batch.NextRunCursor == "" || batch.NextNoteCursor == "" || !batch.Truncated {
		t.Fatalf("scroll batch = %+v", batch)
	}
	stalled, err := ds.Workflow(context.Background(), fakeStalledWorkflow, WorkflowQuery{MaxRuns: workflowMaxRuns, MaxNotes: workflowMaxNotes})
	if err != nil {
		t.Fatalf("Workflow stalled: %v", err)
	}
	if stalled.State != "stalled" || stalled.StalledForS != 2400 || stalled.Coordinator.SessionID == "" || stalled.WorkDir == "" || len(stalled.Runs) == 0 || len(stalled.Notes) == 0 {
		t.Fatalf("stalled mission log = %+v", stalled)
	}
}

func TestFakeWorkflowIndexContractOrderingAndDeterminism(t *testing.T) {
	t.Setenv("FAKEFEED_WORKFLOWS", "1")
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	get := func() ([]byte, []WorkflowIndexEntry) {
		t.Helper()
		resp, err := http.Get(srv.URL + "/api/workflows")
		if err != nil {
			t.Fatalf("GET workflows: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read workflows: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d body=%s", resp.StatusCode, body)
		}
		var entries []WorkflowIndexEntry
		if err := json.Unmarshal(body, &entries); err != nil {
			t.Fatalf("decode workflows: %v", err)
		}
		return body, entries
	}

	body1, entries := get()
	body2, _ := get()
	if !bytes.Equal(body1, body2) {
		t.Fatalf("workflow index changed across identical reads\nfirst=%s\nsecond=%s", body1, body2)
	}
	if len(entries) < 7 || entries[0].State != "stalled" || entries[1].State != "active" || entries[2].State != "active" {
		t.Fatalf("state ordering = %+v", entries)
	}
	if entries[1].LastAt < entries[2].LastAt {
		t.Fatalf("active ordering is not newest-first: %+v", entries[:3])
	}
	var plain *WorkflowIndexEntry
	summaries := 0
	for i := range entries {
		entry := &entries[i]
		if entry.Summary != "" {
			summaries++
		}
		if entry.Label == "officeqa-point-formula" {
			plain = entry
		}
		if entry.Auto {
			t.Fatalf("workflow index emitted deprecated auto row = %+v", entry)
		}
		if entry.Counts.Jobs == 0 || entry.FirstAt == 0 || entry.LastAt == 0 || len(entry.Repos) == 0 {
			t.Fatalf("incomplete workflow row = %+v", entry)
		}
	}
	if plain == nil || plain.Namespace != "" || plain.Campaign != plain.Label || summaries < 3 {
		t.Fatalf("split/summary contract plain=%+v summaries=%d", plain, summaries)
	}
	for _, field := range []string{"\"auto\"", "\"session_id\"", "\"stalled_for_s\"", "\"tokens_in\"", "\"last_note\""} {
		if !bytes.Contains(body1, []byte(field)) {
			t.Fatalf("wire payload missing %s: %s", field, body1)
		}
	}
}

func TestWorkflowSummaryJSONContract(t *testing.T) {
	indexBody, err := json.Marshal(WorkflowIndexEntry{Label: "fable/example", Summary: "A readable workflow summary."})
	if err != nil {
		t.Fatalf("marshal workflow index entry: %v", err)
	}
	if !bytes.Contains(indexBody, []byte(`"summary":"A readable workflow summary."`)) {
		t.Fatalf("workflow index summary missing from JSON: %s", indexBody)
	}

	detailBody, err := json.Marshal(WorkflowView{Summary: WorkflowSummary{Label: "fable/example", Summary: "A readable workflow summary."}})
	if err != nil {
		t.Fatalf("marshal workflow detail: %v", err)
	}
	if !bytes.Contains(detailBody, []byte(`"summary":{"label":"fable/example","summary":"A readable workflow summary."`)) {
		t.Fatalf("workflow detail summary missing from JSON: %s", detailBody)
	}

	var absent WorkflowIndexEntry
	if err := json.Unmarshal([]byte(`{"label":"fable/example"}`), &absent); err != nil {
		t.Fatalf("unmarshal workflow index entry without summary: %v", err)
	}
	if absent.Summary != "" {
		t.Fatalf("absent workflow summary = %q, want empty", absent.Summary)
	}
}

func TestFakeWorkflowDisabled(t *testing.T) {
	t.Setenv("FAKEFEED_WORKFLOWS", "0")
	ds := NewFakeDataSource()
	state, err := ds.State(context.Background(), fakeRunID)
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if state.Workflow != "" {
		t.Fatalf("disabled State.Workflow = %q", state.Workflow)
	}
	graph, err := ds.Graph(context.Background(), "")
	if err != nil {
		t.Fatalf("Graph: %v", err)
	}
	for _, node := range graph.Nodes {
		if node.Type == "workflow" {
			t.Fatalf("disabled graph contains workflow node: %+v", node)
		}
	}
	for _, link := range graph.Links {
		if link.Kind == "workflow" {
			t.Fatalf("disabled graph contains workflow link: %+v", link)
		}
	}
	if _, err := ds.Workflow(context.Background(), fakeWorkflow, WorkflowQuery{}); err != ErrWorkflowNotFound {
		t.Fatalf("disabled Workflow error = %v, want %v", err, ErrWorkflowNotFound)
	}
	entries, err := ds.Workflows(context.Background())
	if err != nil || len(entries) != 0 {
		t.Fatalf("disabled Workflows = (%+v, %v), want empty", entries, err)
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
	// Newest first, exactly one Current marker, states cover current/canary/
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
	for _, want := range []string{"current", "canary", "pending"} {
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

func TestHandleAttention(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var att Attention
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/attention"), &att); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if att.Gates == nil || att.SynthItems == nil || att.Candidates == nil {
		t.Fatalf("attention lists must be non-nil: %+v", att)
	}
	if len(att.Gates) == 0 || len(att.SynthItems) == 0 || len(att.Candidates) == 0 {
		t.Fatalf("fake attention should have items in every bucket: %+v", att)
	}
	if want := len(att.Gates) + len(att.SynthItems) + len(att.Candidates); att.Total != want {
		t.Fatalf("Total = %d, want %d", att.Total, want)
	}
	for _, g := range att.Gates {
		if g.JobID == "" || g.Need == "" {
			t.Fatalf("gate missing jobId/need: %+v", g)
		}
	}
}

func TestHandleAttentionDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()
	a := getRaw(t, srv.URL+"/api/attention")
	b := getRaw(t, srv.URL+"/api/attention")
	if !bytes.Equal(a, b) {
		t.Fatalf("attention not byte-stable across calls")
	}
}

func TestHandleJobChecks(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var jc JobChecks
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/job/job-5/checks"), &jc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if jc.JobID != "job-5" {
		t.Fatalf("JobID = %q, want job-5", jc.JobID)
	}
	if jc.Mode != "block" {
		t.Fatalf("Mode = %q, want block", jc.Mode)
	}
	if len(jc.Failed) == 0 {
		t.Fatalf("job-5 should have failed checks")
	}
	for _, c := range jc.Failed {
		if c.CheckID == "" || c.Question == "" {
			t.Fatalf("failed check missing id/question: %+v", c)
		}
	}
}

func TestHandleJobChecksUnknownJobNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// An unknown job is NOT a 404 — it returns the resolved policy Mode with an
	// empty (non-nil) Failed list.
	var jc JobChecks
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/job/does-not-exist/checks"), &jc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if jc.Mode == "" {
		t.Fatalf("Mode must be resolved even for an unknown job: %+v", jc)
	}
	if jc.Failed == nil {
		t.Fatalf("Failed must be non-nil")
	}
	if len(jc.Failed) != 0 {
		t.Fatalf("unknown job should have no failed checks: %+v", jc.Failed)
	}
}

func TestHandleBinaryVerdicts(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var v BinaryVerdicts
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/run/"+fakeVerdictRun+"/verdicts"), &v); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v.RunID != fakeVerdictRun {
		t.Fatalf("RunID = %q, want %q", v.RunID, fakeVerdictRun)
	}
	if len(v.Verdicts) == 0 {
		t.Fatalf("expected verdicts for %q", fakeVerdictRun)
	}
	if v.Passed+v.Failed != len(v.Verdicts) {
		t.Fatalf("passed(%d)+failed(%d) != len(%d)", v.Passed, v.Failed, len(v.Verdicts))
	}
	pass, fail := 0, 0
	for _, q := range v.Verdicts {
		if q.Pass != (q.Verdict == "yes") {
			t.Fatalf("Pass/Verdict mismatch: %+v", q)
		}
		if q.Pass {
			pass++
		} else {
			fail++
		}
	}
	if pass != v.Passed || fail != v.Failed {
		t.Fatalf("counts mismatch: got passed=%d failed=%d, recomputed passed=%d failed=%d", v.Passed, v.Failed, pass, fail)
	}
	// Ordering must be (dimension, questionId) ascending.
	for i := 1; i < len(v.Verdicts); i++ {
		prev, cur := v.Verdicts[i-1], v.Verdicts[i]
		if prev.Dimension > cur.Dimension || (prev.Dimension == cur.Dimension && prev.QuestionID > cur.QuestionID) {
			t.Fatalf("verdicts not ordered by (dimension, questionId) at %d: %+v then %+v", i, prev, cur)
		}
	}
}

func TestHandleBinaryVerdictsUnknownRun(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var v BinaryVerdicts
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/run/nope/verdicts"), &v); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v.Verdicts == nil {
		t.Fatalf("Verdicts must be non-nil")
	}
	if len(v.Verdicts) != 0 || v.Passed != 0 || v.Failed != 0 {
		t.Fatalf("unknown run should be empty: %+v", v)
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

func TestHandleConfig(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	raw := getRaw(t, srv.URL+"/api/config")
	var cfg ConfigSnapshot
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cfg.ContractVersion != 1 {
		t.Fatalf("contract_version = %d, want 1", cfg.ContractVersion)
	}
	if !cfg.Exists || cfg.Path == "" || cfg.ModifiedAt == 0 {
		t.Fatalf("config file metadata incomplete: %+v", cfg)
	}
	if cfg.Sections == nil || cfg.Agents == nil || cfg.UnknownKeys == nil {
		t.Fatalf("config slices must be non-nil: %+v", cfg)
	}

	// Every fixture knob must survive the API with its typed value, default,
	// classification and override state intact.
	wantKnobs := map[string]struct {
		value, def string
		kind       string
		isDefault  bool
	}{
		"chat.auto_respond":              {"false", "false", "flag", true},
		"github.rate_limit_per_hour":     {"4200", "4500", "int", false},
		"memory.cluster_depth_cap":       {"3", "3", "int", true},
		"memory.cluster_fanout":          {"6", "6", "int", true},
		"memory.distill_enabled":         {"false", "false", "flag", true},
		"memory.groom_split_llm":         {"true", "false", "flag", false},
		"memory.groom_split_max_per_run": {"8", "4", "int", false},
		"memory.groom_split_model":       {`"gpt-5.6-sol"`, `""`, "string", false},
		"memory.groom_split_runtime":     {`"codex"`, `"codex"`, "string", true},
		"memory.max_entries":             {"1200", "1000", "int", false},
		"memory.token_budget":            {"12000", "12000", "int", true},
		"orchestrate.blocked_ttl":        {`"30m"`, `"30m"`, "duration", true},
		"skillopt.auto_promote":          {"false", "false", "flag", true},
		"skillopt.pace_gate":             {"true", "true", "flag", true},
	}
	gotKnobs := map[string]bool{}
	for si, section := range cfg.Sections {
		if si > 0 && cfg.Sections[si-1].Name >= section.Name {
			t.Fatalf("sections not strictly name-sorted: %+v", cfg.Sections)
		}
		for ki, knob := range section.Knobs {
			if ki > 0 && section.Knobs[ki-1].Key >= knob.Key {
				t.Fatalf("section %s knobs not strictly key-sorted: %+v", section.Name, section.Knobs)
			}
			id := section.Name + "." + knob.Key
			want, ok := wantKnobs[id]
			if !ok {
				t.Fatalf("unexpected fixture knob %q", id)
			}
			value, err := json.Marshal(knob.Value)
			if err != nil {
				t.Fatalf("marshal %s value: %v", id, err)
			}
			def, err := json.Marshal(knob.Default)
			if err != nil {
				t.Fatalf("marshal %s default: %v", id, err)
			}
			if string(value) != want.value || string(def) != want.def || knob.Kind != want.kind || knob.IsDefault != want.isDefault || knob.Doc == "" {
				t.Fatalf("knob %s did not round-trip: got=%+v value=%s default=%s want=%+v", id, knob, value, def, want)
			}
			gotKnobs[id] = true
		}
	}
	if len(gotKnobs) != len(wantKnobs) {
		t.Fatalf("round-tripped knobs = %d, want %d: %+v", len(gotKnobs), len(wantKnobs), gotKnobs)
	}

	if len(cfg.Agents) != 4 {
		t.Fatalf("agents = %d, want 4", len(cfg.Agents))
	}
	for i, agent := range cfg.Agents {
		if i > 0 && cfg.Agents[i-1].Name >= agent.Name {
			t.Fatalf("agents not strictly name-sorted: %+v", cfg.Agents)
		}
		if agent.Name == "" || agent.Runtime == "" || agent.AutonomyPolicy == "" || agent.Capabilities == nil {
			t.Fatalf("agent row incomplete: %+v", agent)
		}
		for j := 1; j < len(agent.Capabilities); j++ {
			if agent.Capabilities[j-1] >= agent.Capabilities[j] {
				t.Fatalf("agent %s capabilities not sorted: %+v", agent.Name, agent.Capabilities)
			}
		}
	}
	for i := 1; i < len(cfg.UnknownKeys); i++ {
		if cfg.UnknownKeys[i-1] >= cfg.UnknownKeys[i] {
			t.Fatalf("unknown_keys not strictly sorted: %+v", cfg.UnknownKeys)
		}
	}
	if len(cfg.UnknownKeys) != 2 {
		t.Fatalf("unknown_keys = %+v, want two fixture names", cfg.UnknownKeys)
	}

	// Unknown entries are names-only by contract: each JSON element must be a
	// string, never an object that could grow a value field.
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("decode wire payload: %v", err)
	}
	unknown, ok := wire["unknown_keys"].([]any)
	if !ok {
		t.Fatalf("unknown_keys wire shape = %T, want array", wire["unknown_keys"])
	}
	for i, item := range unknown {
		if _, ok := item.(string); !ok {
			t.Fatalf("unknown_keys[%d] = %T, want name string only", i, item)
		}
	}
}

func TestHandleConfigDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/config"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("config not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
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

// realSkillStates is the set of version states the live SkillOpt store can emit;
// the fake feed must never present a state outside it. Promotion writes 'current'
// to the live version and 'superseded' to the one it replaces (store.go
// PromoteAgentTemplateVersion); there is NO 'promoted' version state. Candidates
// are 'pending' or an in-flight 'canary', and a declined candidate is 'rejected'.
var realSkillStates = map[string]bool{"current": true, "superseded": true, "canary": true, "pending": true, "rejected": true}

func TestHandleLearningSkills(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var skills Skills
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/learning/skills"), &skills); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if skills.Templates == nil {
		t.Fatalf("Templates must be non-nil: %+v", skills)
	}
	if n := len(skills.Templates); n < 2 || n > 3 {
		t.Fatalf("len(Templates) = %d, want 2..3", n)
	}

	var canaryTemplates, pendingSum, roseFive int
	for _, tpl := range skills.Templates {
		if tpl.TemplateID == "" {
			t.Fatalf("template missing id: %+v", tpl)
		}
		if tpl.Versions == nil || len(tpl.Versions) == 0 {
			t.Fatalf("template %s has no versions", tpl.TemplateID)
		}
		if tpl.Pending == nil {
			t.Fatalf("template %s Pending must be non-nil", tpl.TemplateID)
		}
		// Versions ascending by Number (sparkline order); states real-emittable;
		// scored versions carry HasScore.
		var scored []float64
		for i, v := range tpl.Versions {
			if !realSkillStates[v.State] {
				t.Fatalf("template %s version %d has non-real state %q", tpl.TemplateID, v.Number, v.State)
			}
			if i > 0 && tpl.Versions[i-1].Number >= v.Number {
				t.Fatalf("template %s versions not ascending by Number: %+v", tpl.TemplateID, tpl.Versions)
			}
			if v.HasScore {
				scored = append(scored, v.Score)
			}
		}
		if tpl.CanarySample > 0 {
			canaryTemplates++
		}
		pendingSum += len(tpl.Pending)
		// The healthy template is the 5-version one with a strictly rising score.
		if len(tpl.Versions) == 5 {
			rising := len(scored) == 5
			for i := 1; i < len(scored); i++ {
				if scored[i] <= scored[i-1] {
					rising = false
				}
			}
			if rising {
				roseFive++
			}
		}
		// Every pending candidate must map to a real version number in the template.
		for _, c := range tpl.Pending {
			if c.VersionID == "" {
				t.Fatalf("pending candidate missing versionId: %+v", c)
			}
			found := false
			for _, v := range tpl.Versions {
				if v.Number == c.Number {
					found = true
				}
			}
			if !found {
				t.Fatalf("pending candidate %d not among template %s versions", c.Number, tpl.TemplateID)
			}
		}
	}
	if roseFive != 1 {
		t.Fatalf("want exactly one healthy 5-version rising-score template, got %d", roseFive)
	}
	if canaryTemplates == 0 {
		t.Fatalf("expected at least one template with an active canary")
	}
	if skills.ActiveCanaries != canaryTemplates {
		t.Fatalf("ActiveCanaries = %d, want %d (templates with CanarySample>0)", skills.ActiveCanaries, canaryTemplates)
	}
	if skills.PendingTotal != pendingSum {
		t.Fatalf("PendingTotal = %d, want %d (sum of per-template Pending)", skills.PendingTotal, pendingSum)
	}
	if skills.PendingTotal == 0 {
		t.Fatalf("expected at least one pending candidate")
	}

	// Sort order: pending-first, then most-recently-promoted (LastPromotedAt desc).
	for i := 1; i < len(skills.Templates); i++ {
		a, b := skills.Templates[i-1], skills.Templates[i]
		ap, bp := len(a.Pending) > 0, len(b.Pending) > 0
		if !ap && bp {
			t.Fatalf("templates not pending-first: %q(pending=%v) before %q(pending=%v)", a.TemplateID, ap, b.TemplateID, bp)
		}
		if ap == bp && a.LastPromotedAt < b.LastPromotedAt {
			t.Fatalf("templates not most-recently-promoted within group: %q(%d) before %q(%d)", a.TemplateID, a.LastPromotedAt, b.TemplateID, b.LastPromotedAt)
		}
	}
}

func TestHandleLearningSkillsDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/learning/skills"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("skills not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

func TestHandleLearningKnowledge(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var k Knowledge
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/learning/knowledge"), &k); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if k.Agents == nil || k.Facts == nil || k.Clusters == nil || k.Edges == nil {
		t.Fatalf("knowledge slices must be non-nil: %+v", k)
	}

	// Exactly three enrolled agents; every agent is enrolled and named.
	agentNames := map[string]bool{}
	enrolled := 0
	for _, a := range k.Agents {
		if a.Name == "" {
			t.Fatalf("knowledge agent missing name: %+v", a)
		}
		agentNames[a.Name] = true
		if a.Enrolled {
			enrolled++
		}
	}
	if enrolled != 3 {
		t.Fatalf("enrolled agents = %d, want 3", enrolled)
	}
	// Agents sorted by name asc.
	for i := 1; i < len(k.Agents); i++ {
		if k.Agents[i-1].Name > k.Agents[i].Name {
			t.Fatalf("agents not name-sorted: %+v", k.Agents)
		}
	}

	// Facts: enough to fill the graph, witnesses within 1..7, two superseded
	// facts, facts span more than one repo scope plus a general scope.
	if len(k.Facts) < 8 {
		t.Fatalf("len(Facts) = %d, want >= 8", len(k.Facts))
	}
	factIDs := map[string]bool{}
	superseded, generalScope := 0, 0
	repoUnclusteredGhost := ""
	repos := map[string]bool{}
	for _, fct := range k.Facts {
		if fct.ID == "" || fct.Content == "" || fct.Owner == "" {
			t.Fatalf("fact missing id/content/owner: %+v", fct)
		}
		if !agentNames[fct.Owner] {
			t.Fatalf("fact %s owner %q is not an enrolled agent", fct.ID, fct.Owner)
		}
		if fct.Witnesses < 1 || fct.Witnesses > 7 {
			t.Fatalf("fact %s witnesses = %d, want 1..7", fct.ID, fct.Witnesses)
		}
		factIDs[fct.ID] = true
		if fct.Superseded {
			superseded++
			if fct.Repo != "" && fct.Cluster == "" {
				repoUnclusteredGhost = fct.ID
			}
		}
		if fct.Repo == "" {
			generalScope++
		} else {
			repos[fct.Repo] = true
		}
	}
	if superseded != 2 {
		t.Fatalf("superseded facts = %d, want exactly 2", superseded)
	}
	if repoUnclusteredGhost == "" {
		t.Fatal("fixture missing repo-scoped superseded fact without a cluster")
	}
	if len(repos) < 2 {
		t.Fatalf("repo-scoped facts span %d repos, want >= 2", len(repos))
	}
	if generalScope < 2 {
		t.Fatalf("general-scope facts = %d, want >= 2", generalScope)
	}

	// Edges: only the three real kinds, at least one of each; owner/category
	// sources and supersede endpoints reference known facts; owner targets are
	// enrolled agents.
	// Clusters (gitmoot #763, #779): every cluster has an id/label and a positive
	// count. Leaf counts match direct fact assignments; parent counts aggregate
	// their children. Facts always attach to leaves and parent ids are valid.
	clusterIDs := map[string]bool{}
	clusterMembers := map[string]int{}
	clusterByID := map[string]KnowledgeCluster{}
	childrenByParent := map[string][]string{}
	if len(k.Clusters) == 0 {
		t.Fatalf("expected >=1 cluster, got 0")
	}
	for _, c := range k.Clusters {
		if c.ID == "" || c.Label == "" {
			t.Fatalf("cluster missing id/label: %+v", c)
		}
		if clusterIDs[c.ID] {
			t.Fatalf("duplicate cluster id %q", c.ID)
		}
		clusterIDs[c.ID] = true
		clusterByID[c.ID] = c
		if c.ParentID != "" {
			childrenByParent[c.ParentID] = append(childrenByParent[c.ParentID], c.ID)
		}
	}
	for _, c := range k.Clusters {
		if c.ParentID != "" && !clusterIDs[c.ParentID] {
			t.Fatalf("cluster %s parent_id %q is not a known cluster", c.ID, c.ParentID)
		}
	}
	clusterOf := map[string]string{}
	for _, fct := range k.Facts {
		if fct.Cluster == "" {
			continue
		}
		if !clusterIDs[fct.Cluster] {
			t.Fatalf("fact %s cluster %q is not a known cluster", fct.ID, fct.Cluster)
		}
		clusterMembers[fct.Cluster]++
		clusterOf[fct.ID] = fct.Cluster
		if len(childrenByParent[fct.Cluster]) != 0 {
			t.Fatalf("fact %s attaches to parent cluster %q, want a leaf", fct.ID, fct.Cluster)
		}
		// Provenance is one of source job / source file, never both.
		if fct.SourceJob != "" && fct.SourceFile != "" {
			t.Fatalf("fact %s carries both sourceJob and sourceFile", fct.ID)
		}
		// Linked fact ids must reference known facts (no dangling wikilinks).
		for _, id := range fct.Links {
			if !factIDs[id] {
				t.Fatalf("fact %s links to unknown fact %q", fct.ID, id)
			}
		}
	}
	var subtreeMembers func(string, map[string]bool) int
	subtreeMembers = func(id string, seen map[string]bool) int {
		if seen[id] {
			return 0
		}
		seen[id] = true
		total := clusterMembers[id]
		for _, childID := range childrenByParent[id] {
			total += subtreeMembers(childID, seen)
		}
		return total
	}
	for _, c := range k.Clusters {
		want := subtreeMembers(c.ID, map[string]bool{})
		if c.Count != want {
			t.Fatalf("cluster %s count = %d, want recursive hierarchy member total %d", c.ID, c.Count, want)
		}
		if c.Count <= 0 {
			t.Fatalf("cluster %s has non-positive count %d", c.ID, c.Count)
		}
		if c.Medoid != "" {
			medoidCluster := clusterOf[c.Medoid]
			cur, found := medoidCluster, medoidCluster == c.ID
			for steps := 0; !found && cur != "" && steps <= len(k.Clusters); steps++ {
				cur = clusterByID[cur].ParentID
				found = cur == c.ID
			}
			if !found {
				t.Fatalf("cluster %s medoid %q is not one of its descendant facts", c.ID, c.Medoid)
			}
		}
	}
	if got := len(childrenByParent["cluster:4"]); got != 2 {
		t.Fatalf("cluster:4 children = %d, want 2", got)
	}
	if clusterByID["cluster:3:delivery:build"].ParentID != "cluster:3:delivery" || clusterByID["cluster:3:delivery"].ParentID != "cluster:3" {
		t.Fatalf("default fixture missing depth-three chain: %+v", clusterByID)
	}
	// Clusters sorted by id asc (deterministic ordering for the sig-skip).
	for i := 1; i < len(k.Clusters); i++ {
		if k.Clusters[i-1].ID > k.Clusters[i].ID {
			t.Fatalf("clusters not id-sorted: %+v", k.Clusters)
		}
	}

	// Edges: the five real kinds, at least one of each; owner/category/cluster
	// sources and supersede endpoints reference known facts; owner targets are
	// enrolled agents; cluster targets are known clusters. Link edges are scored
	// undirected fact pairs emitted exactly once.
	kinds := map[string]int{}
	linkPairs := map[string]bool{}
	crossRepoLinks := 0
	ghostSupersede := false
	for _, e := range k.Edges {
		switch e.Kind {
		case "owner", "category", "cluster", "supersede", "link":
		default:
			t.Fatalf("edge has unexpected kind %q: %+v", e.Kind, e)
		}
		kinds[e.Kind]++
		if !factIDs[e.Source] {
			t.Fatalf("edge source %q is not a known fact", e.Source)
		}
		switch e.Kind {
		case "owner":
			if !agentNames[e.Target] {
				t.Fatalf("owner edge target %q is not an enrolled agent", e.Target)
			}
		case "cluster":
			if !clusterIDs[e.Target] {
				t.Fatalf("cluster edge target %q is not a known cluster", e.Target)
			}
		case "supersede":
			if !factIDs[e.Target] {
				t.Fatalf("supersede edge target %q is not a known fact", e.Target)
			}
			if e.Target == repoUnclusteredGhost {
				ghostSupersede = true
			}
		case "link":
			if !factIDs[e.Target] {
				t.Fatalf("link edge target %q is not a known fact", e.Target)
			}
			if e.Score <= 0 || e.Score > 1 {
				t.Fatalf("link edge score = %v, want (0,1]: %+v", e.Score, e)
			}
			a, b := e.Source, e.Target
			if a > b {
				a, b = b, a
			}
			pair := a + "|" + b
			if linkPairs[pair] {
				t.Fatalf("duplicate undirected link pair %q", pair)
			}
			linkPairs[pair] = true
			var sourceRepo, targetRepo string
			for _, fact := range k.Facts {
				if fact.ID == e.Source {
					sourceRepo = fact.Repo
				}
				if fact.ID == e.Target {
					targetRepo = fact.Repo
				}
			}
			if sourceRepo != targetRepo {
				crossRepoLinks++
			}
		}
	}
	for _, kind := range []string{"owner", "category", "cluster", "supersede", "link"} {
		if kinds[kind] == 0 {
			t.Fatalf("expected at least one %q edge: %+v", kind, kinds)
		}
	}
	if crossRepoLinks == 0 {
		t.Fatal("expected at least one cross-repo link edge")
	}
	if !ghostSupersede {
		t.Fatalf("expected supersede edge targeting repo-scoped unclustered ghost %q", repoUnclusteredGhost)
	}
	// One owner + one category edge per fact; one cluster edge per clustered fact.
	if kinds["owner"] != len(k.Facts) {
		t.Fatalf("owner edges = %d, want one per fact (%d)", kinds["owner"], len(k.Facts))
	}
	if kinds["category"] != len(k.Facts) {
		t.Fatalf("category edges = %d, want one per fact (%d)", kinds["category"], len(k.Facts))
	}
	clustered := 0
	for _, fct := range k.Facts {
		if fct.Cluster != "" {
			clustered++
		}
	}
	if kinds["cluster"] != clustered {
		t.Fatalf("cluster edges = %d, want one per clustered fact (%d)", kinds["cluster"], clustered)
	}
}

func TestHandleLearningKnowledgeFlatFixture(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSourceFlatKnowledge()))
	defer srv.Close()

	var k Knowledge
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/learning/knowledge"), &k); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(k.Clusters) != 7 {
		t.Fatalf("base fixture clusters = %d, want 7", len(k.Clusters))
	}
	byID := map[string]KnowledgeCluster{}
	for _, c := range k.Clusters {
		byID[c.ID] = c
	}
	if byID["cluster:3:delivery:build"].ParentID != "cluster:3:delivery" || byID["cluster:3:delivery"].ParentID != "cluster:3" {
		t.Fatalf("base fixture missing depth-three chain: %+v", byID)
	}
	if byID["cluster:4"].ParentID != "" {
		t.Fatalf("base fixture unexpectedly carries the default cluster:4 split: %+v", byID["cluster:4"])
	}
	for _, fct := range k.Facts {
		if fct.ID == "fact:4" && fct.Cluster != "cluster:4" {
			t.Fatalf("flat fixture fact:4 cluster = %q, want cluster:4", fct.Cluster)
		}
	}
}

func TestHandleLearningKnowledgeDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/learning/knowledge"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("knowledge not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

// realPipelineStates is the set of run states the live pipeline store can emit;
// the fake feed must never present a run state outside it.
var realPipelineStates = map[string]bool{
	"running":   true,
	"succeeded": true,
	"blocked":   true,
	"failed":    true,
	"cancelled": true,
}

// realPipelineTriggers is the set of run triggers the live pipeline store can
// emit; bridge means a run fired through the gitmoot bridge, e.g. email.
var realPipelineTriggers = map[string]bool{
	"manual":   true,
	"schedule": true,
	"bridge":   true,
}

// realPipelineStageStates is the set of per-stage states the live pipeline store
// can emit; the fake feed must never present a stage state outside it.
var realPipelineStageStates = map[string]bool{
	"pending":   true,
	"queued":    true,
	"running":   true,
	"succeeded": true,
	"blocked":   true,
	"failed":    true,
	"skipped":   true,
	"cancelled": true,
}

func TestHandlePipelines(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var pipelines []PipelineSummary
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/pipelines"), &pipelines); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(pipelines) < 3 {
		t.Fatalf("len(pipelines) = %d, want >= 3", len(pipelines))
	}

	// Sorted by name asc.
	for i := 1; i < len(pipelines); i++ {
		if pipelines[i-1].Name > pipelines[i].Name {
			t.Fatalf("pipelines not name-sorted: %+v", pipelines)
		}
	}

	enabled, disabled := 0, 0
	byName := map[string]PipelineSummary{}
	for _, p := range pipelines {
		if p.Name == "" {
			t.Fatalf("pipeline missing name: %+v", p)
		}
		byName[p.Name] = p
		if p.StageCount <= 0 {
			t.Fatalf("pipeline %s StageCount = %d, want > 0", p.Name, p.StageCount)
		}
		// Recent must always be a JSON array, never nil.
		if p.Recent == nil {
			t.Fatalf("pipeline %s Recent must be non-nil", p.Name)
		}
		if p.Enabled {
			enabled++
		} else {
			disabled++
		}
		// LastStatus, when set, is a real run state.
		if p.LastStatus != "" && !realPipelineStates[p.LastStatus] {
			t.Fatalf("pipeline %s LastStatus = %q not a real run state", p.Name, p.LastStatus)
		}
		// Recent: newest-first by StartedAt desc, every state within the allow-list.
		for i, r := range p.Recent {
			if r.ID == "" || r.State == "" {
				t.Fatalf("pipeline %s recent[%d] missing id/state: %+v", p.Name, i, r)
			}
			if !realPipelineStates[r.State] {
				t.Fatalf("pipeline %s recent[%d] state = %q not a real run state", p.Name, i, r.State)
			}
			if r.Trigger != "" && !realPipelineTriggers[r.Trigger] {
				t.Fatalf("pipeline %s recent[%d] trigger = %q not a real trigger", p.Name, i, r.Trigger)
			}
			if i > 0 && p.Recent[i-1].StartedAt < r.StartedAt {
				t.Fatalf("pipeline %s Recent not newest-first (StartedAt desc): %+v", p.Name, p.Recent)
			}
		}
	}

	// The fixture spans both schedule states so the enabled chip is exercised in
	// both directions.
	if enabled == 0 || disabled == 0 {
		t.Fatalf("want at least one enabled and one disabled pipeline; got enabled=%d disabled=%d", enabled, disabled)
	}

	// nightly-deploy: enabled scheduled pipeline with a next-due time whose recent
	// strip carries an in-flight running run alongside a succeeded and a failed run.
	nd, ok := byName["nightly-deploy"]
	if !ok {
		t.Fatalf("expected a nightly-deploy pipeline: %+v", pipelines)
	}
	if !nd.Enabled {
		t.Fatalf("expected nightly-deploy enabled")
	}
	if nd.NextDueAt == 0 {
		t.Fatalf("expected nightly-deploy to carry a NextDueAt")
	}
	if nd.Interval == "" {
		t.Fatalf("expected nightly-deploy to carry a schedule Interval")
	}
	if nd.Mode != "email-triggered (bound), scheduled 24h" {
		t.Fatalf("nightly-deploy Mode = %q", nd.Mode)
	}
	recentStates := map[string]bool{}
	recentTriggers := map[string]bool{}
	for _, r := range nd.Recent {
		recentStates[r.State] = true
		recentTriggers[r.Trigger] = true
	}
	for _, want := range []string{"running", "succeeded", "failed"} {
		if !recentStates[want] {
			t.Fatalf("nightly-deploy recent missing a %q run: %+v", want, nd.Recent)
		}
	}
	if !recentTriggers["bridge"] {
		t.Fatalf("nightly-deploy recent missing a bridge-triggered run: %+v", nd.Recent)
	}

	// listing-refresh: manual pipeline whose last run is parked-blocked.
	if lr := byName["listing-refresh"]; lr.LastStatus != "blocked" || lr.Mode != "manual" {
		t.Fatalf("expected listing-refresh LastStatus=blocked + Mode=manual, got status=%q mode=%q", lr.LastStatus, lr.Mode)
	}
	// bench-suite: disabled pipeline whose last run failed.
	if bs := byName["bench-suite"]; bs.Enabled || bs.LastStatus != "failed" || bs.Mode != "scheduled 168h" {
		t.Fatalf("expected bench-suite disabled + LastStatus=failed + Mode=scheduled 168h, got enabled=%v status=%q mode=%q", bs.Enabled, bs.LastStatus, bs.Mode)
	}
}

func TestHandlePipelinesDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/pipelines"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("pipelines not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

func TestHandlePipelineRun(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// The parked-blocked diamond fixture exercises needs + a skipped branch.
	var run PipelineRun
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/pipeline/run/prun-listing-refresh-0001"), &run); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if run.ID != "prun-listing-refresh-0001" {
		t.Fatalf("run.ID = %q, want prun-listing-refresh-0001", run.ID)
	}
	if run.State != "blocked" {
		t.Fatalf("run.State = %q, want blocked", run.State)
	}
	if run.HaltStage != "score" {
		t.Fatalf("run.HaltStage = %q, want score", run.HaltStage)
	}
	// Persisted blocked-needs aggregated at the run level.
	if len(run.Needs) == 0 {
		t.Fatalf("expected run-level Needs on a blocked run: %+v", run)
	}

	// Stages: non-nil, in spec (topological) order — deliberately NOT alphabetical
	// (which would be dedupe, fetch, publish, score).
	if run.Stages == nil {
		t.Fatalf("Stages must be non-nil")
	}
	wantOrder := []string{"fetch", "score", "dedupe", "publish"}
	if len(run.Stages) != len(wantOrder) {
		t.Fatalf("len(Stages) = %d, want %d", len(run.Stages), len(wantOrder))
	}
	byID := map[string]PipelineStage{}
	for i, st := range run.Stages {
		if st.ID != wantOrder[i] {
			t.Fatalf("Stages not in spec order: got %q at %d, want %q\n%+v", st.ID, i, wantOrder[i], run.Stages)
		}
		if !realPipelineStageStates[st.State] {
			t.Fatalf("stage %s state = %q not a real stage state", st.ID, st.State)
		}
		byID[st.ID] = st
	}

	// The blocked stage carries its own persisted needs.
	if score := byID["score"]; score.State != "blocked" || len(score.Needs) == 0 {
		t.Fatalf("expected score stage blocked with needs: %+v", score)
	}
	if score := byID["score"]; score.Kind != "agent_ask" || score.AgentRuntime != "codex" {
		t.Fatalf("expected score stage to carry agent_ask/codex metadata: %+v", score)
	}
	if fetch := byID["fetch"]; fetch.Kind != "shell" {
		t.Fatalf("expected fetch stage Kind=shell: %+v", fetch)
	}
	// The downstream branch of the blocked stage is skipped.
	if publish := byID["publish"]; publish.State != "skipped" || publish.Kind != "gate" {
		t.Fatalf("expected publish stage skipped: %+v", publish)
	}
	// Deps present on downstream stages (the client derives the DAG edges from them).
	if score := byID["score"]; len(score.Deps) != 1 || score.Deps[0] != "fetch" {
		t.Fatalf("score.Deps = %v, want [fetch]", score.Deps)
	}
	if dedupe := byID["dedupe"]; len(dedupe.Deps) != 1 || dedupe.Deps[0] != "fetch" {
		t.Fatalf("dedupe.Deps = %v, want [fetch]", dedupe.Deps)
	}
	if publish := byID["publish"]; len(publish.Deps) != 2 {
		t.Fatalf("publish.Deps = %v, want 2 (the diamond join)", publish.Deps)
	}
}

func TestHandlePipelineRunDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/pipeline/run/prun-listing-refresh-0001"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("pipeline run detail not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

func TestHandlePipelineRunNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/pipeline/run/does-not-exist")
	if err != nil {
		t.Fatalf("GET /api/pipeline/run/does-not-exist: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandlePipelineDetail(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// The flaky-diamond fixture exercises the declared DAG + a multi-run history
	// whose score stage keeps changing state.
	var detail PipelineDetail
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/pipelines/listing-refresh"), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if detail.Name != "listing-refresh" {
		t.Fatalf("detail.Name = %q, want listing-refresh", detail.Name)
	}

	// Declared: current spec DAG, in spec (topological) order — deliberately NOT
	// alphabetical — every stage pending, carrying deps/cmd; the flaky score stage
	// carries a retry budget.
	if detail.Declared == nil {
		t.Fatalf("Declared must be non-nil")
	}
	wantDeclared := []string{"fetch", "score", "dedupe", "publish"}
	if len(detail.Declared) != len(wantDeclared) {
		t.Fatalf("len(Declared) = %d, want %d: %+v", len(detail.Declared), len(wantDeclared), detail.Declared)
	}
	declaredIDs := map[string]bool{}
	byDeclID := map[string]PipelineStage{}
	retrySeen := false
	for i, st := range detail.Declared {
		if st.ID != wantDeclared[i] {
			t.Fatalf("Declared not in spec order: got %q at %d, want %q\n%+v", st.ID, i, wantDeclared[i], detail.Declared)
		}
		if st.State != "pending" {
			t.Fatalf("declared stage %s state = %q, want pending", st.ID, st.State)
		}
		if st.Cmd == "" {
			t.Fatalf("declared stage %s missing cmd", st.ID)
		}
		if st.Retry > 0 {
			retrySeen = true
		}
		declaredIDs[st.ID] = true
		byDeclID[st.ID] = st
	}
	// The diamond's dependency edges are carried on the downstream stages.
	if score := byDeclID["score"]; len(score.Deps) != 1 || score.Deps[0] != "fetch" {
		t.Fatalf("declared score.Deps = %v, want [fetch]", score.Deps)
	}
	if score := byDeclID["score"]; score.Kind != "agent_ask" || score.AgentRuntime != "codex" {
		t.Fatalf("declared score stage missing agent metadata: %+v", score)
	}
	if publish := byDeclID["publish"]; len(publish.Deps) != 2 {
		t.Fatalf("declared publish.Deps = %v, want 2 (the diamond join)", publish.Deps)
	}
	if publish := byDeclID["publish"]; publish.Kind != "gate" {
		t.Fatalf("declared publish.Kind = %q, want gate", publish.Kind)
	}
	if !retrySeen {
		t.Fatalf("expected at least one declared stage to carry a retry budget: %+v", detail.Declared)
	}

	// Runs: newest-first, capped at 100, never nil; each run's Stages non-nil with
	// mark ids within the declared set and states within the allow-lists.
	if detail.Runs == nil {
		t.Fatalf("Runs must be non-nil")
	}
	if len(detail.Runs) > 100 {
		t.Fatalf("Runs not capped at 100: %d", len(detail.Runs))
	}
	if len(detail.Runs) < 2 {
		t.Fatalf("expected a multi-run history for listing-refresh, got %d", len(detail.Runs))
	}
	// Newest run is the parked-blocked …-0001.
	if detail.Runs[0].ID != "prun-listing-refresh-0001" {
		t.Fatalf("newest run = %q, want prun-listing-refresh-0001", detail.Runs[0].ID)
	}
	// Strictly newest-first throughout (StartedAt desc, ID desc tie-break). The
	// fixture's history literal is deliberately declared out of chronological
	// order, so this fails if finalize() ever stops sorting.
	for i := 1; i < len(detail.Runs); i++ {
		prev, cur := detail.Runs[i-1], detail.Runs[i]
		if cur.StartedAt > prev.StartedAt || (cur.StartedAt == prev.StartedAt && cur.ID > prev.ID) {
			t.Fatalf("Runs not newest-first at %d: %s(%d) before %s(%d)", i, prev.ID, prev.StartedAt, cur.ID, cur.StartedAt)
		}
	}
	scoreStates := map[string]bool{}
	for i, run := range detail.Runs {
		if run.ID == "" || run.State == "" {
			t.Fatalf("run[%d] missing id/state: %+v", i, run)
		}
		if !realPipelineStates[run.State] {
			t.Fatalf("run[%d] state = %q not a real run state", i, run.State)
		}
		if run.Trigger != "" && !realPipelineTriggers[run.Trigger] {
			t.Fatalf("run[%d] trigger = %q not a real trigger", i, run.Trigger)
		}
		if i > 0 && detail.Runs[i-1].StartedAt < run.StartedAt {
			t.Fatalf("Runs not newest-first (StartedAt desc): %+v", detail.Runs)
		}
		if run.Stages == nil {
			t.Fatalf("run[%d] (%s) Stages must be non-nil", i, run.ID)
		}
		for _, mk := range run.Stages {
			if !declaredIDs[mk.ID] {
				t.Fatalf("run[%d] mark id %q not within the declared set %v", i, mk.ID, wantDeclared)
			}
			if !realPipelineStageStates[mk.State] {
				t.Fatalf("run[%d] mark %s state = %q not a real stage state", i, mk.ID, mk.State)
			}
			if mk.ID == "score" {
				scoreStates[mk.State] = true
			}
		}
	}
	// Flaky-stage variety: the score stage reaches >=2 distinct states across the
	// history (the "which stage keeps failing" demo).
	if len(scoreStates) < 2 {
		t.Fatalf("expected the score stage to be flaky (>=2 distinct states), got %v", scoreStates)
	}
}

func TestHandlePipelineDetailDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/pipelines/listing-refresh"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("pipeline detail not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

func TestHandlePipelineDetailNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/pipelines/does-not-exist")
	if err != nil {
		t.Fatalf("GET /api/pipelines/does-not-exist: %v", err)
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

// realChatMessageKinds is the set of message kinds the live chat store can emit;
// the fake feed must not invent shapes the client will not otherwise see.
var realChatMessageKinds = map[string]bool{
	"chat":              true,
	"system":            true,
	"job_result":        true,
	"promotion_request": true,
}

var realChatAuthorKinds = map[string]bool{
	"human":  true,
	"agent":  true,
	"system": true,
}

func TestHandleChatThreads(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	var threads []ChatThreadSummary
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/chat/threads"), &threads); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(threads) < 3 {
		t.Fatalf("len(threads) = %d, want >= 3", len(threads))
	}

	// Sorted most-recently-active first (UpdatedAt desc).
	for i := 1; i < len(threads); i++ {
		if threads[i-1].UpdatedAt < threads[i].UpdatedAt {
			t.Fatalf("threads not UpdatedAt-desc sorted: %+v", threads)
		}
	}

	var haveOpen, haveArchived, haveUnread bool
	for _, th := range threads {
		if th.ID == "" || th.Name == "" {
			t.Fatalf("thread missing id/name: %+v", th)
		}
		if th.State != "open" && th.State != "archived" {
			t.Fatalf("thread %q has unexpected state %q", th.ID, th.State)
		}
		if th.State == "open" {
			haveOpen = true
		}
		if th.State == "archived" {
			haveArchived = true
		}
		if th.UnreadMentions > 0 {
			haveUnread = true
		}
		if th.MessageCount <= 0 {
			t.Fatalf("thread %q has non-positive messageCount %d", th.ID, th.MessageCount)
		}
		if th.LastKind != "" && !realChatMessageKinds[th.LastKind] {
			t.Fatalf("thread %q has unexpected lastKind %q", th.ID, th.LastKind)
		}
		if th.Participants == nil {
			t.Fatalf("thread %q participants is nil (want [])", th.ID)
		}
	}
	if !haveOpen || !haveArchived || !haveUnread {
		t.Fatalf("fixture must cover open+archived+unread; got open=%v archived=%v unread=%v", haveOpen, haveArchived, haveUnread)
	}
}

func TestHandleChatThreadsDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/chat/threads"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("chat threads not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

func TestHandleChatThread(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// The release-room fixture exercises a promotion_request + job_result + refs.
	var detail ChatThreadDetail
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/chat/thread?id=chat-release-room"), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if detail.ID != "chat-release-room" {
		t.Fatalf("detail.ID = %q, want chat-release-room", detail.ID)
	}
	if len(detail.Messages) == 0 {
		t.Fatalf("thread has no messages")
	}
	// Messages ascending by Seq; kinds/author-kinds within the real store's set.
	var sawPromotion, sawJobResult bool
	for i, msg := range detail.Messages {
		if i > 0 && detail.Messages[i-1].Seq > msg.Seq {
			t.Fatalf("messages not Seq-ascending: %+v", detail.Messages)
		}
		if !realChatMessageKinds[msg.Kind] {
			t.Fatalf("message %q has unexpected kind %q", msg.ID, msg.Kind)
		}
		if !realChatAuthorKinds[msg.AuthorKind] {
			t.Fatalf("message %q has unexpected authorKind %q", msg.ID, msg.AuthorKind)
		}
		if msg.Kind == "promotion_request" {
			sawPromotion = true
			if msg.PromotedJobID == "" {
				t.Fatalf("promotion_request %q missing promotedJobId", msg.ID)
			}
		}
		if msg.Kind == "job_result" {
			sawJobResult = true
		}
	}
	if !sawPromotion || !sawJobResult {
		t.Fatalf("release-room must carry a promotion_request + job_result; got promotion=%v jobResult=%v", sawPromotion, sawJobResult)
	}
}

func TestHandleChatThreadSystemAskGate(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	// The adapter-review fixture exercises an ask-gate `system` message + a
	// human reply_to answer.
	var detail ChatThreadDetail
	if err := json.Unmarshal(getRaw(t, srv.URL+"/api/chat/thread?id=chat-adapter-review"), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var sawSystem, sawReply bool
	for _, msg := range detail.Messages {
		if msg.Kind == "system" && msg.AuthorKind == "system" {
			sawSystem = true
		}
		if msg.ReplyTo != "" {
			sawReply = true
		}
	}
	if !sawSystem || !sawReply {
		t.Fatalf("adapter-review must carry a system ask-gate + a reply; got system=%v reply=%v", sawSystem, sawReply)
	}
}

func TestHandleChatThreadDeterministic(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	url := srv.URL + "/api/chat/thread?id=chat-release-room"
	if a, b := getRaw(t, url), getRaw(t, url); !bytes.Equal(a, b) {
		t.Fatalf("chat thread not byte-stable across calls\nfirst:  %s\nsecond: %s", a, b)
	}
}

func TestHandleChatThreadNotFound(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/chat/thread?id=does-not-exist")
	if err != nil {
		t.Fatalf("GET /api/chat/thread?id=does-not-exist: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleChatThreadMissingID(t *testing.T) {
	srv := httptest.NewServer(Serve(NewFakeDataSource()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/chat/thread")
	if err != nil {
		t.Fatalf("GET /api/chat/thread: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
