package dashboard

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sentinel errors returned by FakeDataSource (and mapped to HTTP status codes
// by the API/SSE handlers).
var (
	// ErrRunNotFound indicates the requested run does not exist.
	ErrRunNotFound = errors.New("run not found")
	// ErrJobNotFound indicates the requested job/node does not exist.
	ErrJobNotFound = errors.New("job not found")
)

const (
	fakeRunID    = "run-noted-001"
	fakeRunTitle = "noted: add note search, delete, and export"
	// fakeRepo is the repository the fake run operates on (shared by Jobs/Graph).
	fakeRepo = "acme/webapp"
	// fakeTickInterval is how often the background goroutine advances the run.
	fakeTickInterval = 1200 * time.Millisecond
)

// FakeDataSource is an in-memory DataSource used for local development and
// tests. It models a realistic `noted` orchestration run: a coordinator that
// fans out three parallel implement jobs, an integrate/review/open-PR job that
// depends on all three, and a final synthesis continuation. A background
// goroutine advances node states (queued -> running -> succeeded) on a timer
// and broadcasts each new snapshot to SSE subscribers.
//
// FakeDataSource is safe for concurrent use.
type FakeDataSource struct {
	interval time.Duration
	broker   *broker

	mu      sync.Mutex
	st      State
	step    int
	started bool
}

// NewFakeDataSource returns a FakeDataSource seeded with the noted run and
// starts the background goroutine that advances it.
func NewFakeDataSource() *FakeDataSource {
	f := &FakeDataSource{
		interval: fakeTickInterval,
		broker:   newBroker(),
		st:       initialFakeState(),
	}
	f.start()
	return f
}

// initialFakeState builds the seeded graph with the coordinator running and
// every downstream node queued.
func initialFakeState() State {
	now := time.Now().UnixMilli()
	return State{
		RunID: fakeRunID,
		Title: fakeRunTitle,
		Nodes: []Node{
			{
				ID:        "job-1",
				Title:     "coordinate: note search, delete, export",
				Agent:     "project-lead",
				Runtime:   "codex",
				Model:     "gpt-5.5",
				State:     "running",
				Depth:     0,
				StartedAt: now,
				WorkerID:  "codex-coordinator",
				Events: []Event{
					{T: now, Label: "queued"},
					{T: now, Label: "started"},
					{T: now, Label: "decomposed into 3 delegations"},
				},
			},
			{
				ID:       "job-2",
				ParentID: "job-1",
				Title:    "implement: note search",
				Agent:    "implementer",
				Runtime:  "codex",
				Model:    "gpt-5.5",
				State:    "queued",
				Depth:    1,
				Events:   []Event{{T: now, Label: "delegation_enqueued"}},
			},
			{
				ID:       "job-3",
				ParentID: "job-1",
				Title:    "implement: note delete",
				Agent:    "implementer",
				Runtime:  "codex",
				Model:    "gpt-5.5",
				State:    "queued",
				Depth:    1,
				Events:   []Event{{T: now, Label: "delegation_enqueued"}},
			},
			{
				ID:       "job-4",
				ParentID: "job-1",
				Title:    "implement: note export",
				Agent:    "implementer",
				Runtime:  "codex",
				Model:    "gpt-5.5",
				State:    "queued",
				Depth:    1,
				Events:   []Event{{T: now, Label: "delegation_enqueued"}},
			},
			{
				ID:       "job-5",
				ParentID: "job-1",
				Deps:     []string{"job-2", "job-3", "job-4"},
				Title:    "integrate + review + open PR",
				Agent:    "integrator",
				Runtime:  "codex",
				Model:    "gpt-5.5",
				State:    "queued",
				Depth:    1,
				Events:   []Event{{T: now, Label: "delegation_enqueued (awaiting deps)"}},
			},
			{
				ID:       "job-6",
				ParentID: "job-1",
				Deps:     []string{"job-5"},
				Title:    "synthesize: summarize outcome",
				Agent:    "project-lead",
				Runtime:  "codex",
				Model:    "gpt-5.5",
				State:    "queued",
				Depth:    1,
				Events:   []Event{{T: now, Label: "delegation_enqueued (continuation)"}},
			},
		},
	}
}

// fakeStep mutates the run for a single tick. Each step is applied with f.mu held.
type fakeStep func(f *FakeDataSource)

// fakeSteps is the ordered timeline the background goroutine walks. The initial
// snapshot already has the coordinator running and children queued.
var fakeSteps = []fakeStep{
	func(f *FakeDataSource) { f.transition("job-2", "running", "worker started: note search") },
	func(f *FakeDataSource) { f.transition("job-3", "running", "worker started: note delete") },
	func(f *FakeDataSource) { f.transition("job-4", "running", "worker started: note export") },
	func(f *FakeDataSource) { f.transition("job-2", "succeeded", "PR opened, review clean") },
	func(f *FakeDataSource) { f.transition("job-3", "succeeded", "PR opened, review clean") },
	func(f *FakeDataSource) { f.transition("job-4", "succeeded", "PR opened, review clean") },
	func(f *FakeDataSource) { f.transition("job-5", "running", "all deps satisfied; integrating") },
	func(f *FakeDataSource) {
		f.setPRURL("job-5", "https://github.com/jerryfane/noted/pull/42")
		f.transition("job-5", "succeeded", "integration PR opened, review clean")
	},
	func(f *FakeDataSource) { f.transition("job-6", "running", "synthesizing outcome") },
	func(f *FakeDataSource) { f.transition("job-6", "succeeded", "summary posted") },
	func(f *FakeDataSource) { f.transition("job-1", "succeeded", "run complete") },
}

func (f *FakeDataSource) start() {
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return
	}
	f.started = true
	f.mu.Unlock()
	go f.run()
}

// run advances the timeline on a ticker and broadcasts each new snapshot,
// stopping once the run reaches its terminal state.
func (f *FakeDataSource) run() {
	t := time.NewTicker(f.interval)
	defer t.Stop()
	for range t.C {
		f.mu.Lock()
		if f.step >= len(fakeSteps) {
			f.mu.Unlock()
			return
		}
		fakeSteps[f.step](f)
		f.step++
		snap := f.cloneStateLocked()
		f.mu.Unlock()
		f.broker.publish(snap)
	}
}

// transition sets a node's state and appends a timeline event. It must be
// called with f.mu held.
func (f *FakeDataSource) transition(id string, state NodeState, label string) {
	now := time.Now().UnixMilli()
	for i := range f.st.Nodes {
		n := &f.st.Nodes[i]
		if n.ID != id {
			continue
		}
		n.State = state
		if state == "running" && n.StartedAt == 0 {
			n.StartedAt = now
			if n.WorkerID == "" {
				n.WorkerID = "codex-worker-" + id
			}
		}
		if isTerminal(state) && n.EndedAt == 0 {
			n.EndedAt = now
		}
		n.Events = append(n.Events, Event{T: now, Label: label})
		return
	}
}

func (f *FakeDataSource) setPRURL(id, url string) {
	for i := range f.st.Nodes {
		if f.st.Nodes[i].ID == id {
			f.st.Nodes[i].PRURL = url
			return
		}
	}
}

func isTerminal(s NodeState) bool {
	switch s {
	case "succeeded", "failed", "cancelled":
		return true
	}
	return false
}

// cloneStateLocked returns a deep copy of the current run state so callers can
// read/encode it without holding f.mu. Must be called with f.mu held.
func (f *FakeDataSource) cloneStateLocked() State {
	out := State{RunID: f.st.RunID, Title: f.st.Title}
	out.Nodes = make([]Node, len(f.st.Nodes))
	for i, n := range f.st.Nodes {
		cp := n
		if n.Deps != nil {
			cp.Deps = append([]string(nil), n.Deps...)
		}
		cp.Events = append([]Event(nil), n.Events...)
		out.Nodes[i] = cp
	}
	return out
}

// overallStateLocked derives the run-level state from the coordinator (root)
// node, defaulting to running. Must be called with f.mu held.
func (f *FakeDataSource) overallStateLocked() (NodeState, int64) {
	var updated int64
	for _, n := range f.st.Nodes {
		for _, e := range n.Events {
			if e.T > updated {
				updated = e.T
			}
		}
	}
	root := NodeState("running")
	for _, n := range f.st.Nodes {
		if n.ID == "job-1" {
			root = n.State
			break
		}
	}
	return root, updated
}

// Runs implements DataSource.
func (f *FakeDataSource) Runs(ctx context.Context) ([]RunSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, updated := f.overallStateLocked()
	return []RunSummary{{
		RunID:   f.st.RunID,
		Title:   f.st.Title,
		State:   state,
		Updated: updated,
	}}, nil
}

// State implements DataSource. An empty runID returns the active/most-recent run.
func (f *FakeDataSource) State(ctx context.Context, runID string) (State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if runID != "" && runID != fakeRunID {
		return State{}, ErrRunNotFound
	}
	return f.cloneStateLocked(), nil
}

// Job implements DataSource.
func (f *FakeDataSource) Job(ctx context.Context, jobID string) (Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.st.Nodes {
		if n.ID == jobID {
			cp := n
			cp.Deps = append([]string(nil), n.Deps...)
			cp.Events = append([]Event(nil), n.Events...)
			return cp, nil
		}
	}
	return Node{}, ErrJobNotFound
}

// Subscribe implements DataSource. It registers an SSE subscriber, immediately
// delivers the current snapshot, and receives every subsequent snapshot the
// background goroutine broadcasts. The returned cancel func unregisters the
// subscriber; it is also invoked automatically when ctx is done.
func (f *FakeDataSource) Subscribe(ctx context.Context, runID string) (<-chan State, func(), error) {
	if runID != "" && runID != fakeRunID {
		return nil, nil, ErrRunNotFound
	}
	f.mu.Lock()
	snap := f.cloneStateLocked()
	f.mu.Unlock()

	ch, cancel := f.broker.subscribe(snap)

	// Automatically unregister when the request context is cancelled.
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return ch, cancel, nil
}

// Graph implements DataSource. The fake feed has a single run, so the galaxy is
// that run's jobs plus a repo hub and per-agent hubs — enough to exercise the
// Galaxy view standalone.
func (f *FakeDataSource) Graph(ctx context.Context, repo string) (Graph, error) {
	f.mu.Lock()
	snap := f.cloneStateLocked()
	f.mu.Unlock()
	g := Graph{Nodes: []GraphNode{}, Links: []GraphLink{}, Repos: []string{fakeRepo}}
	if repo != "" && repo != fakeRepo {
		return g, nil
	}
	ids := map[string]bool{}
	for _, n := range snap.Nodes {
		ids[n.ID] = true
	}
	agents := map[string]bool{}
	for _, n := range snap.Nodes {
		g.Nodes = append(g.Nodes, GraphNode{ID: n.ID, Type: "job", Label: n.Title, State: n.State, Agent: n.Agent, Repo: fakeRepo, Run: snap.RunID})
		if n.ParentID != "" && ids[n.ParentID] {
			g.Links = append(g.Links, GraphLink{Source: n.ParentID, Target: n.ID, Kind: "parent"})
		}
		for _, d := range n.Deps {
			if ids[d] {
				g.Links = append(g.Links, GraphLink{Source: d, Target: n.ID, Kind: "dep"})
			}
		}
		g.Links = append(g.Links, GraphLink{Source: n.ID, Target: "repo::" + fakeRepo, Kind: "repo"})
		if n.Agent != "" {
			agents[n.Agent] = true
			g.Links = append(g.Links, GraphLink{Source: n.ID, Target: "agent::" + n.Agent, Kind: "agent"})
		}
	}
	g.Nodes = append(g.Nodes, GraphNode{ID: "repo::" + fakeRepo, Type: "repo", Label: fakeRepo, Repo: fakeRepo})
	for a := range agents {
		g.Nodes = append(g.Nodes, GraphNode{ID: "agent::" + a, Type: "agent", Label: a, Agent: a})
	}
	return g, nil
}

// nodeUpdatedLocked returns a node's most-recent activity time (epoch ms): its
// last timeline event, falling back to EndedAt/StartedAt.
func nodeUpdated(n Node) int64 {
	updated := n.StartedAt
	if n.EndedAt > updated {
		updated = n.EndedAt
	}
	for _, e := range n.Events {
		if e.T > updated {
			updated = e.T
		}
	}
	return updated
}

// fakeKind maps a fake node to a plausible job kind (ask|review|implement|
// orchestrate) from its depth/title so the Jobs page has something to group on.
func fakeKind(n Node) string {
	switch {
	case n.Depth == 0:
		return "orchestrate"
	case strings.HasPrefix(n.Title, "implement"):
		return "implement"
	case strings.Contains(n.Title, "review"):
		return "review"
	default:
		return "ask"
	}
}

// prNumberFromURL extracts the trailing integer of a .../pull/<n> URL, or 0.
func prNumberFromURL(url string) int {
	if url == "" {
		return 0
	}
	i := strings.LastIndex(url, "/")
	if i < 0 || i+1 >= len(url) {
		return 0
	}
	n, err := strconv.Atoi(url[i+1:])
	if err != nil {
		return 0
	}
	return n
}

// jobSummaryFor flattens a Node into a JobSummary for the Jobs page. TokensIn/
// TokensOut are synthesized (deterministically from the id) so standalone dev
// shows plausible per-job token usage.
func jobSummaryFor(n Node, runID string) JobSummary {
	updated := nodeUpdated(n)
	var duration int64
	if n.StartedAt > 0 && updated > n.StartedAt {
		duration = updated - n.StartedAt
	}
	tokensIn, tokensOut := 0, 0
	if n.StartedAt > 0 { // only started jobs have accrued tokens
		seed := len(n.ID) + len(n.Title)
		tokensIn = 1500 + seed*130
		tokensOut = 600 + seed*70
	}
	return JobSummary{
		ID:        n.ID,
		Title:     n.Title,
		Agent:     n.Agent,
		Runtime:   n.Runtime,
		Repo:      fakeRepo,
		Kind:      fakeKind(n),
		State:     n.State,
		Depth:     n.Depth,
		Run:       runID,
		PR:        prNumberFromURL(n.PRURL),
		Started:   n.StartedAt,
		Updated:   updated,
		Duration:  duration,
		TokensIn:  tokensIn,
		TokensOut: tokensOut,
	}
}

// Jobs implements DataSource. The fake feed has a single run, so this flattens
// that run's nodes into per-job rows, sorted Updated desc.
func (f *FakeDataSource) Jobs(ctx context.Context) ([]JobSummary, error) {
	f.mu.Lock()
	snap := f.cloneStateLocked()
	f.mu.Unlock()

	jobs := make([]JobSummary, 0, len(snap.Nodes))
	for _, n := range snap.Nodes {
		jobs = append(jobs, jobSummaryFor(n, snap.RunID))
	}
	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Updated != jobs[j].Updated {
			return jobs[i].Updated > jobs[j].Updated // Updated desc
		}
		return jobs[i].ID < jobs[j].ID // stable tiebreak
	})
	return jobs, nil
}

// fakeAgent is the static registration for a fake agent; its live counts are
// filled in from the run's jobs by Agents().
type fakeAgent struct {
	name           string
	role           string
	runtime        string
	repoScope      []string
	model          string
	capabilities   []string
	autonomyPolicy string
	health         string
}

// fakeAgents is a handful of registered agents with varied runtimes/health so
// the Agents page has realistic rows standalone. project-lead/implementer/
// integrator match the names used by the seeded run (so their counts are live);
// the rest are idle registrations.
var fakeAgents = []fakeAgent{
	{name: "project-lead", role: "coordinator", runtime: "codex", model: "gpt-5.5", capabilities: []string{"orchestrate", "review"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo}},
	{name: "implementer", role: "implementer", runtime: "codex", model: "gpt-5.5", capabilities: []string{"implement"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo}},
	{name: "integrator", role: "integrator", runtime: "codex", model: "gpt-5.5", capabilities: []string{"review", "integrate"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo}},
	{name: "researcher", role: "researcher", runtime: "claude", model: "claude-opus-4-8", capabilities: []string{"research"}, autonomyPolicy: "read-only", health: "healthy"},
	{name: "reviewer-kimi", role: "reviewer", runtime: "kimi", model: "kimi-code", capabilities: []string{"review"}, autonomyPolicy: "read-only", health: "degraded"},
	{name: "ci-runner", role: "ci", runtime: "shell", capabilities: []string{"ci", "lint"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo}},
}

// Agents implements DataSource. It returns the registered agents (with job
// counts aggregated live from the seeded run) plus one synthetic rollup row for
// the fleet of ephemeral workers (Ephemeral == true).
func (f *FakeDataSource) Agents(ctx context.Context) ([]AgentSummary, error) {
	f.mu.Lock()
	snap := f.cloneStateLocked()
	f.mu.Unlock()

	// Aggregate per-agent counts from the run's jobs.
	type agg struct {
		jobs, running, succeeded, failed int
		lastActive                       int64
	}
	byAgent := map[string]*agg{}
	for _, n := range snap.Nodes {
		a := byAgent[n.Agent]
		if a == nil {
			a = &agg{}
			byAgent[n.Agent] = a
		}
		a.jobs++
		switch n.State {
		case "running":
			a.running++
		case "succeeded":
			a.succeeded++
		case "failed":
			a.failed++
		}
		if u := nodeUpdated(n); u > a.lastActive {
			a.lastActive = u
		}
	}

	out := make([]AgentSummary, 0, len(fakeAgents)+1)
	for _, fa := range fakeAgents {
		s := AgentSummary{
			Name:           fa.name,
			Role:           fa.role,
			Runtime:        fa.runtime,
			RepoScope:      fa.repoScope,
			Model:          fa.model,
			Capabilities:   fa.capabilities,
			AutonomyPolicy: fa.autonomyPolicy,
			Health:         fa.health,
		}
		if a := byAgent[fa.name]; a != nil {
			s.JobCount = a.jobs
			s.RunningCount = a.running
			s.SucceededCount = a.succeeded
			s.FailedCount = a.failed
			s.LastActive = a.lastActive
		}
		out = append(out, s)
	}

	// One synthetic rollup row for the fleet of ephemeral workers.
	out = append(out, AgentSummary{
		Name:           "ephemeral-workers",
		Role:           "worker",
		Runtime:        "codex",
		Capabilities:   []string{"implement", "review"},
		AutonomyPolicy: "workspace-write",
		Health:         "healthy",
		Ephemeral:      true,
		JobCount:       128,
		RunningCount:   3,
		SucceededCount: 119,
		FailedCount:    6,
		LastActive:     time.Now().UnixMilli(),
	})
	return out, nil
}
