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
	// ErrAgentNotFound indicates the requested agent does not exist.
	ErrAgentNotFound = errors.New("agent not found")
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

// fakeTemplatedAgent is the one seeded agent whose click-through detail carries
// a template and version history; every other agent's AgentDetail has Template
// nil. It is deliberately an agent that does not appear in the seeded run so its
// summary counts (and thus its whole AgentDetail) are byte-stable across calls.
const fakeTemplatedAgent = "researcher"

// fakeAgentTemplate is the template fakeTemplatedAgent is instantiated from. Its
// ResolvedCommit matches the currently-promoted version below. All values are
// constant so the detail is deterministic.
var fakeAgentTemplate = AgentTemplateInfo{
	ID:             "tmpl-researcher",
	Name:           "researcher",
	Description:    "SOTA / best-solution research agent that cites its sources",
	SourceRepo:     "jerryfane/gitmoot",
	SourceRef:      "main",
	SourcePath:     "agents/researcher.md",
	ResolvedCommit: "3c3824f9a1b2c4d5e6f70819a2b3c4d5e6f70819",
}

// fakeAgentVersions is fakeTemplatedAgent's version history, newest first, across
// the pending/canary/promoted states so the version-history UI is fully
// exercised: v1 is the promoted version the template currently resolves to
// (Current), v2 is a canary being sampled (CanarySample), v3 is a newly proposed
// pending candidate. Timestamps are anchored on fakeChartsNow (never time.Now())
// so the detail is byte-stable across calls.
var fakeAgentVersions = []TemplateVersionInfo{
	{
		ID:          "tmpl-researcher-v3",
		Number:      3,
		State:       "pending",
		Name:        "researcher",
		Description: "propose: add adversarial claim-verification pass",
		SourceRef:   "main",
		CreatedAt:   fakeChartsNow.Add(-6 * time.Hour).UnixMilli(),
	},
	{
		ID:             "tmpl-researcher-v2",
		Number:         2,
		State:          "canary",
		Name:           "researcher",
		Description:    "widen source fan-out to 8 parallel searches",
		SourceRef:      "main",
		ResolvedCommit: "9f8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c",
		CreatedAt:      fakeChartsNow.AddDate(0, 0, -2).UnixMilli(),
		CanarySample:   0.15,
	},
	{
		ID:             "tmpl-researcher-v1",
		Number:         1,
		State:          "promoted",
		Name:           "researcher",
		Description:    "initial captured researcher agent",
		SourceRef:      "main",
		ResolvedCommit: "3c3824f9a1b2c4d5e6f70819a2b3c4d5e6f70819",
		CreatedAt:      fakeChartsNow.AddDate(0, 0, -9).UnixMilli(),
		PromotedAt:     fakeChartsNow.AddDate(0, 0, -8).UnixMilli(),
		Current:        true,
	},
}

// Agent implements DataSource. It returns the click-through detail for a single
// agent: the same AgentSummary row Agents() lists (so counts line up with the
// Agents page) plus a template and version history for the one seeded agent that
// carries them (fakeTemplatedAgent) — every other agent's detail has Template
// nil. Unknown names return ErrAgentNotFound.
func (f *FakeDataSource) Agent(ctx context.Context, name string) (AgentDetail, error) {
	agents, err := f.Agents(ctx)
	if err != nil {
		return AgentDetail{}, err
	}
	for _, a := range agents {
		if a.Name != name {
			continue
		}
		detail := AgentDetail{AgentSummary: a, Versions: []TemplateVersionInfo{}}
		if name == fakeTemplatedAgent {
			tmpl := fakeAgentTemplate
			detail.Template = &tmpl
			detail.Versions = append([]TemplateVersionInfo(nil), fakeAgentVersions...)
		}
		return detail, nil
	}
	return AgentDetail{}, ErrAgentNotFound
}

// fakeChartsNow is the fixed reference instant the Charts/Health fake views
// treat as "now". Unlike the live-advancing run state (Runs/State/Jobs/Agents,
// which embed time.Now() and evolve as the background goroutine ticks), the
// Charts and Health views must be byte-stable across polls so their handlers'
// repeat-call equality tests hold. Every relative value they derive — day
// buckets, "queued older than 10 min", recent-failure ordering, lock ages — is
// anchored on this constant rather than time.Now(). It is an arbitrary fixed
// UTC instant and is the max timestamp in the seeded charts/health fixture.
var fakeChartsNow = time.Date(2026, 6, 27, 14, 30, 0, 0, time.UTC)

// fakeJobRecord is one synthetic job in the fixed Charts/Health fixture. All
// fields are constant (derived from fakeChartsNow, never time.Now()) so the two
// views are deterministic and identical across calls.
type fakeJobRecord struct {
	ID        string
	Title     string
	Agent     string
	Runtime   string
	Repo      string
	State     NodeState
	Started   int64 // epoch ms
	TokensIn  int
	TokensOut int
}

// fakeChartRepos are the repositories the fixture spreads jobs across (more than
// one so the Charts top-repos breakdown has something to rank).
var fakeChartRepos = []string{fakeRepo, "jerryfane/noted", "acme/api"}

// fakeChartAgents pairs the fixture's agents with their runtimes (a subset of
// fakeAgents so agent names line up with the Agents page).
var fakeChartAgents = []struct{ name, runtime string }{
	{"project-lead", "codex"},
	{"implementer", "codex"},
	{"integrator", "codex"},
	{"researcher", "claude"},
	{"reviewer-kimi", "kimi"},
	{"ci-runner", "shell"},
}

// fakeUTCDayStart returns UTC midnight of the day that is daysAgo days before
// fakeChartsNow.
func fakeUTCDayStart(daysAgo int) time.Time {
	d := fakeChartsNow.UTC()
	day := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	return day.AddDate(0, 0, -daysAgo)
}

// fakeHistory builds the fixed set of jobs behind Charts and Health. Days 9..1
// hold terminal history (bucketed by their Started day); day 0 (today) holds a
// live mix including running/queued/blocked work so Health has current jobs, a
// stuck job and a recent failure. The result is identical on every call.
func fakeHistory() []fakeJobRecord {
	var recs []fakeJobRecord

	// Days 9..1: terminal history only, a deterministic handful per day.
	termStates := []NodeState{"succeeded", "succeeded", "succeeded", "failed", "succeeded", "cancelled"}
	idx := 0
	for d := 9; d >= 1; d-- {
		n := 2 + (d*3+1)%4 // 2..5 jobs that day, deterministic
		for k := 0; k < n; k++ {
			a := fakeChartAgents[idx%len(fakeChartAgents)]
			repo := fakeChartRepos[idx%len(fakeChartRepos)]
			st := termStates[idx%len(termStates)]
			started := fakeUTCDayStart(d).Add(time.Duration((idx*53)%1400) * time.Minute).UnixMilli()
			recs = append(recs, fakeJobRecord{
				ID:        "hist-" + strconv.Itoa(d) + "-" + strconv.Itoa(k),
				Title:     fakeHistTitle(st, repo),
				Agent:     a.name,
				Runtime:   a.runtime,
				Repo:      repo,
				State:     st,
				Started:   started,
				TokensIn:  1200 + (idx%9)*450,
				TokensOut: 500 + (idx%7)*220,
			})
			idx++
		}
	}

	// Day 0 (today): a live mix. minsAgo stays well under fakeChartsNow's
	// time-of-day so every job lands in today's UTC bucket.
	today := []struct {
		id, title, agent, runtime, repo string
		state                           NodeState
		minsAgo                         int
		tin, tout                       int
	}{
		{"job-live-1", "orchestrate: nightly maintenance", "project-lead", "codex", fakeRepo, "running", 6, 4200, 1800},
		{"job-live-2", "implement: export to CSV", "implementer", "codex", fakeRepo, "running", 3, 3100, 900},
		{"job-live-3", "implement: bulk delete", "implementer", "codex", "jerryfane/noted", "queued", 22, 0, 0},
		{"job-live-4", "review: auth refactor", "reviewer-kimi", "kimi", "acme/api", "blocked", 47, 800, 120},
		{"job-live-5", "ci: lint + test", "ci-runner", "shell", fakeRepo, "succeeded", 90, 600, 200},
		{"job-live-6", "implement: search index", "implementer", "codex", "jerryfane/noted", "failed", 35, 2600, 700},
		{"job-live-7", "research: rate-limit design", "researcher", "claude", "acme/api", "succeeded", 120, 5200, 2600},
	}
	for _, t := range today {
		recs = append(recs, fakeJobRecord{
			ID:        t.id,
			Title:     t.title,
			Agent:     t.agent,
			Runtime:   t.runtime,
			Repo:      t.repo,
			State:     t.state,
			Started:   fakeChartsNow.Add(-time.Duration(t.minsAgo) * time.Minute).UnixMilli(),
			TokensIn:  t.tin,
			TokensOut: t.tout,
		})
	}
	return recs
}

// fakeHistTitle gives a terminal history job a plausible title from its state.
func fakeHistTitle(st NodeState, repo string) string {
	switch st {
	case "failed":
		return "implement: " + repo + " (failed)"
	case "cancelled":
		return "review: " + repo + " (cancelled)"
	default:
		return "implement: " + repo
	}
}

// fakeUTCDayKey returns the UTC YYYY-MM-DD bucket key for an epoch-ms instant.
func fakeUTCDayKey(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02")
}

// Charts implements DataSource. It aggregates the fixed fakeHistory fixture into
// a zero-filled per-day series plus top-agent/top-repo/totals breakdowns over
// the requested window (days 7|30|90; 0 = all history). Output is deterministic
// and byte-stable across calls.
func (f *FakeDataSource) Charts(ctx context.Context, days int) (Charts, error) {
	recs := fakeHistory()

	now := fakeChartsNow.UTC()
	anchorDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// Window start (inclusive). days > 0 => the last `days` days ending today;
	// days <= 0 => all history (earliest seeded job day .. today).
	start := anchorDay
	if days > 0 {
		start = anchorDay.AddDate(0, 0, -(days - 1))
	} else {
		for _, r := range recs {
			d := time.UnixMilli(r.Started).UTC()
			dd := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
			if dd.Before(start) {
				start = dd
			}
		}
	}

	// Continuous zero-filled buckets, oldest -> newest.
	idxByDate := map[string]int{}
	daysOut := []ChartDay{}
	for day := start; !day.After(anchorDay); day = day.AddDate(0, 0, 1) {
		key := day.Format("2006-01-02")
		idxByDate[key] = len(daysOut)
		daysOut = append(daysOut, ChartDay{Date: key})
	}

	type agAcc struct {
		jobs, tokensOut int
		runtime         string
	}
	agents := map[string]*agAcc{}
	repos := map[string]int{}
	activeAgents := map[string]bool{}
	totals := ChartTotals{}

	for _, r := range recs {
		bi, in := idxByDate[fakeUTCDayKey(r.Started)]
		if !in {
			continue // outside the window
		}
		d := &daysOut[bi]
		switch r.State {
		case "succeeded":
			d.Succeeded++
			totals.Succeeded++
		case "failed":
			d.Failed++
			totals.Failed++
		case "cancelled":
			d.Cancelled++
		case "blocked":
			d.Blocked++
		case "queued":
			d.Queued++
		case "running":
			d.Running++
		}
		d.TokensIn += r.TokensIn
		d.TokensOut += r.TokensOut

		totals.Jobs++
		totals.TokensIn += r.TokensIn
		totals.TokensOut += r.TokensOut

		a := agents[r.Agent]
		if a == nil {
			a = &agAcc{runtime: r.Runtime}
			agents[r.Agent] = a
		}
		a.jobs++
		a.tokensOut += r.TokensOut
		repos[r.Repo]++
		activeAgents[r.Agent] = true
	}
	totals.ActiveAgents = len(activeAgents)

	// Top 12 agents by Jobs desc, name tie-break.
	agOut := make([]ChartAgent, 0, len(agents))
	for name, a := range agents {
		agOut = append(agOut, ChartAgent{Name: name, Runtime: a.runtime, Jobs: a.jobs, TokensOut: a.tokensOut})
	}
	sort.Slice(agOut, func(i, j int) bool {
		if agOut[i].Jobs != agOut[j].Jobs {
			return agOut[i].Jobs > agOut[j].Jobs
		}
		return agOut[i].Name < agOut[j].Name
	})
	if len(agOut) > 12 {
		agOut = agOut[:12]
	}

	// Top 12 repos by Jobs desc, repo tie-break.
	rpOut := make([]ChartRepo, 0, len(repos))
	for repo, n := range repos {
		rpOut = append(rpOut, ChartRepo{Repo: repo, Jobs: n})
	}
	sort.Slice(rpOut, func(i, j int) bool {
		if rpOut[i].Jobs != rpOut[j].Jobs {
			return rpOut[i].Jobs > rpOut[j].Jobs
		}
		return rpOut[i].Repo < rpOut[j].Repo
	})
	if len(rpOut) > 12 {
		rpOut = rpOut[:12]
	}

	return Charts{Days: daysOut, Agents: agOut, Repos: rpOut, Totals: totals}, nil
}

// fakeDaemonVersion is the version the fake running daemon reports, and
// fakeDaemonLatest is a newer release so the update badge is exercised in dev.
// Both are constant so Health stays byte-stable across calls.
const (
	fakeDaemonVersion = "v0.8.1"
	fakeDaemonLatest  = "v0.8.3"
)

// Health implements DataSource. It derives the daemon liveness, fleet totals,
// held locks, wedged jobs and recent failures from the fixed fakeHistory fixture
// (and fixed lock fixtures), anchored on fakeChartsNow. Output is deterministic
// and byte-stable across calls.
func (f *FakeDataSource) Health(ctx context.Context) (Health, error) {
	recs := fakeHistory()
	now := fakeChartsNow.UnixMilli()
	const minute = int64(60 * 1000)
	const stuckCutoff = 10 * minute

	// Fleet totals by state (lifetime terminal counts + today's live jobs).
	totals := HealthTotals{}
	for _, r := range recs {
		switch r.State {
		case "queued":
			totals.Queued++
		case "running":
			totals.Running++
		case "blocked":
			totals.Blocked++
		case "succeeded":
			totals.Succeeded++
		case "failed":
			totals.Failed++
		case "cancelled":
			totals.Cancelled++
		}
	}

	// Branch/checkout locks, oldest first.
	locks := []HealthLock{
		{Repo: fakeRepo, Branch: "feat/export-csv", Owner: "implementer", AcquiredAt: now - 8*minute},
		{Repo: "jerryfane/noted", Branch: "feat/bulk-delete", Owner: "implementer", AcquiredAt: now - 3*minute},
	}
	sort.SliceStable(locks, func(i, j int) bool { return locks[i].AcquiredAt < locks[j].AcquiredAt })

	// Non-branch resource locks (runtime sessions, etc.), oldest first.
	resLocks := []HealthResourceLock{
		{Key: "runtime:claude:sess-7f3a", Owner: "researcher", AcquiredAt: now - 12*minute, ExpiresAt: now + 18*minute},
		{Key: "skillopt-train-generation:acme", Owner: "ci-runner", AcquiredAt: now - 2*minute, ExpiresAt: now + 28*minute},
	}
	sort.SliceStable(resLocks, func(i, j int) bool { return resLocks[i].AcquiredAt < resLocks[j].AcquiredAt })

	// Stuck: blocked jobs + queued older than 10 min, oldest first.
	stuck := []HealthStuckJob{}
	for _, r := range recs {
		var reason string
		switch {
		case r.State == "blocked":
			reason = "blocked awaiting human"
		case r.State == "queued" && now-r.Started > stuckCutoff:
			reason = "queued > 10m (no free worker slot)"
		default:
			continue
		}
		stuck = append(stuck, HealthStuckJob{
			ID:     r.ID,
			Title:  r.Title,
			Agent:  r.Agent,
			Repo:   r.Repo,
			State:  string(r.State),
			Reason: reason,
			Since:  r.Started,
		})
	}
	sort.SliceStable(stuck, func(i, j int) bool {
		if stuck[i].Since != stuck[j].Since {
			return stuck[i].Since < stuck[j].Since // oldest first
		}
		return stuck[i].ID < stuck[j].ID
	})

	// Recent failures, newest first, capped at 10.
	failures := []HealthFailure{}
	for _, r := range recs {
		if r.State != "failed" {
			continue
		}
		failures = append(failures, HealthFailure{
			ID:    r.ID,
			Title: r.Title,
			Agent: r.Agent,
			Repo:  r.Repo,
			At:    r.Started,
		})
	}
	sort.SliceStable(failures, func(i, j int) bool {
		if failures[i].At != failures[j].At {
			return failures[i].At > failures[j].At // newest first
		}
		return failures[i].ID < failures[j].ID
	})
	if len(failures) > 10 {
		failures = failures[:10]
	}

	return Health{
		Daemon: HealthDaemon{Running: true, PID: 4242, StartedAt: now - 6*60*minute, Version: fakeDaemonVersion},
		Update: &HealthUpdate{
			Current:         fakeDaemonVersion,
			Latest:          fakeDaemonLatest,
			ReleaseURL:      "https://github.com/jerryfane/gitmoot/releases/tag/" + fakeDaemonLatest,
			UpdateAvailable: true,
			CheckedAt:       now - 30*minute,
		},
		Totals:         totals,
		Locks:          locks,
		ResourceLocks:  resLocks,
		Stuck:          stuck,
		RecentFailures: failures,
	}, nil
}
