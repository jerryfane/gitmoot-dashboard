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
	// ErrPipelineRunNotFound indicates the requested pipeline run does not exist.
	ErrPipelineRunNotFound = errors.New("pipeline run not found")
	// ErrPipelineNotFound indicates the requested pipeline does not exist.
	ErrPipelineNotFound = errors.New("pipeline not found")
	// ErrChatThreadNotFound indicates the requested chat thread does not exist.
	ErrChatThreadNotFound = errors.New("chat thread not found")
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
	interval             time.Duration
	broker               *broker
	flatKnowledgeFixture bool

	mu      sync.Mutex
	st      State
	step    int
	started bool
}

// NewFakeDataSource returns a FakeDataSource seeded with the noted run and
// starts the background goroutine that advances it.
func NewFakeDataSource() *FakeDataSource {
	return newFakeDataSource(false)
}

// NewFakeDataSourceFlatKnowledge returns the same fake dashboard feed with the
// pre-hierarchy Knowledge fixture. It is intended for visual regression checks
// that must prove a payload without parent_id still takes the original path.
func NewFakeDataSourceFlatKnowledge() *FakeDataSource {
	return newFakeDataSource(true)
}

func newFakeDataSource(flatKnowledgeFixture bool) *FakeDataSource {
	f := &FakeDataSource{
		interval:             fakeTickInterval,
		broker:               newBroker(),
		flatKnowledgeFixture: flatKnowledgeFixture,
		st:                   initialFakeState(),
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
	// memory mirrors the agent's config memory switch: it drives the card's
	// "memory" chip (AgentSummary.MemoryEnabled) and, when a config is present,
	// the config section's memory on/off row.
	memory bool
	// config is the agent's [agents.<name>] section for the detail panel, or nil
	// when the agent has no config entry. memFacts/memObs are the pretend memory
	// pool sizes surfaced by the detail panel. Constant (no time.Now()) so a
	// clicked-through AgentDetail stays byte-stable.
	config   *AgentConfigInfo
	memFacts int
	memObs   int
}

// fakeAgents is a handful of registered agents with varied runtimes/health so
// the Agents page has realistic rows standalone. project-lead/implementer/
// integrator match the names used by the seeded run (so their counts are live);
// the rest are idle registrations.
// The fake agents span every config/memory UI branch so the Agents page renders
// them all standalone: a memory-on agent with a config and a live pool
// (researcher); a memory-on agent whose pool is still empty (project-lead); a
// config with memory OFF (implementer); a degraded kimi agent that is likewise
// memory-on with a config and pool (reviewer-kimi — enrolled agents always carry
// a config section, so memory-on implies a config just like real enrollment); and
// plain agents with neither (integrator/ci-runner). All config/pool values are
// constant (no time.Now()) so a clicked-through detail is byte-stable.
var fakeAgents = []fakeAgent{
	{name: "project-lead", role: "coordinator", runtime: "codex", model: "gpt-5.5", capabilities: []string{"orchestrate", "review"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo},
		memory: true, memFacts: 0, memObs: 0,
		config: &AgentConfigInfo{Memory: true, MaxBackground: 4, IdleTimeout: "10m", JobTimeout: "1h", Model: "gpt-5.5", Template: "coordinator", Capabilities: []string{"orchestrate", "review"}}},
	{name: "implementer", role: "implementer", runtime: "codex", model: "gpt-5.5", capabilities: []string{"implement"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo},
		config: &AgentConfigInfo{Memory: false, MaxBackground: 6, IdleTimeout: "5m", JobTimeout: "45m", Model: "gpt-5.5", Capabilities: []string{"implement"}}},
	{name: "integrator", role: "integrator", runtime: "codex", model: "gpt-5.5", capabilities: []string{"review", "integrate"}, autonomyPolicy: "workspace-write", health: "healthy", repoScope: []string{fakeRepo}},
	{name: "researcher", role: "researcher", runtime: "claude", model: "claude-opus-4-8", capabilities: []string{"research"}, autonomyPolicy: "read-only", health: "healthy",
		memory: true, memFacts: 42, memObs: 17,
		config: &AgentConfigInfo{Memory: true, MaxBackground: 2, IdleTimeout: "15m", JobTimeout: "30m", Model: "claude-opus-4-8", Template: "researcher", Capabilities: []string{"research"}}},
	{name: "reviewer-kimi", role: "reviewer", runtime: "kimi", model: "kimi-code", capabilities: []string{"review"}, autonomyPolicy: "read-only", health: "degraded",
		memory: true, memFacts: 8, memObs: 3,
		config: &AgentConfigInfo{Memory: true, MaxBackground: 3, IdleTimeout: "10m", JobTimeout: "30m", Model: "kimi-code", Template: "reviewer", Capabilities: []string{"review"}}},
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
			MemoryEnabled:  fa.memory,
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

// The three prompt bodies below are fakeTemplatedAgent's full template content at
// each version. They are multi-line markdown and deliberately DIFFERENT per
// version (v1 base → v2 widens fan-out → v3 adds a verification pass) so the
// detail view's content viewer is exercised, and each contains angle brackets,
// ampersands, and quotes so the client's HTML-escaping is exercised too. Constant
// (no time.Now()) so the whole AgentDetail stays byte-stable across calls.
const (
	fakeResearcherPromptV1 = `# Researcher agent

You are a **research agent**. Given a question, find the current
state-of-the-art answer & cite every source.

## Method
1. Decompose the question into sub-queries.
2. Search the web; prefer primary sources & standards bodies over blog summaries.
3. Synthesize a concise answer.

## Output
Return findings as <finding> blocks and quote sources verbatim inside
"double quotes". Never invent a citation.
`
	fakeResearcherPromptV2 = `# Researcher agent

You are a **research agent**. Given a question, find the current
state-of-the-art answer & cite every source.

## Method
1. Decompose the question into up to 8 sub-queries.
2. Fan out all sub-queries in parallel, then dedupe the results.
3. Search the web; prefer primary sources & standards bodies over blog summaries.
4. Synthesize a concise answer.

## Output
Return findings as <finding> blocks and quote sources verbatim inside
"double quotes". Never invent a citation.
`
	fakeResearcherPromptV3 = `# Researcher agent

You are a **research agent**. Given a question, find the current
state-of-the-art answer & cite every source.

## Method
1. Decompose the question into up to 8 sub-queries.
2. Fan out all sub-queries in parallel, then dedupe the results.
3. Search the web; prefer primary sources & standards bodies over blog summaries.
4. Adversarially verify each claim against a second source; drop any
   claim you cannot corroborate.
5. Synthesize a concise answer.

## Output
Return findings as <finding> blocks and quote sources verbatim inside
"double quotes". Never invent a citation.
`
)

// fakeAgentTemplate is the template fakeTemplatedAgent is instantiated from. Its
// ResolvedCommit and Content match the currently-promoted version below (v1, the
// version the template currently resolves to). All values are constant so the
// detail is deterministic.
var fakeAgentTemplate = AgentTemplateInfo{
	ID:             "tmpl-researcher",
	Name:           "researcher",
	Description:    "SOTA / best-solution research agent that cites its sources",
	SourceRepo:     "jerryfane/gitmoot",
	SourceRef:      "main",
	SourcePath:     "agents/researcher.md",
	ResolvedCommit: "3c3824f9a1b2c4d5e6f70819a2b3c4d5e6f70819",
	Content:        fakeResearcherPromptV1,
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
		Content:     fakeResearcherPromptV3,
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
		Content:        fakeResearcherPromptV2,
	},
	{
		ID:             "tmpl-researcher-v1",
		Number:         1,
		State:          "current", // live version (store emits 'current', never 'promoted')
		Name:           "researcher",
		Description:    "initial captured researcher agent",
		SourceRef:      "main",
		ResolvedCommit: "3c3824f9a1b2c4d5e6f70819a2b3c4d5e6f70819",
		CreatedAt:      fakeChartsNow.AddDate(0, 0, -9).UnixMilli(),
		PromotedAt:     fakeChartsNow.AddDate(0, 0, -8).UnixMilli(),
		Current:        true,
		Content:        fakeResearcherPromptV1,
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
		// Config section + memory pool sizes come from the registered fakeAgent (nil
		// config => "no config entry" in the panel). Constant, so the detail stays
		// byte-stable across calls.
		for i := range fakeAgents {
			fa := &fakeAgents[i]
			if fa.name != name {
				continue
			}
			if fa.config != nil {
				cfg := *fa.config
				detail.Config = &cfg
			}
			detail.MemoryFacts = fa.memFacts
			detail.MemoryObservations = fa.memObs
			break
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

// fakeSkills builds the fixed SkillOpt evolution fixture behind the Learning
// page's Skills view. Every timestamp is anchored on fakeChartsNow (never
// time.Now()) so the view is byte-stable across polls. It models three templates
// that together exercise the UI: a healthy rising-score template evolved through
// five versions (researcher); a template with an in-flight canary being sampled
// AND a freshly proposed pending candidate (reviewer); and a flat two-version
// template whose score never moved (coordinator). Only real-emittable version
// states are used: the live version is "current" and each promoted-then-replaced
// predecessor is "superseded" (the store never emits a "promoted" version state —
// PromoteAgentTemplateVersion writes 'current'/'superseded'), plus "canary" and
// "pending" for in-flight candidates.
func fakeSkills() Skills {
	dA := func(n int) int64 { return fakeChartsNow.AddDate(0, 0, -n).UnixMilli() }
	hA := func(n int) int64 { return fakeChartsNow.Add(-time.Duration(n) * time.Hour).UnixMilli() }

	templates := []SkillTemplate{
		// Healthy, rising score: four superseded predecessors and the live "current"
		// version (the store supersedes the old current on each promotion).
		{
			TemplateID: "skill-researcher",
			Name:       "researcher",
			Agents:     []string{"researcher"},
			Versions: []SkillVersion{
				{Number: 1, State: "superseded", Score: 0.62, HasScore: true, CreatedAt: dA(20), PromotedAt: dA(19)},
				{Number: 2, State: "superseded", Score: 0.68, HasScore: true, CreatedAt: dA(15), PromotedAt: dA(14)},
				{Number: 3, State: "superseded", Score: 0.71, HasScore: true, CreatedAt: dA(10), PromotedAt: dA(9)},
				{Number: 4, State: "superseded", Score: 0.77, HasScore: true, CreatedAt: dA(5), PromotedAt: dA(4)},
				{Number: 5, State: "current", Score: 0.83, HasScore: true, CreatedAt: dA(2), PromotedAt: dA(1)},
			},
			CurrentVersion: 5,
			CurrentState:   "current",
			LastPromotedAt: dA(1),
			Pending:        []SkillCandidate{},
		},
		// Active canary being sampled at 0.15, plus a pending candidate awaiting
		// review while the live "current" version stays in production.
		{
			TemplateID: "skill-reviewer",
			Name:       "reviewer",
			Agents:     []string{"reviewer-kimi"},
			Versions: []SkillVersion{
				{Number: 1, State: "current", Score: 0.70, HasScore: true, CreatedAt: dA(9), PromotedAt: dA(8)},
				{Number: 2, State: "canary", CreatedAt: dA(2)}, // mid-canary: no final score yet
				{Number: 3, State: "pending", CreatedAt: hA(6)},
			},
			CurrentVersion:  1,
			CurrentState:    "current",
			CanarySample:    0.15,
			CanaryStartedAt: dA(2),
			LastPromotedAt:  dA(8),
			Pending: []SkillCandidate{
				{VersionID: "skill-reviewer-v3", Number: 3, Score: "0.81", CreatedAt: hA(6)},
			},
		},
		// Flat: a superseded predecessor and the live "current" version, unchanged score.
		{
			TemplateID: "skill-coordinator",
			Name:       "coordinator",
			Agents:     []string{"project-lead"},
			Versions: []SkillVersion{
				{Number: 1, State: "superseded", Score: 0.75, HasScore: true, CreatedAt: dA(12), PromotedAt: dA(11)},
				{Number: 2, State: "current", Score: 0.75, HasScore: true, CreatedAt: dA(6), PromotedAt: dA(5)},
			},
			CurrentVersion: 2,
			CurrentState:   "current",
			LastPromotedAt: dA(5),
			Pending:        []SkillCandidate{},
		},
	}

	// Pending-first, then most-recently-promoted (LastPromotedAt desc), with a
	// TemplateID tie-break so the order is fully deterministic.
	sort.SliceStable(templates, func(i, j int) bool {
		pi, pj := len(templates[i].Pending) > 0, len(templates[j].Pending) > 0
		if pi != pj {
			return pi
		}
		if templates[i].LastPromotedAt != templates[j].LastPromotedAt {
			return templates[i].LastPromotedAt > templates[j].LastPromotedAt
		}
		return templates[i].TemplateID < templates[j].TemplateID
	})

	skills := Skills{Templates: templates}
	for i := range templates {
		if templates[i].CanarySample > 0 {
			skills.ActiveCanaries++
		}
		skills.PendingTotal += len(templates[i].Pending)
	}
	return skills
}

// Skills implements DataSource. It returns the fixed fakeSkills fixture; output
// is deterministic and byte-stable across calls.
func (f *FakeDataSource) Skills(ctx context.Context) (Skills, error) {
	return fakeSkills(), nil
}

// fakeKnowledge builds the fixed memory brain-graph fixture behind the Learning
// page's Knowledge view. Timestamps are anchored on fakeChartsNow (never
// time.Now()) so the view is byte-stable across polls. It models three enrolled
// agents owning ten facts spread across two repos and two general-scope entries,
// with witness counts varied across 1..7 and one superseded chain (an older
// auth-flow fact replaced by a newer one). Some fact bodies carry angle brackets,
// ampersands and quotes so the client's HTML-escaping is exercised.
func fakeKnowledge() Knowledge {
	dA := func(n int) int64 { return fakeChartsNow.AddDate(0, 0, -n).UnixMilli() }

	// Fact bodies deliberately mix angle brackets, ampersands and quotes (to
	// exercise the client's escape-FIRST HTML escaping) with a safe markdown
	// subset — **bold**, `inline code`, - lists, fenced code, plain https links
	// and [[fact:id]] wikilinks — so the detail panel's markdown renderer is
	// exercised end-to-end. The wikilinks mirror the Links slice.
	facts := []KnowledgeFact{
		{ID: "fact:1", Content: `Build with GOTOOLCHAIN=local & GOFLAGS=-mod=mod; the pinned go1.26.4 toolchain lives under /root/.local.`, Repo: fakeRepo, Key: "build-toolchain", Owner: "researcher", Witnesses: 5, FirstSeen: dA(18), LastSeen: dA(1), Cluster: "cluster:3", SourceJob: "job:build-31", Links: []string{"fact:8"}},
		{ID: "fact:2", Content: `TestExport is flaky under -race; retry once before failing the job.`, Repo: fakeRepo, Key: "test-flake", Owner: "reviewer-kimi", Witnesses: 3, FirstSeen: dA(16), LastSeen: dA(2), Cluster: "cluster:2", SourceJob: "job:test-88"},
		{ID: "fact:3", Content: `Auth uses <bearer> tokens & refreshes 5m before expiry; header is "Authorization".`, Repo: fakeRepo, Key: "auth-flow", Owner: "researcher", Witnesses: 7, FirstSeen: dA(15), LastSeen: dA(2), Superseded: true, Cluster: "cluster:1", SourceJob: "job:auth-12"},
		{ID: "fact:4", Content: `Exports default to CSV; JSON is opt-in via --format json.`, Repo: "jerryfane/noted", Key: "export-format", Owner: "project-lead", Witnesses: 2, FirstSeen: dA(12), LastSeen: dA(3), Cluster: "cluster:4", SourceFile: "jerryfane/noted:docs/exports.md"},
		{ID: "fact:5", Content: `Search index rebuilds lazily on the first query after a write.`, Repo: "jerryfane/noted", Key: "search-index", Owner: "researcher", Witnesses: 4, FirstSeen: dA(10), LastSeen: dA(1), Cluster: "cluster:4", SourceJob: "job:idx-40", Links: []string{"fact:7"}},
		{ID: "fact:6", Content: `Bulk delete requires a confirm token & is soft-delete for 30 days.`, Repo: "jerryfane/noted", Key: "delete-safety", Owner: "reviewer-kimi", Witnesses: 1, FirstSeen: dA(9), LastSeen: dA(4), Cluster: "cluster:4", SourceFile: "jerryfane/noted:docs/deletion.md"},
		{ID: "fact:7", Content: `Rate limiting is per-token: 100 req/min, burst 20; 429 carries Retry-After.`, Repo: "jerryfane/noted", Key: "rate-limit", Owner: "researcher", Witnesses: 4, FirstSeen: dA(7), LastSeen: dA(2), Cluster: "cluster:4", SourceJob: "job:rl-19"},
		{ID: "fact:8", Content: "Cut GA releases only with explicit sign-off; \"deploy latest\" means build & install locally.\n\nChecklist:\n- run `make release`\n- verify the tag & sha256sums\n- see https://gitmoot.io/docs/releasing\n\n```sh\ngh release create vX.Y.Z --latest\n```\n\nRelated: [[fact:1]].", Key: "release-policy", Owner: "researcher", Witnesses: 6, FirstSeen: dA(5), LastSeen: dA(1), Cluster: "cluster:3", SourceFile: "docs/RELEASING.md", Links: []string{"fact:1"}},
		{ID: "fact:9", Content: "Auth migrated to <PASETO> tokens & rotating keys; refresh 10m before \"expiry\". Supersedes [[fact:3]]; **rotate keys** nightly via `authctl rotate`.", Repo: fakeRepo, Key: "auth-flow", Owner: "researcher", Witnesses: 2, FirstSeen: dA(2), LastSeen: dA(1), Cluster: "cluster:1", SourceJob: "job:auth-77", Links: []string{"fact:3"}},
		{ID: "fact:10", Content: `Prefer table-driven tests & gofmt; avoid naked returns in long functions.`, Key: "coding-style", Owner: "project-lead", Witnesses: 3, FirstSeen: dA(4), LastSeen: dA(1), Cluster: "cluster:2", SourceFile: "CONTRIBUTING.md"},
	}
	// Newest-first by FirstSeen (distinct across the fixture), ID tie-break.
	sort.SliceStable(facts, func(i, j int) bool {
		if facts[i].FirstSeen != facts[j].FirstSeen {
			return facts[i].FirstSeen > facts[j].FirstSeen
		}
		return facts[i].ID < facts[j].ID
	})

	// Enrolled agents. Facts is the INJECTABLE count (the real datasource fills it
	// from CountConfirmedMemoriesForOwner, which excludes superseded_by IS NOT NULL
	// rows), so it deliberately differs from the on-graph owned-node count where an
	// agent has a superseded fact. researcher OWNS six fact nodes but one (fact:3,
	// the old auth-flow entry superseded by fact:9) is excluded, so its injectable
	// count is 5; reviewer-kimi owns 2 and project-lead owns 2 (none superseded).
	agents := []KnowledgeAgent{
		{Name: "project-lead", Enrolled: true, Facts: 2, Observations: 3},
		{Name: "researcher", Enrolled: true, Facts: 5, Observations: 12},
		{Name: "reviewer-kimi", Enrolled: true, Facts: 2, Observations: 4},
	}
	sort.SliceStable(agents, func(i, j int) bool { return agents[i].Name < agents[j].Name })

	// Emergent clusters (gitmoot #763): four communities over the ten facts,
	// anchored to a medoid fact for label stability. Repo is the dominant repo
	// scope ("" = general/mixed), so the client nests repo -> cluster -> fact.
	// The client renders Label verbatim (an owner override wins server-side).
	//
	// The member distribution is deliberately UNEVEN (4 / 2 / 2 / 2 facts) so the
	// clustered four-column layout shows visibly grouped fact bands with gaps
	// between cluster groups. This non-empty slice drives the clustered left ->
	// right column view; a build that returns an EMPTY clusters slice (and empty
	// per-fact Cluster fields) instead exercises the legacy fallback layout
	// (the scope/category columns), so both code paths stay reachable in fake mode.
	clusters := []KnowledgeCluster{
		{ID: "cluster:1", Label: "auth & tokens", Repo: fakeRepo, Medoid: "fact:9"},
		{ID: "cluster:2", Label: "testing & style", Medoid: "fact:2"},
		{ID: "cluster:3", Label: "build & release", Medoid: "fact:1"},
		{ID: "cluster:4", Label: "noted api & data", Repo: "jerryfane/noted", Medoid: "fact:7"},
	}
	// Count is derived from the assignments so it can never drift from the facts.
	clusterCount := map[string]int{}
	for _, fct := range facts {
		if fct.Cluster != "" {
			clusterCount[fct.Cluster]++
		}
	}
	for i := range clusters {
		clusters[i].Count = clusterCount[clusters[i].ID]
	}
	sort.SliceStable(clusters, func(i, j int) bool { return clusters[i].ID < clusters[j].ID })

	// Owner + category + cluster edges per fact, then the one supersede chain
	// (the newer auth-flow fact supersedes the older one). Category edges stay
	// for the pre-cluster fallback view; cluster edges back the repo -> cluster
	// -> fact hierarchy. Scored link edges are undirected and emitted once per
	// pair. They deliberately cover tight in-cluster pairs, cross-cluster pairs,
	// and cross-repo pairs so the dev harness exercises every galaxy treatment.
	edges := make([]KnowledgeEdge, 0, len(facts)*3+10)
	for _, fct := range facts {
		edges = append(edges, KnowledgeEdge{Source: fct.ID, Target: fct.Owner, Kind: "owner"})
		cat := fct.Repo
		if cat == "" {
			cat = "general"
		}
		edges = append(edges, KnowledgeEdge{Source: fct.ID, Target: cat, Kind: "category"})
		if fct.Cluster != "" {
			edges = append(edges, KnowledgeEdge{Source: fct.ID, Target: fct.Cluster, Kind: "cluster"})
		}
	}
	edges = append(edges, KnowledgeEdge{Source: "fact:9", Target: "fact:3", Kind: "supersede"})
	edges = append(edges,
		KnowledgeEdge{Source: "fact:3", Target: "fact:9", Kind: "link", Score: 0.95},
		KnowledgeEdge{Source: "fact:1", Target: "fact:8", Kind: "link", Score: 0.92},
		KnowledgeEdge{Source: "fact:6", Target: "fact:7", Kind: "link", Score: 0.88},
		KnowledgeEdge{Source: "fact:2", Target: "fact:10", Kind: "link", Score: 0.84},
		KnowledgeEdge{Source: "fact:4", Target: "fact:5", Kind: "link", Score: 0.78},
		KnowledgeEdge{Source: "fact:5", Target: "fact:7", Kind: "link", Score: 0.62},
		KnowledgeEdge{Source: "fact:1", Target: "fact:2", Kind: "link", Score: 0.48},
		KnowledgeEdge{Source: "fact:1", Target: "fact:4", Kind: "link", Score: 0.33},
		KnowledgeEdge{Source: "fact:9", Target: "fact:5", Kind: "link", Score: 0.15},
	)
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].Kind != edges[j].Kind {
			return edges[i].Kind < edges[j].Kind
		}
		if edges[i].Source != edges[j].Source {
			return edges[i].Source < edges[j].Source
		}
		return edges[i].Target < edges[j].Target
	})

	return Knowledge{Agents: agents, Facts: facts, Clusters: clusters, Edges: edges}
}

// fakeKnowledgeWithSubclusters derives the hierarchy fixture from the original
// flat fixture. cluster:4 becomes a parent with two leaf children; the children
// deliberately omit Repo so the dashboard must inherit the parent's lane.
func fakeKnowledgeWithSubclusters() Knowledge {
	k := fakeKnowledge()
	leafByFact := map[string]string{
		"fact:4": "cluster:4:storage",
		"fact:5": "cluster:4:storage",
		"fact:6": "cluster:4:safety",
		"fact:7": "cluster:4:safety",
	}
	for i := range k.Facts {
		if leaf := leafByFact[k.Facts[i].ID]; leaf != "" {
			k.Facts[i].Cluster = leaf
		}
	}
	k.Clusters = append(k.Clusters,
		KnowledgeCluster{ID: "cluster:4:safety", Label: "limits & deletion", Count: 2, Medoid: "fact:7", ParentID: "cluster:4"},
		KnowledgeCluster{ID: "cluster:4:storage", Label: "exports & search", Count: 2, Medoid: "fact:5", ParentID: "cluster:4"},
	)
	for i := range k.Edges {
		if k.Edges[i].Kind != "cluster" {
			continue
		}
		if leaf := leafByFact[k.Edges[i].Source]; leaf != "" {
			k.Edges[i].Target = leaf
		}
	}
	sort.SliceStable(k.Clusters, func(i, j int) bool { return k.Clusters[i].ID < k.Clusters[j].ID })
	sort.SliceStable(k.Edges, func(i, j int) bool {
		if k.Edges[i].Kind != k.Edges[j].Kind {
			return k.Edges[i].Kind < k.Edges[j].Kind
		}
		if k.Edges[i].Source != k.Edges[j].Source {
			return k.Edges[i].Source < k.Edges[j].Source
		}
		return k.Edges[i].Target < k.Edges[j].Target
	})
	return k
}

// Knowledge implements DataSource. The default fixture carries one split
// cluster; NewFakeDataSourceFlatKnowledge keeps the no-parent regression path.
// Both variants are deterministic and byte-stable across calls.
func (f *FakeDataSource) Knowledge(ctx context.Context) (Knowledge, error) {
	if f.flatKnowledgeFixture {
		return fakeKnowledge(), nil
	}
	return fakeKnowledgeWithSubclusters(), nil
}

// fakePipelineRuns builds the fixed set of pipeline run details (gitmoot #681)
// behind the Pipelines section, keyed by run id. Every timestamp is anchored on
// fakeChartsNow (never time.Now()) so the section is byte-stable across polls.
// The five runs together exercise every real-emittable shape: a healthy linear
// run that fully succeeded (prun-nightly-deploy-0001); an in-flight run with a
// done stage, a running stage and a pending stage (…-0002); a parked-blocked
// diamond carrying persisted needs at both the stage and the run level, with a
// downstream stage skipped (prun-listing-refresh-0001); a failed run with a
// retried stage and a skipped report (prun-bench-suite-0001); and an older
// failed run for run-history/sparkline variety (…-0000). Stages are emitted in
// spec (topological) order — deliberately NOT alphabetical for the diamond — so
// the client renders the DAG the same way the CLI funnel prints it. Some cmds
// and summaries carry angle brackets and ampersands so the client's
// HTML-escaping is exercised.
func fakePipelineRuns() map[string]PipelineRun {
	const (
		h = time.Hour
		m = time.Minute
	)
	// ago returns the epoch-ms d before fakeChartsNow (never time.Now()).
	ago := func(d time.Duration) int64 { return fakeChartsNow.Add(-d).UnixMilli() }

	return map[string]PipelineRun{
		// Healthy linear scheduled run: source -> build -> deploy, all succeeded.
		// The most recent nightly-deploy run (StartedAt 6h ago), so it is the
		// pipeline's LastRun.
		"prun-nightly-deploy-0001": {
			ID:         "prun-nightly-deploy-0001",
			Pipeline:   "nightly-deploy",
			Repo:       "acme/webapp",
			Trigger:    "schedule",
			State:      "succeeded",
			SpecHash:   "sha256:9c1f0ade",
			StartedAt:  ago(6 * h),
			FinishedAt: ago(6*h - 13*m),
			Stages: []PipelineStage{
				{ID: "source", State: "succeeded", Cmd: "git fetch --all && git checkout main", JobID: "job-nd1-source", Summary: "checked out main @ a1b2c3d", StartedAt: ago(6 * h), FinishedAt: ago(6*h - 3*m)},
				{ID: "build", State: "succeeded", Deps: []string{"source"}, Cmd: "make build", JobID: "job-nd1-build", Summary: "built 42 packages, 0 warnings", StartedAt: ago(6*h - 3*m), FinishedAt: ago(6*h - 10*m)},
				{ID: "deploy", State: "succeeded", Deps: []string{"build"}, Cmd: "./scripts/deploy.sh --env prod", JobID: "job-nd1-deploy", Summary: "deployed to prod, health green", StartedAt: ago(6*h - 10*m), FinishedAt: ago(6*h - 13*m)},
			},
		},
		// In-flight run: source succeeded, build running, deploy pending (no job
		// assigned yet). A static snapshot of a previous hung cycle (StartedAt 30h
		// ago), so poll-stable byte-equality still holds.
		"prun-nightly-deploy-0002": {
			ID:        "prun-nightly-deploy-0002",
			Pipeline:  "nightly-deploy",
			Repo:      "acme/webapp",
			Trigger:   "schedule",
			State:     "running",
			SpecHash:  "sha256:9c1f0ade",
			StartedAt: ago(30 * h),
			Stages: []PipelineStage{
				{ID: "source", State: "succeeded", Cmd: "git fetch --all && git checkout main", JobID: "job-nd2-source", Summary: "checked out main @ b2c3d4e", StartedAt: ago(30 * h), FinishedAt: ago(30*h - 4*m)},
				{ID: "build", State: "running", Deps: []string{"source"}, Cmd: "make build", JobID: "job-nd2-build", Summary: "compiling packages", StartedAt: ago(30*h - 4*m)},
				{ID: "deploy", State: "pending", Deps: []string{"build"}, Cmd: "./scripts/deploy.sh --env prod"},
			},
		},
		// Older failed run for run-history/sparkline variety (StartedAt 3 days ago):
		// source + build succeeded, deploy failed.
		"prun-nightly-deploy-0000": {
			ID:         "prun-nightly-deploy-0000",
			Pipeline:   "nightly-deploy",
			Repo:       "acme/webapp",
			Trigger:    "schedule",
			State:      "failed",
			SpecHash:   "sha256:9c1f0ade",
			HaltStage:  "deploy",
			HaltReason: "prod healthcheck failed after deploy",
			StartedAt:  ago(3 * 24 * h),
			FinishedAt: ago(3*24*h - 9*m),
			Stages: []PipelineStage{
				{ID: "source", State: "succeeded", Cmd: "git fetch --all && git checkout main", JobID: "job-nd0-source", Summary: "checked out main @ 0f1e2d3", StartedAt: ago(3 * 24 * h), FinishedAt: ago(3*24*h - 2*m)},
				{ID: "build", State: "succeeded", Deps: []string{"source"}, Cmd: "make build", JobID: "job-nd0-build", Summary: "built 41 packages", StartedAt: ago(3*24*h - 2*m), FinishedAt: ago(3*24*h - 7*m)},
				{ID: "deploy", State: "failed", Deps: []string{"build"}, Cmd: "./scripts/deploy.sh --env prod", JobID: "job-nd0-deploy", Summary: "deploy script exited 1: prod healthcheck failed", StartedAt: ago(3*24*h - 7*m), FinishedAt: ago(3*24*h - 9*m)},
			},
		},
		// Parked-blocked diamond: fetch -> {score, dedupe} -> publish. score is
		// BLOCKED with persisted needs (also aggregated at the run level), dedupe
		// succeeded, and the downstream publish is SKIPPED. Stage order is spec
		// order (fetch, score, dedupe, publish), NOT alphabetical, so the client
		// lays out the DAG correctly.
		"prun-listing-refresh-0001": {
			ID:         "prun-listing-refresh-0001",
			Pipeline:   "listing-refresh",
			Repo:       "jerryfane/noted",
			Trigger:    "manual",
			State:      "blocked",
			SpecHash:   "sha256:41ab77e0",
			HaltStage:  "score",
			HaltReason: "scoring model needs the R2 token",
			Needs:      []string{"set R2 token: gitmoot config set r2.token"},
			StartedAt:  ago(2 * h),
			Stages: []PipelineStage{
				{ID: "fetch", State: "succeeded", Cmd: "gitmoot listings fetch --source noted", JobID: "job-lr1-fetch", Summary: "fetched 128 listings", StartedAt: ago(2 * h), FinishedAt: ago(2*h - 3*m)},
				{ID: "score", State: "blocked", Deps: []string{"fetch"}, Cmd: "gitmoot listings score --model r2", JobID: "job-lr1-score", Retry: 2, Needs: []string{"set R2 token: gitmoot config set r2.token"}, Summary: "blocked: scoring needs the R2 token & <credentials> before it can run", StartedAt: ago(2*h - 3*m)},
				{ID: "dedupe", State: "succeeded", Deps: []string{"fetch"}, Cmd: "gitmoot listings dedupe", JobID: "job-lr1-dedupe", Summary: "removed 9 duplicates", StartedAt: ago(2*h - 3*m), FinishedAt: ago(2*h - 6*m)},
				{ID: "publish", State: "skipped", Deps: []string{"score", "dedupe"}, Cmd: "gitmoot listings publish", Summary: "skipped: upstream stage score is blocked"},
			},
		},
		// Failed run with a retried stage: setup succeeded, bench FAILED on attempt
		// 2, report SKIPPED. The bench cmd and summary carry angle brackets and
		// ampersands to exercise the client's HTML-escaping.
		"prun-bench-suite-0001": {
			ID:         "prun-bench-suite-0001",
			Pipeline:   "bench-suite",
			Repo:       "acme/api",
			Trigger:    "manual",
			State:      "failed",
			SpecHash:   "sha256:7d3c9b12",
			HaltStage:  "bench",
			HaltReason: "benchmark stage exceeded the 30m timeout on attempt 2",
			StartedAt:  ago(26 * h),
			FinishedAt: ago(26*h - 34*m),
			Stages: []PipelineStage{
				{ID: "setup", State: "succeeded", Cmd: "make bench-setup", JobID: "job-bs1-setup", Summary: "warmed caches, seeded 10k rows", StartedAt: ago(26 * h), FinishedAt: ago(26*h - 4*m)},
				{ID: "bench", State: "failed", Deps: []string{"setup"}, Cmd: `./scripts/bench.sh --filter "p<95> && q>1"`, JobID: "job-bs1-bench", Attempt: 2, Retry: 2, Summary: "benchmark timeout after 2 retries (filter p<95> && q>1)", StartedAt: ago(26*h - 4*m), FinishedAt: ago(26*h - 34*m)},
				{ID: "report", State: "skipped", Deps: []string{"bench"}, Cmd: "./scripts/report.sh", Summary: "skipped: upstream stage bench failed"},
			},
		},
	}
}

// fakePipelineRunSummary projects a full PipelineRun detail down to the
// lightweight listing entry shown in a pipeline's Recent strip. Duration is the
// finished-started span in ms when both are set, else 0 (a still-running or
// parked run reports 0).
func fakePipelineRunSummary(run PipelineRun) PipelineRunSummary {
	var duration int64
	if run.StartedAt > 0 && run.FinishedAt > run.StartedAt {
		duration = run.FinishedAt - run.StartedAt
	}
	return PipelineRunSummary{
		ID:         run.ID,
		Trigger:    run.Trigger,
		State:      run.State,
		HaltStage:  run.HaltStage,
		StartedAt:  run.StartedAt,
		FinishedAt: run.FinishedAt,
		Duration:   duration,
	}
}

// fakePipelines builds the fixed Pipelines-list fixture (gitmoot #681): three
// declared pipelines that together exercise the UI — a healthy enabled scheduled
// pipeline with a next-due time and a linear last run (nightly-deploy); an
// enabled manual pipeline whose last run is parked-blocked (listing-refresh);
// and a disabled scheduled pipeline whose last run failed (bench-suite). Every
// timestamp is anchored on fakeChartsNow (never time.Now()); the pipeline list
// is sorted by name and each Recent strip is sorted newest-first (StartedAt
// desc, then ID desc — matching the store's ORDER BY started_at DESC, id DESC)
// and coerced non-nil, so the whole view is byte-stable across polls.
func fakePipelines() []PipelineSummary {
	// fut returns the epoch-ms d after fakeChartsNow (a future next-due time).
	fut := func(d time.Duration) int64 { return fakeChartsNow.Add(d).UnixMilli() }
	runs := fakePipelineRuns()

	// runsFor projects the named runs into Recent entries, newest-first (StartedAt
	// desc, ID desc), capped at 10 and never nil.
	runsFor := func(ids ...string) []PipelineRunSummary {
		out := make([]PipelineRunSummary, 0, len(ids))
		for _, id := range ids {
			out = append(out, fakePipelineRunSummary(runs[id]))
		}
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].StartedAt != out[j].StartedAt {
				return out[i].StartedAt > out[j].StartedAt // newest first
			}
			return out[i].ID > out[j].ID // ID desc tie-break (unique)
		})
		if len(out) > 10 {
			out = out[:10]
		}
		return out
	}

	pipelines := []PipelineSummary{
		{
			Name:       "nightly-deploy",
			Repo:       "acme/webapp",
			Enabled:    true,
			Interval:   "24h",
			Jitter:     "15m",
			StageCount: 3,
			LastRunID:  "prun-nightly-deploy-0001",
			LastStatus: "succeeded",
			LastRunAt:  runs["prun-nightly-deploy-0001"].StartedAt,
			NextDueAt:  fut(7 * time.Hour),
			Recent:     runsFor("prun-nightly-deploy-0002", "prun-nightly-deploy-0001", "prun-nightly-deploy-0000"),
		},
		{
			Name:       "listing-refresh",
			Repo:       "jerryfane/noted",
			Enabled:    true,
			StageCount: 4,
			LastRunID:  "prun-listing-refresh-0001",
			LastStatus: "blocked",
			LastRunAt:  runs["prun-listing-refresh-0001"].StartedAt,
			Recent:     runsFor("prun-listing-refresh-0001"),
		},
		{
			Name:       "bench-suite",
			Repo:       "acme/api",
			Enabled:    false,
			Interval:   "168h",
			StageCount: 3,
			LastRunID:  "prun-bench-suite-0001",
			LastStatus: "failed",
			LastRunAt:  runs["prun-bench-suite-0001"].StartedAt,
			Recent:     runsFor("prun-bench-suite-0001"),
		},
	}

	// Sorted by name (deterministic; the UI polls with a signature-skip). Names
	// are unique, so the sort is fully determined.
	sort.SliceStable(pipelines, func(i, j int) bool {
		return pipelines[i].Name < pipelines[j].Name
	})
	return pipelines
}

// Pipelines implements DataSource. It returns the fixed fakePipelines fixture;
// output is deterministic and byte-stable across calls.
func (f *FakeDataSource) Pipelines(ctx context.Context) ([]PipelineSummary, error) {
	return fakePipelines(), nil
}

// PipelineRun implements DataSource. It returns the fixed detail for a run by id
// from the fakePipelineRuns fixture; unknown ids return ErrPipelineRunNotFound.
// Output is deterministic and byte-stable across calls.
func (f *FakeDataSource) PipelineRun(ctx context.Context, id string) (PipelineRun, error) {
	run, ok := fakePipelineRuns()[id]
	if !ok {
		return PipelineRun{}, ErrPipelineRunNotFound
	}
	return run, nil
}

// fakePipelineHistoryEntry projects a full PipelineRun detail into a run-history
// entry (gitmoot #708): the run summary plus its per-stage marks in the run's
// (spec) stage order. Duration is the finished-started span in ms when both are
// set, else 0 (a still-running or parked run reports 0).
func fakePipelineHistoryEntry(run PipelineRun) PipelineRunHistoryEntry {
	var duration int64
	if run.StartedAt > 0 && run.FinishedAt > run.StartedAt {
		duration = run.FinishedAt - run.StartedAt
	}
	marks := make([]PipelineStageMark, 0, len(run.Stages))
	for _, st := range run.Stages {
		marks = append(marks, PipelineStageMark{ID: st.ID, State: st.State})
	}
	return PipelineRunHistoryEntry{
		ID:         run.ID,
		Trigger:    run.Trigger,
		State:      run.State,
		HaltStage:  run.HaltStage,
		StartedAt:  run.StartedAt,
		FinishedAt: run.FinishedAt,
		Duration:   duration,
		Stages:     marks,
	}
}

// fakePipelineDetail builds the fixed click-through detail (gitmoot #708) for one
// pipeline: its currently declared stage DAG (every stage pending, carrying the
// spec's deps/cmd/retry) plus the run history newest-first. Every timestamp is
// anchored on fakeChartsNow (never time.Now()) so the section is byte-stable
// across polls; each history list is sorted newest-first (StartedAt desc, then
// ID desc — a unique tie-break) and every run's marks are emitted in spec order.
// The three declared pipelines exercise the history matrix:
//   - nightly-deploy: an ~8-run, 8-day history that is mostly succeeded with one
//     failed run mid-history and the in-flight run (…-0002) present — enough for
//     a meaningful stage×run matrix and success-rate math.
//   - listing-refresh: a ~6-run history whose score stage is FLAKY (blocked in
//     several runs, failed in one, succeeded in others — the "which stage keeps
//     failing" demo); the newest run is the parked-blocked …-0001.
//   - bench-suite: a single failed run.
//
// Unknown names return (PipelineDetail{}, false).
func fakePipelineDetail(name string) (PipelineDetail, bool) {
	const (
		m = time.Minute
		d = 24 * time.Hour
	)
	ago := func(x time.Duration) int64 { return fakeChartsNow.Add(-x).UnixMilli() }
	runs := fakePipelineRuns()

	// entry derives a history entry from an existing full run detail (keeps the
	// history rows byte-consistent with the /api/pipeline/run/{id} endpoint).
	entry := func(id string) PipelineRunHistoryEntry { return fakePipelineHistoryEntry(runs[id]) }

	// sm is a per-stage mark; mk builds a synthetic history entry with a computed
	// duration (finished-started, 0 when unfinished/parked).
	sm := func(id, state string) PipelineStageMark { return PipelineStageMark{ID: id, State: state} }
	mk := func(id, trigger, state, halt string, started, finished int64, marks ...PipelineStageMark) PipelineRunHistoryEntry {
		var duration int64
		if started > 0 && finished > started {
			duration = finished - started
		}
		return PipelineRunHistoryEntry{
			ID:         id,
			Trigger:    trigger,
			State:      state,
			HaltStage:  halt,
			StartedAt:  started,
			FinishedAt: finished,
			Duration:   duration,
			Stages:     marks,
		}
	}

	// finalize sorts a run history newest-first (StartedAt desc, ID desc) and caps
	// it at 100 so the view is fully deterministic.
	finalize := func(pname string, declared []PipelineStage, history []PipelineRunHistoryEntry) PipelineDetail {
		sort.SliceStable(history, func(i, j int) bool {
			if history[i].StartedAt != history[j].StartedAt {
				return history[i].StartedAt > history[j].StartedAt // newest first
			}
			return history[i].ID > history[j].ID // ID desc tie-break (unique)
		})
		if len(history) > 100 {
			history = history[:100]
		}
		return PipelineDetail{Name: pname, Declared: declared, Runs: history}
	}

	switch name {
	case "nightly-deploy":
		declared := []PipelineStage{
			{ID: "source", State: "pending", Cmd: "git fetch --all && git checkout main"},
			{ID: "build", State: "pending", Deps: []string{"source"}, Cmd: "make build"},
			{ID: "deploy", State: "pending", Deps: []string{"build"}, Cmd: "./scripts/deploy.sh --env prod"},
		}
		ok := []PipelineStageMark{sm("source", "succeeded"), sm("build", "succeeded"), sm("deploy", "succeeded")}
		history := []PipelineRunHistoryEntry{
			entry("prun-nightly-deploy-0001"), // succeeded, 6h ago (newest)
			entry("prun-nightly-deploy-0002"), // running (in-flight), 30h ago
			entry("prun-nightly-deploy-0000"), // failed, 3d ago (mid-history)
			mk("prun-nightly-deploy-0100", "schedule", "succeeded", "", ago(2*d), ago(2*d-12*m), ok...),
			mk("prun-nightly-deploy-0101", "schedule", "succeeded", "", ago(4*d), ago(4*d-11*m), ok...),
			mk("prun-nightly-deploy-0102", "schedule", "succeeded", "", ago(5*d), ago(5*d-14*m), ok...),
			mk("prun-nightly-deploy-0103", "schedule", "succeeded", "", ago(6*d), ago(6*d-12*m), ok...),
			mk("prun-nightly-deploy-0104", "schedule", "succeeded", "", ago(8*d), ago(8*d-13*m), ok...),
		}
		return finalize("nightly-deploy", declared, history), true

	case "listing-refresh":
		declared := []PipelineStage{
			{ID: "fetch", State: "pending", Cmd: "gitmoot listings fetch --source noted"},
			{ID: "score", State: "pending", Deps: []string{"fetch"}, Cmd: "gitmoot listings score --model r2", Retry: 2},
			{ID: "dedupe", State: "pending", Deps: []string{"fetch"}, Cmd: "gitmoot listings dedupe"},
			{ID: "publish", State: "pending", Deps: []string{"score", "dedupe"}, Cmd: "gitmoot listings publish"},
		}
		// okRun returns a fresh all-succeeded diamond in spec order.
		okRun := func() []PipelineStageMark {
			return []PipelineStageMark{sm("fetch", "succeeded"), sm("score", "succeeded"), sm("dedupe", "succeeded"), sm("publish", "succeeded")}
		}
		// Deliberately declared OUT of chronological order so finalize()'s
		// newest-first sort is load-bearing (a regression that drops the sort
		// fails TestHandlePipelineDetail's descending assertion).
		history := []PipelineRunHistoryEntry{
			mk("prun-listing-refresh-0103", "schedule", "succeeded", "", ago(3*d), ago(3*d-9*m), okRun()...),
			mk("prun-listing-refresh-0105", "manual", "succeeded", "", ago(5*d), ago(5*d-7*m), okRun()...),
			entry("prun-listing-refresh-0001"), // blocked on score, 2h ago (newest)
			mk("prun-listing-refresh-0102", "manual", "failed", "score", ago(2*d), ago(2*d-5*m),
				sm("fetch", "succeeded"), sm("score", "failed"), sm("dedupe", "succeeded"), sm("publish", "skipped")),
			mk("prun-listing-refresh-0104", "schedule", "blocked", "score", ago(4*d), 0,
				sm("fetch", "succeeded"), sm("score", "blocked"), sm("dedupe", "succeeded"), sm("publish", "skipped")),
			mk("prun-listing-refresh-0101", "schedule", "succeeded", "", ago(1*d), ago(1*d-8*m), okRun()...),
		}
		return finalize("listing-refresh", declared, history), true

	case "bench-suite":
		declared := []PipelineStage{
			{ID: "setup", State: "pending", Cmd: "make bench-setup"},
			{ID: "bench", State: "pending", Deps: []string{"setup"}, Cmd: `./scripts/bench.sh --filter "p<95> && q>1"`, Retry: 2},
			{ID: "report", State: "pending", Deps: []string{"bench"}, Cmd: "./scripts/report.sh"},
		}
		history := []PipelineRunHistoryEntry{
			entry("prun-bench-suite-0001"), // the single failed run
		}
		return finalize("bench-suite", declared, history), true
	}

	return PipelineDetail{}, false
}

// PipelineDetail implements DataSource. It returns the fixed detail for a
// pipeline by name from the fakePipelineDetail fixture; unknown names return
// ErrPipelineNotFound. Output is deterministic and byte-stable across calls.
func (f *FakeDataSource) PipelineDetail(ctx context.Context, name string) (PipelineDetail, error) {
	detail, ok := fakePipelineDetail(name)
	if !ok {
		return PipelineDetail{}, ErrPipelineNotFound
	}
	return detail, nil
}

// fakeChatThreadDetails builds the fixed set of chat threads (gitmoot #534),
// keyed by thread id. Every timestamp is anchored on fakeChartsNow (never
// time.Now()) so the section is byte-stable across polls. The four threads
// together exercise every real-emittable shape:
//   - chat-release-room: a busy multi-agent thread with a promotion_request that
//     spawned a job (promotedJobId) and the agent's job_result posted back, plus
//     refs to a job and a PR;
//   - chat-adapter-review: an ask-gate flow — an agent message, a `system`
//     ask-gate question from a paused job, the human's answer (reply_to), and the
//     resumed agent's job_result;
//   - chat-triage-inbox: a fresh thread with pending @mentions (unread) and no
//     replies yet;
//   - chat-sqlite-migration: an archived, wrapped-up thread.
//
// Some bodies carry angle brackets, ampersands and a fake <script> so the
// client's HTML-escaping of the UNTRUSTED body is exercised. Messages are
// emitted ascending by Seq.
func fakeChatThreadDetails() map[string]*ChatThreadDetail {
	const (
		h = time.Hour
		m = time.Minute
		d = 24 * time.Hour
	)
	// at returns the epoch-ms d before fakeChartsNow (never time.Now()).
	at := func(x time.Duration) int64 { return fakeChartsNow.Add(-x).UnixMilli() }

	details := []*ChatThreadDetail{
		{
			ChatThreadSummary: ChatThreadSummary{
				ID: "chat-release-room", Slug: "release-room", Name: "Release room",
				Repo: "jerryfane/gitmoot", State: "open", CreatedBy: "jerry",
				UnreadMentions: 0, Participants: []string{"codex-b", "jerry", "researcher"},
			},
			Messages: []ChatMessage{
				{ID: "msg-rr-1", Seq: 1, TsMs: at(3 * h), AuthorKind: "human", AuthorName: "jerry", Kind: "chat",
					Body: "@codex-b can you inspect the runtime adapter seam? @researcher can you compare the A2A & ACP protocol options (latency vs schema flexibility)?"},
				{ID: "msg-rr-2", Seq: 2, TsMs: at(3*h - 22*m), AuthorKind: "agent", AuthorName: "researcher", Kind: "chat",
					Body: "Compared 3 options. Trade-off: ANP negotiates schema at runtime (flexible but adds latency & cost) whereas a fixed schema is O(1). For V1 I'd pick a fixed message schema (text + optional refs) — see <taxonomy §4.2>. Recommendation: don't do dynamic negotiation yet.",
					Refs: []ChatRef{{Kind: "artifact", Repo: "jerryfane/gitmoot", ID: "note-2606.19135", URL: "https://arxiv.org/abs/2606.19135"}}},
				{ID: "msg-rr-3", Seq: 3, TsMs: at(3*h - 40*m), AuthorKind: "human", AuthorName: "jerry", Kind: "promotion_request",
					Body:          "/implement @codex-b implement the adapter manifest (fixed schema, text + refs). Promote this to a real job.",
					PromotedJobID: "job-adapter-01", Refs: []ChatRef{{Kind: "job", Repo: "jerryfane/gitmoot", ID: "job-adapter-01"}}},
				{ID: "msg-rr-4", Seq: 4, TsMs: at(3*h - 74*m), AuthorKind: "agent", AuthorName: "codex-b", Kind: "job_result", ReplyTo: "msg-rr-3",
					Body: "> job-adapter-01 · **implemented**\n\n**Decision:** implemented\n**Summary:** added the adapter manifest (`manifest.go` + a schema test). Fixed schema `{kind, body, refs[]}` — no runtime negotiation.\n\n**Changed:**\n- internal/runtime/manifest.go\n- internal/runtime/manifest_test.go\n\n**Verify:**\n```\ngo test ./internal/runtime/ -> ok (0.42s)\n```",
					Refs: []ChatRef{{Kind: "job", Repo: "jerryfane/gitmoot", ID: "job-adapter-01"}, {Kind: "pr", Repo: "jerryfane/gitmoot", ID: "742", URL: "https://github.com/jerryfane/gitmoot/pull/742"}}},
				{ID: "msg-rr-5", Seq: 5, TsMs: at(40 * m), AuthorKind: "agent", AuthorName: "codex-b", Kind: "chat",
					Body: "Opened PR #742 with the manifest. @jerry ready for review — CI (build & vet & test) is green."},
				// XSS/inertness fixture: an untrusted body carrying a literal <script>,
				// an <img onerror> and a javascript: URL. The SAFE markdown renderer
				// must show these as inert text (escaped, links NOT clickable) while
				// still formatting the **bold** / `code` / > quote around them.
				{ID: "msg-rr-6", Seq: 6, TsMs: at(30 * m), AuthorKind: "human", AuthorName: "jerry", Kind: "chat",
					Body: "Sanity-checking the renderer with a hostile body:\n\n**bold <script>alert(1)</script>** and inline `<img src=x onerror=alert(1)>`.\n\n> quoted <b>not-bold</b> & a bare link javascript:alert(document.cookie) stays plain text.\n\n```\n<script>alert('fenced too')</script>\n```"},
			},
		},
		{
			ChatThreadSummary: ChatThreadSummary{
				ID: "chat-adapter-review", Slug: "adapter-review", Name: "Adapter review",
				Repo: "jerryfane/gitmoot", State: "open", CreatedBy: "jerry",
				UnreadMentions: 0, Participants: []string{"jerry", "reviewer"},
			},
			Messages: []ChatMessage{
				{ID: "msg-ar-1", Seq: 1, TsMs: at(2*h + 30*m), AuthorKind: "agent", AuthorName: "reviewer", Kind: "chat",
					Body: "Starting an adversarial review of the adapter manifest PR (#742)."},
				{ID: "msg-ar-2", Seq: 2, TsMs: at(2*h + 8*m), AuthorKind: "system", AuthorName: "", Kind: "system",
					Body: "Job job-adapter-review-07 is paused awaiting an answer:\n\nThe manifest omits a network_access flag. Should ephemeral codex workers be granted network access by default? (yes/no)",
					Refs: []ChatRef{{Kind: "job", Repo: "jerryfane/gitmoot", ID: "job-adapter-review-07"}}},
				{ID: "msg-ar-3", Seq: 3, TsMs: at(2 * h), AuthorKind: "human", AuthorName: "jerry", Kind: "chat", ReplyTo: "msg-ar-2",
					Body: "yes — codex ephemeral workers need [sandbox_workspace_write] network_access=true to push branches & open PRs (default sandbox blocks network -> gh \"auth invalid\")."},
				{ID: "msg-ar-4", Seq: 4, TsMs: at(2*h - 12*m), AuthorKind: "agent", AuthorName: "reviewer", Kind: "job_result", ReplyTo: "msg-ar-3",
					Body: "**Decision:** approved\n**Summary:** resumed after the ask-gate answer. Approved with a note that `network_access=true` is required for the ephemeral worker path. No blocking findings.\n**Verify:** re-ran the manifest schema test -> ok",
					Refs: []ChatRef{{Kind: "job", Repo: "jerryfane/gitmoot", ID: "job-adapter-review-07"}, {Kind: "pr", Repo: "jerryfane/gitmoot", ID: "742", URL: "https://github.com/jerryfane/gitmoot/pull/742"}}},
			},
		},
		{
			ChatThreadSummary: ChatThreadSummary{
				ID: "chat-triage-inbox", Slug: "triage-inbox", Name: "Triage inbox",
				Repo: "jerryfane/noted", State: "open", CreatedBy: "gaijinjoe",
				UnreadMentions: 2, Participants: []string{"claude-a", "gaijinjoe", "researcher"},
			},
			Messages: []ChatMessage{
				{ID: "msg-ti-1", Seq: 1, TsMs: at(6 * m), AuthorKind: "human", AuthorName: "gaijinjoe", Kind: "chat",
					Body: "@claude-a the nightly-deploy pipeline is parked (score stage blocked on the R2 token). Can you look? @researcher any known R2 token rotation issues this week? <urgent> but no auto-run — I'll promote if needed."},
			},
		},
		{
			ChatThreadSummary: ChatThreadSummary{
				ID: "chat-sqlite-migration", Slug: "sqlite-migration", Name: "SQLite migration",
				Repo: "acme/webapp", State: "archived", CreatedBy: "jerry",
				UnreadMentions: 0, Participants: []string{"codex-b", "jerry"},
			},
			Messages: []ChatMessage{
				{ID: "msg-sm-1", Seq: 1, TsMs: at(6 * d), AuthorKind: "human", AuthorName: "jerry", Kind: "chat",
					Body: "Let's coordinate the modernc pure-Go SQLite migration here (no cgo, single static binary stays sacred)."},
				{ID: "msg-sm-2", Seq: 2, TsMs: at(6*d - 3*h), AuthorKind: "agent", AuthorName: "codex-b", Kind: "chat",
					Body: "Done — swapped mattn/go-sqlite3 -> modernc.org/sqlite. All migrations pass, `CGO_ENABLED=0 go build` is clean, binary is fully static."},
				{ID: "msg-sm-3", Seq: 3, TsMs: at(6*d - 4*h), AuthorKind: "human", AuthorName: "jerry", Kind: "chat",
					Body: "Perfect. Archiving this thread, thanks."},
			},
		},
	}

	out := make(map[string]*ChatThreadDetail, len(details))
	for _, det := range details {
		// Derive the summary rollup from the message history so the list and the
		// detail can never disagree.
		det.MessageCount = len(det.Messages)
		if n := len(det.Messages); n > 0 {
			last := det.Messages[n-1]
			det.UpdatedAt = last.TsMs
			det.LastAuthor = last.AuthorName
			if last.AuthorKind == "system" {
				det.LastAuthor = "system"
			}
			det.LastKind = last.Kind
			det.LastSnippet = chatSnippet(last.Body)
		}
		sort.Strings(det.Participants)
		out[det.ID] = det
	}
	return out
}

// chatSnippet collapses a message body to a single-line, server-truncated
// preview (matching what the live store would send so the client never has to
// re-truncate). Newlines become spaces and the result is capped at 90 runes.
func chatSnippet(body string) string {
	s := strings.Join(strings.Fields(body), " ")
	const cap = 90
	r := []rune(s)
	if len(r) > cap {
		return strings.TrimRight(string(r[:cap]), " ") + "…"
	}
	return s
}

// fakeChatThreads projects the fixed thread details into the list summaries,
// sorted most-recently-active first (UpdatedAt desc, id desc tie-break) so the
// view is byte-stable across polls.
func fakeChatThreads() []ChatThreadSummary {
	details := fakeChatThreadDetails()
	out := make([]ChatThreadSummary, 0, len(details))
	for _, det := range details {
		s := det.ChatThreadSummary
		// copy the participants slice so callers can't mutate the fixture
		s.Participants = append([]string(nil), det.Participants...)
		out = append(out, s)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt // most recent first
		}
		return out[i].ID > out[j].ID // id desc tie-break (unique)
	})
	return out
}

// ChatThreads implements DataSource. It returns the fixed fakeChatThreads
// fixture; output is deterministic and byte-stable across calls.
func (f *FakeDataSource) ChatThreads(ctx context.Context) ([]ChatThreadSummary, error) {
	return fakeChatThreads(), nil
}

// ChatThread implements DataSource. It returns the fixed detail for a thread by
// id from the fakeChatThreadDetails fixture; unknown ids return
// (nil, ErrChatThreadNotFound). Output is deterministic and byte-stable.
func (f *FakeDataSource) ChatThread(ctx context.Context, id string) (*ChatThreadDetail, error) {
	det, ok := fakeChatThreadDetails()[id]
	if !ok {
		return nil, ErrChatThreadNotFound
	}
	// copy so callers can't mutate the fixture's shared slices
	cp := *det
	cp.Participants = append([]string(nil), det.Participants...)
	cp.Messages = append([]ChatMessage(nil), det.Messages...)
	return &cp, nil
}

// fakeVerdictRun is the SkillOpt eval run id the fake binary-verdicts fixture is
// keyed on (matches the pending candidate skill-reviewer-v3's evaluation).
const fakeVerdictRun = "eval-reviewer-0007"

// fakeJobChecks builds the fixed job-detail failed-check fixture (gitmoot #711),
// keyed by job id. job-5 (the integrate/review/open-PR job) failed two
// deterministic result checks under a block policy; every other job passed
// (empty Failed) under the default warn policy. Explanations carry angle
// brackets/ampersands so the client's escaping of untrusted text is exercised.
func fakeJobChecks() map[string]JobChecks {
	return map[string]JobChecks{
		"job-5": {
			JobID: "job-5", Mode: "block",
			Failed: []ResultCheck{
				{CheckID: "pr-opened", Question: "Did the job open a pull request and record its URL?",
					Explanation: "No PR URL was recorded on the result — the branch was pushed but `gh pr create` returned <auth invalid> (sandbox network blocked)."},
				{CheckID: "tests-run", Question: "Does the result show tests were actually run (a command + outcome)?",
					Explanation: "The summary claims \"all green\" but includes no command output; `go test ./...` was never invoked & no results block is present."},
			},
		},
		"job-6": {JobID: "job-6", Mode: "warn", Failed: []ResultCheck{}},
	}
}

// JobChecks implements DataSource. It returns the fixed fakeJobChecks fixture for
// a job id; an unknown job is not an error — it returns the default warn Mode
// with an empty Failed list. Output is deterministic and byte-stable.
func (f *FakeDataSource) JobChecks(ctx context.Context, jobID string) (JobChecks, error) {
	if jc, ok := fakeJobChecks()[jobID]; ok {
		jc.Failed = append([]ResultCheck(nil), jc.Failed...)
		return jc, nil
	}
	return JobChecks{JobID: jobID, Mode: "warn", Failed: []ResultCheck{}}, nil
}

// fakeBinaryVerdicts builds the fixed per-run binary-check breakdown (gitmoot
// #714): five decomposed questions across two dimensions, three passing and two
// failing, ordered by (dimension, questionId) — the same order the live store
// reads. Passed/Failed are derived from Verdict == "yes".
func fakeBinaryVerdicts() BinaryVerdicts {
	verdicts := []BinaryVerdict{
		{QuestionID: "q-cites-sources", Dimension: "correctness", Verdict: "yes",
			Explanation: "Every claim in the review cites a file:line.", Weight: 1},
		{QuestionID: "q-no-false-pass", Dimension: "correctness", Verdict: "no",
			Explanation: "Approved a change whose test <TestExport> is skipped, so the pass is unearned.", Weight: 2},
		{QuestionID: "q-actionable", Dimension: "usefulness", Verdict: "yes",
			Explanation: "Findings name a concrete fix & location.", Weight: 1},
		{QuestionID: "q-no-nits-as-blockers", Dimension: "usefulness", Verdict: "yes",
			Explanation: "Nits are labelled, not raised as blockers.", Weight: 1},
		{QuestionID: "q-scoped", Dimension: "usefulness", Verdict: "no",
			Explanation: "Two findings are outside the diff & should have been dropped.", Weight: 1},
	}
	out := BinaryVerdicts{RunID: fakeVerdictRun, Verdicts: verdicts}
	for i := range verdicts {
		out.Verdicts[i].Pass = verdicts[i].Verdict == "yes"
		if out.Verdicts[i].Pass {
			out.Passed++
		} else {
			out.Failed++
		}
	}
	return out
}

// BinaryVerdicts implements DataSource. It returns the fixed fakeBinaryVerdicts
// fixture for fakeVerdictRun; any other run id yields zero counts and an empty
// (never nil) list. Output is deterministic and byte-stable.
func (f *FakeDataSource) BinaryVerdicts(ctx context.Context, runID string) (BinaryVerdicts, error) {
	if runID == fakeVerdictRun {
		return fakeBinaryVerdicts(), nil
	}
	return BinaryVerdicts{RunID: runID, Verdicts: []BinaryVerdict{}}, nil
}

// fakeAttention builds the fixed "Needs a human" fixture (gitmoot #528): two
// blocked job gates, two pending synth approvals and one candidate awaiting
// promotion. Every timestamp is anchored on fakeChartsNow (never time.Now()) so
// the section is byte-stable across polls.
func fakeAttention() Attention {
	const h = time.Hour
	at := func(x time.Duration) int64 { return fakeChartsNow.Add(-x).UnixMilli() }
	att := Attention{
		Gates: []AttentionGate{
			{JobID: "job-5", Need: "human:confirm-pr-target", Title: "integrate + review + open PR",
				Agent: "integrator", Repo: "jerryfane/noted", State: "blocked", PR: 42, CreatedAt: at(2 * h)},
			{JobID: "job-nightly-score-19", Need: "secret:r2-token", Title: "nightly-deploy · score stage",
				Agent: "claude-a", Repo: "jerryfane/noted", State: "blocked", CreatedAt: at(90 * time.Minute)},
		},
		SynthItems: []AttentionSynthItem{
			{ID: "synth-reviewer-0031", TemplateID: "tmpl-reviewer", Repo: "jerryfane/gitmoot",
				Question: "Should a review flag an unearned pass when the asserting test is skipped?",
				Gap:      0.29, WeakAgent: "reviewer@v2", StrongAgent: "reviewer@v3", JudgeAgent: "cross-family-judge", CreatedAt: at(4 * h)},
			{ID: "synth-reviewer-0030", TemplateID: "tmpl-reviewer", Repo: "jerryfane/gitmoot",
				Question: "Does the review keep findings scoped to the diff under test?",
				Gap:      0.18, WeakAgent: "reviewer@v2", StrongAgent: "reviewer@v3", JudgeAgent: "cross-family-judge", CreatedAt: at(5 * h)},
		},
		Candidates: []AttentionCandidate{
			{TemplateID: "tmpl-reviewer", Name: "reviewer", VersionID: "skill-reviewer-v3", Number: 3, Score: "0.81", CreatedAt: at(6 * h)},
		},
	}
	att.Total = len(att.Gates) + len(att.SynthItems) + len(att.Candidates)
	return att
}

// Attention implements DataSource. It returns the fixed fakeAttention fixture;
// output is deterministic and byte-stable across calls.
func (f *FakeDataSource) Attention(ctx context.Context) (Attention, error) {
	return fakeAttention(), nil
}
