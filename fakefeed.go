package dashboard

import (
	"context"
	"errors"
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
	const fakeRepo = "acme/webapp"
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
