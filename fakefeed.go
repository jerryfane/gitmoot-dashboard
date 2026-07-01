package dashboard

import (
	"context"
	"errors"
)

// FakeDataSource is an in-memory DataSource used for local development and
// tests. Task 3 enriches it into a realistic multi-node run with live updates;
// for now it exposes a single static run.
type FakeDataSource struct{}

// NewFakeDataSource returns a FakeDataSource seeded with one static run.
func NewFakeDataSource() *FakeDataSource {
	return &FakeDataSource{}
}

const fakeRunID = "run-noted-001"

func (f *FakeDataSource) state() State {
	return State{
		RunID: fakeRunID,
		Title: "noted: add note search",
		Nodes: []Node{
			{
				ID:      "job-1",
				Title:   "coordinate: add note search",
				Agent:   "project-lead",
				Runtime: "codex",
				State:   NodeState("running"),
				Depth:   0,
				Events: []Event{
					{T: 1, Label: "queued"},
					{T: 2, Label: "started"},
				},
			},
		},
	}
}

// Runs implements DataSource.
func (f *FakeDataSource) Runs(ctx context.Context) ([]RunSummary, error) {
	s := f.state()
	return []RunSummary{{
		RunID:   s.RunID,
		Title:   s.Title,
		State:   NodeState("running"),
		Updated: 2,
	}}, nil
}

// State implements DataSource. An empty runID returns the active/most-recent run.
func (f *FakeDataSource) State(ctx context.Context, runID string) (State, error) {
	if runID == "" || runID == fakeRunID {
		return f.state(), nil
	}
	return State{}, errors.New("run not found")
}

// Job implements DataSource.
func (f *FakeDataSource) Job(ctx context.Context, jobID string) (Node, error) {
	for _, n := range f.state().Nodes {
		if n.ID == jobID {
			return n, nil
		}
	}
	return Node{}, errors.New("job not found")
}

// Subscribe implements DataSource. It emits the current state once and then
// blocks until the caller cancels; Task 3 replaces this with live updates.
func (f *FakeDataSource) Subscribe(ctx context.Context, runID string) (<-chan State, func(), error) {
	ch := make(chan State, 1)
	ch <- f.state()
	cancel := func() { /* no-op for the static feed */ }
	return ch, cancel, nil
}
