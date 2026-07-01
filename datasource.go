// Package dashboard provides the read-only backend for the gitmoot web
// dashboard: a small HTTP server that serves a live orchestration-DAG UI and
// reads live state from gitmoot's store through the DataSource interface.
package dashboard

import "context"

// NodeState is the lifecycle state of a single orchestration node.
type NodeState string // "queued" "running" "succeeded" "failed" "blocked" "cancelled"

// Event is a label attached to a node's timeline.
//
// Wall-clock timestamps in this package (Node.StartedAt/EndedAt,
// RunSummary.Updated) are epoch milliseconds (JS Date compatible). Event.T is a
// monotonic ordering key for a node's timeline: epoch milliseconds where the
// feed has per-event times, or a 1-based index otherwise. Clients should sort a
// node's events by T rather than treat it as wall-clock.
type Event struct {
	T     int64  `json:"t"` // monotonic ordering key (see type doc)
	Label string `json:"label"`
}

// Node is a single job/agent in the orchestration graph. Edges are derived
// client-side from ParentID + Deps.
type Node struct {
	ID        string    `json:"id"`
	ParentID  string    `json:"parentId,omitempty"`
	Deps      []string  `json:"deps,omitempty"`
	Title     string    `json:"title"`
	Agent     string    `json:"agent"`
	Runtime   string    `json:"runtime"` // codex | claude | kimi | shell
	Model     string    `json:"model,omitempty"`
	State     NodeState `json:"state"`
	Depth     int       `json:"depth"`
	StartedAt int64     `json:"startedAt,omitempty"` // epoch milliseconds
	EndedAt   int64     `json:"endedAt,omitempty"`   // epoch milliseconds
	WorkerID  string    `json:"workerId,omitempty"`
	PRURL     string    `json:"prUrl,omitempty"`
	Prompt    string    `json:"prompt,omitempty"`
	Output    string    `json:"output,omitempty"`
	Events    []Event   `json:"events"`
}

// State is a snapshot of one orchestration run.
type State struct {
	RunID string `json:"runId"`
	Title string `json:"title"`
	Nodes []Node `json:"nodes"` // edges are derived client-side from ParentID + Deps
}

// RunSummary is a lightweight listing entry for a run. Beyond identity/state it
// carries enough shape for the Runs column to group, disambiguate, and search
// runs without fetching each run's full graph: kind/significance drive the
// Active/Orchestrations/one-shots sectioning, and the counts/snippet
// disambiguate same-titled runs.
type RunSummary struct {
	RunID string    `json:"runId"`
	Title string    `json:"title"`
	State NodeState `json:"state"`
	// Kind is the run's entrypoint: ask | review | implement | orchestrate | goal.
	Kind string `json:"kind,omitempty"`
	// Significance is "orchestration" for a multi-node delegation tree, else
	// "one-shot" for a single-node ask/review (used to fold noise in the UI).
	Significance string `json:"significance,omitempty"`
	Agent        string `json:"agent,omitempty"` // coordinator/agent name
	Repo         string `json:"repo,omitempty"`
	PR           int    `json:"pr,omitempty"`
	NodeCount    int    `json:"nodeCount"`          // jobs in the run tree
	Depth        int    `json:"depth"`              // delegation levels (1-based)
	DoneCount    int    `json:"doneCount"`          // finished (terminal) nodes
	Snippet      string `json:"snippet,omitempty"`  // first line of the root prompt
	Started      int64  `json:"started,omitempty"`  // epoch milliseconds
	Updated      int64  `json:"updated"`            // epoch milliseconds
	Duration     int64  `json:"duration,omitempty"` // milliseconds (updated-started)
}

// DataSource is the read-only feed the dashboard renders. Implementations must
// be safe for concurrent use.
type DataSource interface {
	Runs(ctx context.Context) ([]RunSummary, error)
	State(ctx context.Context, runID string) (State, error) // runID "" => active/most-recent
	Job(ctx context.Context, jobID string) (Node, error)
	Subscribe(ctx context.Context, runID string) (<-chan State, func(), error) // for SSE
}
