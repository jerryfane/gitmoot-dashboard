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

// JobSummary is a flattened listing entry for a single job, across every run.
// Unlike RunSummary (which rolls a whole delegation tree into one row) each
// JobSummary is one node in one run, carrying enough identity/timing/state for
// the Jobs page to list, group by Run, and search without fetching each run's
// full graph.
type JobSummary struct {
	ID      string `json:"id"`
	Title   string `json:"title"` // first line of prompt, fallback id
	Agent   string `json:"agent,omitempty"`
	Runtime string `json:"runtime,omitempty"` // codex | claude | kimi | shell
	Repo    string `json:"repo,omitempty"`
	// Kind is the job's action/type: ask | review | implement | ...
	Kind      string    `json:"kind,omitempty"`
	State     NodeState `json:"state"`
	Depth     int       `json:"depth"`         // delegation depth (0 = root)
	Run       string    `json:"run,omitempty"` // root/run id this job belongs to
	PR        int       `json:"pr,omitempty"`
	Started   int64     `json:"started,omitempty"`  // epoch ms
	Updated   int64     `json:"updated,omitempty"`  // epoch ms
	Duration  int64     `json:"duration,omitempty"` // ms
	TokensIn  int       `json:"tokensIn,omitempty"`
	TokensOut int       `json:"tokensOut,omitempty"`
}

// AgentSummary is a listing entry for the Agents page: one row per registered
// agent, plus a single synthetic rollup row for the fleet of ephemeral workers.
// The counts aggregate every job the agent has run across all runs.
type AgentSummary struct {
	Name           string   `json:"name"`
	Role           string   `json:"role,omitempty"`
	Runtime        string   `json:"runtime"`
	RepoScope      []string `json:"repoScope,omitempty"`
	Model          string   `json:"model,omitempty"`
	Capabilities   []string `json:"capabilities,omitempty"`
	AutonomyPolicy string   `json:"autonomyPolicy,omitempty"`
	Health         string   `json:"health,omitempty"`
	// Ephemeral is true only for the synthetic ephemeral-workers rollup row.
	Ephemeral      bool  `json:"ephemeral,omitempty"`
	JobCount       int   `json:"jobCount"`
	RunningCount   int   `json:"runningCount"`
	SucceededCount int   `json:"succeededCount"`
	FailedCount    int   `json:"failedCount"`
	LastActive     int64 `json:"lastActive,omitempty"` // epoch ms of most recent job update
}

// GraphNode is a node in the whole-history "galaxy" graph. Type is "job" (a real
// job, colored by State), "repo" (a repository hub) or "agent" (an agent hub);
// the hubs are synthetic grouping nodes that cluster jobs by repo/agent and give
// the force-directed graph its structure.
type GraphNode struct {
	ID    string    `json:"id"`
	Type  string    `json:"type"` // job | repo | agent
	Label string    `json:"label"`
	State NodeState `json:"state,omitempty"`
	Agent string    `json:"agent,omitempty"`
	Repo  string    `json:"repo,omitempty"`
	Run   string    `json:"run,omitempty"`
}

// GraphLink is an edge in the galaxy graph. Kind is "parent"/"dep" (delegation
// and sibling links between jobs), "repo" (job -> its repo hub) or "agent"
// (job -> its agent hub).
type GraphLink struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Kind   string `json:"kind"`
}

// Graph is the whole-history galaxy view: every job across every run, plus
// repo/agent hub nodes, unioned into one force-directed graph.
type Graph struct {
	Nodes []GraphNode `json:"nodes"`
	Links []GraphLink `json:"links"`
	Repos []string    `json:"repos"` // distinct repos, for the filter
}

// DataSource is the read-only feed the dashboard renders. Implementations must
// be safe for concurrent use.
type DataSource interface {
	Runs(ctx context.Context) ([]RunSummary, error)
	State(ctx context.Context, runID string) (State, error) // runID "" => active/most-recent
	Job(ctx context.Context, jobID string) (Node, error)
	// Jobs returns every job across all runs, flattened, sorted Updated desc.
	Jobs(ctx context.Context) ([]JobSummary, error)
	// Agents returns the registered agents plus one synthetic rollup row for the
	// fleet of ephemeral workers (the row with Ephemeral == true).
	Agents(ctx context.Context) ([]AgentSummary, error)
	// Graph returns the whole-history galaxy graph. Empty repo => all runs; a
	// non-empty repo scopes to that repository's jobs (and their hubs).
	Graph(ctx context.Context, repo string) (Graph, error)
	Subscribe(ctx context.Context, runID string) (<-chan State, func(), error) // for SSE
}
