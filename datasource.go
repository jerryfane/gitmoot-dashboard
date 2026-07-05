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
	// MemoryEnabled is true when the agent's [agents.<name>] config section turns
	// the memory feature on; the Agents page renders a small "memory" chip for it.
	MemoryEnabled bool `json:"memoryEnabled,omitempty"`
	// Ephemeral is true only for the synthetic ephemeral-workers rollup row.
	Ephemeral      bool  `json:"ephemeral,omitempty"`
	JobCount       int   `json:"jobCount"`
	RunningCount   int   `json:"runningCount"`
	SucceededCount int   `json:"succeededCount"`
	FailedCount    int   `json:"failedCount"`
	LastActive     int64 `json:"lastActive,omitempty"` // epoch ms of most recent job update
}

// TemplateVersionInfo is one entry in an agent template's version history.
type TemplateVersionInfo struct {
	ID             string  `json:"id"`
	Number         int     `json:"number"`
	State          string  `json:"state"` // e.g. promoted | pending | canary | rejected (pass through the store's value)
	Name           string  `json:"name,omitempty"`
	Description    string  `json:"description,omitempty"`
	SourceRef      string  `json:"sourceRef,omitempty"`
	ResolvedCommit string  `json:"resolvedCommit,omitempty"`
	CreatedAt      int64   `json:"createdAt,omitempty"`  // epoch ms, 0 unknown
	PromotedAt     int64   `json:"promotedAt,omitempty"` // epoch ms, 0 unknown
	CanarySample   float64 `json:"canarySample,omitempty"`
	Current        bool    `json:"current,omitempty"` // true for the version the template currently resolves to
	// Content is this version's full prompt text (the template body captured at
	// this version). Can be multi-KB; served only by the per-agent detail endpoint.
	Content string `json:"content,omitempty"`
}

// AgentTemplateInfo describes the template an agent is instantiated from: its
// identity and where its definition is sourced/resolved from.
type AgentTemplateInfo struct {
	ID             string `json:"id"`
	Name           string `json:"name,omitempty"`
	Description    string `json:"description,omitempty"`
	SourceRepo     string `json:"sourceRepo,omitempty"`
	SourceRef      string `json:"sourceRef,omitempty"`
	SourcePath     string `json:"sourcePath,omitempty"`
	ResolvedCommit string `json:"resolvedCommit,omitempty"`
	// Content is the agent's full prompt text (the currently resolved template
	// body). Can be multi-KB; served only by the per-agent detail endpoint.
	Content string `json:"content,omitempty"`
}

// AgentConfigInfo holds the agent's [agents.<name>] config-section values as
// resolved at config parse time (parse-time defaults included, so a field can be
// populated even when the section did not set it explicitly). These are
// configured knobs, not live runtime state: the pool knobs (MaxBackground,
// IdleTimeout, JobTimeout) only take effect for the managed instances / temp
// workers gitmoot spins up for this agent, so they do not describe a one-off
// foreground invocation.
type AgentConfigInfo struct {
	Memory        bool     `json:"memory"`
	MaxBackground int      `json:"maxBackground,omitempty"`
	IdleTimeout   string   `json:"idleTimeout,omitempty"`
	JobTimeout    string   `json:"jobTimeout,omitempty"`
	Model         string   `json:"model,omitempty"`
	Template      string   `json:"template,omitempty"`
	Capabilities  []string `json:"capabilities,omitempty"`
}

// AgentDetail is the Agents page's click-through detail: the summary plus the
// agent's template and its version history (newest first). Template is nil for
// agents with no template.
type AgentDetail struct {
	AgentSummary
	Template *AgentTemplateInfo    `json:"template,omitempty"`
	Versions []TemplateVersionInfo `json:"versions"`
	// Config is the agent's [agents.<name>] config section, or nil when the agent
	// has no such section.
	Config *AgentConfigInfo `json:"config,omitempty"`
	// MemoryFacts is the count of confirmed_memories rows owned by this agent
	// (across all owner versions).
	MemoryFacts int `json:"memoryFacts"`
	// MemoryObservations is the count of memory_observations rows owned by this agent.
	MemoryObservations int `json:"memoryObservations"`
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

// ChartDay is one UTC day bucket. Jobs bucket by their Started day; token sums
// are that day's jobs' usage. Explicit state fields keep the JSON deterministic.
type ChartDay struct {
	Date      string `json:"date"` // UTC YYYY-MM-DD
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Cancelled int    `json:"cancelled"`
	Blocked   int    `json:"blocked"`
	Queued    int    `json:"queued"`
	Running   int    `json:"running"`
	TokensIn  int    `json:"tokensIn"`
	TokensOut int    `json:"tokensOut"`
}

// ChartAgent is one agent's aggregate activity over the charted range.
type ChartAgent struct {
	Name      string `json:"name"`
	Runtime   string `json:"runtime,omitempty"`
	Jobs      int    `json:"jobs"`
	TokensOut int    `json:"tokensOut,omitempty"`
}

// ChartRepo is one repository's job count over the charted range.
type ChartRepo struct {
	Repo string `json:"repo"`
	Jobs int    `json:"jobs"`
}

// ChartTotals rolls up the whole charted range into headline figures.
type ChartTotals struct {
	Jobs         int `json:"jobs"`
	Succeeded    int `json:"succeeded"`
	Failed       int `json:"failed"`
	TokensIn     int `json:"tokensIn"`
	TokensOut    int `json:"tokensOut"`
	ActiveAgents int `json:"activeAgents"` // distinct agents with >=1 job in range
}

// Charts is the data behind the Charts page: a per-day time series plus
// top-agent/top-repo breakdowns and range totals.
type Charts struct {
	Days   []ChartDay   `json:"days"`   // oldest->newest, continuous zero-filled range
	Agents []ChartAgent `json:"agents"` // top 12 by Jobs desc, name tie-break
	Repos  []ChartRepo  `json:"repos"`  // top 12 by Jobs desc, repo tie-break
	Totals ChartTotals  `json:"totals"`
}

// HealthDaemon reports the orchestration daemon's liveness.
type HealthDaemon struct {
	Running   bool   `json:"running"`
	PID       int    `json:"pid,omitempty"`
	StartedAt int64  `json:"startedAt,omitempty"` // epoch ms, 0 when unknown
	Version   string `json:"version,omitempty"`   // the RUNNING daemon binary's version
}

// HealthUpdate reports the daemon-binary update check. Omitted entirely when the
// check is unavailable (offline / rate-limited / no release) — fail-open, never an error.
type HealthUpdate struct {
	Current         string `json:"current,omitempty"` // version the running daemon reports
	Latest          string `json:"latest,omitempty"`
	ReleaseURL      string `json:"releaseUrl,omitempty"`
	UpdateAvailable bool   `json:"updateAvailable"`
	CheckedAt       int64  `json:"checkedAt,omitempty"` // epoch ms of the underlying check
}

// HealthTotals is the current fleet-wide job-state snapshot.
type HealthTotals struct {
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Blocked   int `json:"blocked"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
}

// HealthLock is a held branch/checkout lock.
type HealthLock struct {
	Repo       string `json:"repo"`
	Branch     string `json:"branch"`
	Owner      string `json:"owner"`
	AcquiredAt int64  `json:"acquiredAt,omitempty"` // epoch ms
}

// HealthResourceLock is a held non-branch resource lock (runtime session, etc.).
type HealthResourceLock struct {
	Key        string `json:"key"`
	Owner      string `json:"owner,omitempty"`
	AcquiredAt int64  `json:"acquiredAt,omitempty"`
	ExpiresAt  int64  `json:"expiresAt,omitempty"`
}

// HealthStuckJob is a job that appears wedged: blocked, or queued too long.
type HealthStuckJob struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Agent  string `json:"agent,omitempty"`
	Repo   string `json:"repo,omitempty"`
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
	Since  int64  `json:"since,omitempty"` // epoch ms
}

// HealthFailure is a recently failed job.
type HealthFailure struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Agent string `json:"agent,omitempty"`
	Repo  string `json:"repo,omitempty"`
	At    int64  `json:"at,omitempty"` // epoch ms
}

// Health is the data behind the Health page: daemon liveness, fleet totals, held
// locks, wedged jobs and recent failures.
type Health struct {
	Daemon         HealthDaemon         `json:"daemon"`
	Update         *HealthUpdate        `json:"update,omitempty"` // daemon-binary update check; nil when unavailable
	Totals         HealthTotals         `json:"totals"`
	Locks          []HealthLock         `json:"locks"`          // branch/checkout locks, oldest first
	ResourceLocks  []HealthResourceLock `json:"resourceLocks"`  // runtime-session etc., oldest first
	Stuck          []HealthStuckJob     `json:"stuck"`          // blocked jobs + queued older than 10 min, oldest first
	RecentFailures []HealthFailure      `json:"recentFailures"` // last 10 failed, newest first
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
	// Agent returns the click-through detail for a single agent by name: its
	// summary plus template and version history. Unknown names return a
	// not-found sentinel (mapped to 404 by the API layer).
	Agent(ctx context.Context, name string) (AgentDetail, error)
	// Graph returns the whole-history galaxy graph. Empty repo => all runs; a
	// non-empty repo scopes to that repository's jobs (and their hubs).
	Graph(ctx context.Context, repo string) (Graph, error)
	// Charts returns the per-day time series plus top-agent/top-repo/totals
	// breakdowns for the Charts page. days selects the window: 7, 30 or 90; a
	// days of 0 means all history. Days is oldest->newest and zero-filled
	// continuous across the whole window.
	Charts(ctx context.Context, days int) (Charts, error)
	// Health returns the daemon liveness, fleet totals, held locks, wedged jobs
	// and recent failures behind the Health page.
	Health(ctx context.Context) (Health, error)
	Subscribe(ctx context.Context, runID string) (<-chan State, func(), error) // for SSE
}
