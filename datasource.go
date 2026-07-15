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
	RunID    string `json:"runId"`
	Title    string `json:"title"`
	Workflow string `json:"workflow,omitempty"` // root workflow label, when this run is labeled
	Nodes    []Node `json:"nodes"`              // edges are derived client-side from ParentID + Deps
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
// job, colored by State), "repo" (a repository hub), "agent" (an agent hub), or
// "workflow" (a label hub). The hubs are synthetic grouping nodes that cluster
// jobs and give the force-directed graph its structure. Rollup fields are only
// populated on workflow hubs.
type GraphNode struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"` // job | repo | agent | workflow
	Label     string    `json:"label"`
	State     NodeState `json:"state,omitempty"`
	Agent     string    `json:"agent,omitempty"`
	Repo      string    `json:"repo,omitempty"`
	Run       string    `json:"run,omitempty"`
	JobCount  int       `json:"jobCount,omitempty"`
	NoteCount int       `json:"noteCount,omitempty"`
	TokensIn  int       `json:"tokensIn,omitempty"`
	TokensOut int       `json:"tokensOut,omitempty"`
}

// GraphLink is an edge in the galaxy graph. Kind is "parent"/"dep" (delegation
// and sibling links between jobs), "repo" (job -> its repo hub), "agent"
// (job -> its agent hub), or "workflow" (labeled job -> its workflow hub).
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

// WorkflowSummary is the aggregate header for one workflow label. State counts
// are job counts, and timestamps are epoch milliseconds.
type WorkflowSummary struct {
	Label string `json:"label"`
	// Summary is an optional human-readable one-liner from workflow_meta. It is
	// empty when unset and contains untrusted free text that clients must escape.
	Summary   string `json:"summary"`
	Jobs      int    `json:"jobs"`
	Queued    int    `json:"queued"`
	Running   int    `json:"running"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Blocked   int    `json:"blocked"`
	Cancelled int    `json:"cancelled"`
	Notes     int    `json:"notes"`
	TokensIn  int    `json:"tokensIn"`
	TokensOut int    `json:"tokensOut"`
	FirstAt   int64  `json:"firstAt"`
	LastAt    int64  `json:"lastAt"`
}

// WorkflowCoordinator identifies the read-only handoff point for a workflow.
// SessionID is opaque and may be empty for unattended synthesized workflows.
type WorkflowCoordinator struct {
	Author    string `json:"author"`
	Pane      string `json:"pane"`
	SessionID string `json:"session_id"`
}

// WorkflowCounts contains the index-level job outcomes and journal note count.
type WorkflowCounts struct {
	Jobs      int `json:"jobs"`
	Running   int `json:"running"`
	Queued    int `json:"queued"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Blocked   int `json:"blocked"`
	Notes     int `json:"notes"`
}

// WorkflowIndexEntry is one row in GET /api/workflows. Namespace and Campaign
// are the first-slash split of Label; labels without a slash have an empty
// Namespace. Timestamps are epoch milliseconds.
type WorkflowIndexEntry struct {
	Label string `json:"label"`
	// Summary is an optional human-readable one-liner from workflow_meta. It is
	// empty when unset and contains untrusted free text that clients must escape.
	Summary     string              `json:"summary"`
	Namespace   string              `json:"namespace"`
	Campaign    string              `json:"campaign"`
	Auto        bool                `json:"auto"`
	Coordinator WorkflowCoordinator `json:"coordinator"`
	State       string              `json:"state"` // active | settled | stalled
	StalledForS int64               `json:"stalled_for_s"`
	Counts      WorkflowCounts      `json:"counts"`
	TokensIn    int                 `json:"tokens_in"`
	TokensOut   int                 `json:"tokens_out"`
	FirstAt     int64               `json:"first_at"`
	LastAt      int64               `json:"last_at"`
	LastNote    string              `json:"last_note"`
	Repos       []string            `json:"repos,omitempty"`
}

// WorkflowNode is the compact job shape retained for complete workflow run
// trees. Full job prompt, output, and events remain available from /api/job/{id}.
type WorkflowNode struct {
	ID        string    `json:"id"`
	ParentID  string    `json:"parentId,omitempty"`
	Deps      []string  `json:"deps,omitempty"`
	Title     string    `json:"title"`
	Agent     string    `json:"agent"`
	Runtime   string    `json:"runtime"`
	Model     string    `json:"model,omitempty"`
	State     NodeState `json:"state"`
	StartedAt int64     `json:"startedAt,omitempty"`
	EndedAt   int64     `json:"endedAt,omitempty"`
}

// WorkflowChild is the compact inline child row rendered when a mission-log
// run block is expanded.
type WorkflowChild struct {
	ID       string    `json:"id"`
	Action   string    `json:"action"`
	Agent    string    `json:"agent"`
	Runtime  string    `json:"runtime,omitempty"`
	State    NodeState `json:"state"`
	ElapsedS int64     `json:"elapsed_s"`
}

// WorkflowRun is one complete run tree in a workflow page. Pagination is by
// these roots, so Nodes is never split across pages.
type WorkflowRun struct {
	RunID     string          `json:"runId"`
	Title     string          `json:"title"`
	Agent     string          `json:"agent,omitempty"`
	Runtime   string          `json:"runtime,omitempty"`
	Repo      string          `json:"repo,omitempty"`
	State     NodeState       `json:"state"`
	StartedAt int64           `json:"started_at,omitempty"`
	EndedAt   int64           `json:"ended_at,omitempty"`
	ElapsedS  int64           `json:"elapsed_s,omitempty"`
	Children  []WorkflowChild `json:"children,omitempty"`
	Nodes     []WorkflowNode  `json:"nodes"`
}

// WorkflowNoteView is an untrusted journal note. Clients must escape Author and
// Body before placing them in HTML.
type WorkflowNoteView struct {
	ID        int64  `json:"id"`
	Author    string `json:"author"`
	Body      string `json:"body"`
	Repo      string `json:"repo"`
	CreatedAt int64  `json:"createdAt"`
}

// WorkflowView is a paginated set of complete run trees and an independently
// paginated note journal. The lifecycle/coordinator fields drive the mission-log
// header while the original run-tree fields remain source-compatible.
type WorkflowView struct {
	Summary        WorkflowSummary     `json:"summary"`
	State          string              `json:"state,omitempty"` // active | settled | stalled
	StalledForS    int64               `json:"stalled_for_s,omitempty"`
	Coordinator    WorkflowCoordinator `json:"coordinator,omitempty"`
	WorkDir        string              `json:"work_dir,omitempty"`
	Runs           []WorkflowRun       `json:"runs"`
	Notes          []WorkflowNoteView  `json:"notes"`
	NextRunCursor  string              `json:"nextRunCursor"`
	NextNoteCursor string              `json:"nextNoteCursor"`
	Truncated      bool                `json:"truncated"`
}

// WorkflowQuery carries independent cursors and requested page sizes. A
// WorkflowDataSource must default and cap non-positive or oversized maxima.
type WorkflowQuery struct {
	RunCursor  string
	NoteCursor string
	MaxRuns    int
	MaxNotes   int
}

// WorkflowDataSource is the optional workflow visibility extension. Keeping it
// separate preserves the core DataSource contract for older bridges.
type WorkflowDataSource interface {
	Workflows(ctx context.Context) ([]WorkflowIndexEntry, error)
	Workflow(ctx context.Context, label string, q WorkflowQuery) (WorkflowView, error)
}

// OverviewNeedsYou is one operator-attention card on GET /api/overview.
// Link is either a dashboard path or an external URL. Stalled workflow cards
// additionally carry the read-only coordinator handoff fields.
type OverviewNeedsYou struct {
	Kind      string `json:"kind"` // pr_awaiting_merge | blocked_job | groom_proposal | stalled_workflow
	Repo      string `json:"repo,omitempty"`
	Ref       string `json:"ref,omitempty"`
	Title     string `json:"title"`
	AgeS      int64  `json:"age_s"`
	CI        string `json:"ci,omitempty"` // green | red | pending | ""
	Link      string `json:"link,omitempty"`
	Label     string `json:"label,omitempty"`
	Pane      string `json:"pane,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	LastNote  string `json:"last_note,omitempty"`
}

// OverviewWorkflowActivity is one active workflow row. Namespace and Campaign
// are the first-slash split of Label; Agents is a stable display-order list.
type OverviewWorkflowActivity struct {
	Label       string   `json:"label"`
	Namespace   string   `json:"namespace"`
	Campaign    string   `json:"campaign"`
	Running     int      `json:"running"`
	Agents      []string `json:"agents"`
	StartedAgoS int64    `json:"started_ago_s"`
}

// OverviewActivity is the live fleet block on the Overview page.
type OverviewActivity struct {
	Workflows      []OverviewWorkflowActivity `json:"workflows"`
	UnattendedNote string                     `json:"unattended_note,omitempty"`
	Queued         int                        `json:"queued"`
}

// OverviewNotable is one of the five most-recent terminal jobs shown today.
type OverviewNotable struct {
	Agent    string `json:"agent"`
	Title    string `json:"title"`
	Outcome  string `json:"outcome"` // succeeded | failed | cancelled
	ElapsedS int64  `json:"elapsed_s"`
	AgeS     int64  `json:"age_s"`
}

// OverviewToday is the rolling 24-hour summary. PerHour is oldest-to-newest.
type OverviewToday struct {
	Completed int               `json:"completed"`
	Failed    int               `json:"failed"`
	Cancelled int               `json:"cancelled"`
	TokensIn  int               `json:"tokens_in"`
	TokensOut int               `json:"tokens_out"`
	PerHour   [24]int           `json:"per_hour"`
	Notable   []OverviewNotable `json:"notable"`
}

// OverviewScheduled is one upcoming declared pipeline.
type OverviewScheduled struct {
	Name       string `json:"name"`
	Schedule   string `json:"schedule"`
	LastStatus string `json:"last_status"`
	NextInS    int64  `json:"next_in_s"`
}

// OverviewFleet is one registered agent's compact daily rollup.
type OverviewFleet struct {
	Agent     string `json:"agent"`
	Runtime   string `json:"runtime"`
	Running   bool   `json:"running"`
	JobsToday int    `json:"jobs_today"`
}

// Overview is the additive operator-first landing-page snapshot.
type Overview struct {
	NeedsYou  []OverviewNeedsYou  `json:"needs_you"`
	Activity  OverviewActivity    `json:"activity"`
	Today     OverviewToday       `json:"today"`
	Scheduled []OverviewScheduled `json:"scheduled"`
	Fleet     []OverviewFleet     `json:"fleet"`
}

// OverviewDataSource is an optional extension so older gitmoot bridges remain
// source-compatible while the dashboard can degrade to a quiet teaching state.
type OverviewDataSource interface {
	Overview(ctx context.Context) (Overview, error)
}

// TaskSummary is one read-only lifecycle card on GET /api/tasks. UpdatedAt is
// epoch milliseconds; AgeS is the server-computed display age. Merged entries
// are limited to the most recent seven days by the data source.
type TaskSummary struct {
	ID            string `json:"id"`
	Title         string `json:"title"`
	Repo          string `json:"repo"`
	State         string `json:"state"` // planned | implementing | pr_open | blocked | merged
	Agent         string `json:"agent,omitempty"`
	PRNumber      int    `json:"pr_number,omitempty"`
	CI            string `json:"ci,omitempty"` // green | red | pending | ""
	BlockedReason string `json:"blocked_reason,omitempty"`
	UpdatedAt     int64  `json:"updated_at"`
	AgeS          int64  `json:"age_s"`
}

// TasksDataSource is the optional task-board extension. Keeping it separate
// preserves the core DataSource contract for older bridges.
type TasksDataSource interface {
	Tasks(ctx context.Context) ([]TaskSummary, error)
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

// ConfigKnob is one allowlisted effective setting. Value and Default carry only
// registered, non-secret settings projected by the datasource; implementations
// must never reflect arbitrary config fields into this contract. Kind is one of
// flag, int, string, duration or list.
type ConfigKnob struct {
	Key       string `json:"key"`
	Value     any    `json:"value"`
	Default   any    `json:"default"`
	IsDefault bool   `json:"is_default"`
	Kind      string `json:"kind"`
	Doc       string `json:"doc"`
}

// ConfigSection groups allowlisted knobs by their config.toml section. Sections
// and Knobs are emitted in deterministic name/key order.
type ConfigSection struct {
	Name  string       `json:"name"`
	Knobs []ConfigKnob `json:"knobs"`
}

// ConfigAgent is the sanitized per-agent behavior visible in config.toml.
// Credentials, environment and template contents are intentionally absent.
type ConfigAgent struct {
	Name            string   `json:"name"`
	Runtime         string   `json:"runtime"`
	Model           string   `json:"model"`
	Memory          bool     `json:"memory"`
	ChatAutorespond bool     `json:"chat_autorespond"`
	Capabilities    []string `json:"capabilities"`
	AutonomyPolicy  string   `json:"autonomy_policy"`
	MaxBackground   int      `json:"max_background"`
}

// ConfigSnapshot is the read-only, sanitized effective configuration consumed
// by the Config page. ContractVersion starts at 1 and makes future additions
// additive. UnknownKeys contains names only: unknown values are excluded by the
// type itself. ModifiedAt is epoch milliseconds (0 when the file is absent).
type ConfigSnapshot struct {
	ContractVersion int             `json:"contract_version"`
	Path            string          `json:"path"`
	ModifiedAt      int64           `json:"modified_at"`
	Exists          bool            `json:"exists"`
	Sections        []ConfigSection `json:"sections"`
	Agents          []ConfigAgent   `json:"agents"`
	UnknownKeys     []string        `json:"unknown_keys"`
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

// Skills — the SkillOpt evolution overview.

// SkillVersion is one version in a skill template's evolution history, in
// sparkline order. Score is present only when the version was scored (HasScore).
type SkillVersion struct {
	Number     int     `json:"number"`
	State      string  `json:"state"`
	Score      float64 `json:"score,omitempty"`
	HasScore   bool    `json:"hasScore,omitempty"`
	CreatedAt  int64   `json:"createdAt,omitempty"` // epoch ms
	PromotedAt int64   `json:"promotedAt,omitempty"`
}

// SkillCandidate is a pending version awaiting review/promotion. Score is passed
// through in the review's stored string form (e.g. a decimal or a verdict).
type SkillCandidate struct {
	VersionID string `json:"versionId"`
	Number    int    `json:"number"`
	Score     string `json:"score,omitempty"` // pass through the review's stored form
	CreatedAt int64  `json:"createdAt,omitempty"`
}

// SkillTemplate is one skill template's evolution: its version history (for the
// sparkline), the version it currently resolves to, any in-flight canary, and
// its pending candidates.
type SkillTemplate struct {
	TemplateID      string           `json:"templateId"`
	Name            string           `json:"name,omitempty"`
	Agents          []string         `json:"agents,omitempty"` // registered agents using it, sorted
	Versions        []SkillVersion   `json:"versions"`         // ascending by Number (sparkline order)
	CurrentVersion  int              `json:"currentVersion,omitempty"`
	CurrentState    string           `json:"currentState,omitempty"`
	CanarySample    float64          `json:"canarySample,omitempty"`
	CanaryStartedAt int64            `json:"canaryStartedAt,omitempty"`
	LastPromotedAt  int64            `json:"lastPromotedAt,omitempty"`
	Pending         []SkillCandidate `json:"pending"`
}

// Skills is the data behind the Learning page's Skills view: every skill
// template's evolution, plus range rollups for the header.
type Skills struct {
	Templates      []SkillTemplate `json:"templates"` // sorted: pending-first, then most-recently-promoted
	ActiveCanaries int             `json:"activeCanaries"`
	PendingTotal   int             `json:"pendingTotal"`
}

// Knowledge — the memory brain graph.

// KnowledgeAgent is one memory-enrolled agent hub in the brain graph, with the
// size of its confirmed-fact / observation pool.
type KnowledgeAgent struct {
	Name         string `json:"name"`
	Enrolled     bool   `json:"enrolled"`
	Facts        int    `json:"facts"`
	Observations int    `json:"observations"`
}

// KnowledgeCluster is an emergent memory cluster (Knowledge graph v2, gitmoot
// #763): a community of similar facts derived deterministically over the
// fact-similarity graph (the same FTS/bm25 signal that seeds vault [[links]]).
// The same DB yields byte-identical clusters, matching the vault byte-identity
// house rule. Label is the display label (an owner override wins server-side,
// so the client renders Label verbatim); Count is the number of member facts;
// Repo is the cluster's dominant repo scope ("" = general/mixed) so the client
// can nest repo -> cluster -> fact; Medoid anchors the label to a representative
// fact for stability across recomputes. ParentID is additive hierarchy metadata:
// facts attach to leaf clusters, while every ancestor aggregates its descendants.
//
// Additive contract: Track A (the gitmoot bridge) fills this. A gitmoot build
// that predates clusters simply omits the enclosing Clusters slice and leaves
// each fact's Cluster empty, so the client falls back to its pre-cluster view.
type KnowledgeCluster struct {
	ID       string `json:"id"`                  // stable unique id (e.g. "cluster:<n>")
	Label    string `json:"label"`               // display label (owner override wins server-side)
	Count    int    `json:"count"`               // direct member count for leaves; aggregate for parents
	Repo     string `json:"repo,omitempty"`      // dominant repo scope, "" = general/mixed
	Medoid   string `json:"medoid,omitempty"`    // anchor fact id (label stability)
	ParentID string `json:"parent_id,omitempty"` // parent cluster id; empty for top-level clusters
}

// KnowledgeFact is a single confirmed memory. Repo scopes the fact ("" = general
// scope); Superseded marks a fact replaced by a newer one on the same key.
//
// The Cluster/SourceJob/SourceFile/Links fields back the Knowledge graph v2
// detail panel (gitmoot #763) and are all additive + optional: a pre-cluster
// gitmoot build leaves them empty and the client degrades gracefully.
// Created/updated are already carried by FirstSeen/LastSeen.
type KnowledgeFact struct {
	ID         string `json:"id"` // stable unique id (e.g. "fact:<rowid>")
	Content    string `json:"content"`
	Repo       string `json:"repo,omitempty"` // "" = general scope
	Key        string `json:"key,omitempty"`
	Owner      string `json:"owner"` // agent name
	Witnesses  int    `json:"witnesses"`
	FirstSeen  int64  `json:"firstSeen,omitempty"`
	LastSeen   int64  `json:"lastSeen,omitempty"`
	Superseded bool   `json:"superseded,omitempty"`
	// Cluster is the id of the fact's owning KnowledgeCluster ("" = unclustered).
	Cluster string `json:"cluster,omitempty"`
	// SourceJob is the job id the fact was learned from (provenance).
	SourceJob string `json:"sourceJob,omitempty"`
	// SourceFile is the file the fact was ingested from (provenance).
	SourceFile string `json:"sourceFile,omitempty"`
	// Links are the ids of facts this fact references (the vault [[wikilinks]]),
	// rendered as clickable cross-references in the detail panel.
	Links []string `json:"links,omitempty"`
}

// KnowledgeEdge is one edge in the brain graph: a fact to its owner agent
// (owner), a fact to its category/scope hub (category), a fact to its emergent
// cluster hub (cluster, gitmoot #763), a newer fact to the older fact it
// supersedes (supersede), or an undirected fact-to-fact wiki link (link). Link
// edges are emitted once per pair and carry a score in (0,1].
type KnowledgeEdge struct {
	Source string  `json:"source"`
	Target string  `json:"target"`
	Kind   string  `json:"kind"` // owner | category | cluster | supersede | link
	Score  float64 `json:"score,omitempty"`
}

// Knowledge is the data behind the Learning page's Knowledge view: the memory
// brain graph of enrolled agents, their facts, the emergent clusters those
// facts belong to (gitmoot #763) and the edges between them.
type Knowledge struct {
	Agents []KnowledgeAgent `json:"agents"`
	Facts  []KnowledgeFact  `json:"facts"`
	// Clusters are the emergent memory clusters (gitmoot #763). Additive: a
	// pre-cluster gitmoot build leaves this empty and the client falls back to
	// its scope/category view.
	Clusters []KnowledgeCluster `json:"clusters"`
	Edges    []KnowledgeEdge    `json:"edges"`
}

// Pipelines — the declared shell-stage pipelines (gitmoot #681).

// PipelineSummary is one row of the Pipelines list: a declared pipeline
// (gitmoot #681) plus its schedule state and recent run outcomes.
type PipelineSummary struct {
	Name string `json:"name"`
	Repo string `json:"repo,omitempty"`
	// Group is the display section resolved by the server from the pipeline spec's
	// group, or its repository when the spec leaves group unset. It is never empty.
	Group      string               `json:"group"`
	Enabled    bool                 `json:"enabled"`
	Mode       string               `json:"mode,omitempty"`     // display mode from the server: "email-triggered (bound|pending|error|unbound)[, scheduled <interval>]" | "scheduled <interval>" | "after: <pipeline>" | "manual"
	Interval   string               `json:"interval,omitempty"` // Go duration, e.g. "24h"
	Jitter     string               `json:"jitter,omitempty"`
	StageCount int                  `json:"stageCount"`
	LastRunID  string               `json:"lastRunId,omitempty"`
	LastStatus string               `json:"lastStatus,omitempty"` // running | succeeded | blocked | failed | cancelled
	LastRunAt  int64                `json:"lastRunAt,omitempty"`  // epoch milliseconds
	NextDueAt  int64                `json:"nextDueAt,omitempty"`  // epoch milliseconds
	Recent     []PipelineRunSummary `json:"recent"`               // newest-first, capped at 10, never nil
}

// PipelineRunSummary is a lightweight listing entry for one run of a pipeline.
type PipelineRunSummary struct {
	ID         string `json:"id"`
	Trigger    string `json:"trigger,omitempty"` // manual | schedule | bridge (bridge = fired through the gitmoot bridge, e.g. by an email trigger)
	State      string `json:"state"`             // running | succeeded | blocked | failed | cancelled
	HaltStage  string `json:"haltStage,omitempty"`
	StartedAt  int64  `json:"startedAt,omitempty"`  // epoch milliseconds
	FinishedAt int64  `json:"finishedAt,omitempty"` // epoch milliseconds
	Duration   int64  `json:"duration,omitempty"`   // milliseconds (finished-started, 0 while running)
}

// PipelineRun is the full detail of one run: identity/halt state plus the
// stage rows in spec (topological) order — the same order the CLI funnel prints.
type PipelineRun struct {
	ID         string          `json:"id"`
	Pipeline   string          `json:"pipeline"`
	Repo       string          `json:"repo,omitempty"`
	Trigger    string          `json:"trigger,omitempty"` // manual | schedule | bridge
	State      string          `json:"state"`             // running | succeeded | blocked | failed | cancelled
	SpecHash   string          `json:"specHash,omitempty"`
	HaltStage  string          `json:"haltStage,omitempty"`
	HaltReason string          `json:"haltReason,omitempty"`
	Needs      []string        `json:"needs,omitempty"`      // persisted blocked-needs, aggregated at run level
	StartedAt  int64           `json:"startedAt,omitempty"`  // epoch milliseconds
	FinishedAt int64           `json:"finishedAt,omitempty"` // epoch milliseconds
	Stages     []PipelineStage `json:"stages"`               // spec order, never nil
}

// PipelineStage is one shell stage of a pipeline run. Deps are the DAG
// dependency edges (the spec's needs list — edges derived client-side, like
// Node.Deps); Needs are the persisted blocked-needs of a parked stage. The two
// are deliberately distinct.
type PipelineStage struct {
	ID           string   `json:"id"`
	State        string   `json:"state"`                  // pending | queued | running | succeeded | blocked | failed | skipped | cancelled
	Kind         string   `json:"kind,omitempty"`         // shell | agent_ask | agent_review | agent_implement | produce | gate | orchestrate
	AgentRuntime string   `json:"agentRuntime,omitempty"` // runtime backing an agent stage, when known
	Deps         []string `json:"deps,omitempty"`
	Cmd          string   `json:"cmd,omitempty"`
	JobID        string   `json:"jobId,omitempty"`
	Attempt      int      `json:"attempt,omitempty"`
	Retry        int      `json:"retry,omitempty"` // the stage's retry budget from the spec
	Needs        []string `json:"needs,omitempty"`
	Summary      string   `json:"summary,omitempty"`
	// ProgressActivity is the sanitized last output line from gitmoot #816's
	// latest-only progress event. It is normally absent, and is populated only
	// for running stages after the 60-second progress threshold.
	ProgressActivity string `json:"progressActivity,omitempty"`
	// ProgressAt is the epoch-millisecond time of the #816 progress event.
	// Like ProgressActivity, its absence is normal before the threshold.
	ProgressAt int64 `json:"progressAt,omitempty"`
	StartedAt  int64 `json:"startedAt,omitempty"`  // epoch milliseconds
	FinishedAt int64 `json:"finishedAt,omitempty"` // epoch milliseconds
}

// PipelineStageMark is the minimal per-run stage outcome used by the history
// matrix: which stage, and how it ended (or stands).
type PipelineStageMark struct {
	ID    string `json:"id"`
	State string `json:"state"` // pending | queued | running | succeeded | blocked | failed | skipped | cancelled
}

// PipelineRunHistoryEntry is one row of a pipeline's run history: the run
// summary plus its per-stage marks (in that run's stage order).
type PipelineRunHistoryEntry struct {
	ID         string              `json:"id"`
	Trigger    string              `json:"trigger,omitempty"` // manual | schedule | bridge
	State      string              `json:"state"`             // running | succeeded | blocked | failed | cancelled
	HaltStage  string              `json:"haltStage,omitempty"`
	StartedAt  int64               `json:"startedAt,omitempty"`  // epoch milliseconds
	FinishedAt int64               `json:"finishedAt,omitempty"` // epoch milliseconds
	Duration   int64               `json:"duration,omitempty"`   // milliseconds (finished-started, 0 while running)
	Stages     []PipelineStageMark `json:"stages"`               // never nil
}

// PipelineDetail is the click-through detail for one pipeline: its currently
// declared stage DAG (from the stored spec, every stage pending) plus the run
// history, newest-first, capped at 100 runs.
type PipelineDetail struct {
	Name string `json:"name"`
	// Description is optional free-form display text from the pipeline spec. It
	// may be absent when the pipeline has no declared description.
	Description string                    `json:"description,omitempty"`
	Declared    []PipelineStage           `json:"declared"` // current spec DAG, state "pending"; never nil
	Runs        []PipelineRunHistoryEntry `json:"runs"`     // newest-first, capped at 100; never nil
}

// Chat — the Gitmoot-native agent chat layer (gitmoot #534): durable,
// repo-scoped coordination threads where agents and humans exchange messages,
// tag each other, and promote messages into real jobs.

// ChatRef is a structured reference carried by a chat message: a link to a
// Gitmoot entity (a job, PR, artifact, repo, …). Kind names the entity type;
// URL, when present and http(s), is a safe external link the client can render.
type ChatRef struct {
	Kind string `json:"kind"`           // job | pr | artifact | repo | thread | …
	Repo string `json:"repo,omitempty"` // owning repo, when scoped
	ID   string `json:"id"`             // the entity id (job id, PR number, …)
	URL  string `json:"url,omitempty"`  // external link, when the entity has one
}

// ChatMessage is one durable message in a thread. Body is untrusted,
// agent/human-authored markdown-ish plain text and MUST be escaped by any
// renderer. Kind drives how the client styles the message: chat (a normal
// message), system (an ask-gate question from a paused job), job_result (an
// agent's job result posted back), or promotion_request (a message that spawned
// a job — see PromotedJobID).
type ChatMessage struct {
	ID            string    `json:"id"`
	Seq           int       `json:"seq"`                     // 1-based per-thread ordering key
	TsMs          int64     `json:"tsMs"`                    // epoch milliseconds
	AuthorKind    string    `json:"authorKind"`              // human | agent | system
	AuthorName    string    `json:"authorName"`              // agent/human name (empty for system)
	Kind          string    `json:"kind"`                    // chat | system | job_result | promotion_request
	Body          string    `json:"body"`                    // UNTRUSTED markdown-ish text — escape everything
	Refs          []ChatRef `json:"refs,omitempty"`          // structured entity references
	ReplyTo       string    `json:"replyTo,omitempty"`       // id of the message this replies to
	PromotedJobID string    `json:"promotedJobId,omitempty"` // set on promotion_request: the job it spawned
}

// ChatThreadSummary is one row of the Chat threads list: a repo-scoped
// coordination ledger plus a rollup of its activity (message count, unread
// mentions, and a server-truncated preview of the last message) so the list can
// render without fetching each thread's full history.
type ChatThreadSummary struct {
	ID             string   `json:"id"`
	Slug           string   `json:"slug,omitempty"`
	Name           string   `json:"name"`
	Repo           string   `json:"repo,omitempty"`
	State          string   `json:"state"` // open | archived
	CreatedBy      string   `json:"createdBy,omitempty"`
	UpdatedAt      int64    `json:"updatedAt,omitempty"` // epoch ms of the last activity
	MessageCount   int      `json:"messageCount"`
	UnreadMentions int      `json:"unreadMentions"`         // pending @mentions across enrolled agents
	LastAuthor     string   `json:"lastAuthor,omitempty"`   // author of the most recent message
	LastKind       string   `json:"lastKind,omitempty"`     // kind of the most recent message
	LastSnippet    string   `json:"lastSnippet,omitempty"`  // server-truncated preview of the last message body
	Participants   []string `json:"participants,omitempty"` // enrolled agents/humans, sorted
}

// ChatThreadDetail is the click-through detail for one thread: its summary plus
// the full message history (ascending by Seq).
type ChatThreadDetail struct {
	ChatThreadSummary
	Messages []ChatMessage `json:"messages"` // ascending by Seq; never nil
}

// Attention + binary checks — surfacing evaluator output and human gates where a
// human (or the planned Slack/media bridge, gitmoot #519) manages work
// (gitmoot #528).

// ResultCheck is one deterministic result check (gitmoot #526/#711): the
// question the evaluator asked and, when it failed, the explanation of why. Used
// in the job-detail failed-check section.
type ResultCheck struct {
	CheckID     string `json:"checkId"`
	Question    string `json:"question"`
	Explanation string `json:"explanation,omitempty"`
}

// JobChecks is the job-detail failed-check section (gitmoot #711): the
// deterministic result checks a job's result failed, plus the home-wide
// [workflow] result_checks policy mode in force. Mode "off" means checks were
// not run; an empty Failed under "warn"/"block" means the job passed every
// check.
type JobChecks struct {
	JobID  string        `json:"jobId"`
	Mode   string        `json:"mode"`   // off | warn | block
	Failed []ResultCheck `json:"failed"` // failed checks, insertion order; never nil
}

// BinaryVerdict is one per-question verdict from a SkillOpt binary evaluation
// (gitmoot #714 skillopt_binary_verdicts): a yes/no answer to a decomposed
// question with the evaluator's explanation. Pass mirrors Verdict == "yes".
type BinaryVerdict struct {
	QuestionID  string  `json:"questionId"`
	Dimension   string  `json:"dimension,omitempty"`
	Verdict     string  `json:"verdict"` // yes | no
	Pass        bool    `json:"pass"`
	Explanation string  `json:"explanation,omitempty"`
	Weight      float64 `json:"weight,omitempty"`
}

// BinaryVerdicts is the per-run binary-check breakdown (gitmoot #714): the
// verdicts ordered by (dimension, questionId) plus pass/fail headline counts.
// An unknown run yields zero counts and an empty (never nil) list.
type BinaryVerdicts struct {
	RunID    string          `json:"runId"`
	Passed   int             `json:"passed"`
	Failed   int             `json:"failed"`
	Verdicts []BinaryVerdict `json:"verdicts"` // ordered (dimension, questionId); never nil
}

// AttentionGate is a blocked job waiting on a human-satisfiable gate (gitmoot
// #693 job_gates): one open (unsatisfied) need on a job.
type AttentionGate struct {
	JobID     string    `json:"jobId"`
	Need      string    `json:"need"`
	Title     string    `json:"title,omitempty"`
	Agent     string    `json:"agent,omitempty"`
	Repo      string    `json:"repo,omitempty"`
	State     NodeState `json:"state,omitempty"`
	PR        int       `json:"pr,omitempty"`
	CreatedAt int64     `json:"createdAt,omitempty"` // epoch ms
}

// AttentionSynthItem is a synthesized SkillOpt review item awaiting the human
// approval gate (gitmoot skillopt_synth_items, status pending_human_approval).
type AttentionSynthItem struct {
	ID          string  `json:"id"`
	TemplateID  string  `json:"templateId,omitempty"`
	Repo        string  `json:"repo,omitempty"`
	Question    string  `json:"question,omitempty"`
	Gap         float64 `json:"gap,omitempty"`
	WeakAgent   string  `json:"weakAgent,omitempty"`
	StrongAgent string  `json:"strongAgent,omitempty"`
	JudgeAgent  string  `json:"judgeAgent,omitempty"`
	CreatedAt   int64   `json:"createdAt,omitempty"` // epoch ms
}

// AttentionCandidate is an agent-template candidate version awaiting human
// promotion (a template version in the "pending" state). Score passes through
// the review's stored form (a decimal, or empty when unscored).
type AttentionCandidate struct {
	TemplateID string `json:"templateId"`
	Name       string `json:"name,omitempty"`
	VersionID  string `json:"versionId"`
	Number     int    `json:"number"`
	Score      string `json:"score,omitempty"`
	CreatedAt  int64  `json:"createdAt,omitempty"` // epoch ms
}

// Attention is the "Needs a human" view (gitmoot #528): the three kinds of item
// that require an explicit human decision — blocked job gates, pending synth
// approvals and template candidates awaiting promotion — plus a headline total.
// Every list is non-nil and deterministically ordered (the UI polls with a
// signature-skip).
type Attention struct {
	Gates      []AttentionGate      `json:"gates"`      // open job gates, oldest-first
	SynthItems []AttentionSynthItem `json:"synthItems"` // pending synth approvals, newest-first
	Candidates []AttentionCandidate `json:"candidates"` // versions awaiting promotion, (templateId, number)
	Total      int                  `json:"total"`      // gates + synthItems + candidates
}

// ChangeCursorDataSource is the optional liveness extension behind
// GET /api/events. ChangeCursor returns an opaque cursor that changes whenever
// dashboard-visible data changes. Implementations should make this a cheap,
// monotonic store query; datasources without the extension retain polling-only
// behavior.
type ChangeCursorDataSource interface {
	ChangeCursor(ctx context.Context) (string, error)
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
	// Config returns the versioned, sanitized effective configuration behind the
	// Config page. Values are strictly allowlisted; unknown config entries surface
	// by name only. Sections, knobs, agents and unknown keys are deterministic.
	Config(ctx context.Context) (ConfigSnapshot, error)
	// Skills returns the SkillOpt evolution overview behind the Learning page's
	// Skills view: per-template version history, active canaries and pending
	// candidates. Ordering must be deterministic (the UI polls with a
	// signature-skip): templates pending-first then most-recently-promoted, each
	// template's versions ascending by Number.
	Skills(ctx context.Context) (Skills, error)
	// Knowledge returns the memory brain graph behind the Learning page's
	// Knowledge view: enrolled agents, their facts, the emergent clusters those
	// facts belong to (gitmoot #763) and the owner/category/cluster/supersede
	// edges between them. Clusters are additive — a pre-cluster gitmoot build
	// returns an empty Clusters slice and empty per-fact Cluster fields, and the
	// client falls back to its scope/category view. Ordering must be
	// deterministic (the UI polls with a signature-skip).
	Knowledge(ctx context.Context) (Knowledge, error)
	// Pipelines returns every declared pipeline with its schedule state and recent
	// run outcomes (newest-first, capped at 10), sorted by name. Ordering must be
	// deterministic (the UI polls with a signature-skip).
	Pipelines(ctx context.Context) ([]PipelineSummary, error)
	// PipelineRun returns the full detail for a single run by run id, stages in
	// spec (topological) order. Unknown ids return ErrPipelineRunNotFound.
	PipelineRun(ctx context.Context, id string) (PipelineRun, error)
	// PipelineDetail returns one pipeline's declared stage DAG and its run history
	// (newest-first, capped at 100 runs, each with per-stage marks). Unknown names
	// return ErrPipelineNotFound. Ordering must be deterministic (the UI polls with
	// a signature-skip).
	PipelineDetail(ctx context.Context, name string) (PipelineDetail, error)
	// ChatThreads returns every chat thread (gitmoot #534) with its activity
	// rollup, sorted most-recently-active first (UpdatedAt desc, id desc
	// tie-break). Ordering must be deterministic (the UI polls with a
	// signature-skip).
	ChatThreads(ctx context.Context) ([]ChatThreadSummary, error)
	// ChatThread returns one thread's detail by id: its summary plus the full
	// message history (ascending by Seq). Unknown ids return
	// (nil, ErrChatThreadNotFound). Output must be deterministic.
	ChatThread(ctx context.Context, id string) (*ChatThreadDetail, error)
	// Attention returns the "Needs a human" view (gitmoot #528): blocked job
	// gates, pending synth approvals and template candidates awaiting promotion.
	// Ordering must be deterministic (the UI polls with a signature-skip).
	Attention(ctx context.Context) (Attention, error)
	// JobChecks returns the job-detail failed-check section for one job (gitmoot
	// #711): the deterministic result checks its result failed plus the home-wide
	// policy mode. An unknown job yields an empty Failed with the resolved Mode.
	JobChecks(ctx context.Context, jobID string) (JobChecks, error)
	// BinaryVerdicts returns the per-run SkillOpt binary-check breakdown (gitmoot
	// #714) for a skillopt eval run id: verdicts ordered by (dimension,
	// questionId) plus pass/fail counts. An unknown run yields zero counts.
	BinaryVerdicts(ctx context.Context, runID string) (BinaryVerdicts, error)
	Subscribe(ctx context.Context, runID string) (<-chan State, func(), error) // for SSE
}
