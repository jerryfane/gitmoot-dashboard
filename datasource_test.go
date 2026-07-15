package dashboard

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestPipelineStageProgressJSON(t *testing.T) {
	t.Run("legacy payload", func(t *testing.T) {
		const payload = `{"id":"build","state":"running"}`
		var stage PipelineStage
		if err := json.Unmarshal([]byte(payload), &stage); err != nil {
			t.Fatalf("unmarshal legacy PipelineStage: %v", err)
		}
		if stage.ID != "build" || stage.State != "running" {
			t.Fatalf("legacy PipelineStage identity = %+v", stage)
		}
		if stage.ProgressActivity != "" || stage.ProgressAt != 0 {
			t.Fatalf("legacy PipelineStage gained progress fields: %+v", stage)
		}
		stage.Deps = []string{}

		got, err := json.Marshal(stage)
		if err != nil {
			t.Fatalf("marshal legacy PipelineStage: %v", err)
		}
		const want = `{"id":"build","state":"running","deps":[]}`
		if string(got) != want {
			t.Fatalf("legacy PipelineStage JSON = %s, want %s", got, want)
		}
	})

	t.Run("progress payload", func(t *testing.T) {
		const payload = `{"id":"build","state":"running","progressActivity":"compiled package 42/100","progressAt":1720000123456}`
		var stage PipelineStage
		if err := json.Unmarshal([]byte(payload), &stage); err != nil {
			t.Fatalf("unmarshal progress PipelineStage: %v", err)
		}
		if stage.ID != "build" || stage.State != "running" {
			t.Fatalf("progress PipelineStage identity = %+v", stage)
		}
		if stage.ProgressActivity != "compiled package 42/100" || stage.ProgressAt != 1_720_000_123_456 {
			t.Fatalf("progress PipelineStage fields = %+v", stage)
		}
		stage.Deps = []string{}

		got, err := json.Marshal(stage)
		if err != nil {
			t.Fatalf("marshal progress PipelineStage: %v", err)
		}
		const want = `{"id":"build","state":"running","deps":[],"progressActivity":"compiled package 42/100","progressAt":1720000123456}`
		if string(got) != want {
			t.Fatalf("progress PipelineStage JSON = %s, want %s", got, want)
		}
	})

	t.Run("kind runtime payload", func(t *testing.T) {
		const payload = `{"id":"answer","state":"running","kind":"agent_ask","agentRuntime":"codex","jobId":"job-1"}`
		var stage PipelineStage
		if err := json.Unmarshal([]byte(payload), &stage); err != nil {
			t.Fatalf("unmarshal kind/runtime PipelineStage: %v", err)
		}
		if stage.Kind != "agent_ask" || stage.AgentRuntime != "codex" || stage.JobID != "job-1" {
			t.Fatalf("kind/runtime PipelineStage fields = %+v", stage)
		}
		stage.Deps = []string{}

		got, err := json.Marshal(stage)
		if err != nil {
			t.Fatalf("marshal kind/runtime PipelineStage: %v", err)
		}
		const want = `{"id":"answer","state":"running","kind":"agent_ask","agentRuntime":"codex","deps":[],"jobId":"job-1"}`
		if string(got) != want {
			t.Fatalf("kind/runtime PipelineStage JSON = %s, want %s", got, want)
		}
	})
}

func TestWorkflowAdditiveJSONContracts(t *testing.T) {
	t.Run("legacy graph node and state omit workflow fields", func(t *testing.T) {
		node := GraphNode{ID: "job-1", Type: "job", Label: "legacy", State: "running"}
		got, err := json.Marshal(node)
		if err != nil {
			t.Fatalf("marshal legacy GraphNode: %v", err)
		}
		const wantNode = `{"id":"job-1","type":"job","label":"legacy","state":"running"}`
		if string(got) != wantNode {
			t.Fatalf("legacy GraphNode JSON = %s, want %s", got, wantNode)
		}

		const statePayload = `{"runId":"run-1","title":"legacy","nodes":[]}`
		var state State
		if err := json.Unmarshal([]byte(statePayload), &state); err != nil {
			t.Fatalf("unmarshal legacy State: %v", err)
		}
		if state.Workflow != "" {
			t.Fatalf("legacy State.Workflow = %q, want empty", state.Workflow)
		}
		stateJSON, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("marshal legacy State: %v", err)
		}
		if string(stateJSON) != statePayload {
			t.Fatalf("legacy State JSON = %s, want %s", stateJSON, statePayload)
		}
	})

	t.Run("labeled graph node and state round trip", func(t *testing.T) {
		const payload = `{"id":"workflow::panel","type":"workflow","label":"panel","jobCount":7,"noteCount":3,"tokensIn":22400,"tokensOut":9700}`
		var node GraphNode
		if err := json.Unmarshal([]byte(payload), &node); err != nil {
			t.Fatalf("unmarshal workflow GraphNode: %v", err)
		}
		if node.Type != "workflow" || node.JobCount != 7 || node.NoteCount != 3 || node.TokensIn != 22400 || node.TokensOut != 9700 {
			t.Fatalf("workflow GraphNode fields = %+v", node)
		}
		got, err := json.Marshal(node)
		if err != nil {
			t.Fatalf("marshal workflow GraphNode: %v", err)
		}
		if !bytes.Equal(got, []byte(payload)) {
			t.Fatalf("workflow GraphNode JSON = %s, want %s", got, payload)
		}

		state := State{RunID: "run-1", Title: "labeled", Workflow: "panel", Nodes: []Node{}}
		stateJSON, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("marshal labeled State: %v", err)
		}
		const wantState = `{"runId":"run-1","title":"labeled","workflow":"panel","nodes":[]}`
		if string(stateJSON) != wantState {
			t.Fatalf("labeled State JSON = %s, want %s", stateJSON, wantState)
		}
		var roundTrip State
		if err := json.Unmarshal(stateJSON, &roundTrip); err != nil {
			t.Fatalf("unmarshal labeled State: %v", err)
		}
		if roundTrip.Workflow != "panel" {
			t.Fatalf("round-trip State.Workflow = %q, want panel", roundTrip.Workflow)
		}
	})
}
