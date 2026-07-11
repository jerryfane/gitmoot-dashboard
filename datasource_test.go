package dashboard

import (
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

		got, err := json.Marshal(stage)
		if err != nil {
			t.Fatalf("marshal legacy PipelineStage: %v", err)
		}
		if string(got) != payload {
			t.Fatalf("legacy PipelineStage JSON = %s, want %s", got, payload)
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

		got, err := json.Marshal(stage)
		if err != nil {
			t.Fatalf("marshal progress PipelineStage: %v", err)
		}
		if string(got) != payload {
			t.Fatalf("progress PipelineStage JSON = %s, want %s", got, payload)
		}
	})
}
