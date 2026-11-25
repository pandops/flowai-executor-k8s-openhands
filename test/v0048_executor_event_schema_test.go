package k8sopenhands_test

// The executor event schema allows each reason only on its own kind: the
// three release reasons on released, the pending reasons on pending.

import (
	"encoding/json"
	"testing"
)

func TestExecutorEventSchemaReasonIsPerKind(t *testing.T) {
	s := loadSchemas(t)["executor-event"]
	valid := func(ev map[string]any) error {
		var v any
		data, _ := json.Marshal(ev)
		_ = json.Unmarshal(data, &v)
		return s.Validate(v)
	}
	base := func(kind, reason string) map[string]any {
		return map[string]any{
			"publisher": "executor", "publisher_seq": 12, "team_id": 3, "executor_id": 7, "kind": kind,
			"occurred_at": "2026-11-08T05:20:10.000Z", "task_id": 1, "attempt": 1, "claim_seq": 1, "reason": reason,
		}
	}
	accept := map[string]map[string]any{
		"released executor_shutdown":     base("released", "executor_shutdown"),
		"released executor_restarted":    base("released", "executor_restarted"),
		"released pending_timeout":       base("released", "pending_timeout"),
		"pending no_active_model":        base("pending", "no_active_model"),
		"pending dependency_unavailable": base("pending", "dependency_unavailable"),
	}
	for name, ev := range accept {
		if err := valid(ev); err != nil {
			t.Errorf("%s rejected: %v", name, err)
		}
	}
	reject := map[string]map[string]any{
		"pending pending_timeout":  base("pending", "pending_timeout"),
		"released no_active_model": base("released", "no_active_model"),
	}
	for name, ev := range reject {
		if valid(ev) == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
