package k8sopenhands_test

// Requirement: Concrete OpenHands Executors consume pinned task configuration (rows 347–350).

import (
	"strings"
	"testing"
)

// Scenario: Claimed task uses its pinned prompt and model (row 347). The
// spec's example alias acme/gpt-4o is written as 3/gpt-4o, the alias form
// model-registry's schema allows.
func TestClaimedTaskUsesItsPinnedPromptAndModel(t *testing.T) {
	h := newHarness(t)
	h.setKeyEdge(func(string) (int, any) {
		return 200, map[string]any{"OPENAI_BASE_URL": "http://litellm-gateway-data:4000/v1", "OPENAI_MODEL": "3/gpt-4o", "OPENAI_API_KEY": "sk-task-litellm-key"}
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	var start map[string]any
	for _, b := range h.oh.StartBodies() {
		if strings.Contains(mustJSON(b), "Fix the bug of task "+itoa(tk.TaskID)) {
			start = b
		}
	}
	agent, _ := start["agent"].(map[string]any)
	llm, _ := agent["llm"].(map[string]any)
	if agent["kind"] != "Agent" || llm["model"] != "openai/3/gpt-4o" || llm["api_key"] != "sk-task-litellm-key" {
		t.Fatalf("start body %v", start)
	}
	msg := mustJSON(start["initial_message"])
	if !strings.Contains(msg, "Fix the bug of task "+itoa(tk.TaskID)) {
		t.Fatalf("initial message %s", msg)
	}
}

// Scenario: Process agent profile conflicts with task configuration (row 348).
func TestProcessAgentProfileConflictsWithTaskConfiguration(t *testing.T) {
	t.Setenv("OPENHANDS_AGENT_PROFILE_ID", "stale-profile")
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	for _, b := range h.oh.StartBodies() {
		if _, ok := b["agent_profile_id"]; ok {
			t.Fatalf("selected the process profile: %v", b)
		}
		if b["agent"] == nil {
			t.Fatalf("no inline agent")
		}
	}
}

// Scenario: Task snapshot without an agent field starts OpenHands (row 349).
func TestTaskSnapshotWithoutAnAgentFieldStartsOpenHands(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask(func(m map[string]any) {
		m["execution"].(map[string]any)["execution_parameters"] = map[string]any{}
	})
	h.waitRunning(tk)
	if h.hasKind(tk, "failed") {
		t.Fatalf("failed for a missing agent")
	}
}

// Scenario: Incomplete pinned revision fails without retry (row 350).
func TestIncompletePinnedRevisionFailsWithoutRetry(t *testing.T) {
	for name, edit := range map[string]func(m map[string]any){
		"empty prompt":   func(m map[string]any) { m["execution"].(map[string]any)["rendered_prompt"] = "" },
		"no prompt":      func(m map[string]any) { delete(m["execution"].(map[string]any), "rendered_prompt") },
		"no execution":   func(m map[string]any) { delete(m, "execution") },
		"no revision id": func(m map[string]any) { delete(m["execution"].(map[string]any), "execution_revision_id") },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.start()
			h.waitConnected()
			tk := h.enqueueTask(edit)
			f := h.waitKind(tk, "failed")
			if f["failure_reason"] != "invalid_flow_revision" || f["retryable"] != false {
				t.Fatalf("failed %v", f)
			}
			if len(h.oh.Conversations()) != 0 || !h.noPodCreated(tk) {
				t.Fatalf("conversation or Pod started")
			}
		})
	}
}
