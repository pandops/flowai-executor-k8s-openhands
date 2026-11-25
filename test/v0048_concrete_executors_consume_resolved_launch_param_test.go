package k8sopenhands_test

// Requirement: Concrete Executors consume resolved launch parameters (rows 336–341).

import (
	"strings"
	"testing"
	"time"
)

// llmOf returns the agent llm block of the conversation start for a task.
func (h *harness) llmOf(tk task) map[string]any {
	want := "task " + itoa(tk.TaskID)
	for _, b := range h.oh.StartBodies() {
		if strings.Contains(mustJSON(b["initial_message"]), want) {
			agent, _ := b["agent"].(map[string]any)
			llm, _ := agent["llm"].(map[string]any)
			return llm
		}
	}
	return nil
}

// Scenario: Claimed task has merged parameters (row 336).
func TestClaimedTaskHasMergedParameters(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	a := h.enqueueTask()
	h.waitRunning(a)
	h.setEnvOpen(func(id string) (int, any) {
		return 200, map[string]any{"values": map[string]string{"REPO_URL": "https://git.example.com/b.git", "ONLY_B": "b-value"}}
	})
	b := h.enqueueTask()
	h.waitRunning(b)
	if n := len(h.edgeRequests("/v1/tasks/" + itoa(a.TaskID) + "/launch-parameters/open")); n != 1 {
		t.Fatalf("opened task a %d times", n)
	}
	pa, _ := h.pod(a)
	pb, _ := h.pod(b)
	if pa.Spec.Env["DB_PASSWORD"] != "launch-secret-value" || pa.Spec.Env["REPO_URL"] != "https://git.example.com/acme/app.git" {
		t.Fatalf("task a env %v", pa.Spec.Env)
	}
	if pb.Spec.Env["ONLY_B"] != "b-value" || pb.Spec.Env["DB_PASSWORD"] != "" {
		t.Fatalf("task b got another task's values: %v", pb.Spec.Env)
	}
}

// Scenario: Claim token is no longer current (row 337).
func TestClaimTokenIsNoLongerCurrent(t *testing.T) {
	for _, edge := range []string{"open", "key"} {
		t.Run(edge, func(t *testing.T) {
			h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1 }))
			if edge == "open" {
				h.setEnvOpen(func(string) (int, any) { return envError(404, "claim_not_current", "") })
			} else {
				h.setKeyEdge(func(string) (int, any) { return edgeError(404, "claim_not_current", "") })
			}
			h.start()
			h.waitConnected()
			tk := h.enqueueTask()
			eventually(t, 10*time.Second, func() bool { return len(h.edgeRequests("")) > 0 }, "edge call")
			h.sim(40 * time.Second)
			if got := h.kinds(tk); len(got) != 1 || got[0] != "created" {
				t.Fatalf("events %v", got)
			}
			if !h.noPodCreated(tk) {
				t.Fatalf("Pod created")
			}
			h.waitSlots(0)
		})
	}
}

// Scenario: Model key edge reports a configuration error (row 338).
func TestModelKeyEdgeReportsAConfigurationError(t *testing.T) {
	h := newHarness(t)
	h.setKeyEdge(func(string) (int, any) { return edgeError(422, "configuration_error", "llm_configuration_incomplete") })
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	f := h.waitKind(tk, "failed")
	if f["failure_reason"] != "llm_configuration_incomplete" || f["retryable"] != false {
		t.Fatalf("failed %v", f)
	}
	if !h.noPodCreated(tk) {
		t.Fatalf("Pod created")
	}
}

// Scenario: Task key edge supplies the LiteLLM configuration (row 339).
func TestTaskKeyEdgeSuppliesTheLiteLLMConfiguration(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	llm := h.llmOf(tk)
	if llm["model"] != "openai/3/m1" || llm["api_key"] != "sk-task-litellm-key" || llm["base_url"] != "http://litellm-gateway-data:4000/v1" {
		t.Fatalf("conversation llm %v", llm)
	}
	if s := containsAny(h.logs.String()+h.allBrokerBytes(), "sk-task-litellm-key"); s != "" {
		t.Fatalf("key logged or published")
	}
}

// Scenario: Rate-limited model calls are retried (row 340).
func TestRateLimitedModelCallsAreRetried(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	llm := h.llmOf(tk)
	if num(llm["num_retries"]) != 8 || num(llm["retry_min_wait"]) != 8 || num(llm["retry_max_wait"]) != 60 {
		t.Fatalf("retry policy %v", llm)
	}
}

// Scenario: Model configuration is incomplete (row 341).
func TestModelConfigurationIsIncomplete(t *testing.T) {
	for _, missing := range []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL"} {
		t.Run(missing, func(t *testing.T) {
			t.Setenv("OPENHANDS_LLM_MODEL", "fallback-model")
			t.Setenv("OPENHANDS_LLM_API_KEY", "fallback-key")
			t.Setenv("OPENHANDS_LLM_BASE_URL", "http://fallback")
			h := newHarness(t)
			h.setKeyEdge(func(string) (int, any) {
				v := map[string]any{"OPENAI_BASE_URL": "http://litellm-gateway-data:4000/v1", "OPENAI_MODEL": "3/m1", "OPENAI_API_KEY": "sk-task-litellm-key"}
				delete(v, missing)
				return 200, v
			})
			h.start()
			h.waitConnected()
			tk := h.enqueueTask()
			f := h.waitKind(tk, "failed")
			if f["failure_reason"] != "llm_configuration_incomplete" || f["retryable"] != false {
				t.Fatalf("failed %v", f)
			}
			if len(h.oh.Conversations()) != 0 || !h.noPodCreated(tk) {
				t.Fatalf("submitted a conversation or created a Pod")
			}
		})
	}
}
