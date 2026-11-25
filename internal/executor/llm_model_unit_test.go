package executor

import "testing"

// The task open supplies the pinned LiteLLM alias (for example
// "<team_id>/m1"). OpenHands' LiteLLM SDK needs the OpenAI-compatible
// provider prefix to call the LiteLLM data plane with that alias.
func TestConversationModelUsesTheOpenAICompatibleAlias(t *testing.T) {
	e := &Executor{cfg: &Config{}}
	for alias, want := range map[string]string{
		"acme/m1":        "openai/acme/m1",
		"openai/acme/m1": "openai/acme/m1",
	} {
		cfg := e.conversationConfig("prompt", "sk-task", "http://litellm-gateway-data:4000/v1", alias)
		if cfg.Agent == nil || cfg.Agent.LLM.Model != want {
			t.Fatalf("model for %q = %+v, want %q", alias, cfg.Agent, want)
		}
		if cfg.Agent.LLM.APIKey != "sk-task" || cfg.Agent.LLM.BaseURL != "http://litellm-gateway-data:4000/v1" {
			t.Fatalf("LLM values = %+v, want the task's key and data URL", cfg.Agent.LLM)
		}
	}
}

// The task's LiteLLM values are the conversation's only LLM configuration,
// even when an agent profile is configured.
func TestConversationUsesTaskLLMEvenWithAgentProfile(t *testing.T) {
	t.Setenv("OPENHANDS_AGENT_PROFILE_ID", "flowai-default")
	cfg := &Config{}
	e := &Executor{cfg: cfg}
	conv := e.conversationConfig("prompt", "sk-task", "http://litellm-gateway-data:4000/v1", "acme/m1")
	if conv.Agent == nil || conv.Agent.LLM.APIKey != "sk-task" || conv.Agent.LLM.Model != "openai/acme/m1" {
		t.Fatalf("conversation = %+v, want the task's LLM values", conv)
	}
	if conv.AgentProfileID != "" {
		t.Fatalf("agent profile %q would replace the task's LLM values", conv.AgentProfileID)
	}
}

// Rate limits (429) are left to the agent's retries, which must outlast a
// one-minute RPM window: at least 60 s of waiting in total.
func TestConversationRetriesOutlastAnRPMWindow(t *testing.T) {
	e := &Executor{cfg: &Config{}}
	conv := e.conversationConfig("prompt", "sk-task", "http://litellm-gateway-data:4000/v1", "acme/m1")
	llm := conv.Agent.LLM
	if llm.NumRetries == nil || llm.RetryMinWait == nil || llm.RetryMaxWait == nil {
		t.Fatalf("retry policy missing: %+v", llm)
	}
	total, wait := 0, *llm.RetryMinWait
	for i := 0; i < *llm.NumRetries; i++ {
		total += wait
		wait *= 2
		if wait > *llm.RetryMaxWait {
			wait = *llm.RetryMaxWait
		}
	}
	if total < 60 {
		t.Fatalf("retries wait %d s in total, want at least 60 s", total)
	}
}
