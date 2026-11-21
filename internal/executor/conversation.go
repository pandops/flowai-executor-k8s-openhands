package executor

import (
	"strings"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/openhands"
)

// conversationConfig is the inline OpenHands Agent of one task: its pinned
// prompt and only the task's LiteLLM values.
func (e *Executor) conversationConfig(prompt, taskAPIKey, taskBaseURL, taskModel string) openhands.ConversationConfig {
	cfg := openhands.ConversationConfig{
		Workspace:      openhands.Workspace{Kind: "LocalWorkspace", WorkingDir: e.cfg.OpenHandsWorkspace},
		InitialMessage: openhands.InitialMessage{Role: "user", Content: []openhands.ContentPart{{Type: "text", Text: prompt}}, Run: e.cfg.OpenHandsInitialRun},
	}
	cfg.Agent = &openhands.Agent{
		Kind: "Agent",
		LLM: openhands.LLM{Model: openAICompatibleModel(taskModel), APIKey: taskAPIKey, UsageID: e.cfg.OpenHandsLLMUsageID, BaseURL: taskBaseURL,
			// 429s from the LiteLLM RPM/TPM limits are retried by the agent
			// for longer than a one-minute window.
			NumRetries: intPtr(llmNumRetries), RetryMinWait: intPtr(llmRetryMinWait), RetryMaxWait: intPtr(llmRetryMaxWait)},
		Tools: []openhands.Tool{{Name: "terminal"}},
	}
	return cfg
}

// openAICompatibleModel names the task's LiteLLM alias for OpenHands' LiteLLM
// SDK, which calls the OpenAI-compatible data plane with the "openai/"
// provider prefix.
func openAICompatibleModel(alias string) string {
	if strings.HasPrefix(alias, "openai/") {
		return alias
	}
	return "openai/" + alias
}

// conversationStatus finds the normalized execution_status in a frame.
func conversationStatus(value any) string {
	object, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	if raw, ok := object["execution_status"].(string); ok {
		if status := strings.ToLower(strings.TrimSpace(raw)); status != "" {
			return status
		}
	}
	for _, raw := range object {
		if status := conversationStatus(raw); status != "" {
			return status
		}
	}
	for _, key := range []string{"status", "kind"} {
		if raw, ok := object[key].(string); ok {
			if status := strings.ToLower(strings.TrimSpace(raw)); status != "" {
				return status
			}
		}
	}
	return ""
}
