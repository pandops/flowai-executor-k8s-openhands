package executor

import "strings"

// failureReasonBudgetExceeded is the failure_reason of a task whose agent was
// stopped by an exhausted per-task or flow budget.
const failureReasonBudgetExceeded = "llm_budget_exceeded"

// isBudgetExceededError reports whether an OpenHands event is the budget
// rejection recorded in the v0022 qualification: a ConversationErrorEvent
// with code LLMBadRequestError whose detail names the exceeded budget. The
// agent-server drops LiteLLM's structured budget_exceeded type, so the
// detail text is the discriminator; rate limits arrive as LLMRateLimitError.
func isBudgetExceededError(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		kind, _ := typed["kind"].(string)
		code, _ := typed["code"].(string)
		detail, _ := typed["detail"].(string)
		if kind == "ConversationErrorEvent" && code == "LLMBadRequestError" &&
			strings.Contains(detail, "Budget has been exceeded!") {
			return true
		}
		for _, nested := range typed {
			if isBudgetExceededError(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if isBudgetExceededError(nested) {
				return true
			}
		}
	}
	return false
}

// conversationErrorCode returns the code of an OpenHands
// ConversationErrorEvent, or "".
func conversationErrorCode(value any) string {
	switch typed := value.(type) {
	case map[string]any:
		if kind, _ := typed["kind"].(string); kind == "ConversationErrorEvent" {
			code, _ := typed["code"].(string)
			return code
		}
		for _, nested := range typed {
			if code := conversationErrorCode(nested); code != "" {
				return code
			}
		}
	case []any:
		for _, nested := range typed {
			if code := conversationErrorCode(nested); code != "" {
				return code
			}
		}
	}
	return ""
}

// The agent's retry policy for failed model calls: 8 retries waiting 8 s,
// doubling up to 60 s, outlast a one-minute RPM window several times over.
const (
	llmNumRetries   = 8
	llmRetryMinWait = 8
	llmRetryMaxWait = 60
)

func intPtr(v int) *int { return &v }
