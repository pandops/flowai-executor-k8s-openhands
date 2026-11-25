package executor

import (
	"encoding/json"
	"testing"
)

// The pinned OpenHands agent-server reports a LiteLLM budget rejection as a
// ConversationErrorEvent with code LLMBadRequestError and a detail naming the
// budget (svc/litellm-gateway/qualification/README.md); rate limits and other
// errors are not budget failures.
func TestBudgetExceededConversationError(t *testing.T) {
	cases := map[string]bool{
		`{"kind":"ConversationErrorEvent","code":"LLMBadRequestError","detail":"litellm.BadRequestError: OpenAIException - Budget has been exceeded! Key=key (sk-...****) Current cost: 0.01, Max budget: 0.01"}`: true,
		`{"event":{"kind":"ConversationErrorEvent","code":"LLMBadRequestError","detail":"Budget has been exceeded! Team=t Current cost: 0.1, Max budget: 0.1"}}`:                                                  true,
		`{"kind":"ConversationErrorEvent","code":"LLMRateLimitError","detail":"Rate limit exceeded"}`:                                                                                                             false,
		`{"kind":"ConversationErrorEvent","code":"LLMBadRequestError","detail":"model not found"}`:                                                                                                                false,
		`{"kind":"MessageEvent","detail":"Budget has been exceeded!"}`:                                                                                                                                            false,
	}
	for raw, want := range cases {
		var value any
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatal(err)
		}
		if got := isBudgetExceededError(value); got != want {
			t.Errorf("isBudgetExceededError(%s) = %v, want %v", raw, got, want)
		}
	}
}

// The code of the latest OpenHands conversation error is reported with a
// failed task, so an operator can tell a rate limit from a provider error.
func TestConversationErrorCode(t *testing.T) {
	for raw, want := range map[string]string{
		`{"event":{"kind":"ConversationErrorEvent","code":"LLMRateLimitError","detail":"x"}}`: "LLMRateLimitError",
		`{"kind":"ConversationErrorEvent","code":"LLMBadRequestError"}`:                       "LLMBadRequestError",
		`{"kind":"MessageEvent","code":"LLMRateLimitError"}`:                                  "",
	} {
		var value any
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatal(err)
		}
		if got := conversationErrorCode(value); got != want {
			t.Errorf("conversationErrorCode(%s) = %q, want %q", raw, got, want)
		}
	}
}
