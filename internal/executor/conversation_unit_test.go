package executor

import "testing"

func TestConversationStatusFindsNestedExecutionStatus(t *testing.T) {
	event := map[string]any{"kind": "ConversationStateUpdateEvent", "payload": map[string]any{"execution_status": " FINISHED "}}
	if got := conversationStatus(event); got != "finished" {
		t.Fatalf("conversationStatus() = %q", got)
	}
}
