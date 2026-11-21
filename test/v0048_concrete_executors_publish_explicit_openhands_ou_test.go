package k8sopenhands_test

// Requirement: Concrete Executors publish explicit OpenHands output (rows 342–343).

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Scenario: OpenHands publishes assistant and tool output (row 342).
func TestOpenHandsPublishesAssistantAndToolOutput(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.push(tk, map[string]any{"kind": "MessageEvent", "source": "agent", "llm_message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "I will run the tests"}}}})
	h.push(tk, map[string]any{"kind": "ObservationEvent", "source": "environment", "observation": map[string]any{"content": []any{map[string]any{"type": "text", "text": "12 passed"}}}})
	h.push(tk, map[string]any{"kind": "MessageEvent", "source": "user", "llm_message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "user input"}}}})
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")

	var logs []map[string]any
	for _, e := range h.taskEvents(tk) {
		if e["kind"] == "log" {
			logs = append(logs, e)
		}
	}
	if len(logs) != 2 || logs[0]["stream"] != "reasoning" || logs[0]["text"] != "I will run the tests" || logs[1]["stream"] != "work" || logs[1]["text"] != "12 passed" {
		t.Fatalf("logs %v", logs)
	}
	if num(logs[1]["publisher_seq"]) >= num(h.eventOf(tk, "finished")["publisher_seq"]) || num(logs[0]["publisher_seq"]) == num(logs[1]["publisher_seq"]) {
		t.Fatalf("log order or sequence wrong")
	}
	for _, l := range logs {
		if _, ok := l["chunk_id"]; ok {
			t.Fatalf("log carries a chunk identifier")
		}
	}
}

// An action and its observation publish the command and its output once; a
// repeated command (a new tool call) is published again.
func TestOpenHandsActionAndObservationPublishCommandAndOutputOnce(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	frames := []any{
		map[string]any{"kind": "ActionEvent", "source": "agent", "tool_name": "terminal", "tool_call_id": "call-1",
			"thought": []any{map[string]any{"type": "text", "text": "I'll list the project."}},
			"action":  map[string]any{"kind": "TerminalAction", "command": "pwd && ls -la"}},
		map[string]any{"kind": "ObservationEvent", "source": "environment", "tool_name": "terminal", "tool_call_id": "call-1",
			"observation": map[string]any{"kind": "TerminalObservation", "command": "pwd && ls -la", "exit_code": 0,
				"content": []any{map[string]any{"type": "text", "text": "/workspace/project"}}}},
		map[string]any{"kind": "ActionEvent", "source": "agent", "tool_name": "terminal", "tool_call_id": "call-2",
			"action": map[string]any{"kind": "TerminalAction", "command": "pwd && ls -la"}},
		map[string]any{"kind": "ObservationEvent", "source": "environment", "tool_name": "terminal", "tool_call_id": "call-2",
			"observation": map[string]any{"kind": "TerminalObservation", "command": "pwd && ls -la", "exit_code": 0,
				"content": []any{map[string]any{"type": "text", "text": "/workspace/project"}}}},
		map[string]any{"kind": "ActionEvent", "source": "agent", "tool_name": "finish", "tool_call_id": "call-3",
			"action": map[string]any{"kind": "FinishAction", "message": "Done: marker written."}},
		map[string]any{"kind": "ObservationEvent", "source": "environment", "tool_name": "finish", "tool_call_id": "call-3",
			"observation": map[string]any{"kind": "FinishObservation", "content": []any{map[string]any{"type": "text", "text": "Done: marker written."}}}},
	}
	want := [][2]string{{"reasoning", "I'll list the project."}, {"work", "pwd && ls -la"}, {"work", "/workspace/project"},
		{"work", "pwd && ls -la"}, {"work", "/workspace/project"}, {"work", "Done: marker written."}}
	for _, f := range frames {
		h.push(tk, f)
	}
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")
	var got [][2]string
	for _, e := range h.taskEvents(tk) {
		if e["kind"] == "log" {
			got = append(got, [2]string{e["stream"].(string), e["text"].(string)})
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("logs = %q, want %q (a repeated command runs again; an echo of the same tool call is not repeated)", got, want)
	}
}

// Scenario: OpenHands emits only protocol state (row 343).
func TestOpenHandsEmitsOnlyProtocolState(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.push(tk, map[string]any{"kind": "ConversationStateUpdateEvent", "key": "execution_status", "value": "running"})
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")
	h.sim(5 * time.Second)
	if h.hasKind(tk, "log") {
		t.Fatalf("protocol state published as a log: %v", h.kinds(tk))
	}
}

// A failed log publish is logged and changes no lifecycle event.
func TestLogPublishFailureDoesNotChangeTheLifecycle(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.br.LoseAcksOn(evPrefix+"log.pub", 1000)
	h.push(tk, map[string]any{"kind": "MessageEvent", "source": "agent", "llm_message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "x"}}}})
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")
	if got := h.terminalKinds(tk); len(got) != 1 {
		t.Fatalf("terminal %v", got)
	}
	eventually(t, 10*time.Second, func() bool { return strings.Contains(h.logs.String(), "event_publish_failed") }, "publish failure logged")
}
