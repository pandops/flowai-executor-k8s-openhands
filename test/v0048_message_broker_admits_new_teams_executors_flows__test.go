package k8sopenhands_test

// message-broker: message-broker admits new teams, Executors, Flows and
// Sources without a restart (row 592), the Executor's part.

import (
	"testing"
	"time"
)

// Scenario: Executor is created without a restart (row 592).
func TestExecutorIsCreatedWithoutARestart(t *testing.T) {
	h := newHarness(t)
	h.token = makeJWT(teamID, 12, tag)
	h.cfg.Broker.Token = h.token
	h.start()
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > 0 }, "Executor 12 pulls tasks-3-<tag_hash>")
	cs := h.br.Creates()
	if len(cs) == 0 || cs[0].Subject != "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-12.task-manager.command.12.sub" {
		t.Fatalf("Executor 12 creates %v", cs)
	}
	for _, r := range h.br.Requests("$JS.API.>") {
		if r.Subject != cs[0].Subject {
			t.Fatalf("requested %s: only its own command consumer create is allowed", r.Subject)
		}
	}
	if h.br.LastDial().InboxPrefix != "_INBOX.executor-12" {
		t.Fatalf("inbox %q", h.br.LastDial().InboxPrefix)
	}
}
