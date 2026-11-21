package k8sopenhands_test

// Requirement: Concrete Executors create their own command consumer (rows 255–257).

import (
	"strings"
	"testing"
	"time"
)

const commandCreateSubject = "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-7.task-manager.command.7.sub"

// Scenario: Executor creates its own command consumer on first connect (row 255).
func TestExecutorCreatesItsOwnCommandConsumerOnFirstConnect(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > 0 && len(h.br.Fetches("commands-7")) > 0 }, "task and command pulls")

	creates := h.br.Creates()
	if len(creates) == 0 {
		t.Fatal("no consumer create")
	}
	for _, c := range creates {
		if c.Subject != commandCreateSubject {
			t.Fatalf("consumer create on %s", c.Subject)
		}
	}
	c := creates[0]
	if c.Order > h.taskFetches()[0].Order {
		t.Fatalf("command consumer created after the first task pull")
	}
	if c.Body["stream_name"] != "TASK_MANAGER_COMMAND" {
		t.Fatalf("stream_name %v", c.Body["stream_name"])
	}
	cfg, _ := c.Body["config"].(map[string]any)
	want := map[string]any{
		"durable_name":       "commands-7",
		"filter_subject":     "task-manager.command.7.sub",
		"deliver_policy":     "all",
		"ack_policy":         "explicit",
		"ack_wait":           float64(30 * time.Second),
		"inactive_threshold": float64(24 * time.Hour),
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Fatalf("consumer config %s = %#v, want %#v (config %v)", k, cfg[k], v, cfg)
		}
	}
	if md, ok := cfg["max_deliver"]; ok && md != float64(-1) && md != float64(0) {
		t.Fatalf("max_deliver = %v, want unlimited", md)
	}
	for _, r := range h.br.Requests("$JS.API.>") {
		if !strings.HasPrefix(r.Subject, "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-7.") {
			t.Fatalf("requested JetStream API %s", r.Subject)
		}
	}
	if fetches := h.br.Fetches("commands-7"); fetches[0].Stream != "TASK_MANAGER_COMMAND" {
		t.Fatalf("command pull from stream %s", fetches[0].Stream)
	}
}
