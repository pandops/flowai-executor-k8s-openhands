package k8sopenhands_test

// Requirement: Docker Executor shuts down gracefully (rows 314–317), K8s
// analogue of each scenario.

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

// Scenario: SIGTERM during idle polling (row 314).
func TestSIGTERMDuringIdlePolling(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	if err := h.sigterm(); err != nil {
		t.Fatalf("exit: %v", err)
	}
	if len(h.events("executor_stopping")) != 1 || len(h.events("executor_stopped")) != 1 {
		t.Fatalf("stopping %d stopped %d", len(h.events("executor_stopping")), len(h.events("executor_stopped")))
	}
}

// Scenario: SIGTERM during in-flight tasks (row 315): one task fails within
// the drain and is reported normally; the other is released at the deadline.
func TestSIGTERMDuringInFlightTasks(t *testing.T) {
	h := newHarness(t)
	a, b, ca, _ := twoRunning(h)
	done := make(chan error, 1)
	go func() { done <- h.sigterm() }()
	h.waitSelf("executor_stopping")
	_ = h.oh.PushTerminal(ca, "error")
	<-done
	if f := h.eventOf(a, "failed"); f == nil || f["failure_reason"] != "openhands_error" {
		t.Fatalf("task a %v", h.kinds(a))
	}
	if r := h.eventOf(b, "released"); r == nil || r["reason"] != "executor_shutdown" {
		t.Fatalf("task b %v", h.kinds(b))
	}
	if h.kube.Count() != 0 {
		t.Fatalf("Pods left %v", h.kube.PodNames())
	}
	for _, tk := range []task{a, b} {
		if n := len(h.terminalKinds(tk)); n != 1 {
			t.Fatalf("task %d has %d terminal events", tk.TaskID, n)
		}
	}
}

// Scenario: SIGTERM with created and pending claims (row 316).
func TestSIGTERMWithCreatedAndPendingClaims(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	var createdID int64
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(r fakebroker.Record) (*broker.Reply, error) {
		if num(body(r)["task_id"]) == createdID {
			select {
			case <-release:
			case <-time.After(5 * time.Second):
			}
			return fakebroker.ErrorReply(409, "claim_not_processed"), nil
		}
		return claimTokenReply(r, ""), nil
	})
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "no_active_model"}), nil
	})
	h.start()
	h.waitConnected()
	pending := h.enqueueTask()
	h.waitKind(pending, "pending")
	created := h.newTask()
	createdID = created.TaskID
	h.enqueue(created)
	h.waitKind(created, "created")
	start := time.Now()
	err := h.sigterm()
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > h.clk.Real(20*time.Second)+5*time.Second {
		t.Fatalf("waited for the drain: %v", took)
	}
	for _, tk := range []task{pending, created} {
		if r := h.eventOf(tk, "released"); r == nil || r["reason"] != "executor_shutdown" {
			t.Fatalf("task %d %v", tk.TaskID, h.kinds(tk))
		}
		if !h.noPodCreated(tk) {
			t.Fatalf("Pod for task %d", tk.TaskID)
		}
	}
}
