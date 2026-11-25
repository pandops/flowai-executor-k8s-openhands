package k8sopenhands_test

// Requirement: Docker Executor cleans up on startup (rows 311–313), K8s
// analogue: leftover task Pods are found by the Executor's labels.

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
)

// Scenario: Executor finds leftover containers on startup (row 311).
func TestExecutorFindsLeftoverContainersOnStartup(t *testing.T) {
	h := newHarness(t)
	h.kube.AddPod(k8sclient.PodSpec{Name: "leftover-1", Namespace: namespace, Image: "x", Labels: map[string]string{
		"flowai.executor_id": "7", "flowai.runtime": "k8s", "flowai.task_id": "55", "flowai.attempt": "1", "flowai.claim_seq": "1", "flowai.team_id": "3",
	}})
	h.kube.AddPod(k8sclient.PodSpec{Name: "other-executor", Namespace: namespace, Image: "x", Labels: map[string]string{
		"flowai.executor_id": "8", "flowai.runtime": "k8s", "flowai.task_id": "56", "flowai.team_id": "3",
	}})
	h.enqueueTask()
	h.start()
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > 0 }, "first pull")
	deleteIdx := -1
	orders := h.kube.DeleteOrders()
	for i, d := range h.kube.Deletes() {
		if d == "leftover-1" {
			deleteIdx = i
		}
		if d == "other-executor" {
			t.Fatalf("deleted another Executor's Pod")
		}
	}
	if deleteIdx < 0 {
		t.Fatalf("leftover Pod not deleted: %v", h.kube.PodNames())
	}
	if first := h.taskFetches()[0].Order; orders[deleteIdx] > first {
		t.Fatalf("leftover Pod deleted (order %d) after the first task pull (order %d)", orders[deleteIdx], first)
	}
	for _, n := range h.kube.PodNames() {
		if n == "leftover-1" {
			t.Fatalf("leftover Pod still present")
		}
	}
	if len(h.events("released")) != 0 {
		t.Fatalf("released published for an uncached Pod")
	}
}

// Scenario: Restart within W releases cached claims (row 312).
func TestRestartWithinWReleasesCachedClaims(t *testing.T) {
	h := newHarness(t, withFactor(400))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.crash()
	h.sim(2 * time.Minute)
	fetchesBefore := len(h.taskFetches())
	h.start()
	r := h.waitKind(tk, "released")
	if r["reason"] != "executor_restarted" {
		t.Fatalf("reason %v", r["reason"])
	}
	if _, ok := h.pod(tk); ok {
		t.Fatalf("cached claim's Pod kept")
	}
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > fetchesBefore }, "pull")
	if o := h.orderOf(tk, "released"); o > h.taskFetches()[fetchesBefore].Order {
		t.Fatalf("released after the first pull")
	}
	h.waitHeartbeats(len(h.heartbeats()) + 1)
	if len(h.heldClaims()) != 0 {
		t.Fatalf("holds claims %v", h.heldClaims())
	}
}

// Scenario: Start after a graceful shutdown releases nothing again (row 313).
func TestStartAfterAGracefulShutdownReleasesNothingAgain(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.DrainTimeoutSeconds = 2 }))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	if err := h.sigterm(); err != nil {
		t.Fatal(err)
	}
	if r := h.eventOf(tk, "released"); r == nil || r["reason"] != "executor_shutdown" {
		t.Fatalf("shutdown released %v", h.kinds(tk))
	}
	before := len(h.publishedRecords())
	h.start()
	eventually(t, 10*time.Second, func() bool { return len(h.events("executor_started")) == 2 }, "restart")
	h.sim(30 * time.Second)
	for _, r := range h.publishedRecords()[before:] {
		if r.JSON()["kind"] == "released" {
			t.Fatalf("released again after a graceful shutdown: %s", r.Data)
		}
	}
}
