package k8sopenhands_test

// Requirement: Concrete Executors enforce max_parallel_tasks slots locally (rows 297–299).

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

// Scenario: Local capacity is reached (row 297).
func TestLocalCapacityIsReached(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 2; c.FinishedCleanupDelay = 20 * time.Second }))
	h.start()
	h.waitConnected()
	a, b := h.enqueueTask(), h.enqueueTask()
	h.waitRunning(a)
	h.waitRunning(b)
	before := len(h.taskFetches())
	c := h.enqueueTask()
	h.sim(30 * time.Second)
	if len(h.taskFetches()) != before || c.Q.Settled() || h.kube.Count() != 2 {
		t.Fatalf("pulled or started beyond capacity")
	}
	// The slot frees only after the terminal event and its cleanup delay.
	h.finish(a, "finished")
	h.waitKind(a, "finished")
	h.sim(10 * time.Second)
	if c.Q.Settled() {
		t.Fatalf("pulled during the cleanup delay")
	}
	h.waitRunning(c)
}

// Scenario: Pending task occupies a slot (row 298).
func TestPendingTaskOccupiesASlot(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1 }))
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "no_active_model"}), nil
	})
	h.start()
	h.waitConnected()
	p := h.enqueueTask(func(m map[string]any) { m["pending_limit_seconds"] = 90 })
	h.waitKind(p, "pending")
	next := h.enqueueTask()
	h.sim(60 * time.Second)
	if next.Q.Settled() || h.eventOf(next, "created") != nil {
		t.Fatalf("pulled while the pending claim holds the only slot")
	}
	h.waitKind(p, "released")
	h.waitKind(next, "created")
}

// Scenario: Capacity observation is informational (row 299).
func TestCapacityObservationIsInformational(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1 }))
	// The reply carries extra fields that look like capacity decisions.
	h.setHeartbeat(func(r fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{
			"connection_state": "connected", "claims_known": true, "claims": claimRefs(r),
			"max_parallel_tasks": 5, "slots_in_use": 0, "accept_tasks": true,
		}), nil
	})
	h.start()
	h.waitConnected()
	a := h.enqueueTask()
	h.waitRunning(a)
	h.waitSlots(1)
	b := h.enqueueTask()
	h.sim(45 * time.Second)
	if b.Q.Settled() {
		t.Fatalf("pulled because a reply field suggested capacity")
	}
	h.finish(a, "finished")
	h.waitRunning(b)
}
