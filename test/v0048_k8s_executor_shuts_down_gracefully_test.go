package k8sopenhands_test

// Requirement: K8s Executor shuts down gracefully (rows 300–303).

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

// Scenario: K8s SIGTERM during in-flight tasks (row 300).
func TestK8sSIGTERMDuringInFlightTasks(t *testing.T) {
	h := newHarness(t)
	a, b, ca, _ := twoRunning(h)
	done := make(chan error, 1)
	go func() { done <- h.sigterm() }()
	h.waitSelf("executor_stopping")
	hbs, fetches := len(h.heartbeats()), len(h.taskFetches())
	h.sim(5 * time.Second)
	if err := h.oh.PushTerminal(ca, "finished"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if f := h.eventOf(a, "finished"); f == nil {
		t.Fatalf("finished not published for the task that finished in the drain: %v", h.kinds(a))
	}
	r := h.eventOf(b, "released")
	if r == nil || r["reason"] != "executor_shutdown" {
		t.Fatalf("other claim %v", h.kinds(b))
	}
	stopping := h.events("executor_stopping")[0]
	rt, _ := time.Parse(time.RFC3339Nano, r["occurred_at"].(string))
	st, _ := time.Parse(time.RFC3339Nano, stopping["occurred_at"].(string))
	if d := rt.Sub(st); d < 25*time.Second || d > 40*time.Second {
		t.Fatalf("released %v after stopping, want the 30 s drain deadline", d)
	}
	if len(h.heartbeats()) > hbs+1 || len(h.taskFetches()) > fetches+1 {
		t.Fatalf("heartbeats or pulls continued during the drain")
	}
	if h.kube.Count() != 0 {
		t.Fatalf("Pods left: %v", h.kube.PodNames())
	}
	stopped := h.events("executor_stopped")
	if len(stopped) != 1 || num(stopped[0]["publisher_seq"]) < num(r["publisher_seq"]) {
		t.Fatalf("executor_stopped %v not last", stopped)
	}
}

// Scenario: K8s SIGTERM with a pending claim (row 301).
func TestK8sSIGTERMWithAPendingClaim(t *testing.T) {
	h := newHarness(t)
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "budget_exhausted"}), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitKind(tk, "pending")
	start := time.Now()
	if err := h.sigterm(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > h.clk.Real(20*time.Second) {
		t.Fatalf("pending claim waited for the drain (%v)", took)
	}
	if r := h.eventOf(tk, "released"); r == nil || r["reason"] != "executor_shutdown" {
		t.Fatalf("pending claim %v", h.kinds(tk))
	}
	if !h.noPodCreated(tk) {
		t.Fatalf("Pod created for a pending claim")
	}
}

// Scenario: K8s drain timeout sets the termination grace period (row 302).
// The chart's terminationGracePeriodSeconds is deployment and not covered
// (infrastructure, exercised by the E2E full flow; owner, 2026-11-09); the
// Executor's own drain timing stays.
func TestK8sDrainTimeoutSetsTheTerminationGracePeriod(t *testing.T) {
	// The Executor drains for at most 45 s, and 30 s without the key.
	for _, drain := range []int{45, 0} {
		h := newHarness(t, withConfig(func(c *configT) { c.DrainTimeoutSeconds = drain }))
		if drain == 0 {
			rendered := *h.cfg
			h.installAndLoad(&rendered, "drain_timeout_seconds")
			if h.cfg.DrainTimeoutSeconds != 30 {
				t.Fatalf("default drain_timeout_seconds %d, want 30", h.cfg.DrainTimeoutSeconds)
			}
		}
		h.start()
		h.waitConnected()
		tk := h.enqueueTask()
		h.waitRunning(tk)
		if err := h.sigterm(); err != nil {
			t.Fatal(err)
		}
		want := time.Duration(drain) * time.Second
		if drain == 0 {
			want = 30 * time.Second
		}
		st, _ := time.Parse(time.RFC3339Nano, h.events("executor_stopping")[0]["occurred_at"].(string))
		rt, _ := time.Parse(time.RFC3339Nano, h.eventOf(tk, "released")["occurred_at"].(string))
		if d := rt.Sub(st); d < want-5*time.Second || d > want+10*time.Second {
			t.Fatalf("drain %d: released after %v, want %v", drain, d, want)
		}
	}
}
