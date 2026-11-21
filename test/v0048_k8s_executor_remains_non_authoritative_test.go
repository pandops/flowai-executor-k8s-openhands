package k8sopenhands_test

// Requirement: K8s Executor remains non-authoritative (row 330).

import (
	"testing"
	"time"
)

// Scenario: K8s Executor restarts (row 330).
func TestK8sExecutorRestarts(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.crash()
	creates := len(h.kube.Creates())
	h.start()
	r := h.waitKind(tk, "released")
	if r["reason"] != "executor_restarted" {
		t.Fatalf("reason %v", r["reason"])
	}
	h.sim(30 * time.Second)
	if len(h.kube.Creates()) != creates {
		t.Fatalf("created a Pod for a cached claim")
	}
	n := 0
	for _, k := range h.kinds(tk) {
		if k == "running" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d running events", n)
	}
	if tk.Q.Settled() && len(h.claimTokenRequests()) != 1 {
		t.Fatalf("claimed the cached task again")
	}
	if d := h.br.LastDial(); d.Token != h.token || d.InboxPrefix != "_INBOX.executor-7" {
		t.Fatalf("identity changed across the restart")
	}
	if f := h.taskFetches(); f[len(f)-1].Durable != taskDurable() {
		t.Fatalf("pulls from %s", f[len(f)-1].Durable)
	}
}
