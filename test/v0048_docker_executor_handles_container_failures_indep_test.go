package k8sopenhands_test

// Requirement: Docker Executor handles container failures independently
// (rows 309–310), K8s analogue: task Pods fail independently.

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
)

// Scenario: One OpenHands container exits unexpectedly (row 309).
func TestOneOpenHandsContainerExitsUnexpectedly(t *testing.T) {
	h := newHarness(t)
	a, b, _, cb := twoRunning(h)
	pa, _ := h.pod(a)
	h.kube.SetStatus(pa.Spec.Name, k8sclient.PodStatusFailed)
	f := h.waitKind(a, "failed")
	if f["failure_reason"] != "pod_failed" || f["retryable"] != true {
		t.Fatalf("failed %v", f)
	}
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(a); return !ok }, "failed Pod deleted")
	if _, ok := h.pod(b); !ok || len(h.terminalKinds(b)) != 0 || h.oh.Paused(cb) != 0 {
		t.Fatalf("other task disturbed")
	}
	next := h.enqueueTask()
	h.waitRunning(next)
}

// Scenario: Docker daemon or Executor process fails (row 310): the
// Kubernetes API becomes unreachable.
func TestDockerDaemonOrExecutorProcessFails(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	h.kube.SetHealthy(false)
	h.waitSelf("executor_failed")
	fetches := len(h.taskFetches())
	queued := h.enqueueTask()
	h.sim(30 * time.Second)
	if len(h.taskFetches()) != fetches || queued.Q.Settled() {
		t.Fatalf("pulled while Kubernetes is unreachable")
	}
	if h.probe("/v1/readyz") != 503 {
		t.Fatalf("ready while Kubernetes is unreachable")
	}
}
