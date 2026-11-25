package k8sopenhands_test

// Requirement: Docker OpenHands concrete service uses the correctly spelled
// identifier (row 323), K8s analogue: executor_k8s_openhands is used as the
// wire value everywhere.

import (
	"testing"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

// Scenario: Docker OpenHands starts under the corrected wire value (row 323).
func TestDockerOpenHandsStartsUnderTheCorrectedWireValue(t *testing.T) {
	if platform.ExecutorTypeK8sOpenHands != "executor_k8s_openhands" || platform.ServiceName != "executor_k8s_openhands" {
		t.Fatalf("constants %q %q", platform.ExecutorTypeK8sOpenHands, platform.ServiceName)
	}
	h := newHarness(t)
	h.start()
	h.waitConnected()
	if s := h.waitSelf("executor_started"); s["executor_type"] != "executor_k8s_openhands" {
		t.Fatalf("executor_started %v", s)
	}
	if b := h.lastHeartbeat(); b["executor_type"] != "executor_k8s_openhands" {
		t.Fatalf("heartbeat executor_type %v", b["executor_type"])
	}
}

// TestExecutorTypeK8sOpenHandsWireValue pins the Go constant and its wire value.
func TestExecutorTypeK8sOpenHandsWireValue(t *testing.T) {
	if platform.ExecutorTypeK8sOpenHands != "executor_k8s_openhands" {
		t.Fatalf("ExecutorTypeK8sOpenHands = %q", platform.ExecutorTypeK8sOpenHands)
	}
}
