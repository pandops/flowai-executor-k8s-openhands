package platform

import (
	"strings"
	"testing"
)

// TestExecutorTypeK8sOpenHandsWireValue pins the canonical
// executor_type wire value to the concrete service directory name
// and the executor_<runtime>_<tool> convention. Renaming the wire
// value (or the directory) without updating the other will fail this
// test. The v0005 task 9 contract pins the wire value to
// executor_k8s_openhands.
func TestExecutorTypeK8sOpenHandsWireValue(t *testing.T) {
	t.Parallel()
	const want = "executor_k8s_openhands"
	if ExecutorTypeK8sOpenHands != want {
		t.Fatalf("ExecutorTypeK8sOpenHands = %q, want %q", ExecutorTypeK8sOpenHands, want)
	}
	if !strings.HasPrefix(ExecutorTypeK8sOpenHands, "executor_") {
		t.Fatalf("wire value %q must start with executor_", ExecutorTypeK8sOpenHands)
	}
}
