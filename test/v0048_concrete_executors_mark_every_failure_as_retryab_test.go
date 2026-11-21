package k8sopenhands_test

// Requirement: Concrete Executors mark every failure as retryable or not (rows 276–277).

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
)

// Scenario: Runtime failure is retryable (row 276). K8s analogue of the
// Docker container exit: every failure after the checks passed carries
// retryable = true.
func TestRuntimeFailureIsRetryable(t *testing.T) {
	cases := map[string]struct {
		act    func(h *harness, tk task)
		reason string
	}{
		"agent container restarts": {func(h *harness, tk task) {
			p, _ := h.pod(tk)
			h.kube.IncrementRestart(p.Spec.Name)
		}, "pod_restarted_before_finish"},
		"pod fails": {func(h *harness, tk task) {
			p, _ := h.pod(tk)
			h.kube.SetStatus(p.Spec.Name, k8sclient.PodStatusFailed)
		}, "pod_failed"},
		"openhands error":  {func(h *harness, tk task) { h.finish(tk, "error") }, "openhands_error"},
		"openhands stuck":  {func(h *harness, tk task) { h.finish(tk, "stuck") }, "openhands_stuck"},
		"openhands failed": {func(h *harness, tk task) { h.finish(tk, "failed") }, "openhands_failed"},
		"openhands paused": {func(h *harness, tk task) { h.finish(tk, "paused") }, "openhands_paused"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.start()
			h.waitConnected()
			tk := h.enqueueTask()
			h.waitRunning(tk)
			c.act(h, tk)
			f := h.waitKind(tk, "failed")
			if f["failure_reason"] != c.reason || f["retryable"] != true {
				t.Fatalf("failed %v, want %s retryable", f, c.reason)
			}
			if got := h.terminalKinds(tk); len(got) != 1 {
				t.Fatalf("terminal events %v", got)
			}
		})
	}
}

// Scenario: Image cannot be pulled (row 276, image_pull_failed is retryable).
func TestImagePullFailureIsRetryable(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(tk); return ok }, "Pod created")
	p, _ := h.pod(tk)
	h.kube.SetWaitingReason(p.Spec.Name, "ImagePullBackOff")
	f := h.waitKind(tk, "failed")
	if f["failure_reason"] != "image_pull_failed" || f["retryable"] != true {
		t.Fatalf("failed %v", f)
	}
	if h.hasKind(tk, "running") {
		t.Fatalf("running published for an image that never started")
	}
}

// Scenario: Configuration error is not retryable (row 277).
func TestConfigurationErrorIsNotRetryable(t *testing.T) {
	h := newHarness(t)
	h.setKeyEdge(func(string) (int, any) {
		return 200, map[string]any{"OPENAI_BASE_URL": "http://litellm-gateway-data:4000/v1", "OPENAI_API_KEY": "sk-task-litellm-key"}
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	f := h.waitKind(tk, "failed")
	if f["failure_reason"] != "llm_configuration_incomplete" || f["retryable"] != false {
		t.Fatalf("failed %v", f)
	}
	if !h.noPodCreated(tk) || len(h.oh.Conversations()) != 0 {
		t.Fatalf("runtime started")
	}
}
