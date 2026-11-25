package k8sopenhands_test

// Requirement: K8s Executor runs only Kubernetes Pods (rows 324, 325, 328,
// 329). Rows 326 and 327 describe the Docker Executor only and are not
// applicable to executor_k8s_openhands.

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

func podDeleted(h *harness, name string) bool {
	for _, d := range h.kube.Deletes() {
		if d == name {
			return true
		}
	}
	return false
}

// Scenario: OpenHands completion precedes delayed Pod cleanup (row 324).
func TestOpenHandsCompletionPrecedesDelayedPodCleanup(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1; c.FinishedCleanupDelay = 20 * time.Second }))
	h.br.LoseAcksOn(evPrefix+"finished.pub", 1) // the first publish is not acknowledged
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.finish(tk, "finished")
	eventually(t, 10*time.Second, func() bool { return len(h.br.Attempts(evPrefix+"finished.pub")) >= 2 }, "acknowledged finished")
	acked := h.br.Attempts(evPrefix + "finished.pub")[1].At
	p, _ := h.pod(tk)
	next := h.enqueueTask()
	h.sim(10 * time.Second)
	if _, ok := h.pod(tk); !ok {
		t.Fatalf("Pod deleted before finished_cleanup_delay")
	}
	if next.Q.Settled() {
		t.Fatalf("retained Pod did not count against the slot")
	}
	eventually(t, 10*time.Second, func() bool { return podDeleted(h, p.Spec.Name) }, "Pod deleted after the delay")
	if d := h.clk.Now().Sub(acked); d < 20*time.Second {
		t.Fatalf("deleted %v after the acknowledgement", d)
	}
	// Pod exit after the delay publishes nothing more.
	h.sim(10 * time.Second)
	if got := h.terminalKinds(tk); len(got) != 1 || got[0] != "finished" {
		t.Fatalf("terminal events %v", got)
	}
	h.waitRunning(next)
}

// A Pod that exits successfully without an OpenHands finished is not success.
func TestPodExitCodeIsNotTheCompletionSignal(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	p, _ := h.pod(tk)
	h.kube.SetStatus(p.Spec.Name, k8sclient.PodStatusSucceeded)
	f := h.waitKind(tk, "failed")
	if f["retryable"] != true || h.hasKind(tk, "finished") {
		t.Fatalf("Pod success reported as %v", h.kinds(tk))
	}
}

// Scenario: Pod restarts before OpenHands finishes (row 325).
func TestPodRestartsBeforeOpenHandsFinishes(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.FailedCleanupDelay = 15 * time.Second }))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	p, _ := h.pod(tk)
	starts := len(h.oh.Conversations())
	h.kube.IncrementRestart(p.Spec.Name)
	f := h.waitKind(tk, "failed")
	if f["failure_reason"] != "pod_restarted_before_finish" || f["retryable"] != true {
		t.Fatalf("failed %v", f)
	}
	h.sim(5 * time.Second)
	if _, ok := h.pod(tk); !ok {
		t.Fatalf("Pod deleted before failed_cleanup_delay")
	}
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(tk); return !ok }, "Pod deleted after the delay")
	if len(h.oh.Conversations()) != starts || h.podCreates(tk) != 1 {
		t.Fatalf("execution resumed or recreated")
	}
	if got := h.terminalKinds(tk); len(got) != 1 {
		t.Fatalf("terminal events %v", got)
	}
}

// Scenario: K8s Executor starts a Pod for a won claim (row 328).
func TestK8sExecutorStartsAPodForAWonClaim(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	if h.podCreates(tk) != 1 {
		t.Fatalf("%d Pods for one claim", h.podCreates(tk))
	}
	p, _ := h.pod(tk)
	if p.Spec.Image != "registry.example.com/team-3/runtime:v1" {
		t.Fatalf("image %q", p.Spec.Image)
	}
	for _, k := range []string{"flowai.executor_id", "flowai.team_id", "flowai.task_id", "flowai.attempt", "flowai.claim_seq", "flowai.resolved_image_source", "flowai.runtime"} {
		if p.Spec.Labels[k] == "" {
			t.Fatalf("label %s missing: %v", k, p.Spec.Labels)
		}
	}
	if o := h.orderOf(tk, "running"); o == 0 || len(h.oh.Conversations()) == 0 {
		t.Fatalf("running not published after the agent started")
	}
}

// Scenario: Another Executor already claimed the task (row 329).
func TestAnotherExecutorAlreadyClaimedTheTask(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1 }))
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.ErrorReply(409, "claim_rejected"), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	eventually(t, 10*time.Second, func() bool { return len(h.claimTokenRequests()) > 0 }, "claim-token request")
	h.waitSlots(0)
	if !h.noPodCreated(tk) || h.hasKind(tk, "running") {
		t.Fatalf("Pod or running for a rejected claim")
	}
}

// TestK8sPodCarriesLiteLLMDataLabels keeps the v0041 behavior: every task Pod
// carries the configured LiteLLM data-plane labels.
func TestK8sPodCarriesLiteLLMDataLabels(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) {
		c.LiteLLMDataPodLabels = map[string]string{"flowai.io/litellm-data": "client", "team.example/role": "agent"}
	}))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	p, _ := h.pod(tk)
	if p.Spec.Labels["flowai.io/litellm-data"] != "client" || p.Spec.Labels["team.example/role"] != "agent" {
		t.Fatalf("labels %v", p.Spec.Labels)
	}
	if p.Spec.Labels["flowai.runtime"] != "k8s" {
		t.Fatalf("LiteLLM labels replaced the identity labels: %v", p.Spec.Labels)
	}

}
