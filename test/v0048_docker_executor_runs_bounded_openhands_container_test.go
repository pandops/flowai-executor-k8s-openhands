package k8sopenhands_test

// Requirement: Docker Executor runs bounded OpenHands containers (rows 304–308),
// written as the K8s analogue (agents.md rule 13): the K8s Executor runs
// bounded task Pods the same way.

import (
	"fmt"
	"testing"
	"time"
)

// Scenario: Executor starts a same-team, same-tag task up to capacity (row 304).
func TestExecutorStartsASameTeamSameTagTaskUpToCapacity(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask(func(m map[string]any) {
		m["image_source"] = "flow_default"
		m["resolved_image"] = "registry.example.com/flow-2/runtime:v7"
	})
	h.waitRunning(tk)
	creates := h.kube.Creates()
	if len(creates) != 1 {
		t.Fatalf("%d Pods", len(creates))
	}
	spec := creates[0]
	if spec.Image != "registry.example.com/flow-2/runtime:v7" || spec.Namespace != namespace {
		t.Fatalf("Pod image %q namespace %q", spec.Image, spec.Namespace)
	}
	want := map[string]string{
		"flowai.executor_id": "7", "flowai.team_id": "3", "flowai.task_id": fmt.Sprint(tk.TaskID),
		"flowai.attempt": "1", "flowai.claim_seq": "1", "flowai.resolved_image_source": "flow_default", "flowai.runtime": "k8s",
	}
	for k, v := range want {
		if spec.Labels[k] != v {
			t.Fatalf("label %s = %q, want %q (labels %v)", k, spec.Labels[k], v, spec.Labels)
		}
	}
	if r := h.eventOf(tk, "running"); r["image_source"] != nil && r["image_source"] != "flow_default" {
		t.Fatalf("running image_source %v", r["image_source"])
	}
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")
}

// Scenario: Executor rejects a different image than task-manager resolved (row 305).
func TestExecutorRejectsADifferentImageThanTaskManagerResolved(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask(func(m map[string]any) { m["resolved_image"] = "registry.example.com/other/unknown:v9" })
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(tk); return ok }, "Pod created")
	p, _ := h.pod(tk)
	if p.Spec.Image != "registry.example.com/other/unknown:v9" {
		t.Fatalf("image %q, want the resolved image verbatim", p.Spec.Image)
	}
	h.kube.SetWaitingReason(p.Spec.Name, "ErrImagePull")
	f := h.waitKind(tk, "failed")
	if f["failure_reason"] != "image_pull_failed" || f["retryable"] != true {
		t.Fatalf("failed %v", f)
	}
	if h.podCreates(tk) != 1 {
		t.Fatalf("substituted another image: %d Pods", h.podCreates(tk))
	}
}

// Scenario: Executor respects FIFO (row 306).
func TestExecutorRespectsFIFO(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1 }))
	tasks := []task{h.enqueueTask(), h.enqueueTask(), h.enqueueTask()}
	h.start()
	for _, tk := range tasks {
		h.finish(tk, "finished")
		h.waitKind(tk, "finished")
	}
	var order []int64
	for _, r := range h.claimTokenRequests() {
		order = append(order, num(body(r)["task_id"]))
	}
	for i, tk := range tasks {
		if order[i] != tk.TaskID {
			t.Fatalf("claim order %v", order)
		}
	}
}

// Scenario: Executor leaves excess tasks (row 307).
func TestExecutorLeavesExcessTasks(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 2 }))
	var tasks []task
	for i := 0; i < 5; i++ {
		tasks = append(tasks, h.enqueueTask())
	}
	h.start()
	h.waitRunning(tasks[0])
	h.waitRunning(tasks[1])
	h.sim(30 * time.Second)
	for _, f := range h.taskFetches() {
		if f.Batch > 2 {
			t.Fatalf("pull batch %d with two slots", f.Batch)
		}
	}
	for _, tk := range tasks[2:] {
		if tk.Q.Settled() {
			t.Fatalf("task %d taken beyond the free slots", tk.TaskID)
		}
	}
}

// Scenario: Executor reports capacity as an observation (row 308).
func TestExecutorReportsCapacityAsAnObservation(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.waitSlots(1)
	if b := h.lastHeartbeat(); num(b["max_parallel_tasks"]) != 2 {
		t.Fatalf("max_parallel_tasks %v", b["max_parallel_tasks"])
	}
}
