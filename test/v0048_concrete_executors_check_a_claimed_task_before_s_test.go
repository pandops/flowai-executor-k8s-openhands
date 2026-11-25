package k8sopenhands_test

// Requirement: Concrete Executors check a claimed task before starting the agent (rows 267–275).

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

func (h *harness) setCheck(fn func(r fakebroker.Record) (*broker.Reply, error)) {
	h.br.Respond("model-registry.executor.*.check.pub", fn)
}

// pendingEvents returns the pending events of a claim.
func (h *harness) pendingEvents(tk task) []map[string]any {
	var out []map[string]any
	for _, e := range h.taskEvents(tk) {
		if e["kind"] == "pending" {
			out = append(out, e)
		}
	}
	return out
}

// Scenario: Model check answers claim_not_current (row 267).
func TestModelCheckAnswersClaimNotCurrent(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1 }))
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.ErrorReply(404, "claim_not_current"), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	eventually(t, 10*time.Second, func() bool { return len(h.checkRequests()) > 0 }, "model check")
	h.sim(40 * time.Second)
	if got := h.kinds(tk); len(got) != 1 || got[0] != "created" {
		t.Fatalf("events %v, want only created", got)
	}
	if !h.noPodCreated(tk) {
		t.Fatalf("Pod created")
	}
	h.waitSlots(0)
	for _, c := range h.heldClaims() {
		if num(c["task_id"]) == tk.TaskID {
			t.Fatalf("dropped claim still reported")
		}
	}
}

// Scenario: Paused models keep the task pending (row 269).
func TestPausedModelsKeepTheTaskPending(t *testing.T) {
	h := newHarness(t)
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "all_selected_models_paused"}), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	p := h.waitKind(tk, "pending")
	if p["reason"] != "all_selected_models_paused" {
		t.Fatalf("reason %v", p["reason"])
	}
	first := len(h.checkRequests())
	h.sim(35 * time.Second)
	if len(h.checkRequests()) <= first {
		t.Fatalf("no recheck after 30 s")
	}
	if !h.noPodCreated(tk) {
		t.Fatalf("Pod created")
	}
	h.waitSlots(1)
}

// Scenario: Model check carries no claim token (row 270; message-broker row 600).
func TestModelCheckCarriesNoClaimToken(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	r := h.checkRequests()[0]
	if r.Subject != "model-registry.executor.7.check.pub" {
		t.Fatalf("subject %s", r.Subject)
	}
	b := body(r)
	if num(b["task_id"]) != tk.TaskID || num(b["attempt"]) != 1 || num(b["claim_seq"]) != 1 || len(b) != 3 {
		t.Fatalf("check body %v, want exactly task_id, attempt, claim_seq", b)
	}
	if strings.Contains(string(r.Data), "claim-token-") {
		t.Fatalf("check carries the claim token: %s", r.Data)
	}
	for k, v := range r.Header {
		if strings.Contains(v, "claim-token-") || strings.EqualFold(k, "Authorization") {
			t.Fatalf("check header %s=%s", k, v)
		}
	}
	params, _ := r.JSON()["params"].(map[string]any)
	if len(params) != 0 {
		t.Fatalf("check params %v: the Executor must come only from the subject", params)
	}
}

// Scenario: Pending task starts once the reason clears (row 272).
func TestPendingTaskStartsOnceTheReasonClears(t *testing.T) {
	h := newHarness(t)
	var paused atomic.Bool
	paused.Store(true)
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		if paused.Load() {
			return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "budget_exhausted"}), nil
		}
		return fakebroker.JSONReply(200, map[string]any{"result": "ok", "model_alias": "3/m1"}), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitKind(tk, "pending")
	tokenReqs := len(h.claimTokenRequests())
	paused.Store(false)
	h.waitRunning(tk)
	got := h.kinds(tk)
	if got[0] != "created" || got[1] != "pending" || got[2] != "running" {
		t.Fatalf("events %v, want created, pending, running", got)
	}
	n := 0
	for _, k := range got {
		if k == "created" {
			n++
		}
	}
	if n != 1 || len(h.claimTokenRequests()) != tokenReqs {
		t.Fatalf("new created (%d) or claim-token request after the recheck", n)
	}
	if h.podCreates(tk) != 1 {
		t.Fatalf("%d Pods", h.podCreates(tk))
	}
}

// Scenario: Pending limit releases the task (row 273).
func TestPendingLimitReleasesTheTask(t *testing.T) {
	h := newHarness(t)
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "no_active_model"}), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask(func(m map[string]any) { m["pending_limit_seconds"] = 90 })
	p := h.waitKind(tk, "pending")
	rel := h.waitKind(tk, "released")
	if rel["reason"] != "pending_timeout" {
		t.Fatalf("reason %v", rel["reason"])
	}
	pt, _ := time.Parse(time.RFC3339Nano, p["occurred_at"].(string))
	rt, _ := time.Parse(time.RFC3339Nano, rel["occurred_at"].(string))
	if d := rt.Sub(pt); d < 85*time.Second || d > 125*time.Second {
		t.Fatalf("released %v after pending, want about 90 s", d)
	}
	if !h.noPodCreated(tk) {
		t.Fatalf("Pod created")
	}
	h.waitSlots(0)
	checks := len(h.checkRequests())
	h.sim(65 * time.Second)
	if len(h.checkRequests()) != checks {
		t.Fatalf("rechecked a released claim")
	}
}

// Scenario: Missing image fails without retry (row 274).
func TestMissingImageFailsWithoutRetry(t *testing.T) {
	for name, edit := range map[string]func(m map[string]any){
		"null":   func(m map[string]any) { m["resolved_image"] = nil },
		"absent": func(m map[string]any) { delete(m, "resolved_image") },
		"empty":  func(m map[string]any) { m["resolved_image"] = "" },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.start()
			h.waitConnected()
			tk := h.enqueueTask(edit)
			f := h.waitKind(tk, "failed")
			if f["failure_reason"] != "image_missing" || f["retryable"] != false {
				t.Fatalf("failed %v", f)
			}
			// The image check runs only after the claim token arrived.
			reqs := h.claimTokenRequests()
			if len(reqs) != 1 || num(body(reqs[0])["task_id"]) != tk.TaskID || reqs[0].Order > h.orderOf(tk, "failed") {
				t.Fatalf("claim-token request does not precede failed image_missing")
			}
			if !h.noPodCreated(tk) {
				t.Fatalf("Pod created")
			}
			h.waitSlots(0)
		})
	}
}

// Scenario: Missing required launch parameter fails without retry (row 275).
func TestMissingRequiredLaunchParameterFailsWithoutRetry(t *testing.T) {
	h := newHarness(t)
	h.setEnvOpen(func(string) (int, any) {
		return envError(422, "configuration_error", "missing_required_parameter")
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	f := h.waitKind(tk, "failed")
	if f["failure_reason"] != "missing_required_parameter" || f["retryable"] != false {
		t.Fatalf("failed %v", f)
	}
	if !h.noPodCreated(tk) || len(h.oh.Conversations()) != 0 {
		t.Fatalf("runtime started")
	}
	h.waitSlots(0)
}

// Row 260 / 274: a claim without an image whose token is rejected gets no
// image check result: nothing but created is published.
func TestMissingImageIsCheckedOnlyAfterTheClaimToken(t *testing.T) {
	h := newHarness(t)
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.ErrorReply(409, "claim_rejected"), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask(func(m map[string]any) { m["resolved_image"] = nil })
	eventually(t, 10*time.Second, func() bool { return len(h.claimTokenRequests()) > 0 }, "claim-token request")
	h.sim(20 * time.Second)
	if got := h.kinds(tk); len(got) != 1 || got[0] != "created" {
		t.Fatalf("events %v: the image was checked before the claim token", got)
	}
}
