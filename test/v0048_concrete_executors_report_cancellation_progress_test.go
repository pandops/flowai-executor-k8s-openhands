package k8sopenhands_test

// Requirement: Concrete Executors report cancellation progress (rows 344–346).

import (
	"errors"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

// Scenario: Cancellation reaches OpenHands (row 344).
func TestCancellationReachesOpenHands(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	conv := h.waitRunning(tk)
	p, _ := h.pod(tk)
	cmd := h.cancelCommand(tk, 77)
	c := h.waitKind(tk, "cancelled")
	eventually(t, 10*time.Second, cmd.Acked, "command acknowledged")
	ack := h.eventOf(tk, "control_acknowledged")
	if ack == nil || num(ack["control_id"]) != 77 || ack["control"] != "cancel" || num(c["control_id"]) != 77 {
		t.Fatalf("control_acknowledged %v cancelled %v", ack, c)
	}
	if h.orderOf(tk, "control_acknowledged") > h.orderOf(tk, "cancelled") {
		t.Fatalf("cancelled before control_acknowledged")
	}
	if h.oh.Paused(conv) == 0 || !podDeleted(h, p.Spec.Name) {
		t.Fatalf("agent not paused or Pod not deleted")
	}
	var ackOrder int64
	for _, s := range h.br.Settles() {
		if s.Subject == "task-manager.command.7.sub" && s.Kind == "ack" {
			ackOrder = s.Order
		}
	}
	if ackOrder < h.orderOf(tk, "cancelled") {
		t.Fatalf("command acknowledged before cancelled")
	}
	if got := h.terminalKinds(tk); len(got) != 1 {
		t.Fatalf("terminal %v (our own pause must not produce failed)", got)
	}
	h.waitSlots(0)
}

// Scenario: Cancellation delivery fails (row 345).
func TestCancellationDeliveryFails(t *testing.T) {
	h := newHarness(t)
	h.oh.PauseErr = errors.New("agent server busy")
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	cmd := h.cancelCommand(tk, 78)
	f := h.waitKind(tk, "control_failed")
	eventually(t, 10*time.Second, cmd.Acked, "command acknowledged")
	if num(f["control_id"]) != 78 || f["reason"] == nil || f["reason"] == "" {
		t.Fatalf("control_failed %v", f)
	}
	h.sim(10 * time.Second)
	if h.hasKind(tk, "cancelled") {
		t.Fatalf("cancelled published")
	}
	if _, ok := h.pod(tk); !ok {
		t.Fatalf("task stopped")
	}
}

// Scenario: Cancel of a pending claim (row 346).
func TestCancelOfAPendingClaim(t *testing.T) {
	h := newHarness(t)
	h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "no_active_model"}), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitKind(tk, "pending")
	cmd := h.cancelCommand(tk, 79)
	c := h.waitKind(tk, "cancelled")
	eventually(t, 10*time.Second, cmd.Acked, "command acknowledged")
	if num(c["control_id"]) != 79 {
		t.Fatalf("cancelled %v", c)
	}
	checks := len(h.checkRequests())
	h.sim(65 * time.Second)
	if len(h.checkRequests()) != checks || !h.noPodCreated(tk) {
		t.Fatalf("checks continued or a Pod started")
	}
	h.waitSlots(0)
}

// A cancel for a claim the Executor does not hold is only acknowledged.
func TestCancelOfAnUnknownClaimIsOnlyAcknowledged(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	cmd := h.cancelCommand(task{TaskID: 999, Attempt: 1, ClaimSeq: 1}, 80)
	eventually(t, 10*time.Second, cmd.Acked, "command acknowledged")
	h.sim(5 * time.Second)
	for _, k := range []string{"control_acknowledged", "control_failed", "cancelled"} {
		if len(h.events(k)) != 0 {
			t.Fatalf("published %s for an unknown claim", k)
		}
	}
}
