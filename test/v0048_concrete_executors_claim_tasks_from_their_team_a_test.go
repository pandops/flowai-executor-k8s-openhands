package k8sopenhands_test

// Requirement: Concrete Executors claim tasks from their team and tag consumer (rows 258–266).

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/clock"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/executor"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
	mclock "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/clock"
)

// Scenario: Executor wins a claim (row 258).
func TestExecutorWinsAClaim(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)

	created := h.eventOf(tk, "created")
	if created == nil || num(created["attempt"]) != 1 || num(created["claim_seq"]) != 1 {
		t.Fatalf("created %v", created)
	}
	if !tk.Q.Acked() {
		t.Fatalf("task message not acknowledged")
	}
	token := fmt.Sprintf("claim-token-%d-1-1", tk.TaskID)
	for _, suffix := range []string{"/launch-parameters/open", "/litellm-key"} {
		calls := h.edgeRequests(suffix)
		if len(calls) == 0 || calls[0].Authorization != "Bearer "+token {
			t.Fatalf("edge %s calls %v, want Bearer %s", suffix, calls, token)
		}
	}
	h.waitSlots(1)
}

// Scenario: Executor loses a claim (row 259).
func TestExecutorLosesAClaim(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 1 }))
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.ErrorReply(409, "claim_rejected"), nil
	})
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	eventually(t, 10*time.Second, func() bool { return len(h.claimTokenRequests()) > 0 }, "claim-token request")
	h.sim(30 * time.Second)

	if got := h.kinds(tk); len(got) != 1 || got[0] != "created" {
		t.Fatalf("lost claim events %v, want only created", got)
	}
	if !h.noPodCreated(tk) {
		t.Fatalf("Pod created for a lost claim")
	}
	h.waitSlots(0)
	next := h.enqueueTask()
	h.waitKind(next, "created")
}

// Scenario: Claim token requested before any check (row 260).
func TestClaimTokenRequestedBeforeAnyCheck(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)

	createdOrder := h.orderOf(tk, "created")
	var ackOrder int64
	for _, s := range h.br.Settles() {
		if s.Subject == tk.Subject && s.Kind == "ack" {
			ackOrder = s.Order
		}
	}
	reqs := h.claimTokenRequests()
	if len(reqs) == 0 {
		t.Fatal("no claim-token request")
	}
	req := reqs[0]
	if createdOrder <= 0 || createdOrder >= ackOrder || ackOrder >= req.Order {
		t.Fatalf("order created=%d ack=%d claim-token=%d, want created < ack < claim-token", createdOrder, ackOrder, req.Order)
	}
	b := body(req)
	created := h.eventOf(tk, "created")
	if num(b["task_id"]) != tk.TaskID || num(b["attempt"]) != 1 || num(b["claim_seq"]) != 1 || num(b["created_publisher_seq"]) != num(created["publisher_seq"]) {
		t.Fatalf("claim-token body %v, created publisher_seq %v", b, created["publisher_seq"])
	}
	if c := h.checkRequests(); len(c) == 0 || c[0].Order < req.Order {
		t.Fatalf("model check before the claim-token request")
	}
}

// Scenario: task-manager has not yet processed the created event (row 261).
func TestTaskManagerHasNotYetProcessedTheCreatedEvent(t *testing.T) {
	m := mclock.NewManual(time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC))
	h := newHarness(t, withManualClock(m), withConfig(func(c *configT) { c.PollInterval = manualPollInterval }))
	var calls atomic.Int64
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(r fakebroker.Record) (*broker.Reply, error) {
		if calls.Add(1) <= 3 {
			return fakebroker.ErrorReply(409, "claim_not_processed"), nil
		}
		return claimTokenReply(r, "late-token"), nil
	})
	h.startWithClock(slowNow{m, hostPause})
	stop := freeRun(m)
	h.waitConnected()
	stop()
	tk := h.enqueueTask()
	h.stepClaimToken(m, time.Second, 20*time.Second, func() bool { return calls.Load() >= 4 })
	stop = freeRun(m)
	defer stop()
	h.waitRunning(tk)

	reqs := h.claimTokenRequests()
	if len(reqs) != 4 {
		t.Fatalf("%d claim-token requests, want 4 (first + three repeats)", len(reqs))
	}
	seq := num(body(reqs[0])["created_publisher_seq"])
	for i, r := range reqs {
		if num(body(r)["created_publisher_seq"]) != seq {
			t.Fatalf("repeat %d changed created_publisher_seq", i)
		}
		if i > 0 {
			gap := r.At.Sub(reqs[i-1].At)
			if gap < 800*time.Millisecond || gap > 1500*time.Millisecond {
				t.Fatalf("repeat gap %v, want 1 s", gap)
			}
		}
	}
	if c := h.checkRequests(); len(c) == 0 || c[0].Order < reqs[3].Order {
		t.Fatalf("checks did not wait for the token")
	}
	if calls := h.edgeRequests("/litellm-key"); calls[0].Authorization != "Bearer late-token" {
		t.Fatalf("edge used %q", calls[0].Authorization)
	}
}

// Scenario: Claim token never arrives within 10 s (row 262).
func TestClaimTokenNeverArrivesWithin10s(t *testing.T) {
	h, tk := claimTokenNeverArrives(t, 0)
	reqs := h.claimTokenRequests()
	if len(reqs) < 9 || len(reqs) > 12 {
		t.Fatalf("%d claim-token requests in 25 s, want about 10 (one per second for 10 s)", len(reqs))
	}
	assertClaimTokenDropped(t, h, tk, reqs)
}

// Scenario: Claim token never arrives within 10 s (row 262), with the
// Executor's timers firing late, as on a loaded host: every 1 s wait ends
// 0.6 s late. No claim-token request is sent once 10 s have passed since the
// first one, and the claim is still dropped.
func TestClaimTokenNeverArrivesWithin10sWithLateTimers(t *testing.T) {
	h, tk := claimTokenNeverArrives(t, 600*time.Millisecond)
	reqs := h.claimTokenRequests()
	if len(reqs) < 2 {
		t.Fatalf("%d claim-token requests, want repeats", len(reqs))
	}
	assertClaimTokenDropped(t, h, tk, reqs)
}

// manualPollInterval is the Executor's poll interval on the manual clock:
// unlike the default 1 s it differs from the claim-token wait.
const manualPollInterval = 250 * time.Millisecond

// hostPause is how long the Executor is descheduled after every reading of
// the time on the manual clock: 0.5 s of the 100x scaled clock.
const hostPause = 5 * time.Millisecond

// claimTokenNeverArrives runs one claim whose claim-token requests are all
// answered claim_not_processed, on a manual clock, for 25 s from the pull,
// with the Executor's timers late by late.
func claimTokenNeverArrives(t *testing.T, late time.Duration) (*harness, task) {
	t.Helper()
	m := mclock.NewManual(time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC))
	h := newHarness(t, withManualClock(m), withConfig(func(c *configT) {
		c.MaxParallelTasks = 1
		c.PollInterval = manualPollInterval
	}))
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.ErrorReply(409, "claim_not_processed"), nil
	})
	h.startWithClock(lateTimers{slowNow{m, hostPause}, late})
	stop := freeRun(m)
	h.waitConnected()
	stop()
	tk := h.enqueueTask()
	dropped := func() bool { return strings.Contains(h.logs.String(), `"msg":"claim_dropped"`) }
	h.stepClaimToken(m, time.Second+late, 25*time.Second, dropped)
	stop = freeRun(m)
	t.Cleanup(stop)
	return h, tk
}

// assertClaimTokenDropped checks row 262 after claimTokenNeverArrives: no
// request at or after 10 s since the first, only created published, no
// check or Pod, the slot free.
func assertClaimTokenDropped(t *testing.T, h *harness, tk task, reqs []fakebroker.Record) {
	t.Helper()
	for _, r := range reqs[1:] {
		if after := r.At.Sub(reqs[0].At); after >= 10*time.Second {
			t.Fatalf("claim-token request sent %v after the first, want none at or after 10 s", after)
		}
	}
	if got := h.kinds(tk); len(got) != 1 {
		t.Fatalf("events %v, want only created", got)
	}
	if !h.noPodCreated(tk) || len(h.checkRequests()) != 0 {
		t.Fatalf("started checks or a Pod without a token")
	}
	h.waitSlots(0)
}

// lateTimers is an Executor clock whose timers fire late by a fixed amount.
type lateTimers struct {
	clock.Clock
	late time.Duration
}

func (c lateTimers) After(d time.Duration) <-chan time.Time { return c.Clock.After(d + c.late) }

// slowNow is the Executor's clock on a loaded host: the Executor is
// descheduled for pause (wall time) right after every reading of the time,
// so it acts on a reading that is already old.
type slowNow struct {
	clock.Clock
	pause time.Duration
}

func (c slowNow) Now() time.Time { t := c.Clock.Now(); time.Sleep(c.pause); return t }

// startWithClock is start with another Executor clock.
func (h *harness) startWithClock(clk clock.Clock) {
	h.t.Helper()
	logger := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h.exec = executor.New(executor.Options{
		Config: h.cfg, Dial: h.br.Dial, Kube: h.kube, HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Clock: clk, Net: h.net, Logger: logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan error, 1)
	exec, done := h.exec, h.done
	go func() { done <- exec.Run(ctx) }()
}

// Scenario: Executor pulls only up to free slots (row 263).
func TestExecutorPullsOnlyUpToFreeSlots(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 3 }))
	h.start()
	h.waitConnected()
	a, b := h.enqueueTask(), h.enqueueTask()
	h.waitRunning(a)
	h.waitRunning(b)
	before := len(h.taskFetches())
	var rest []task
	for i := 0; i < 5; i++ {
		rest = append(rest, h.enqueueTask())
	}
	h.waitKind(rest[0], "created")
	h.sim(30 * time.Second)

	for _, f := range h.taskFetches()[before:] {
		if f.Batch > 1 {
			t.Fatalf("pull batch %d with one free slot", f.Batch)
		}
	}
	for _, tk := range rest[1:] {
		if tk.Q.Settled() || h.eventOf(tk, "created") != nil {
			t.Fatalf("task %d taken beyond capacity", tk.TaskID)
		}
	}
}

// Scenario: Executor takes tasks in queue order (row 264).
// A's first claim-token reply is slow (wall time), as on a loaded host: the
// Executor does not move on to B before that exchange ends.
func TestExecutorTakesTasksInQueueOrder(t *testing.T) {
	h := newHarness(t)
	a := h.enqueueTask()
	b := h.enqueueTask()
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(r fakebroker.Record) (*broker.Reply, error) {
		if num(body(r)["task_id"]) == a.TaskID {
			time.Sleep(100 * time.Millisecond)
		}
		return claimTokenReply(r, ""), nil
	})
	h.start()
	h.waitRunning(a)
	h.waitRunning(b)
	if oa, ob := h.orderOf(a, "created"), h.orderOf(b, "created"); oa <= 0 || oa >= ob {
		t.Fatalf("created order A=%d B=%d", oa, ob)
	}
	reqs := h.claimTokenRequests()
	if num(body(reqs[0])["task_id"]) != a.TaskID {
		t.Fatalf("first claim-token request for %v", body(reqs[0])["task_id"])
	}
}

// Scenario: Task message with a non-integer identifier is refused (row 265).
func TestTaskMessageWithANonIntegerIdentifierIsRefused(t *testing.T) {
	cases := map[string]func(m map[string]any){
		"string task_id": func(m map[string]any) { m["task_id"] = "1010" },
		"zero task_id":   func(m map[string]any) { m["task_id"] = 0 },
		"zero flow_id":   func(m map[string]any) { m["flow_id"] = 0 },
		"float attempt":  func(m map[string]any) { m["attempt"] = 1.5 },
		"neg claim_seq":  func(m map[string]any) { m["claim_seq"] = -1 },
		"string team_id": func(m map[string]any) { m["team_id"] = "3" },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.start()
			h.waitConnected()
			tk := h.enqueueTask(edit)
			eventually(t, 10*time.Second, tk.Q.Termed, "message terminated")
			h.waitSelf("protocol_violation")
			if len(h.events("created")) != 0 || h.kube.Count() != 0 || len(h.claimTokenRequests()) != 0 {
				t.Fatalf("refused message produced created, a Pod or a claim-token request")
			}
		})
	}
}

// Scenario: Foreign task message is refused (row 266).
func TestForeignTaskMessageIsRefused(t *testing.T) {
	cases := map[string]func(m map[string]any){
		"other team": func(m map[string]any) { m["team_id"] = 4 },
		"other tag": func(m map[string]any) {
			m["required_tag"] = "cpu"
			m["execution"].(map[string]any)["required_tag"] = "cpu"
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.start()
			h.waitConnected()
			tk := h.enqueueTask(edit)
			eventually(t, 10*time.Second, tk.Q.Termed, "message terminated")
			pv := h.waitSelf("protocol_violation")
			if num(pv["team_id"]) != 3 {
				t.Fatalf("protocol_violation %v", pv)
			}
			if len(h.events("created")) != 0 || h.kube.Count() != 0 {
				t.Fatalf("foreign message produced created or a Pod")
			}
		})
	}
}
