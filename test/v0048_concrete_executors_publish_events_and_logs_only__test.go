package k8sopenhands_test

// Requirement: Concrete Executors publish events and logs only through the broker (rows 278–284).

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/seqfile"
)

// maxSeq returns the highest publisher_seq of every stored event.
func (h *harness) maxSeq() int64 {
	var m int64
	for _, r := range h.publishedRecords() {
		if s := num(r.JSON()["publisher_seq"]); s > m {
			m = s
		}
	}
	return m
}

// Scenario: Executor restart continues its publisher sequence (row 279;
// message-broker row 550).
func TestExecutorRestartContinuesItsPublisherSequence(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")
	if err := h.sigterm(); err != nil {
		t.Fatal(err)
	}
	last := h.maxSeq()
	before := len(h.publishedRecords())

	h.start()
	h.waitSelf("executor_started")
	eventually(t, 10*time.Second, func() bool { return len(h.events("executor_started")) == 2 }, "second executor_started")
	for _, r := range h.publishedRecords()[before:] {
		seq := num(r.JSON()["publisher_seq"])
		if seq <= last {
			t.Fatalf("new message reuses publisher_seq %d (last before restart %d)", seq, last)
		}
		if want := fmt.Sprintf("executor-3-7-%d", seq); r.Header["Nats-Msg-Id"] != want {
			t.Fatalf("Nats-Msg-Id %q, want %q", r.Header["Nats-Msg-Id"], want)
		}
	}
}

// Scenario: Counter is persisted before a publish (row 280).
func TestCounterIsPersistedBeforeAPublish(t *testing.T) {
	h := newHarness(t)
	var mu sync.Mutex
	var bad []string
	prev := h.br.OnPublish
	h.br.OnPublish = func(r fakebroker.Record) {
		prev(r)
		seq := num(r.JSON()["publisher_seq"])
		got, err := seqfile.Read(h.cfg.StateFile)
		if err != nil || got < seq {
			mu.Lock()
			bad = append(bad, fmt.Sprintf("publish %d with state file %d (%v)", seq, got, err))
			mu.Unlock()
		}
	}
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.crash()
	mu.Lock()
	if len(bad) > 0 {
		t.Fatalf("counter not persisted before publish: %v", bad)
	}
	mu.Unlock()
	last := h.maxSeq()
	before := len(h.publishedRecords())
	h.start()
	h.waitKind(tk, "released")
	for _, r := range h.publishedRecords()[before:] {
		if seq := num(r.JSON()["publisher_seq"]); seq <= last {
			t.Fatalf("after the crash a new message reuses %d", seq)
		}
	}
}

// Scenario: Executor records terminal state (row 281).
func TestExecutorRecordsTerminalState(t *testing.T) {
	for _, status := range []string{"finished", "error"} {
		t.Run(status, func(t *testing.T) {
			h := newHarness(t)
			h.start()
			h.waitConnected()
			tk := h.enqueueTask()
			h.finish(tk, status)
			kind := "finished"
			if status != "finished" {
				kind = "failed"
			}
			term := h.waitKind(tk, kind)
			h.sim(20 * time.Second)
			if got := h.terminalKinds(tk); len(got) != 1 {
				t.Fatalf("terminal events %v", got)
			}
			if num(term["publisher_seq"]) <= num(h.eventOf(tk, "running")["publisher_seq"]) {
				t.Fatalf("terminal publisher_seq not above running")
			}
			var prevSeq int64
			var prevAt string
			for _, e := range h.taskEvents(tk) {
				if num(e["publisher_seq"]) <= prevSeq || e["occurred_at"].(string) < prevAt {
					t.Fatalf("events of the claim out of order: %v", h.taskEvents(tk))
				}
				prevSeq, prevAt = num(e["publisher_seq"]), e["occurred_at"].(string)
			}
			h.waitSlots(0)
		})
	}
}

// Scenario: Executor records lifecycle self events (row 282).
func TestExecutorRecordsLifecycleSelfEvents(t *testing.T) {
	h := newHarness(t)
	h.start()
	started := h.waitSelf("executor_started")
	if started["executor_type"] != "executor_k8s_openhands" || num(started["team_id"]) != 3 || num(started["executor_id"]) != 7 {
		t.Fatalf("executor_started %v", started)
	}
	if err := h.sigterm(); err != nil {
		t.Fatal(err)
	}
	stopping, stopped := h.events("executor_stopping"), h.events("executor_stopped")
	if len(stopping) != 1 || len(stopped) != 1 {
		t.Fatalf("stopping %d, stopped %d", len(stopping), len(stopped))
	}
	if num(stopping[0]["publisher_seq"]) >= num(stopped[0]["publisher_seq"]) {
		t.Fatalf("stopped before stopping")
	}

	// Failure: Kubernetes becomes unreachable.
	h2 := newHarness(t)
	h2.start()
	h2.waitConnected()
	h2.kube.SetHealthy(false)
	f := h2.waitSelf("executor_failed")
	if f["phase"] == nil || f["phase"] == "" || num(f["executor_id"]) != 7 {
		t.Fatalf("executor_failed %v", f)
	}
}

// Scenario: Events carry no secret material (row 283; message-broker row 601).
func TestEventsCarryNoSecretMaterial(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.push(tk, map[string]any{"kind": "MessageEvent", "source": "agent", "llm_message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "Working on it"}}}})
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")
	h.sigterm()

	secrets := []string{"launch-secret-value", "https://git.example.com/acme/app.git", "sk-task-litellm-key", fmt.Sprintf("claim-token-%d", tk.TaskID), h.token}
	if s := containsAny(h.allBrokerBytes(), secrets...); s != "" {
		t.Fatalf("broker message carries %q", s)
	}
	if s := containsAny(h.logs.String(), secrets...); s != "" {
		t.Fatalf("log carries %q", s)
	}
	for _, r := range h.publishedRecords() {
		if _, ok := r.JSON()["event_id"]; ok {
			t.Fatalf("event carries event_id: %s", r.Data)
		}
	}
	// The values reach only the runtime.
	creates := h.kube.Creates()
	if len(creates) != 1 || !hasString(creates[0].Env, "launch-secret-value") || !hasString(creates[0].Env, "sk-task-litellm-key") {
		t.Fatalf("Pod env lacks the opened values")
	}
}

// Scenario: Executor publishes only its own release reasons (row 284).
func TestExecutorPublishesOnlyItsOwnReleaseReasons(t *testing.T) {
	t.Run("pending_timeout", func(t *testing.T) {
		h := newHarness(t)
		h.setCheck(func(fakebroker.Record) (*broker.Reply, error) {
			return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "no_active_model"}), nil
		})
		h.start()
		h.waitConnected()
		tk := h.enqueueTask(func(m map[string]any) { m["pending_limit_seconds"] = 60 })
		if r := h.waitKind(tk, "released"); r["reason"] != "pending_timeout" {
			t.Fatalf("reason %v", r["reason"])
		}
	})
	t.Run("executor_restarted", func(t *testing.T) {
		h := newHarness(t)
		h.start()
		h.waitConnected()
		tk := h.enqueueTask()
		h.waitRunning(tk)
		h.crash()
		h.start()
		if r := h.waitKind(tk, "released"); r["reason"] != "executor_restarted" {
			t.Fatalf("reason %v", r["reason"])
		}
	})
	t.Run("executor_shutdown", func(t *testing.T) {
		h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 3; c.DrainTimeoutSeconds = 5 }))
		var created task
		h.br.Respond("task-manager.executor.*.claim-token.pub", func(r fakebroker.Record) (*broker.Reply, error) {
			if num(body(r)["task_id"]) == created.TaskID {
				return fakebroker.ErrorReply(409, "claim_not_processed"), nil
			}
			return claimTokenReply(r, ""), nil
		})
		h.setCheck(func(r fakebroker.Record) (*broker.Reply, error) {
			if num(body(r)["task_id"]) == 1002 {
				return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "budget_exhausted"}), nil
			}
			return fakebroker.JSONReply(200, map[string]any{"result": "ok", "model_alias": "3/m1"}), nil
		})
		h.start()
		h.waitConnected()
		running := h.enqueueTask() // 1001
		h.waitRunning(running)
		pending := h.enqueueTask() // 1002
		h.waitKind(pending, "pending")
		created = h.newTask()
		h.enqueue(created)
		eventually(t, 10*time.Second, func() bool { return len(h.claimTokenRequests()) >= 3 }, "created claim asking for its token")
		if err := h.sigterm(); err != nil {
			t.Fatal(err)
		}
		for _, tk := range []task{running, pending, created} {
			r := h.eventOf(tk, "released")
			if r == nil || r["reason"] != "executor_shutdown" {
				t.Fatalf("task %d released %v", tk.TaskID, r)
			}
		}
	})
	t.Run("never task-manager reasons", func(t *testing.T) {
		h := newHarness(t, withFactor(1000), withConfig(func(c *configT) { c.ConnectionWait = 60 * time.Second }))
		h.start()
		h.waitConnected()
		tk := h.enqueueTask()
		h.waitRunning(tk)
		h.dropHeartbeats()
		h.sim(90 * time.Second)
		h.setHeartbeat(func(r fakebroker.Record) (*broker.Reply, error) {
			return fakebroker.JSONReply(200, map[string]any{"connection_state": "connected", "claims_known": true, "claims": []any{}}), nil
		})
		h.waitSelf("executor_connection_restored")
		for _, e := range h.events("released") {
			if e["reason"] == "executor_lost" || e["reason"] == "claim_missing" {
				t.Fatalf("published released %v", e["reason"])
			}
		}
		for _, k := range []string{"connection_lost", "connection_restored", "executor_lost"} {
			if len(h.events(k)) != 0 {
				t.Fatalf("published executor-registry kind %s", k)
			}
		}
	})
}
