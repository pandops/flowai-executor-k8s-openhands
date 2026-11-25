package k8sopenhands_test

// Requirement: Concrete Executors reconcile after restart without reassignment (rows 331–335).

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/cache"
)

// Scenario: K8s Executor restarts mid-run (row 331).
func TestK8sExecutorRestartsMidRun(t *testing.T) {
	h := newHarness(t, withFactor(400))
	h.start()
	h.waitConnected()
	a, b := h.enqueueTask(), h.enqueueTask()
	ca, cb := h.waitRunning(a), h.waitRunning(b)
	// One log chunk of b stays unsent in the outbox at the crash.
	h.br.FailPublishes(1000)
	h.push(b, map[string]any{"kind": "ObservationEvent", "source": "environment", "observation": map[string]any{"content": []any{map[string]any{"type": "text", "text": "tests passed"}}}})
	eventually(t, 10*time.Second, func() bool { return len(h.br.Attempts(evPrefix+"log.pub")) > 0 }, "log publish attempted")
	unsent := h.br.Attempts(evPrefix + "log.pub")[0]
	h.crash()
	h.br.FailPublishes(0)
	h.sim(3 * time.Minute)
	pa, _ := h.pod(a)
	pb, _ := h.pod(b)
	fetchesBefore := len(h.taskFetches())
	h.start()
	ra, rb := h.waitKind(a, "released"), h.waitKind(b, "released")
	if ra["reason"] != "executor_restarted" || rb["reason"] != "executor_restarted" {
		t.Fatalf("reasons %v %v", ra["reason"], rb["reason"])
	}
	if !podDeleted(h, pa.Spec.Name) || !podDeleted(h, pb.Spec.Name) {
		t.Fatalf("cached Pods not deleted")
	}
	// The pending log chunk is republished with its original sequence before released.
	logs := h.br.Published(evPrefix + "log.pub")
	if len(logs) != 1 || num(logs[0].JSON()["publisher_seq"]) != num(unsent.JSON()["publisher_seq"]) || logs[0].Header["Nats-Msg-Id"] != unsent.Header["Nats-Msg-Id"] {
		t.Fatalf("outbox not republished with its original publisher_seq: %v", logs)
	}
	if logs[0].Order > h.orderOf(b, "released") {
		t.Fatalf("released before the pending outbox event")
	}
	if h.oh.Resumed(ca) != 0 || h.oh.Resumed(cb) != 0 {
		t.Fatalf("continued an OpenHands conversation")
	}
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > fetchesBefore }, "pull")
	if first := h.taskFetches()[fetchesBefore].Order; first < h.orderOf(a, "released") || first < h.orderOf(b, "released") {
		t.Fatalf("pulled before reconciling")
	}
	if h.br.LastDial().Token != h.token {
		t.Fatalf("did not reconnect with the stored JWT")
	}
}

// Scenario: Restart after W drops cached claims (row 332).
func TestRestartAfterWDropsCachedClaims(t *testing.T) {
	h := newHarness(t, withFactor(1000))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.br.FailPublishes(1000)
	h.push(tk, map[string]any{"kind": "MessageEvent", "source": "agent", "llm_message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "hi"}}}})
	eventually(t, 10*time.Second, func() bool { return len(h.br.Attempts(evPrefix+"log.pub")) > 0 }, "log attempted")
	h.crash()
	h.br.FailPublishes(0)
	h.sim(12 * time.Minute)
	before := len(h.publishedRecords())
	h.start()
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(tk); return !ok }, "Pod deleted")
	h.waitHeartbeats(len(h.heartbeats()) + 2)
	for _, r := range h.publishedRecords()[before:] {
		if num(r.JSON()["task_id"]) == tk.TaskID {
			t.Fatalf("published %s for a claim older than W", r.JSON()["kind"])
		}
	}
	if len(h.heldClaims()) != 0 {
		t.Fatalf("holds %v", h.heldClaims())
	}
	next := h.enqueueTask()
	h.waitRunning(next)
}

// Scenario: Docker Executor keeps its token and cache on volumes (row 333),
// K8s analogue: the configuration and the cache live on the PVC and survive
// a Pod restart.
func TestDockerExecutorKeepsItsTokenAndCacheOnVolumes(t *testing.T) {
	h := newHarness(t)
	rendered := *h.cfg
	h.installAndLoad(&rendered)
	h.start()
	h.waitConnected()
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > 0 }, "first pull")
	db := filepath.Join(h.cfg.CacheDir, "executor.db")
	st, err := os.Stat(db)
	if err != nil {
		t.Fatalf("no cache before the first pull: %v", err)
	}
	if st.Mode().Perm()&0o077 != 0 {
		t.Fatalf("cache mode %v is not owner-only", st.Mode().Perm())
	}
	if d, _ := os.Stat(h.cfg.CacheDir); d.Mode().Perm()&0o077 != 0 {
		t.Fatalf("cache dir mode %v", d.Mode().Perm())
	}
	if err := h.sigterm(); err != nil {
		t.Fatal(err)
	}
	st2, _ := os.Stat(db)
	h.start()
	h.waitConnected()
	if st3, _ := os.Stat(db); !os.SameFile(st2, st3) {
		t.Fatalf("cache recreated instead of reused")
	}
	if h.br.LastDial().Token != h.token {
		t.Fatalf("token not reused")
	}
}

// Scenario: Second process cannot use the same cache volume (row 334).
func TestSecondProcessCannotUseTheSameCacheVolume(t *testing.T) {
	h := newHarness(t)
	if err := os.MkdirAll(h.cfg.CacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := cache.Lock(h.cfg.CacheDir)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	defer lock.Release()
	h.kube.AddPod(podOf(namespace, "leftover", map[string]string{"flowai.executor_id": "7", "flowai.runtime": "k8s"}))
	h.start()
	err, exited := h.waitExit(5 * time.Second)
	if !exited || err == nil {
		t.Fatalf("second process exited=%v err=%v", exited, err)
	}
	if h.probe("/v1/readyz") != 503 {
		t.Fatalf("second process ready")
	}
	if h.br.DialCount() != 0 || len(h.kube.Deletes()) != 0 || len(h.kube.Creates()) != 0 {
		t.Fatalf("second process touched the broker or Pods")
	}
}

// A corrupt cache fails closed and leaves Pods for operator recovery.
func TestCorruptCacheFailsClosed(t *testing.T) {
	h := newHarness(t)
	if err := os.MkdirAll(h.cfg.CacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.cfg.CacheDir, "executor.db"), []byte("not a bbolt file at all, corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.kube.AddPod(podOf(namespace, "survivor", map[string]string{"flowai.executor_id": "7", "flowai.runtime": "k8s"}))
	h.start()
	err, exited := h.waitExit(5 * time.Second)
	if !exited || err == nil {
		t.Fatalf("corrupt cache: exited=%v err=%v", exited, err)
	}
	if h.br.DialCount() != 0 || len(h.kube.Deletes()) != 0 {
		t.Fatalf("acted on a corrupt cache")
	}
}

// Scenario: Restart does not duplicate a running event (row 335).
func TestRestartDoesNotDuplicateARunningEvent(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.crash()
	creates := len(h.kube.Creates())
	h.start()
	h.waitKind(tk, "released")
	h.sim(20 * time.Second)
	if n := len(h.br.Published(evPrefix + "running.pub")); n != 1 {
		t.Fatalf("%d running events", n)
	}
	if len(h.kube.Creates()) != creates {
		t.Fatalf("additional Pod created")
	}
	if got := h.terminalKinds(tk); len(got) != 1 || got[0] != "released" {
		t.Fatalf("terminal events %v", got)
	}
}
