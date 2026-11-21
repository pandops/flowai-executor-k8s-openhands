package k8sopenhands_test

// Requirement: Docker Executor remains local and non-authoritative (rows 321–322), K8s analogue.

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// Scenario: Executor does not own task selection or tenancy (row 321).
func TestExecutorDoesNotOwnTaskSelectionOrTenancy(t *testing.T) {
	h := newHarness(t)
	// Messages on another team's or tag's consumer are never read.
	other := h.newTask()
	h.br.Enqueue("TASK_MANAGER_TASK", "tasks-4-"+tagHash(tag), other.Subject, nil, mustMarshal(other.Msg))
	h.br.Enqueue("TASK_MANAGER_TASK", "tasks-3-"+tagHash("cpu"), other.Subject, nil, mustMarshal(other.Msg))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	for _, f := range h.br.Fetches("") {
		if f.Durable != taskDurable() && f.Durable != "commands-7" {
			t.Fatalf("pulled from %s", f.Durable)
		}
	}
	// The claim-token answer is the only claim decision: once it names the
	// winner, nothing else is consulted before the checks.
	if len(h.claimTokenRequests()) != 1 {
		t.Fatalf("%d claim decisions requested", len(h.claimTokenRequests()))
	}
}

// Scenario: Executor does not persist task history or secrets (row 322).
func TestExecutorDoesNotPersistTaskHistoryOrSecrets(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	h.crash() // keep the cache as it is mid-run

	var files []string
	for _, dir := range []string{h.cfg.CacheDir, filepath.Dir(h.cfg.Path)} {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	allowed := map[string]bool{"executor.db": true, "executor.lock": true, "executor.state": true, "config.yaml": true}
	for _, f := range files {
		if !allowed[filepath.Base(f)] {
			t.Fatalf("unexpected durable file %s", f)
		}
		data, _ := os.ReadFile(f)
		secrets := []string{"launch-secret-value", "https://git.example.com/acme/app.git", "sk-task-litellm-key", "claim-token-"}
		if filepath.Base(f) != "config.yaml" {
			secrets = append(secrets, h.token)
		}
		if s := containsAny(string(data), secrets...); s != "" {
			t.Fatalf("%s holds %q", f, s)
		}
	}
	// Values reach only the task's runtime.
	if c := h.kube.Creates(); len(c) != 1 || c[0].Env["DB_PASSWORD"] != "launch-secret-value" {
		t.Fatalf("Pod env %v", h.kube.Creates())
	}
}
