package k8sopenhands_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

type configT = config.Config

// writeConfigYAML renders cfg as the chart would and returns the file path.
func writeConfigYAML(t *testing.T, cfg *config.Config, path string, omit ...string) string {
	t.Helper()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(omit) > 0 {
		var m map[string]any
		if err := yaml.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		for _, k := range omit {
			delete(m, k)
		}
		if data, err = yaml.Marshal(m); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// installAndLoad installs a chart-rendered configuration onto the
// writable volume path of h.cfg and makes the harness use the loaded file.
func (h *harness) installAndLoad(rendered *config.Config, omit ...string) {
	h.t.Helper()
	src := writeConfigYAML(h.t, rendered, filepath.Join(h.t.TempDir(), "secret", "config.yaml"), omit...)
	if err := config.InstallConfig(src, h.cfg.Path); err != nil {
		h.t.Fatalf("install config: %v", err)
	}
	loaded, err := config.Load(h.cfg.Path)
	if err != nil {
		h.t.Fatalf("load installed config: %v", err)
	}
	h.cfg = loaded
}

// selfEvents returns the self events of kind.
func (h *harness) selfEvents(kind string) []map[string]any { return h.events(kind) }

// publishedRecords returns every stored event publish in order.
func (h *harness) publishedRecords() []fakebroker.Record { return h.br.Published(evPrefix + "*.pub") }

// orderOf returns the broker order of the first stored event of a claim and kind (0 if none).
func (h *harness) orderOf(tk task, kind string) int64 {
	for _, r := range h.publishedRecords() {
		e := r.JSON()
		if e["kind"] == kind && num(e["task_id"]) == tk.TaskID && num(e["attempt"]) == tk.Attempt && num(e["claim_seq"]) == tk.ClaimSeq {
			return r.Order
		}
	}
	return 0
}

// firstOrder returns the order of the first record whose subject matches.
func firstOrder(recs []fakebroker.Record) int64 {
	if len(recs) == 0 {
		return 0
	}
	return recs[0].Order
}

// taskFetches returns the pulls from the Executor's task consumer.
func (h *harness) taskFetches() []fakebroker.Fetch { return h.br.Fetches(taskDurable()) }

func (h *harness) waitKind(tk task, kind string) map[string]any {
	h.t.Helper()
	eventually(h.t, 10*time.Second, func() bool { return h.eventOf(tk, kind) != nil }, fmt.Sprintf("%s for task %d", kind, tk.TaskID))
	return h.eventOf(tk, kind)
}

func (h *harness) waitSelf(kind string) map[string]any {
	h.t.Helper()
	eventually(h.t, 10*time.Second, func() bool { return len(h.events(kind)) > 0 }, kind)
	return h.events(kind)[0]
}

// terminalKinds counts terminal events of a claim.
func (h *harness) terminalKinds(tk task) []string {
	var out []string
	for _, k := range h.kinds(tk) {
		switch k {
		case "finished", "failed", "cancelled", "released":
			out = append(out, k)
		}
	}
	return out
}

// noPod asserts that no Pod was ever created for a task.
func (h *harness) noPodCreated(tk task) bool {
	for _, c := range h.kube.Creates() {
		if c.Labels["flowai.task_id"] == fmt.Sprint(tk.TaskID) {
			return false
		}
	}
	return true
}

func (h *harness) podCreates(tk task) int {
	n := 0
	for _, c := range h.kube.Creates() {
		if c.Labels["flowai.task_id"] == fmt.Sprint(tk.TaskID) {
			n++
		}
	}
	return n
}

// slotsInUse returns slots_in_use of the latest heartbeat.
func (h *harness) lastHeartbeat() map[string]any {
	hb := h.heartbeats()
	if len(hb) == 0 {
		return nil
	}
	return body(hb[len(hb)-1])
}

// waitSlots waits until a heartbeat reports n slots in use.
func (h *harness) waitSlots(n int64) {
	h.t.Helper()
	eventually(h.t, 10*time.Second, func() bool {
		b := h.lastHeartbeat()
		return b != nil && num(b["slots_in_use"]) == n
	}, fmt.Sprintf("heartbeat with %d slots in use", n))
}

// heldClaims returns the claims of the latest heartbeat.
func (h *harness) heldClaims() []map[string]any {
	b := h.lastHeartbeat()
	var out []map[string]any
	claims, _ := b["claims"].([]any)
	for _, c := range claims {
		out = append(out, c.(map[string]any))
	}
	return out
}

func containsAny(haystack string, needles ...string) string {
	for _, n := range needles {
		if n != "" && strings.Contains(haystack, n) {
			return n
		}
	}
	return ""
}

// allBrokerBytes concatenates every message the Executor sent.
func (h *harness) allBrokerBytes() string {
	var b strings.Builder
	for _, r := range h.br.Attempts(">") {
		b.Write(r.Data)
		for k, v := range r.Header {
			b.WriteString(k + "=" + v + "\n")
		}
	}
	for _, r := range h.br.Requests(">") {
		b.Write(r.Data)
		for k, v := range r.Header {
			b.WriteString(k + "=" + v + "\n")
		}
	}
	return b.String()
}

// podOf builds a Pod spec as a previous process would have created it.
func podOf(ns, name string, labels map[string]string) k8sclient.PodSpec {
	return k8sclient.PodSpec{Name: name, Namespace: ns, Image: "registry.example.com/team-3/runtime:v1", Labels: labels}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func hasString(v any, s string) bool { return strings.Contains(mustJSON(v), s) }
