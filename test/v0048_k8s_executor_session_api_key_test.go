package k8sopenhands_test

// Requirement: K8s Executor runs only Kubernetes Pods (row 328), ADR 0003:
// the Executor sends X-Session-API-Key on every OpenHands call. The OpenHands
// agent server binds 0.0.0.0 only when SESSION_API_KEY is set; without it
// the server listens on loopback inside the task Pod and the Executor Pod
// cannot reach it (every attempt failed with agent_server_failed).

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
)

// loadedConfig writes the harness configuration as the chart-rendered file
// (with openhands_api_key only when fileKey is set) and loads it the way the
// Executor does on start.
func loadedConfig(t *testing.T, c *configT, fileKey string) *configT {
	t.Helper()
	text := fmt.Sprintf("broker:\n  url: %s\n  token: %s\n  ca_file: %s\nenv_registry_url: %s\nmodel_registry_url: %s\n"+
		"max_parallel_tasks: %d\nexecutor_api_bind: %q\nlog_level: %s\nnamespace: %s\nservice_account: %s\n"+
		"cache_dir: %s\nstate_file: %s\nopenhands_port: %d\n",
		c.Broker.URL, c.Broker.Token, c.Broker.CAFile, c.EnvRegistryURL, c.ModelRegistryURL,
		c.MaxParallelTasks, c.ExecutorAPIBind, c.LogLevel, c.Namespace, c.ServiceAccount,
		c.CacheDir, c.StateFile, c.OpenHandsPort)
	if fileKey != "" {
		text += "openhands_api_key: " + fileKey + "\n"
	}
	if err := os.WriteFile(c.Path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(c.Path)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

// Scenario: the task Pod carries the session key the Executor sends on every
// OpenHands call, also when the configuration file names no key.
func TestK8sPodCarriesTheOpenHandsSessionKeyTheExecutorSends(t *testing.T) {
	for _, tc := range []struct{ name, fileKey string }{{"generated", ""}, {"configured", "file-session-key"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OPENHANDS_API_KEY", "")
			h := newHarness(t, withConfig(func(c *configT) { *c = *loadedConfig(t, c, tc.fileKey) }))
			key := h.cfg.OpenHandsAPIKey
			if key == "" {
				t.Fatalf("the loaded configuration has no OpenHands session key")
			}
			if tc.fileKey != "" && key != tc.fileKey {
				t.Fatalf("configured OpenHands session key not used")
			}
			h.start()
			h.waitConnected()
			tk := h.enqueueTask()
			h.waitRunning(tk)
			p, ok := h.pod(tk)
			if !ok {
				t.Fatalf("no Pod for the claim")
			}
			if got, ok := p.Spec.Env["SESSION_API_KEY"]; !ok || got != key {
				t.Fatalf("Pod SESSION_API_KEY set=%v, equal to the Executor's key=%v", ok, got == key)
			}
			seen := map[string]bool{}
			for _, call := range h.oh.SessionKeys() {
				if call.Key != key {
					t.Fatalf("OpenHands call %s sent X-Session-API-Key set=%v, equal to the Pod's key=false", call.Path, call.Key != "")
				}
				switch {
				case call.Path == "/health":
					seen["health"] = true
				case call.Path == "/api/conversations":
					seen["start"] = true
				case strings.HasPrefix(call.Path, "/sockets/events/"):
					seen["stream"] = true
				}
			}
			for _, k := range []string{"health", "start", "stream"} {
				if !seen[k] {
					t.Fatalf("no OpenHands %s call observed", k)
				}
			}
			if strings.Contains(h.logs.String(), key) {
				t.Fatalf("the OpenHands session key reached the Executor log")
			}
		})
	}
}
