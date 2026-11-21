package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const minimal = "broker:\n  url: tls://nats:4222\n  token: jwt\nenv_registry_url: http://env-registry:8080\nmodel_registry_url: http://model-registry:8080\nnamespace: flowai\n"

func TestLoadAppliesDocumentedDefaults(t *testing.T) {
	c, err := Load(writeFile(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]bool{
		"heartbeat_interval 15s":       c.HeartbeatInterval == 15*time.Second,
		"heartbeat_miss_limit 3":       c.HeartbeatMissLimit == 3,
		"connection_wait 600s":         c.ConnectionWait == 600*time.Second,
		"pending_recheck_interval 30s": c.PendingRecheckInterval == 30*time.Second,
		"drain_timeout_seconds 30":     c.DrainTimeoutSeconds == 30 && c.DrainTimeout() == 30*time.Second,
		"cleanup delays 0":             c.FinishedCleanupDelay == 0 && c.FailedCleanupDelay == 0,
		"listener :8080":               c.ExecutorAPIBind == ":8080",
		"ca file":                      c.Broker.CAFile == "/run/flowai/stand-ca/ca.pem",
		"state file next to config":    c.StatePath() == filepath.Join(filepath.Dir(c.Path), "executor.state"),
		"cache dir":                    c.CacheDir != "",
	}
	for name, ok := range checks {
		if !ok {
			t.Fatalf("default %s not applied: %+v", name, c)
		}
	}
}

func TestLoadRejectsMissingToken(t *testing.T) {
	if _, err := Load(writeFile(t, strings.Replace(minimal, "  token: jwt\n", "", 1))); !errors.Is(err, ErrNoToken) {
		t.Fatalf("err %v", err)
	}
}

func TestLoadRejectsRemovedKeys(t *testing.T) {
	for _, key := range []string{"scope: team", "team_id: team-a", "authorized_tag: gpu", "state_registry_url: http://x", "openhands_agent_profile_id: p", "openhands_llm_model: m"} {
		if _, err := Load(writeFile(t, minimal+key+"\n")); err == nil {
			t.Fatalf("accepted removed key %q", key)
		}
	}
}

func TestValidateRejectsNegativeCleanupDelay(t *testing.T) {
	for _, extra := range []string{"finished_cleanup_delay: -1s", "failed_cleanup_delay: -1s", "drain_timeout_seconds: -1", "max_parallel_tasks: 0"} {
		if _, err := Load(writeFile(t, minimal+extra+"\n")); err == nil {
			t.Fatalf("accepted %q", extra)
		}
	}
}

func TestInstallConfigWritesOwnerOnlyCopyAndReplaces(t *testing.T) {
	src := writeFile(t, minimal)
	dst := filepath.Join(t.TempDir(), "config", "config.yaml")
	if err := InstallConfig(src, dst); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(dst)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("installed %v %v", st, err)
	}
	reissued := writeFile(t, strings.Replace(minimal, "token: jwt", "token: jwt2", 1))
	if err := InstallConfig(reissued, dst); err != nil {
		t.Fatal(err)
	}
	c, err := Load(dst)
	if err != nil || c.Broker.Token != "jwt2" {
		t.Fatalf("reissue not applied: %v %v", c, err)
	}
}

// Without openhands_api_key the Executor mints its own OpenHands session
// key: the agent server binds 0.0.0.0 only when SESSION_API_KEY is set, and
// a loopback listener in the task Pod is unreachable from the Executor Pod.
func TestLoadGeneratesOpenHandsAPIKey(t *testing.T) {
	t.Setenv("OPENHANDS_API_KEY", "")
	path := writeFile(t, minimal)
	first, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if first.OpenHandsAPIKey == "" || second.OpenHandsAPIKey == "" {
		t.Fatalf("no OpenHands session key generated")
	}
	if first.OpenHandsAPIKey == second.OpenHandsAPIKey {
		t.Fatalf("two loads generated the same OpenHands session key")
	}
}

// Precedence: OPENHANDS_API_KEY in the environment over openhands_api_key in
// the file over a generated key (as in the baseline Executor).
func TestLoadPreservesExplicitOpenHandsAPIKey(t *testing.T) {
	t.Setenv("OPENHANDS_API_KEY", "")
	c, err := Load(writeFile(t, minimal+"openhands_api_key: file-session-key\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.OpenHandsAPIKey != "file-session-key" {
		t.Fatalf("file key not kept")
	}
	t.Setenv("OPENHANDS_API_KEY", "env-session-key")
	for _, text := range []string{minimal, minimal + "openhands_api_key: file-session-key\n"} {
		c, err := Load(writeFile(t, text))
		if err != nil {
			t.Fatal(err)
		}
		if c.OpenHandsAPIKey != "env-session-key" {
			t.Fatalf("OPENHANDS_API_KEY did not take precedence")
		}
	}
}
