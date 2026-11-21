package k8sopenhands_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
)

// Configuration validation: the heartbeat timing the Executor reports to
// executor-registry is whole seconds within the heartbeat schema's bounds
// (interval_seconds and wait_seconds at least 1, miss_limit 1 to 1000), as
// the Docker Executor already requires. A sub-second interval would be
// reported as interval_seconds 0, which executor-registry rejects on every
// heartbeat.
func TestV0048K8sExecutorRejectsHeartbeatTimingOutOfBounds(t *testing.T) {
	load := func(t *testing.T, edit func(*config.Config)) error {
		h := newHarness(t)
		rendered := *h.cfg
		edit(&rendered)
		src := writeConfigYAML(t, &rendered, filepath.Join(t.TempDir(), "config.yaml"))
		_, err := config.Load(src)
		return err
	}
	for _, c := range []struct {
		name, key string
		edit      func(*config.Config)
	}{
		{"sub-second heartbeat_interval", "heartbeat_interval", func(c *config.Config) { c.HeartbeatInterval = 500 * time.Millisecond }},
		{"fractional heartbeat_interval", "heartbeat_interval", func(c *config.Config) { c.HeartbeatInterval = 1500 * time.Millisecond }},
		{"sub-second connection_wait", "connection_wait", func(c *config.Config) { c.ConnectionWait = 999 * time.Millisecond }},
		{"heartbeat_miss_limit above 1000", "heartbeat_miss_limit", func(c *config.Config) { c.HeartbeatMissLimit = 1001 }},
	} {
		t.Run(c.name+" is rejected", func(t *testing.T) {
			err := load(t, c.edit)
			if err == nil || !strings.Contains(err.Error(), c.key) {
				t.Fatalf("Load = %v, want an error naming %s", err, c.key)
			}
		})
	}
	t.Run("one-second bounds are accepted", func(t *testing.T) {
		if err := load(t, func(c *config.Config) {
			c.HeartbeatInterval, c.ConnectionWait, c.HeartbeatMissLimit = time.Second, time.Second, 1000
		}); err != nil {
			t.Fatalf("Load = %v, want accepted", err)
		}
	})
}
