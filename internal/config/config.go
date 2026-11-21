// Package config loads the K8s Executor's YAML configuration. The Helm
// chart renders the file, including broker.token, from a Kubernetes Secret
// onto the Executor's writable volume with mode 0600; the Executor reads it
// on every start. Unknown keys (for example the removed scope, team_id,
// authorized_tag and state_registry_url) are rejected.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// ErrNoToken is returned when broker.token is empty.
var ErrNoToken = errors.New("config: broker.token is required")

// Broker is the NATS connection configuration.
type Broker struct {
	URL            string        `yaml:"url"`
	Token          string        `yaml:"token"`
	CAFile         string        `yaml:"ca_file"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
}

// Config is the K8s Executor configuration.
type Config struct {
	Broker Broker `yaml:"broker"`

	EnvRegistryURL   string `yaml:"env_registry_url"`
	ModelRegistryURL string `yaml:"model_registry_url"`

	MaxParallelTasks       int           `yaml:"max_parallel_tasks"`
	HeartbeatInterval      time.Duration `yaml:"heartbeat_interval"`
	HeartbeatMissLimit     int           `yaml:"heartbeat_miss_limit"`
	ConnectionWait         time.Duration `yaml:"connection_wait"`
	PendingRecheckInterval time.Duration `yaml:"pending_recheck_interval"`
	DrainTimeoutSeconds    int           `yaml:"drain_timeout_seconds"`
	PollInterval           time.Duration `yaml:"poll_interval"`
	PodPollInterval        time.Duration `yaml:"pod_poll_interval"`
	FinishedCleanupDelay   time.Duration `yaml:"finished_cleanup_delay"`
	FailedCleanupDelay     time.Duration `yaml:"failed_cleanup_delay"`

	ExecutorAPIBind string `yaml:"executor_api_bind"`
	LogLevel        string `yaml:"log_level"`

	Namespace            string            `yaml:"namespace"`
	ServiceAccount       string            `yaml:"service_account"`
	ImagePullPolicy      string            `yaml:"image_pull_policy"`
	LiteLLMDataPodLabels map[string]string `yaml:"litellm_data_pod_labels"`

	CacheDir  string `yaml:"cache_dir"`
	StateFile string `yaml:"state_file"`

	OpenHandsPort       int    `yaml:"openhands_port"`
	OpenHandsAPIKey     string `yaml:"openhands_api_key"`
	OpenHandsWorkspace  string `yaml:"openhands_workspace"`
	OpenHandsLLMUsageID string `yaml:"openhands_llm_usage_id"`
	OpenHandsInitialRun bool   `yaml:"openhands_initial_run"`

	// Path is the file the configuration was read from.
	Path string `yaml:"-"`
}

// Defaults returns the documented defaults.
func Defaults() *Config {
	return &Config{
		Broker:                 Broker{CAFile: "/run/flowai/stand-ca/ca.pem", RequestTimeout: 5 * time.Second},
		MaxParallelTasks:       2,
		HeartbeatInterval:      15 * time.Second,
		HeartbeatMissLimit:     3,
		ConnectionWait:         600 * time.Second,
		PendingRecheckInterval: 30 * time.Second,
		DrainTimeoutSeconds:    30,
		PollInterval:           time.Second,
		PodPollInterval:        2 * time.Second,
		ExecutorAPIBind:        ":8080",
		LogLevel:               "info",
		ServiceAccount:         "executor-k8s-openhands",
		ImagePullPolicy:        "IfNotPresent",
		LiteLLMDataPodLabels:   map[string]string{"flowai.io/litellm-data": "client"},
		CacheDir:               "/var/lib/flowai/executor-k8s/cache",
		OpenHandsPort:          8000,
		OpenHandsWorkspace:     "/workspace/project",
		OpenHandsLLMUsageID:    "flowai-executor",
		OpenHandsInitialRun:    true,
	}
}

// Load reads the YAML file at path over the defaults and validates it.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	c := Defaults()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(c); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	c.Path = path
	// OPENHANDS_API_KEY overrides openhands_api_key. With neither, the
	// Executor mints an opaque key: SESSION_API_KEY makes the agent server
	// bind 0.0.0.0, so the Executor Pod can reach the task Pod.
	if v := os.Getenv("OPENHANDS_API_KEY"); v != "" {
		c.OpenHandsAPIKey = v
	}
	if c.OpenHandsAPIKey == "" {
		c.OpenHandsAPIKey = "flowai-" + uuid.NewString()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// wholeSeconds is the heartbeat timing rule: the heartbeat reports these
// durations in whole seconds, at least 1.
func wholeSeconds(d time.Duration) bool { return d >= time.Second && d%time.Second == 0 }

// Validate checks the configuration; an empty broker.token is ErrNoToken.
func (c *Config) Validate() error {
	switch {
	case c.Broker.Token == "":
		return ErrNoToken
	case c.Broker.URL == "":
		return errors.New("config: broker.url is required")
	case c.EnvRegistryURL == "" || c.ModelRegistryURL == "":
		return errors.New("config: env_registry_url and model_registry_url are required")
	case c.MaxParallelTasks < 1:
		return errors.New("config: max_parallel_tasks must be at least 1")
	case !wholeSeconds(c.HeartbeatInterval):
		return errors.New("config: heartbeat_interval must be a whole number of seconds, at least 1s")
	case c.HeartbeatMissLimit < 1 || c.HeartbeatMissLimit > 1000:
		return errors.New("config: heartbeat_miss_limit must be between 1 and 1000")
	case !wholeSeconds(c.ConnectionWait):
		return errors.New("config: connection_wait must be a whole number of seconds, at least 1s")
	case c.PendingRecheckInterval <= 0 || c.PollInterval <= 0 || c.PodPollInterval <= 0:
		return errors.New("config: intervals must be positive")
	case c.DrainTimeoutSeconds < 0:
		return errors.New("config: drain_timeout_seconds must not be negative")
	case c.FinishedCleanupDelay < 0 || c.FailedCleanupDelay < 0:
		return errors.New("config: cleanup delays must not be negative")
	case c.Namespace == "":
		return errors.New("config: namespace is required")
	case c.CacheDir == "":
		return errors.New("config: cache_dir is required")
	}
	return nil
}

// DrainTimeout is drain_timeout_seconds as a duration.
func (c *Config) DrainTimeout() time.Duration {
	return time.Duration(c.DrainTimeoutSeconds) * time.Second
}

// StatePath is the publisher_seq state file next to the configuration.
func (c *Config) StatePath() string {
	if c.StateFile != "" {
		return c.StateFile
	}
	return filepath.Join(filepath.Dir(c.Path), "executor.state")
}

// InstallConfig copies the chart-rendered configuration at src to dst with
// mode 0600 (replacing an older copy, which is the reissue path).
func InstallConfig(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", src, err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".config-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dst)
}
