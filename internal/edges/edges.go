// Package edges is the Executor's client of the two HTTP edges it calls
// with a claim token: env-registry's launch-parameter open and
// model-registry's per-task LiteLLM key. The claim token is opaque and is
// never logged.
package edges

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ErrClaimNotCurrent is 404 claim_not_current.
var ErrClaimNotCurrent = errors.New("edges: claim_not_current")

// ErrUnavailable is a 503 or no answer.
var ErrUnavailable = errors.New("edges: service_unavailable")

// ConfigError is 422 configuration_error with its failure_reason.
type ConfigError struct{ FailureReason string }

func (e *ConfigError) Error() string { return "edges: configuration_error: " + e.FailureReason }

// LLM is the key edge's task-local model configuration.
type LLM struct {
	BaseURL string
	Model   string
	APIKey  string
}

// Client calls the edges.
type Client struct {
	EnvRegistryURL   string
	ModelRegistryURL string
	HTTP             *http.Client
}

// OpenLaunchParameters calls GET /v1/tasks/{task_id}/launch-parameters/open.
func (c *Client) OpenLaunchParameters(ctx context.Context, taskID int64, claimToken string) (map[string]string, error) {
	var out struct {
		Values map[string]string `json:"values"`
	}
	if err := c.get(ctx, c.EnvRegistryURL, fmt.Sprintf("/v1/tasks/%d/launch-parameters/open", taskID), claimToken, "missing_required_parameter", &out); err != nil {
		return nil, err
	}
	if out.Values == nil {
		out.Values = map[string]string{}
	}
	return out.Values, nil
}

// LiteLLMKey calls GET /v1/tasks/{task_id}/litellm-key. A 200 answer that
// lacks one of the three values is a configuration error.
func (c *Client) LiteLLMKey(ctx context.Context, taskID int64, claimToken string) (LLM, error) {
	var out map[string]any
	if err := c.get(ctx, c.ModelRegistryURL, fmt.Sprintf("/v1/tasks/%d/litellm-key", taskID), claimToken, "llm_configuration_incomplete", &out); err != nil {
		return LLM{}, err
	}
	src := out
	if nested, ok := out["values"].(map[string]any); ok {
		src = nested
	}
	str := func(k string) string { v, _ := src[k].(string); return strings.TrimSpace(v) }
	llm := LLM{BaseURL: str("OPENAI_BASE_URL"), Model: str("OPENAI_MODEL"), APIKey: str("OPENAI_API_KEY")}
	if llm.BaseURL == "" || llm.Model == "" || llm.APIKey == "" {
		return LLM{}, &ConfigError{FailureReason: "llm_configuration_incomplete"}
	}
	return llm, nil
}

func (c *Client) get(ctx context.Context, base, path, token, defaultReason string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.Unmarshal(data, out); err != nil {
			return ErrUnavailable
		}
		return nil
	case http.StatusNotFound:
		return ErrClaimNotCurrent
	case http.StatusUnprocessableEntity:
		var e struct {
			FailureReason string `json:"failure_reason"`
		}
		_ = json.Unmarshal(data, &e)
		if e.FailureReason == "" {
			e.FailureReason = defaultReason
		}
		return &ConfigError{FailureReason: e.FailureReason}
	default:
		return ErrUnavailable
	}
}
