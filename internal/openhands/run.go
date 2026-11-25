package openhands

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// RunConversation calls POST /api/conversations/{conversation_id}/run, the
// continue command for a paused conversation.
func (c *Client) RunConversation(ctx context.Context, conversationID string) error {
	url := c.baseURL + "/api/conversations/" + conversationID + "/run"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	if c.apiKey != "" {
		req.Header.Set("X-Session-API-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("openhands POST %s: %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// BaseURL is the agent server's base URL.
func (c *Client) BaseURL() string { return c.baseURL }

// APIKey is the agent server's session API key.
func (c *Client) APIKey() string { return c.apiKey }
