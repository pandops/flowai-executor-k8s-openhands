// Package identity reads the Executor's identity from its own NATS user JWT.
// executor-registry puts {team_id, executor_id, tag, executor_type} as
// compact JSON in the JWT name claim; the NATS server enforces the
// permissions that pin the same values, so the Executor does not verify the
// signature itself.
package identity

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// Identity is what the Executor's JWT pins.
type Identity struct {
	TeamID       int64  `json:"team_id"`
	ExecutorID   int64  `json:"executor_id"`
	Tag          string `json:"tag"`
	ExecutorType string `json:"executor_type"`
}

// ErrInvalidToken is returned for a JWT the Executor cannot read.
var ErrInvalidToken = errors.New("identity: invalid executor JWT")

// FromJWT decodes the JWT payload and returns the identity in its name claim.
func FromJWT(token string) (Identity, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return Identity{}, ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return Identity{}, ErrInvalidToken
	}
	var claims struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Name == "" {
		return Identity{}, ErrInvalidToken
	}
	var id Identity
	if err := json.Unmarshal([]byte(claims.Name), &id); err != nil {
		return Identity{}, ErrInvalidToken
	}
	if id.TeamID <= 0 || id.ExecutorID <= 0 || id.Tag == "" {
		return Identity{}, ErrInvalidToken
	}
	return id, nil
}
