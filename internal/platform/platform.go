// Package platform holds the executor_k8s_openhands wire contract: the
// concrete Executor type, broker subjects and consumer names, and the
// message bodies the Executor publishes and consumes. Each concrete
// Executor owns its own copy (AGENTS.md: no shared code between services);
// the publishers' JSON Schemas are the contract.
package platform

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ExecutorTypeK8sOpenHands is the concrete Executor type. Its wire value
// follows executor_<runtime>_<tool>.
const ExecutorTypeK8sOpenHands = "executor_k8s_openhands"

// ServiceName is the slog service and probe label.
const ServiceName = ExecutorTypeK8sOpenHands

// Streams the Executor pulls from.
const (
	StreamTasks    = "TASK_MANAGER_TASK"
	StreamCommands = "TASK_MANAGER_COMMAND"
)

// Task event kinds.
const (
	KindCreated             = "created"
	KindPending             = "pending"
	KindReleased            = "released"
	KindRunning             = "running"
	KindFinished            = "finished"
	KindFailed              = "failed"
	KindCancelled           = "cancelled"
	KindControlAcknowledged = "control_acknowledged"
	KindControlFailed       = "control_failed"
	KindLog                 = "log"
)

// Self event kinds.
const (
	KindExecutorStarted            = "executor_started"
	KindExecutorStopping           = "executor_stopping"
	KindExecutorStopped            = "executor_stopped"
	KindExecutorFailed             = "executor_failed"
	KindExecutorConnectionLost     = "executor_connection_lost"
	KindExecutorConnectionRestored = "executor_connection_restored"
	KindProtocolViolation          = "protocol_violation"
)

// Release reasons the Executor may publish.
const (
	ReleasePendingTimeout    = "pending_timeout"
	ReleaseExecutorRestarted = "executor_restarted"
	ReleaseExecutorShutdown  = "executor_shutdown"
)

// Temporary (pending) reasons.
const (
	PendingDependencyUnavailable = "dependency_unavailable"
)

// Failure reasons.
const (
	FailureImageMissing               = "image_missing"
	FailureInvalidFlowRevision        = "invalid_flow_revision"
	FailureLLMConfigurationIncomplete = "llm_configuration_incomplete"
	FailureImagePullFailed            = "image_pull_failed"
	FailurePodRestartedBeforeFinish   = "pod_restarted_before_finish"
	FailurePodFailed                  = "pod_failed"
	FailureAgentServerFailed          = "agent_server_failed"
	FailureLLMBudgetExceeded          = "llm_budget_exceeded"
)

// TagHash is the first 16 lowercase hex characters of the SHA-256 of the
// tag's UTF-8 bytes.
func TagHash(tag string) string {
	sum := sha256.Sum256([]byte(tag))
	return hex.EncodeToString(sum[:])[:16]
}

// TaskConsumerName is tasks-<team_id>-<tag_hash>.
func TaskConsumerName(teamID int64, tag string) string {
	return fmt.Sprintf("tasks-%d-%s", teamID, TagHash(tag))
}

// CommandConsumerName is commands-<executor_id>.
func CommandConsumerName(executorID int64) string { return fmt.Sprintf("commands-%d", executorID) }

// CommandFilterSubject is task-manager.command.<executor_id>.sub.
func CommandFilterSubject(executorID int64) string {
	return fmt.Sprintf("task-manager.command.%d.sub", executorID)
}

// CommandConsumerCreateSubject is the filtered JetStream create subject of
// the Executor's own command consumer.
func CommandConsumerCreateSubject(executorID int64) string {
	return "$JS.API.CONSUMER.CREATE." + StreamCommands + "." + CommandConsumerName(executorID) + "." + CommandFilterSubject(executorID)
}

// EventSubject is executor.event.<executor_id>.<kind>.pub.
func EventSubject(executorID int64, kind string) string {
	return fmt.Sprintf("executor.event.%d.%s.pub", executorID, kind)
}

// RequestSubject is <svc>.executor.<executor_id>.<operation>.pub.
func RequestSubject(svc string, executorID int64, operation string) string {
	return fmt.Sprintf("%s.executor.%d.%s.pub", svc, executorID, operation)
}

// MsgID is executor-<team_id>-<executor_id>-<publisher_seq>.
func MsgID(teamID, executorID, publisherSeq int64) string {
	return fmt.Sprintf("executor-%d-%d-%d", teamID, executorID, publisherSeq)
}

// ParseTaskSubject splits task-manager.task.<team>.<tag>.<flow>.sub.
func ParseTaskSubject(subject string) (teamID int64, tag string, flowID int64, ok bool) {
	p := strings.Split(subject, ".")
	if len(p) != 6 || p[0] != "task-manager" || p[1] != "task" || p[5] != "sub" {
		return 0, "", 0, false
	}
	team, err1 := strconv.ParseInt(p[2], 10, 64)
	flow, err2 := strconv.ParseInt(p[4], 10, 64)
	if err1 != nil || err2 != nil || team <= 0 || flow <= 0 || p[3] == "" {
		return 0, "", 0, false
	}
	return team, p[3], flow, true
}

// ErrInvalidMessage marks a message whose identifiers are not positive
// JSON integers or that cannot be decoded.
var ErrInvalidMessage = errors.New("platform: invalid message")

// positiveInts checks that every named field of a JSON object is a
// positive integer written as a JSON number.
func positiveInts(data []byte, fields ...string) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	for _, f := range fields {
		v := bytes.TrimSpace(raw[f])
		if len(v) == 0 || v[0] < '1' || v[0] > '9' {
			return fmt.Errorf("%w: %s is not a positive integer", ErrInvalidMessage, f)
		}
		if _, err := strconv.ParseInt(string(v), 10, 64); err != nil {
			return fmt.Errorf("%w: %s is not a positive integer", ErrInvalidMessage, f)
		}
	}
	return nil
}

// ClaimRef names one claim.
type ClaimRef struct {
	TaskID   int64 `json:"task_id"`
	Attempt  int64 `json:"attempt"`
	ClaimSeq int64 `json:"claim_seq"`
}

// Execution is the frozen, non-secret execution snapshot of a task message.
type Execution struct {
	ExecutionRevisionID int64           `json:"execution_revision_id"`
	RequiredTag         string          `json:"required_tag"`
	RenderedPrompt      string          `json:"rendered_prompt"`
	ExecutionParameters json.RawMessage `json:"execution_parameters"`
}

// TaskMessage is task-manager's queue message.
type TaskMessage struct {
	Publisher           string          `json:"publisher"`
	PublisherSeq        int64           `json:"publisher_seq"`
	TeamID              int64           `json:"team_id"`
	TaskID              int64           `json:"task_id"`
	FlowID              int64           `json:"flow_id"`
	Attempt             int64           `json:"attempt"`
	ClaimSeq            int64           `json:"claim_seq"`
	Payload             json.RawMessage `json:"payload"`
	Execution           *Execution      `json:"execution"`
	RequiredTag         string          `json:"required_tag"`
	ResolvedImage       *string         `json:"resolved_image"`
	ImageSource         string          `json:"image_source"`
	PendingLimitSeconds int64           `json:"pending_limit_seconds"`
}

// Claim returns the message's claim reference.
func (m TaskMessage) Claim() ClaimRef {
	return ClaimRef{TaskID: m.TaskID, Attempt: m.Attempt, ClaimSeq: m.ClaimSeq}
}

// ParseTaskMessage decodes a queue message and checks that every
// identifier is a positive JSON integer.
func ParseTaskMessage(data []byte) (TaskMessage, error) {
	if err := positiveInts(data, "team_id", "task_id", "flow_id", "attempt", "claim_seq"); err != nil {
		return TaskMessage{}, err
	}
	var m TaskMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return TaskMessage{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return m, nil
}

// CommandMessage is task-manager's command to one Executor.
type CommandMessage struct {
	Publisher    string `json:"publisher"`
	PublisherSeq int64  `json:"publisher_seq"`
	TeamID       int64  `json:"team_id"`
	ExecutorID   int64  `json:"executor_id"`
	Control      string `json:"control"`
	ControlID    int64  `json:"control_id"`
	TaskID       int64  `json:"task_id"`
	Attempt      int64  `json:"attempt"`
	ClaimSeq     int64  `json:"claim_seq"`
}

// Claim returns the command's claim reference.
func (c CommandMessage) Claim() ClaimRef {
	return ClaimRef{TaskID: c.TaskID, Attempt: c.Attempt, ClaimSeq: c.ClaimSeq}
}

// ParseCommand decodes a command message.
func ParseCommand(data []byte) (CommandMessage, error) {
	if err := positiveInts(data, "team_id", "executor_id", "control_id", "task_id", "attempt", "claim_seq"); err != nil {
		return CommandMessage{}, err
	}
	var c CommandMessage
	if err := json.Unmarshal(data, &c); err != nil {
		return CommandMessage{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return c, nil
}

// HeartbeatClaim is one claim in a heartbeat.
type HeartbeatClaim struct {
	TaskID   int64  `json:"task_id"`
	Attempt  int64  `json:"attempt"`
	ClaimSeq int64  `json:"claim_seq"`
	State    string `json:"state"`
}

// HeartbeatRequest is the heartbeat body.
type HeartbeatRequest struct {
	ExecutorType     string           `json:"executor_type"`
	IntervalSeconds  int64            `json:"interval_seconds"`
	MissLimit        int64            `json:"miss_limit"`
	WaitSeconds      int64            `json:"wait_seconds"`
	MaxParallelTasks int64            `json:"max_parallel_tasks"`
	SlotsInUse       int64            `json:"slots_in_use"`
	IP               *string          `json:"ip"`
	Claims           []HeartbeatClaim `json:"claims"`
}

// HeartbeatReply is executor-registry's heartbeat answer.
type HeartbeatReply struct {
	ConnectionState string     `json:"connection_state"`
	ClaimsKnown     bool       `json:"claims_known"`
	Claims          []ClaimRef `json:"claims"`
}

// ClaimTokenRequest asks task-manager for the claim token of a claim.
type ClaimTokenRequest struct {
	TaskID              int64 `json:"task_id"`
	Attempt             int64 `json:"attempt"`
	ClaimSeq            int64 `json:"claim_seq"`
	CreatedPublisherSeq int64 `json:"created_publisher_seq"`
}

// ClaimTokenReply carries the claim token on 200.
type ClaimTokenReply struct {
	ClaimToken string `json:"claim_token"`
}

// CheckRequest is the model-registry check body.
type CheckRequest struct {
	TaskID   int64 `json:"task_id"`
	Attempt  int64 `json:"attempt"`
	ClaimSeq int64 `json:"claim_seq"`
}

// CheckReply is model-registry's check answer on 200.
type CheckReply struct {
	Result     string `json:"result"`
	Reason     string `json:"reason,omitempty"`
	ModelAlias string `json:"model_alias,omitempty"`
}

// ErrorBody is the error reply body of every request.
type ErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID int64  `json:"request_id"`
}
