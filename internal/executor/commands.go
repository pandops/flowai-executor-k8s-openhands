package executor

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/cache"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

// consumerLoop creates the Executor's own command consumer after every
// (re)connect, retrying a failed create once per heartbeat interval.
func (e *Executor) consumerLoop(ctx context.Context) {
	for {
		wait := e.cfg.PollInterval
		if e.connected.Load() && !e.revoked.Load() && !e.consumerOK.Load() {
			if e.createCommandConsumer(ctx) {
				e.consumerOK.Store(true)
				e.log.Info("command_consumer_ready", "consumer", platform.CommandConsumerName(e.id.ExecutorID))
			} else {
				wait = e.cfg.HeartbeatInterval
			}
		}
		if !e.sleep(ctx, wait) {
			return
		}
	}
}

func (e *Executor) createCommandConsumer(ctx context.Context) bool {
	conn := e.getConn()
	if conn == nil {
		return false
	}
	body, _ := json.Marshal(map[string]any{
		"stream_name": platform.StreamCommands,
		"config": map[string]any{
			"durable_name":       platform.CommandConsumerName(e.id.ExecutorID),
			"filter_subject":     platform.CommandFilterSubject(e.id.ExecutorID),
			"deliver_policy":     "all",
			"ack_policy":         "explicit",
			"ack_wait":           int64(30 * time.Second),
			"max_deliver":        -1,
			"inactive_threshold": int64(24 * time.Hour),
		},
	})
	reply, err := conn.Request(ctx, platform.CommandConsumerCreateSubject(e.id.ExecutorID),
		broker.Header{"team": strconv.FormatInt(e.id.TeamID, 10)}, body, e.cfg.Broker.RequestTimeout)
	if err != nil {
		e.log.Warn("command_consumer_create_failed", "error", err.Error())
		return false
	}
	var r struct {
		Error *struct {
			Code        int    `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if json.Unmarshal(reply.Data, &r) != nil || r.Error != nil {
		desc := ""
		if r.Error != nil {
			desc = r.Error.Description
		}
		e.log.Warn("command_consumer_create_failed", "error", desc)
		return false
	}
	return true
}

// commandLoop pulls the Executor's commands.
func (e *Executor) commandLoop(ctx context.Context) {
	durable := platform.CommandConsumerName(e.id.ExecutorID)
	for e.sleep(ctx, e.cfg.PollInterval) {
		e.mu.Lock()
		stopping := e.stopping
		e.mu.Unlock()
		if stopping || !e.connected.Load() || !e.consumerOK.Load() || e.revoked.Load() {
			continue
		}
		conn := e.getConn()
		if conn == nil {
			continue
		}
		msgs, err := conn.Fetch(ctx, platform.StreamCommands, durable, 10, e.cfg.PollInterval)
		if err != nil {
			continue
		}
		for _, m := range msgs {
			e.handleCommand(m)
		}
	}
}

func (e *Executor) handleCommand(m *broker.Msg) {
	cmd, err := platform.ParseCommand(m.Data)
	if err != nil {
		e.violation(m, "invalid_command_message")
		return
	}
	if cmd.TeamID != e.id.TeamID || cmd.ExecutorID != e.id.ExecutorID {
		e.violation(m, "foreign_command")
		return
	}
	if cmd.Control != "cancel" {
		e.violation(m, "unknown_control")
		return
	}
	ref := cmd.Claim()
	e.mu.Lock()
	c := e.claims[cache.ClaimKey(ref.TaskID, ref.Attempt, ref.ClaimSeq)]
	e.mu.Unlock()
	if c == nil {
		_ = m.Ack()
		return
	}
	c.mu.Lock()
	ended, oh, conv := c.ended, c.oh, c.convID
	c.mu.Unlock()
	if ended {
		_ = m.Ack()
		return
	}
	if !e.emitFor(c, platform.KindControlAcknowledged, map[string]any{"control_id": cmd.ControlID, "control": "cancel"}) {
		_ = m.Ack()
		return
	}
	if oh != nil {
		c.mu.Lock()
		was := c.pausedByUs
		c.pausedByUs = true
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
		err := oh.PauseConversation(ctx, conv)
		cancel()
		if err != nil {
			c.mu.Lock()
			c.pausedByUs = was
			c.mu.Unlock()
			e.log.Warn("cancel_pause_failed", "task_id", ref.TaskID, "error", err.Error())
			e.emitFor(c, platform.KindControlFailed, map[string]any{"control_id": cmd.ControlID, "control": "cancel", "reason": "agent_pause_failed"})
			_ = m.Ack()
			return
		}
	}
	seq, ok := e.end(c, platform.KindCancelled, map[string]any{"control_id": cmd.ControlID})
	if !ok {
		_ = m.Ack()
		return
	}
	e.deletePod(c)
	e.waitSent(e.runCtx, seq)
	_ = m.Ack()
	e.remove(c)
}
