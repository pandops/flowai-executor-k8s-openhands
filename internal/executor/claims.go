package executor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/cache"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/edges"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/openhands"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

// Heartbeat claim states.
const (
	stateCreated = "created"
	statePending = "pending"
	stateRunning = "running"
)

// claim is one held claim.
type claim struct {
	ref platform.ClaimRef
	key string
	msg platform.TaskMessage

	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex // serializes the claim's events
	state      string
	ended      bool
	createdSeq int64
	token      string
	tokenAt    time.Time
	reason     string
	podName    string
	convID     string
	oh         *openhands.Client
	ws         *websocket.Conn
	pausedByUs bool
	lossPaused bool
	// toolLogs holds the logs each tool call published (frame loop only).
	toolLogs toolCallLogs
}

func (c *claim) running() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.convID != "" && !c.ended }

// emitFor publishes a non-terminal event of a claim that has not ended.
func (e *Executor) emitFor(c *claim, kind string, fields map[string]any) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ended || c.ctx.Err() != nil {
		return false
	}
	_, err := e.emit(c, kind, fields)
	return err == nil
}

// end publishes the one terminal event of a claim and stops its work; false
// when the claim already ended. It returns the event's publisher_seq.
func (e *Executor) end(c *claim, kind string, fields map[string]any) (int64, bool) {
	c.mu.Lock()
	if c.ended {
		c.mu.Unlock()
		return 0, false
	}
	c.ended = true
	seq, _ := e.emit(c, kind, fields)
	c.mu.Unlock()
	c.cancel()
	e.closeStream(c)
	e.persistClaim(c, cache.StateTerminal)
	return seq, true
}

// markEnded ends a claim without any event (dropped claims).
func (e *Executor) markEnded(c *claim) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ended {
		return false
	}
	c.ended = true
	c.cancel()
	return true
}

func (e *Executor) closeStream(c *claim) {
	c.mu.Lock()
	ws := c.ws
	c.mu.Unlock()
	if ws != nil {
		_ = ws.Close()
	}
}

// remove frees the claim's slot and its cache record.
func (e *Executor) remove(c *claim) {
	e.mu.Lock()
	if e.claims[c.key] == c {
		delete(e.claims, c.key)
	}
	e.mu.Unlock()
	_ = e.store.DeleteClaim(c.ref.TaskID, c.ref.Attempt, c.ref.ClaimSeq)
}

// drop forgets a claim without publishing anything: its Pod is deleted and
// its unsent events are discarded.
func (e *Executor) drop(c *claim, why string) {
	e.markEnded(c)
	e.closeStream(c)
	e.deletePod(c)
	_ = e.store.DropUnsentForClaim(c.key)
	e.remove(c)
	e.log.Info("claim_dropped", "task_id", c.ref.TaskID, "attempt", c.ref.Attempt, "claim_seq", c.ref.ClaimSeq, "reason", why)
}

func (e *Executor) persistClaim(c *claim, state string) {
	c.mu.Lock()
	rec := cache.Claim{TaskID: c.ref.TaskID, Attempt: c.ref.Attempt, ClaimSeq: c.ref.ClaimSeq, TeamID: e.id.TeamID,
		ExecutorID: e.id.ExecutorID, State: state, CreatedSeq: c.createdSeq, PendingReason: c.reason,
		PodNamespace: e.cfg.Namespace, PodName: c.podName, ConversationID: c.convID, ImageSource: c.msg.ImageSource,
		UpdatedAt: e.now()}
	if c.msg.ResolvedImage != nil {
		rec.ResolvedImage = *c.msg.ResolvedImage
	}
	c.mu.Unlock()
	_ = e.store.PutClaim(rec)
}

func (e *Executor) slotsInUse() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.claims)
}

func (e *Executor) claimList() []*claim {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]*claim, 0, len(e.claims))
	for _, c := range e.claims {
		out = append(out, c)
	}
	return out
}

// canPull reports whether the Executor may take new tasks.
func (e *Executor) canPull() bool {
	e.mu.Lock()
	stopping, loss := e.stopping, e.inLoss
	e.mu.Unlock()
	return !stopping && !loss && e.connected.Load() && e.consumerOK.Load() && !e.revoked.Load() && e.kubeOK.Load()
}

// taskLoop pulls up to the free slots from the team/tag task consumer.
func (e *Executor) taskLoop(ctx context.Context) {
	durable := platform.TaskConsumerName(e.id.TeamID, e.id.Tag)
	for e.sleep(ctx, e.cfg.PollInterval) {
		if !e.canPull() {
			continue
		}
		free := e.cfg.MaxParallelTasks - e.slotsInUse()
		if free <= 0 {
			continue
		}
		conn := e.getConn()
		if conn == nil {
			continue
		}
		msgs, err := conn.Fetch(ctx, platform.StreamTasks, durable, free, e.cfg.PollInterval)
		if err != nil {
			continue
		}
		for _, m := range msgs {
			if ctx.Err() != nil {
				return
			}
			e.handleTask(ctx, m)
		}
	}
}

// violation terminates a refused message and reports it.
func (e *Executor) violation(m *broker.Msg, reason string) {
	_ = m.Term()
	fields := map[string]any{"reason": reason}
	if len(m.Subject) <= 512 {
		fields["subject"] = m.Subject
	}
	var head struct {
		Publisher    string          `json:"publisher"`
		PublisherSeq json.RawMessage `json:"publisher_seq"`
	}
	if json.Unmarshal(m.Data, &head) == nil {
		if head.Publisher != "" && len(head.Publisher) <= 128 {
			fields["message_publisher"] = head.Publisher
		}
		var seq int64
		if json.Unmarshal(head.PublisherSeq, &seq) == nil && seq > 0 {
			fields["message_publisher_seq"] = seq
		}
	}
	e.log.Warn("protocol_violation", "subject", m.Subject, "reason", reason)
	e.emitSelf(platform.KindProtocolViolation, fields)
}

// handleTask claims one task message: created, acknowledgement, then the
// claim worker.
func (e *Executor) handleTask(ctx context.Context, m *broker.Msg) {
	team, tag, _, ok := platform.ParseTaskSubject(m.Subject)
	if !ok {
		e.violation(m, "invalid_task_subject")
		return
	}
	msg, err := platform.ParseTaskMessage(m.Data)
	if err != nil {
		e.violation(m, "invalid_task_message")
		return
	}
	if team != e.id.TeamID || msg.TeamID != e.id.TeamID {
		e.violation(m, "foreign_team")
		return
	}
	if tag != e.id.Tag || msg.RequiredTag != e.id.Tag || (msg.Execution != nil && msg.Execution.RequiredTag != "" && msg.Execution.RequiredTag != e.id.Tag) {
		e.violation(m, "foreign_tag")
		return
	}
	ref := msg.Claim()
	key := cache.ClaimKey(ref.TaskID, ref.Attempt, ref.ClaimSeq)
	e.mu.Lock()
	if _, held := e.claims[key]; held {
		e.mu.Unlock()
		_ = m.Ack()
		return
	}
	cctx, cancel := context.WithCancel(e.runCtx)
	c := &claim{ref: ref, key: key, msg: msg, ctx: cctx, cancel: cancel, state: stateCreated}
	e.claims[key] = c
	e.mu.Unlock()
	e.persistClaim(c, cache.StateClaiming)

	c.mu.Lock()
	seq, err := e.emit(c, platform.KindCreated, nil)
	c.createdSeq = seq
	c.mu.Unlock()
	if err != nil || !e.waitSent(ctx, seq) {
		e.drop(c, "created_not_published")
		_ = m.Nak()
		return
	}
	e.persistClaim(c, cache.StateClaiming)
	if err := m.Ack(); err != nil {
		e.log.Warn("task_ack_failed", "task_id", ref.TaskID, "error", err.Error())
	}
	started := make(chan struct{})
	go e.claimWorker(c, started)
	select {
	case <-started:
	case <-ctx.Done():
	}
}

// claimWorker takes one claim from its token to its terminal event.
func (e *Executor) claimWorker(c *claim, started chan struct{}) {
	if !e.claimToken(c, started) {
		return
	}
	// Checks that need no dependency.
	if c.msg.ResolvedImage == nil || *c.msg.ResolvedImage == "" {
		e.fail(c, platform.FailureImageMissing, false)
		return
	}
	ex := c.msg.Execution
	if ex == nil || ex.RenderedPrompt == "" || ex.ExecutionRevisionID <= 0 {
		e.fail(c, platform.FailureInvalidFlowRevision, false)
		return
	}
	values, llm, ok := e.checkUntilReady(c)
	if !ok {
		return
	}
	e.runTask(c, values, llm)
}

// claimToken asks task-manager for the claim token: claim_not_processed and
// uncertain answers are repeated every second for 10 s. started is closed
// once the first request has ended, so the next task message is handled
// only after this one's first claim-token exchange (queue order); that
// waits at most the broker request timeout, inside the 10 s.
func (e *Executor) claimToken(c *claim, started chan struct{}) bool {
	body := map[string]any{"task_id": c.ref.TaskID, "attempt": c.ref.Attempt, "claim_seq": c.ref.ClaimSeq, "created_publisher_seq": c.createdSeq}
	deadline := e.now().Add(10 * time.Second)
	first := true
	defer func() {
		if first {
			close(started)
		}
	}()
	for {
		reply, err := e.request(c.ctx, "task-manager", "claim-token", body)
		if first {
			close(started)
			first = false
		}
		if c.ctx.Err() != nil {
			return false
		}
		if err == nil && reply.Status == 200 {
			var r platform.ClaimTokenReply
			if json.Unmarshal(reply.Data, &r) == nil && r.ClaimToken != "" {
				c.mu.Lock()
				c.token, c.tokenAt = r.ClaimToken, e.now()
				c.mu.Unlock()
				return true
			}
		}
		if err == nil && reply.Status == 409 && errorCode(reply) == "claim_rejected" {
			e.drop(c, "claim_rejected")
			return false
		}
		// The deadline is checked after the wait, so no request is sent
		// once 10 s have passed, even when the wait ends late.
		if !e.sleep(c.ctx, time.Second) {
			return false
		}
		if !e.now().Before(deadline) {
			e.drop(c, "claim_token_unavailable")
			return false
		}
	}
}

func errorCode(r *broker.Reply) string {
	var b struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(r.Data, &b)
	return b.Code
}

// fail publishes failed for a claim and cleans it up.
func (e *Executor) fail(c *claim, reason string, retryable bool, extra ...map[string]any) {
	fields := map[string]any{"failure_reason": reason, "retryable": retryable}
	for _, x := range extra {
		for k, v := range x {
			fields[k] = v
		}
	}
	e.terminate(c, platform.KindFailed, fields, e.cfg.FailedCleanupDelay)
}

// terminate publishes a terminal event and, after the broker acknowledged
// it and the cleanup delay passed, deletes the Pod and frees the slot.
func (e *Executor) terminate(c *claim, kind string, fields map[string]any, delay time.Duration) {
	seq, ok := e.end(c, kind, fields)
	if !ok {
		return
	}
	e.log.Info("claim_"+kind, "task_id", c.ref.TaskID, "attempt", c.ref.Attempt, "claim_seq", c.ref.ClaimSeq)
	ctx := e.runCtx
	if !e.waitSent(ctx, seq) {
		return
	}
	if c.podName != "" && !e.sleep(ctx, delay) {
		return
	}
	e.deletePod(c)
	e.remove(c)
}

// check outcomes.
type checkResult int

const (
	checkReady checkResult = iota
	checkPending
	checkFatal
	checkDrop
)

// checkUntilReady runs the model check and the two edges until they pass,
// keeping the claim pending (with one pending event per reason) until the
// task's pending limit.
func (e *Executor) checkUntilReady(c *claim) (map[string]string, edges.LLM, bool) {
	var since time.Time
	limit := time.Duration(c.msg.PendingLimitSeconds) * time.Second
	for {
		values, llm, res, reason := e.runChecks(c)
		if c.ctx.Err() != nil {
			return nil, edges.LLM{}, false
		}
		switch res {
		case checkReady:
			return values, llm, true
		case checkDrop:
			e.drop(c, reason)
			return nil, edges.LLM{}, false
		case checkFatal:
			e.fail(c, reason, false)
			return nil, edges.LLM{}, false
		}
		now := e.now()
		if since.IsZero() {
			since = now
		}
		c.mu.Lock()
		changed := c.reason != reason
		c.mu.Unlock()
		if changed {
			if !e.emitFor(c, platform.KindPending, map[string]any{"reason": reason}) {
				return nil, edges.LLM{}, false
			}
			c.mu.Lock()
			c.reason, c.state = reason, statePending
			c.mu.Unlock()
			e.persistClaim(c, cache.StatePending)
		}
		remaining := since.Add(limit).Sub(now)
		if remaining <= 0 {
			e.terminate(c, platform.KindReleased, map[string]any{"reason": platform.ReleasePendingTimeout}, 0)
			return nil, edges.LLM{}, false
		}
		wait := e.cfg.PendingRecheckInterval
		if remaining < wait {
			wait = remaining
		}
		if !e.sleep(c.ctx, wait) {
			return nil, edges.LLM{}, false
		}
	}
}

// runChecks asks model-registry and the two edges once.
func (e *Executor) runChecks(c *claim) (map[string]string, edges.LLM, checkResult, string) {
	body := map[string]any{"task_id": c.ref.TaskID, "attempt": c.ref.Attempt, "claim_seq": c.ref.ClaimSeq}
	reply, err := e.request(c.ctx, "model-registry", "check", body)
	switch {
	case err != nil:
		return nil, edges.LLM{}, checkPending, platform.PendingDependencyUnavailable
	case reply.Status == 404:
		return nil, edges.LLM{}, checkDrop, "claim_not_current"
	case reply.Status != 200:
		return nil, edges.LLM{}, checkPending, platform.PendingDependencyUnavailable
	}
	var r platform.CheckReply
	if json.Unmarshal(reply.Data, &r) != nil {
		return nil, edges.LLM{}, checkPending, platform.PendingDependencyUnavailable
	}
	switch r.Result {
	case "ok":
	case "temporary":
		if r.Reason == "" {
			r.Reason = platform.PendingDependencyUnavailable
		}
		return nil, edges.LLM{}, checkPending, r.Reason
	default:
		return nil, edges.LLM{}, checkPending, platform.PendingDependencyUnavailable
	}
	c.mu.Lock()
	token := c.token
	c.mu.Unlock()
	values, err := e.edges.OpenLaunchParameters(c.ctx, c.ref.TaskID, token)
	if res, reason, done := edgeOutcome(err); done {
		return nil, edges.LLM{}, res, reason
	}
	llm, err := e.edges.LiteLLMKey(c.ctx, c.ref.TaskID, token)
	if res, reason, done := edgeOutcome(err); done {
		return nil, edges.LLM{}, res, reason
	}
	return values, llm, checkReady, ""
}

func edgeOutcome(err error) (checkResult, string, bool) {
	if err == nil {
		return checkReady, "", false
	}
	var ce *edges.ConfigError
	switch {
	case errors.Is(err, edges.ErrClaimNotCurrent):
		return checkDrop, "claim_not_current", true
	case errors.As(err, &ce):
		return checkFatal, ce.FailureReason, true
	}
	return checkPending, platform.PendingDependencyUnavailable, true
}
