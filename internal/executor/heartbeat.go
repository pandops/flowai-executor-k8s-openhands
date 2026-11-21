package executor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/netinfo"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

// heartbeatLoop sends one heartbeat per interval, also in connection loss.
func (e *Executor) heartbeatLoop(ctx context.Context) {
	for {
		e.mu.Lock()
		stopping := e.stopping
		e.mu.Unlock()
		if stopping {
			return
		}
		e.heartbeat(ctx)
		if !e.sleep(ctx, e.cfg.HeartbeatInterval) {
			return
		}
	}
}

func (e *Executor) heartbeatBody() map[string]any {
	claims := []platform.HeartbeatClaim{}
	slots := 0
	for _, c := range e.claimList() {
		slots++
		c.mu.Lock()
		if !c.ended {
			claims = append(claims, platform.HeartbeatClaim{TaskID: c.ref.TaskID, Attempt: c.ref.Attempt, ClaimSeq: c.ref.ClaimSeq, State: c.state})
		}
		c.mu.Unlock()
	}
	var ip any
	if addr, ok := netinfo.PrimaryIP(e.opts.Net); ok {
		ip = addr
	} else {
		e.log.Warn("heartbeat_ip_unavailable")
	}
	return map[string]any{
		"executor_type": platform.ExecutorTypeK8sOpenHands, "interval_seconds": int64(e.cfg.HeartbeatInterval / time.Second),
		"miss_limit": e.cfg.HeartbeatMissLimit, "wait_seconds": int64(e.cfg.ConnectionWait / time.Second),
		"max_parallel_tasks": e.cfg.MaxParallelTasks, "slots_in_use": slots, "ip": ip, "claims": claims,
	}
}

// heartbeat sends one heartbeat and applies its reply.
func (e *Executor) heartbeat(ctx context.Context) {
	sentAt := e.now()
	reply, err := e.request(ctx, "executor-registry", "heartbeat", e.heartbeatBody())
	if ctx.Err() != nil {
		return
	}
	var r platform.HeartbeatReply
	ok := err == nil && reply.Status == 200 && json.Unmarshal(reply.Data, &r) == nil
	if !ok {
		e.missed()
		return
	}
	now := e.now()
	e.mu.Lock()
	e.misses = 0
	e.lastHB = now
	e.mu.Unlock()
	_ = e.store.PutLastHeartbeat(now)
	if !r.ClaimsKnown {
		return
	}
	listed := map[platform.ClaimRef]bool{}
	for _, c := range r.Claims {
		listed[c] = true
	}
	for _, c := range e.claimList() {
		c.mu.Lock()
		drop := !c.ended && !listed[c.ref] && !c.tokenAt.IsZero() && c.tokenAt.Before(sentAt)
		c.mu.Unlock()
		if drop {
			e.drop(c, "claim_missing_in_heartbeat_reply")
		}
	}
	e.restore(now)
}

// missed counts a heartbeat without a reply.
func (e *Executor) missed() {
	e.mu.Lock()
	e.misses++
	enter := e.misses >= e.cfg.HeartbeatMissLimit && !e.inLoss
	e.mu.Unlock()
	if enter {
		e.log.Warn("connection_lost", "missed_heartbeats", e.cfg.HeartbeatMissLimit)
		e.enterLoss()
	}
	e.checkW()
}

// enterLoss pauses every running agent and stops pulling.
func (e *Executor) enterLoss() {
	e.mu.Lock()
	if e.inLoss {
		e.mu.Unlock()
		return
	}
	e.inLoss = true
	e.lostAt = e.now()
	e.lossFlag.Store(true)
	e.mu.Unlock()
	for _, c := range e.claimList() {
		if c.running() {
			e.pauseForLoss(c)
		}
	}
	e.checkW()
}

func (e *Executor) pauseForLoss(c *claim) {
	c.mu.Lock()
	if c.ended || c.oh == nil || c.lossPaused {
		c.mu.Unlock()
		return
	}
	c.pausedByUs, c.lossPaused = true, true
	oh, conv := c.oh, c.convID
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	if err := oh.PauseConversation(ctx, conv); err != nil {
		e.log.Warn("agent_pause_failed", "task_id", c.ref.TaskID, "error", err.Error())
	}
}

// checkW drops every claim once W passed since the last heartbeat reply.
func (e *Executor) checkW() {
	e.mu.Lock()
	expired := e.inLoss && !e.wExpired && !e.lastHB.IsZero() && e.now().Sub(e.lastHB) >= e.cfg.ConnectionWait
	if expired {
		e.wExpired = true
	}
	e.mu.Unlock()
	if !expired {
		return
	}
	e.log.Warn("connection_wait_expired")
	for _, c := range e.claimList() {
		e.drop(c, "connection_wait_expired")
	}
}

// restore ends connection loss after a reply with known claims: the listed
// paused agents continue and the loss is published.
func (e *Executor) restore(now time.Time) {
	e.mu.Lock()
	if !e.inLoss || e.revoked.Load() {
		e.mu.Unlock()
		return
	}
	lostAt := e.lostAt
	e.mu.Unlock()
	for _, c := range e.claimList() {
		c.mu.Lock()
		resume := !c.ended && c.lossPaused && c.oh != nil
		oh, conv := c.oh, c.convID
		c.mu.Unlock()
		if !resume {
			continue
		}
		ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
		err := oh.RunConversation(ctx, conv)
		cancel()
		if err != nil {
			e.log.Warn("agent_resume_failed", "task_id", c.ref.TaskID, "error", err.Error())
		}
		c.mu.Lock()
		c.pausedByUs, c.lossPaused = false, false
		c.mu.Unlock()
	}
	e.emitSelf(platform.KindExecutorConnectionLost, map[string]any{"lost_at": timestamp(lostAt)})
	e.emitSelf(platform.KindExecutorConnectionRestored, map[string]any{"lost_at": timestamp(lostAt), "restored_at": timestamp(now)})
	e.mu.Lock()
	e.inLoss, e.wExpired, e.misses = false, false, 0
	e.lossFlag.Store(false)
	e.mu.Unlock()
	e.log.Info("connection_restored")
}
