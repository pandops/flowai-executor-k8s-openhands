package executor

import (
	"context"
	"strconv"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/cache"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

// startupCleanup deletes every task Pod of this Executor left by a previous
// process and reads the cached claims: within W they are released after the
// connect, older ones are dropped with their unsent events.
func (e *Executor) startupCleanup(ctx context.Context) ([]cache.Claim, error) {
	sel := map[string]string{k8sclient.LabelExecutorID: strconv.FormatInt(e.id.ExecutorID, 10), k8sclient.LabelRuntime: "k8s"}
	pods, err := e.kube.ListMatchingPods(ctx, e.cfg.Namespace, sel)
	if err != nil {
		e.log.Warn("leftover_pod_list_failed", "error", err.Error())
	}
	for _, p := range pods {
		if err := e.kube.DeletePod(ctx, e.cfg.Namespace, p.Ref.Name); err != nil {
			e.log.Warn("leftover_pod_delete_failed", "pod", p.Ref.Name, "error", err.Error())
		} else {
			e.log.Info("leftover_pod_deleted", "pod", p.Ref.Name)
		}
	}
	claims, err := e.store.ListClaims()
	if err != nil {
		e.log.Error("executor_cache_unusable", "error", err.Error())
		return nil, err
	}
	last, _ := e.store.LastHeartbeat()
	expired := !last.IsZero() && e.now().Sub(last) >= e.cfg.ConnectionWait
	var keep []cache.Claim
	for _, c := range claims {
		key := cache.ClaimKey(c.TaskID, c.Attempt, c.ClaimSeq)
		if expired || c.State == cache.StateTerminal {
			if expired {
				_ = e.store.DropUnsentForClaim(key)
			}
			_ = e.store.DeleteClaim(c.TaskID, c.Attempt, c.ClaimSeq)
			continue
		}
		keep = append(keep, c)
	}
	if expired && len(claims) > 0 {
		e.log.Info("cached_claims_dropped_after_connection_wait", "claims", len(claims))
	}
	return keep, nil
}

// releaseRestarted publishes released executor_restarted for each cached claim.
func (e *Executor) releaseRestarted(claims []cache.Claim) {
	for _, rec := range claims {
		ref := platform.ClaimRef{TaskID: rec.TaskID, Attempt: rec.Attempt, ClaimSeq: rec.ClaimSeq}
		ctx, cancel := context.WithCancel(e.runCtx)
		c := &claim{ref: ref, key: cache.ClaimKey(ref.TaskID, ref.Attempt, ref.ClaimSeq), ctx: ctx, cancel: cancel}
		seq, _ := e.end(c, platform.KindReleased, map[string]any{"reason": platform.ReleaseExecutorRestarted})
		e.waitSent(e.runCtx, seq)
		_ = e.store.DeleteClaim(rec.TaskID, rec.Attempt, rec.ClaimSeq)
	}
}

// shutdown is the SIGTERM path.
func (e *Executor) shutdown(ctx context.Context) {
	e.mu.Lock()
	e.stopping = true
	loss := e.inLoss
	e.mu.Unlock()
	e.ready.Store(false)

	if !e.canPublish() {
		// Nothing can be published: delete the Pods at once and forget the
		// claims with their unsent events.
		for _, c := range e.claimList() {
			e.drop(c, "shutdown_without_connection")
		}
		e.log.Info("executor_stopped", "published", false)
		return
	}
	e.emitSelf(platform.KindExecutorStopping, nil)
	deadline := e.now().Add(e.cfg.DrainTimeout())
	release := func(c *claim) {
		if _, ok := e.end(c, platform.KindReleased, map[string]any{"reason": platform.ReleaseExecutorShutdown}); ok {
			e.deletePod(c)
		}
	}
	for _, c := range e.claimList() {
		if loss || !c.running() {
			release(c)
		}
	}
	for e.now().Before(deadline) {
		open := false
		for _, c := range e.claimList() {
			if c.running() {
				open = true
			}
		}
		if !open {
			break
		}
		if !e.sleep(ctx, e.cfg.PollInterval/4) {
			break
		}
	}
	for _, c := range e.claimList() {
		release(c)
		e.deletePod(c)
		e.remove(c)
	}
	e.emitSelf(platform.KindExecutorStopped, nil)
	until := e.now().Add(5 * time.Second)
	for !e.allSent() && e.now().Before(until) && e.sleep(ctx, e.cfg.PollInterval/4) {
		e.flushOutbox()
	}
}

func (e *Executor) allSent() bool {
	p, err := e.store.PendingOutbox()
	return err == nil && len(p) == 0
}
