package executor

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/cache"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// timestamp is RFC 3339 UTC with milliseconds.
func timestamp(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// sanitize removes the JWT and its parts from a log text.
func sanitize(s, token string) string {
	if token == "" {
		return s
	}
	s = strings.ReplaceAll(s, token, "[redacted]")
	for _, p := range strings.Split(token, ".") {
		if len(p) >= 8 {
			s = strings.ReplaceAll(s, p, "[redacted]")
		}
	}
	return s
}

// emit persists one event in the outbox with the next publisher_seq and
// tries to publish every pending event. c is nil for a self event.
func (e *Executor) emit(c *claim, kind string, fields map[string]any) (int64, error) {
	if e.aborted() {
		return 0, context.Canceled
	}
	e.pubMu.Lock()
	seq, err := e.seq.Next()
	if err != nil {
		e.pubMu.Unlock()
		e.log.Error("event_sequence_unavailable", "kind", kind, "error", err.Error())
		return 0, err
	}
	now := e.now()
	ev := map[string]any{
		"publisher": "executor", "publisher_seq": seq, "team_id": e.id.TeamID, "executor_id": e.id.ExecutorID,
		"kind": kind, "occurred_at": timestamp(now),
	}
	key := ""
	if c != nil {
		ev["task_id"], ev["attempt"], ev["claim_seq"] = c.ref.TaskID, c.ref.Attempt, c.ref.ClaimSeq
		key = c.key
	}
	for k, v := range fields {
		ev[k] = v
	}
	data, _ := json.Marshal(ev)
	out := cache.OutboxEvent{PublisherSeq: seq, Subject: platform.EventSubject(e.id.ExecutorID, kind),
		MsgID: platform.MsgID(e.id.TeamID, e.id.ExecutorID, seq), Team: strconv.FormatInt(e.id.TeamID, 10),
		Kind: kind, Body: data, ClaimKey: key, State: cache.OutboxPending, At: now}
	err = e.store.PutOutbox(out)
	e.pubMu.Unlock()
	if err != nil {
		e.log.Error("event_outbox_unavailable", "kind", kind, "error", err.Error())
		return 0, err
	}
	e.flushOutbox()
	return seq, nil
}

// emitSelf publishes an Executor self event.
func (e *Executor) emitSelf(kind string, fields map[string]any) {
	_, _ = e.emit(nil, kind, fields)
}

// flushOutbox publishes every pending outbox event in publisher_seq order
// with its original bytes. A failed publish is logged and retried later; it
// does not hold back the events after it.
func (e *Executor) flushOutbox() {
	if !e.canPublish() || e.aborted() {
		return
	}
	e.flushMu.Lock()
	defer e.flushMu.Unlock()
	pending, err := e.store.PendingOutbox()
	if err != nil {
		return
	}
	for _, ev := range pending {
		if !e.canPublish() || e.aborted() {
			return
		}
		conn := e.getConn()
		if conn == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := conn.Publish(ctx, ev.Subject, broker.Header{"team": ev.Team, "Nats-Msg-Id": ev.MsgID}, ev.Body)
		cancel()
		if err != nil {
			e.log.Warn("event_publish_failed", "kind", ev.Kind, "publisher_seq", ev.PublisherSeq, "error", err.Error())
			continue
		}
		_ = e.store.MarkSent(ev.PublisherSeq)
	}
	_ = e.store.DeleteSent()
}

// outboxLoop retries pending events.
func (e *Executor) outboxLoop(ctx context.Context) {
	for e.sleep(ctx, e.cfg.PollInterval) {
		e.flushOutbox()
	}
}

// sent reports whether an event left the outbox.
func (e *Executor) sent(seq int64) bool {
	pending, err := e.store.PendingOutbox()
	if err != nil {
		return false
	}
	for _, ev := range pending {
		if ev.PublisherSeq == seq {
			return false
		}
	}
	return true
}

// waitSent waits until the broker acknowledged the event; false when ctx
// ended first.
func (e *Executor) waitSent(ctx context.Context, seq int64) bool {
	for !e.sent(seq) {
		if !e.sleep(ctx, e.cfg.PollInterval/4) {
			return false
		}
		e.flushOutbox()
	}
	return true
}
