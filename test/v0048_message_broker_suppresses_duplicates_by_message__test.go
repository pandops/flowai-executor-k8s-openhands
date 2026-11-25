package k8sopenhands_test

// message-broker: message-broker suppresses duplicates by publisher sequence (rows 549–550).

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/seqfile"
)

// Scenario: Equal sequence numbers of two publishers are distinct messages (row 549).
func TestEqualSequenceNumbersOfTwoPublishersAreDistinctMessages(t *testing.T) {
	if got := platform.MsgID(3, 7, 40); got != "executor-3-7-40" {
		t.Fatalf("MsgID = %q", got)
	}
	h := newHarness(t)
	h.start()
	h.waitConnected()
	for _, r := range h.publishedRecords() {
		if id := r.Header["Nats-Msg-Id"]; id != "executor-3-7-"+itoa(num(r.JSON()["publisher_seq"])) {
			t.Fatalf("Nats-Msg-Id %q", id)
		}
	}
}

// Scenario: Executor sequence continues after restart (row 550).
func TestExecutorSequenceContinuesAfterRestart(t *testing.T) {
	h := newHarness(t)
	c, err := seqfile.Open(h.cfg.StateFile)
	if err != nil {
		t.Fatalf("open state file: %v", err)
	}
	for c.Last() < 900 {
		if _, err := c.Next(); err != nil {
			t.Fatal(err)
		}
	}
	h.start()
	h.waitSelf("executor_started")
	h.crash()
	h.start()
	eventually(t, 10*time.Second, func() bool { return len(h.events("executor_started")) == 2 }, "restart")
	seen := map[int64]bool{}
	for _, r := range h.publishedRecords() {
		s := num(r.JSON()["publisher_seq"])
		if s <= 900 || seen[s] {
			t.Fatalf("publisher_seq %d reused or not above 900", s)
		}
		seen[s] = true
	}
}
