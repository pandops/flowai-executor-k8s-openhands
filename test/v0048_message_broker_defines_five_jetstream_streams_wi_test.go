package k8sopenhands_test

// message-broker: message-broker defines five JetStream streams with fixed
// retention (row 543), the Executor's part: consumer names derived from
// identifiers and the tag hash.

import (
	"regexp"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

// Scenario: Consumer names are derived from identifiers and the tag hash (row 543).
func TestConsumerNamesAreDerivedFromIdentifiersAndTheTagHash(t *testing.T) {
	valid := regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)
	want := "tasks-3-" + tagHash("GPU_large-1")
	if got := platform.TaskConsumerName(3, "GPU_large-1"); got != want || !valid.MatchString(got) {
		t.Fatalf("TaskConsumerName = %q, want %q", got, want)
	}
	if got := platform.TagHash("GPU_large-1"); len(got) != 16 || got != tagHash("GPU_large-1") {
		t.Fatalf("TagHash %q", got)
	}
	for id, want := range map[int64]string{7: "commands-7", 8: "commands-8"} {
		if got := platform.CommandConsumerName(id); got != want || !valid.MatchString(got) {
			t.Fatalf("CommandConsumerName(%d) = %q", id, got)
		}
	}

	// Executors 7 and 8 of team 3 with the same tag pull the same task
	// consumer and each create only their own command consumer.
	for _, id := range []int64{7, 8} {
		h := newHarness(t, withConfig(func(c *configT) {}))
		h.token = makeJWT(teamID, id, "GPU_large-1")
		h.cfg.Broker.Token = h.token
		h.start()
		eventually(t, 10*time.Second, func() bool {
			return len(h.br.Fetches("tasks-3-"+tagHash("GPU_large-1"))) > 0
		}, "pull from the shared task consumer")
		for _, c := range h.br.Creates() {
			if c.Subject != "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-"+itoa(id)+".task-manager.command."+itoa(id)+".sub" {
				t.Fatalf("Executor %d created %s", id, c.Subject)
			}
		}
		h.stopQuietly()
	}
}
