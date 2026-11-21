package k8sopenhands_test

// message-broker: message-broker never carries secrets in streams or logs
// (rows 600–601). Row 600 (Model check carries no claim token) is covered by
// TestModelCheckCarriesNoClaimToken; this file covers row 601.

import (
	"strings"
	"testing"
)

// Scenario: Executor receives values only over HTTP edges (row 601).
func TestExecutorReceivesValuesOnlyOverHTTPEdges(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	p, _ := h.pod(tk)
	for k, v := range map[string]string{"DB_PASSWORD": "launch-secret-value", "OPENAI_API_KEY": "sk-task-litellm-key"} {
		if p.Spec.Env[k] != v && !strings.Contains(mustJSON(h.oh.StartBodies()), v) {
			t.Fatalf("%s did not reach the runtime", k)
		}
	}
	if len(h.edgeRequests("/launch-parameters/open")) != 1 || len(h.edgeRequests("/litellm-key")) != 1 {
		t.Fatalf("values not taken from the two HTTP edges")
	}
	if s := containsAny(h.allBrokerBytes(), "launch-secret-value", "sk-task-litellm-key"); s != "" {
		t.Fatalf("broker traffic carries %q", s)
	}
}
