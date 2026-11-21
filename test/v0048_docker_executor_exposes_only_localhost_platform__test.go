package k8sopenhands_test

// Requirement: Docker Executor exposes only localhost platform health probes
// (rows 318–319), K8s analogue: the one :8080 listener serves only
// /v1/livez and /v1/readyz (agents.md rule 16).

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
)

// Scenario: Operator hits the liveness endpoint (row 318).
func TestOperatorHitsTheLivenessEndpoint(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.waitRunning(tk)
	p, _ := h.pod(tk)
	h.kube.SetStatus(p.Spec.Name, k8sclient.PodStatusFailed)
	h.waitKind(tk, "failed")
	if code := h.probe("/v1/livez"); code != 200 {
		t.Fatalf("livez %d with a failed task Pod", code)
	}
	for _, path := range []string{"/v1/tasks", "/v1/tasks/1/interrupt", "/v1/openhands", "/healthz"} {
		rec := httptest.NewRecorder()
		h.exec.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		if rec.Code != 404 && rec.Code != 405 {
			t.Fatalf("%s answered %d", path, rec.Code)
		}
	}
}
