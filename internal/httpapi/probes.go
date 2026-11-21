// Package httpapi provides the platform HTTP server scaffolding shared by all
// FlowAI backend services.
package httpapi

import (
	"net/http"
	"sync/atomic"

	"github.com/go-chi/chi/v5"
)

// LivenessResponse is the GET /v1/livez body.
type LivenessResponse struct {
	Status     string `json:"status"`
	ExecutorID string `json:"executor_id"`
}

// ReadinessResponse is the GET /v1/readyz body.
type ReadinessResponse struct {
	Status          string `json:"status"`
	ExecutorID      string `json:"executor_id"`
	BrokerConnected bool   `json:"broker_connected"`
	KubeReachable   bool   `json:"kube_reachable"`
}

// ReadinessChecker returns true if the service is ready to serve traffic.
// `deps` carries dependency health flags for the readiness response. The
// flags feed the ReadinessResponse body.
type ReadinessChecker func() (ready bool, deps ReadinessDeps)

// ReadinessDeps bundles the dependency flags surfaced by the readiness
// response. Additional flags may be added without breaking the contract.
type ReadinessDeps struct {
	BrokerConnected bool
	KubeReachable   bool
}

// ReadinessFunc adapts a simple func that only knows ready/not-ready.
func ReadinessFunc(fn func() bool) ReadinessChecker {
	return func() (bool, ReadinessDeps) {
		return fn(), ReadinessDeps{}
	}
}

// ReadinessFuncWithDeps adapts a func that owns its dependency flags.
func ReadinessFuncWithDeps(fn func() (bool, bool, bool)) ReadinessChecker {
	return func() (bool, ReadinessDeps) {
		ok, connected, reachable := fn()
		return ok, ReadinessDeps{
			BrokerConnected: connected,
			KubeReachable:   reachable,
		}
	}
}

// RegisterProbes mounts GET /v1/livez and GET /v1/readyz onto the router.
//
//   - /livez returns the LivenessResponse shape (status="ok", executor_id).
//   - /readyz returns the ReadinessResponse shape with broker_connected
//     and kube_reachable, and 503 when the checker reports not ready.
func RegisterProbes(r chi.Router, serviceName, executorID string, ready ReadinessChecker) {
	RegisterProbesWithExecutorID(r, serviceName, func() string { return executorID }, ready)
}

// RegisterProbesWithExecutorID resolves executor_id per request; it is known
// only after the JWT was read.
func RegisterProbesWithExecutorID(r chi.Router, serviceName string, executorID func() string, ready ReadinessChecker) {
	r.Get("/livez", func(w http.ResponseWriter, req *http.Request) {
		JSON(w, req, http.StatusOK, LivenessResponse{
			Status:     "ok",
			ExecutorID: executorID(),
		})
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		ok, deps := ready()
		body := ReadinessResponse{
			ExecutorID: executorID(),
		}
		body.Status = "not_ready"
		if ok {
			body.Status = "ready"
		}
		body.BrokerConnected = deps.BrokerConnected
		body.KubeReachable = deps.KubeReachable
		status := http.StatusOK
		if !ok {
			status = http.StatusServiceUnavailable
		}
		JSON(w, req, status, body)
	})
}

// AtomicBool is a thin wrapper around atomic.Bool for use as a
// ReadinessChecker dependency flag.
type AtomicBool struct {
	v atomic.Bool
}

// Set updates the value.
func (a *AtomicBool) Set(v bool) { a.v.Store(v) }

// Get returns the value.
func (a *AtomicBool) Get() bool { return a.v.Load() }
