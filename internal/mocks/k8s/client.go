// Package k8s holds the in-memory fake of the Executor's k8sclient.Client.
// A created Pod reports Running at PodIP (default 127.0.0.1, where the test
// runs the fake OpenHands agent server); tests can fail a Pod, bump its
// agent container's restart count, or make its image pull fail.
package k8s

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
)

// FakeClient is an in-memory k8sclient.Client.
type FakeClient struct {
	mu sync.Mutex

	Pods        map[string]*PodRecord
	CreateCalls []k8sclient.PodSpec
	DeleteCalls []string
	CreateError error
	DeleteError error
	PodIP       string
	// Order, when set, numbers every DeletePod call among other fakes' calls.
	Order        func() int64
	deleteOrders []int64
	healthy      bool
	uid          int
}

// PodRecord is one fake Pod.
type PodRecord struct {
	Spec     k8sclient.PodSpec
	Ref      k8sclient.PodRef
	Status   k8sclient.PodStatus
	Restarts int32
	Reason   string
	Created  time.Time
}

// NewFakeClient returns an empty, healthy fake.
func NewFakeClient() *FakeClient {
	return &FakeClient{Pods: map[string]*PodRecord{}, healthy: true, PodIP: "127.0.0.1"}
}

// SetHealthy toggles Healthy.
func (f *FakeClient) SetHealthy(v bool) { f.mu.Lock(); f.healthy = v; f.mu.Unlock() }

// SetCreateError makes CreatePod fail.
func (f *FakeClient) SetCreateError(err error) { f.mu.Lock(); f.CreateError = err; f.mu.Unlock() }

// AddPod inserts a Pod as if a previous process had created it.
func (f *FakeClient) AddPod(spec k8sclient.PodSpec) k8sclient.PodRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uid++
	ref := k8sclient.PodRef{Name: spec.Name, Namespace: spec.Namespace, UID: fmt.Sprintf("uid-%d", f.uid)}
	f.Pods[spec.Name] = &PodRecord{Spec: spec, Ref: ref, Status: k8sclient.PodStatusRunning, Created: time.Now()}
	return ref
}

// CreatePod implements k8sclient.Client.
func (f *FakeClient) CreatePod(ctx context.Context, spec k8sclient.PodSpec) (*k8sclient.PodRef, error) {
	f.mu.Lock()
	f.CreateCalls = append(f.CreateCalls, spec)
	if f.CreateError != nil {
		err := f.CreateError
		f.mu.Unlock()
		return nil, err
	}
	if _, exists := f.Pods[spec.Name]; exists {
		f.mu.Unlock()
		return nil, fmt.Errorf("pod %s already exists", spec.Name)
	}
	f.mu.Unlock()
	ref := f.AddPod(spec)
	return &ref, nil
}

func (f *FakeClient) state(rec *PodRecord) k8sclient.PodState {
	labels := map[string]string{}
	for k, v := range rec.Spec.Labels {
		labels[k] = v
	}
	st := k8sclient.PodState{
		Ref: rec.Ref, Status: rec.Status, Image: rec.Spec.Image, Labels: labels,
		RestartCount: rec.Restarts, StartedAt: rec.Created,
		ContainerStatuses: []k8sclient.ContainerStatus{{
			Name: "agent", Ready: rec.Status == k8sclient.PodStatusRunning && rec.Reason == "",
			RestartCount: rec.Restarts, Image: rec.Spec.Image, Reason: rec.Reason,
		}},
	}
	if rec.Status == k8sclient.PodStatusRunning && rec.Reason == "" {
		st.PodIP = f.PodIP
	}
	return st
}

// GetPod implements k8sclient.Client.
func (f *FakeClient) GetPod(ctx context.Context, namespace, name string) (*k8sclient.PodState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.Pods[name]
	if !ok || rec.Spec.Namespace != namespace {
		return nil, ErrNotFound
	}
	st := f.state(rec)
	return &st, nil
}

// DeletePod implements k8sclient.Client; deleting a missing Pod succeeds.
func (f *FakeClient) DeletePod(ctx context.Context, namespace, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.DeleteCalls = append(f.DeleteCalls, name)
	var order int64
	if f.Order != nil {
		order = f.Order()
	}
	f.deleteOrders = append(f.deleteOrders, order)
	if f.DeleteError != nil {
		err := f.DeleteError
		f.DeleteError = nil
		return err
	}
	delete(f.Pods, name)
	return nil
}

// ListMatchingPods implements k8sclient.Client.
func (f *FakeClient) ListMatchingPods(ctx context.Context, namespace string, selector map[string]string) ([]k8sclient.PodState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []k8sclient.PodState
	for _, rec := range f.Pods {
		if rec.Spec.Namespace != namespace || !containsAll(rec.Spec.Labels, selector) {
			continue
		}
		out = append(out, f.state(rec))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.Name < out[j].Ref.Name })
	return out, nil
}

// WatchPod implements k8sclient.Client with a stream that only closes.
func (f *FakeClient) WatchPod(ctx context.Context, namespace, name string) (<-chan k8sclient.PodState, func(), error) {
	watchCtx, cancel := context.WithCancel(ctx)
	ch := make(chan k8sclient.PodState)
	go func() { <-watchCtx.Done(); close(ch) }()
	return ch, cancel, nil
}

// Healthy implements k8sclient.Client.
func (f *FakeClient) Healthy(ctx context.Context) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy
}

// IncrementRestart bumps the agent container's restart count.
func (f *FakeClient) IncrementRestart(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.Pods[name]; ok {
		rec.Restarts++
	}
}

// SetStatus sets a Pod's phase.
func (f *FakeClient) SetStatus(name string, status k8sclient.PodStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.Pods[name]; ok {
		rec.Status = status
	}
}

// SetWaitingReason sets the agent container's waiting reason.
func (f *FakeClient) SetWaitingReason(name, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.Pods[name]; ok {
		rec.Reason = reason
	}
}

// PodNames lists the existing Pods.
func (f *FakeClient) PodNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for n := range f.Pods {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// PodByLabel returns the first Pod whose label key has value.
func (f *FakeClient) PodByLabel(key, value string) (PodRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.Pods))
	for n := range f.Pods {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if f.Pods[n].Spec.Labels[key] == value {
			return *f.Pods[n], true
		}
	}
	return PodRecord{}, false
}

// Creates returns a copy of the CreatePod calls.
func (f *FakeClient) Creates() []k8sclient.PodSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]k8sclient.PodSpec(nil), f.CreateCalls...)
}

// Deletes returns a copy of the DeletePod calls.
func (f *FakeClient) Deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.DeleteCalls...)
}

// DeleteOrders returns the Order value of each DeletePod call, aligned with Deletes.
func (f *FakeClient) DeleteOrders() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.deleteOrders...)
}

// Count is the number of existing Pods.
func (f *FakeClient) Count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.Pods) }

// ErrNotFound is returned for a missing Pod.
var ErrNotFound = errors.New("k8sclient: pod not found")

func containsAll(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

var _ k8sclient.Client = (*FakeClient)(nil)
