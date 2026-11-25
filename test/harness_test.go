// Unit tests of executor_k8s_openhands. Every external dependency is an
// in-process fake: the broker (internal/mocks/broker), Kubernetes
// (internal/mocks/k8s, Pods report Running at 127.0.0.1), the OpenHands
// agent server (internal/mocks/openhands) and the two HTTP edges
// (httptest). A scaled clock runs the protocol timers (15 s heartbeats,
// 10 min W, 30 s rechecks) in milliseconds. Every message the Executor
// publishes or requests is validated against schemas/.
package k8sopenhands_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/clock"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/executor"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
	mclock "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/clock"
	fakekube "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/k8s"
	fakeopenhands "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/openhands"
)

const (
	teamID     = 3
	executorID = 7
	tag        = "gpu"
	namespace  = "flowai-executor-k8s"
	evPrefix   = "executor.event.7."
)

// tagHash is computed here independently of the Executor's code.
func tagHash(t string) string {
	sum := sha256.Sum256([]byte(t))
	return hex.EncodeToString(sum[:])[:16]
}

func taskDurable() string { return fmt.Sprintf("tasks-%d-%s", teamID, tagHash(tag)) }

// makeJWT builds a NATS user JWT whose name claim carries the identity
// (agents.md rule 14). The signature part is a fixed secret-looking value
// so tests can check that no part of the token reaches a log.
func makeJWT(team, executor int64, tg string, jti ...string) string {
	id := "QWERTYJTI0123456789"
	if len(jti) > 0 {
		id = jti[0]
	}
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"typ":"JWT","alg":"ed25519-nkey"}`))
	name, _ := json.Marshal(map[string]any{"team_id": team, "executor_id": executor, "tag": tg, "executor_type": "executor_k8s_openhands"})
	payload, _ := json.Marshal(map[string]any{
		"jti": id, "iat": 1762500000, "iss": "ACCOUNTSIGNINGKEYPUBLIC0000000000000000000000000000000000",
		"sub": "UEXECUTORUSERPUBLICKEY000000000000000000000000000000000000", "name": string(name),
		"nats": map[string]any{"type": "user", "version": 2, "bearer_token": true},
	})
	sig := enc.EncodeToString([]byte("super-secret-signature-bytes-0123456789abcdef-zz"))
	return header + "." + enc.EncodeToString(payload) + "." + sig
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buf.String() }

// fakeNet is a netinfo.Source with settable routes and addresses.
type fakeNet struct {
	mu    sync.Mutex
	v4    string
	v6    string
	addrs map[string][]net.IPNet
}

func (n *fakeNet) DefaultRouteInterface4() (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.v4, n.v4 != ""
}

func (n *fakeNet) DefaultRouteInterface6() (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.v6, n.v6 != ""
}

func (n *fakeNet) InterfaceAddrs(name string) ([]net.IPNet, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]net.IPNet(nil), n.addrs[name]...), nil
}

func (n *fakeNet) set(v4, v6 string, addrs map[string][]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.v4, n.v6 = v4, v6
	n.addrs = map[string][]net.IPNet{}
	for iface, list := range addrs {
		for _, cidr := range list {
			ip, ipnet, err := net.ParseCIDR(cidr)
			if err != nil {
				panic(err)
			}
			ipnet.IP = ip
			n.addrs[iface] = append(n.addrs[iface], *ipnet)
		}
	}
}

// edgeCall is one recorded HTTP edge request.
type edgeCall struct {
	Path          string
	Authorization string
	At            time.Time
}

// harness is one Executor with all fakes.
type harness struct {
	t     *testing.T
	clk   *mclock.Scaled
	ck    clock.Clock // the clock of the Executor and the fakes: clk unless withManualClock
	br    *fakebroker.Fake
	kube  *fakekube.FakeClient
	oh    *fakeopenhands.FakeOpenHands
	net   *fakeNet
	logs  *syncBuffer
	cfg   *config.Config
	token string

	edgeSrv   *httptest.Server
	edgeMu    sync.Mutex
	envOpen   func(taskID string) (int, any)
	keyEdge   func(taskID string) (int, any)
	edgeCalls []edgeCall

	hbMu      sync.Mutex
	heartbeat func(req fakebroker.Record) (*broker.Reply, error)

	exec   *executor.Executor
	cancel context.CancelFunc
	done   chan error
	runErr error

	nextTask   atomic.Int64
	violations []string
	vmu        sync.Mutex
	schemas    map[string]*jsonschema.Schema
}

type hopt func(*harness)

// withFactor sets the clock speed-up.
func withFactor(f float64) hopt { return func(h *harness) { h.clk = mclock.NewScaled(f) } }

// withManualClock runs the Executor and the fakes on m instead of the
// scaled clock; the test moves the time.
func withManualClock(m *mclock.Manual) hopt { return func(h *harness) { h.ck = m } }

// withConfig edits the configuration before the start.
func withConfig(fn func(*config.Config)) hopt { return func(h *harness) { fn(h.cfg) } }

func schemaDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "schemas")
}

func loadSchemas(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	out := map[string]*jsonschema.Schema{}
	for _, name := range []string{"executor-event", "claim-token-request", "heartbeat-request", "check-request"} {
		path := filepath.Join(schemaDir(), name+".schema.json")
		s, err := c.Compile(path)
		if err != nil {
			t.Fatalf("compile schema %s: %v", path, err)
		}
		out[name] = s
	}
	return out
}

func (h *harness) validate(kind string, data []byte) {
	s := h.schemas[kind]
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		h.violation(fmt.Sprintf("%s: invalid JSON %q", kind, data))
		return
	}
	if err := s.Validate(v); err != nil {
		h.violation(fmt.Sprintf("%s does not match its schema: %v\n%s", kind, err, data))
	}
}

func (h *harness) violation(s string) {
	h.vmu.Lock()
	h.violations = append(h.violations, s)
	h.vmu.Unlock()
}

// newHarness builds the fakes and the configuration; start() runs the Executor.
func newHarness(t *testing.T, opts ...hopt) *harness {
	t.Helper()
	h := &harness{t: t, clk: mclock.NewScaled(100), net: &fakeNet{}, logs: &syncBuffer{}, schemas: loadSchemas(t)}
	h.nextTask.Store(1000)
	h.net.set("eth0", "", map[string][]string{"eth0": {"10.42.0.9/24"}})
	h.oh = &fakeopenhands.FakeOpenHands{EmitPausedOnPause: true}
	h.oh.Start()
	t.Cleanup(h.oh.Stop)
	h.kube = fakekube.NewFakeClient()
	h.token = makeJWT(teamID, executorID, tag)

	h.envOpen = func(string) (int, any) {
		return 200, map[string]any{"values": map[string]string{"REPO_URL": "https://git.example.com/acme/app.git", "DB_PASSWORD": "launch-secret-value"}}
	}
	h.keyEdge = func(string) (int, any) {
		return 200, map[string]any{"OPENAI_BASE_URL": "http://litellm-gateway-data:4000/v1", "OPENAI_MODEL": "3/m1", "OPENAI_API_KEY": "sk-task-litellm-key"}
	}
	h.edgeSrv = httptest.NewServer(http.HandlerFunc(h.serveEdge))
	t.Cleanup(h.edgeSrv.Close)

	cfgDir := filepath.Join(t.TempDir(), "config")
	cacheDir := filepath.Join(t.TempDir(), "cache")
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatal(err)
	}
	h.cfg = &config.Config{
		Broker:                 config.Broker{URL: "tls://nats:4222", Token: h.token, CAFile: "/run/flowai/stand-ca/ca.pem", RequestTimeout: 5 * time.Second},
		EnvRegistryURL:         h.edgeSrv.URL,
		ModelRegistryURL:       h.edgeSrv.URL,
		MaxParallelTasks:       2,
		HeartbeatInterval:      15 * time.Second,
		HeartbeatMissLimit:     3,
		ConnectionWait:         600 * time.Second,
		PendingRecheckInterval: 30 * time.Second,
		DrainTimeoutSeconds:    30,
		PollInterval:           time.Second,
		PodPollInterval:        2 * time.Second,
		ExecutorAPIBind:        "127.0.0.1:0",
		LogLevel:               "debug",
		Namespace:              namespace,
		ServiceAccount:         "executor-k8s-openhands",
		ImagePullPolicy:        "IfNotPresent",
		CacheDir:               cacheDir,
		StateFile:              filepath.Join(cfgDir, "executor.state"),
		OpenHandsPort:          h.oh.Port(),
		OpenHandsAPIKey:        "session-api-key",
		OpenHandsWorkspace:     "/workspace/project",
		OpenHandsLLMUsageID:    "flowai-executor",
		OpenHandsInitialRun:    true,
		Path:                   filepath.Join(cfgDir, "config.yaml"),
	}
	for _, o := range opts {
		o(h)
	}
	if h.ck == nil {
		h.ck = h.clk
	}
	h.br = fakebroker.New(h.ck)
	h.kube.Order = h.br.NextOrder
	h.br.OnPublish = func(r fakebroker.Record) {
		if strings.HasPrefix(r.Subject, "executor.event.") {
			h.validate("executor-event", r.Data)
		}
	}
	h.br.OnRequest = func(r fakebroker.Record) {
		switch {
		case strings.HasSuffix(r.Subject, ".heartbeat.pub"):
			h.validate("heartbeat-request", r.Data)
		case strings.HasSuffix(r.Subject, ".claim-token.pub"):
			h.validate("claim-token-request", r.Data)
		case strings.HasSuffix(r.Subject, ".check.pub"):
			h.validate("check-request", r.Data)
		}
	}
	h.heartbeat = echoHeartbeat
	h.br.Respond("executor-registry.executor.*.heartbeat.pub", func(r fakebroker.Record) (*broker.Reply, error) {
		h.hbMu.Lock()
		fn := h.heartbeat
		h.hbMu.Unlock()
		return fn(r)
	})
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(r fakebroker.Record) (*broker.Reply, error) {
		return claimTokenReply(r, ""), nil
	})
	h.br.Respond("model-registry.executor.*.check.pub", func(fakebroker.Record) (*broker.Reply, error) {
		return fakebroker.JSONReply(200, map[string]any{"result": "ok", "model_alias": "3/m1"}), nil
	})
	t.Cleanup(func() {
		h.stopQuietly()
		h.vmu.Lock()
		defer h.vmu.Unlock()
		for _, v := range h.violations {
			t.Errorf("schema violation: %s", v)
		}
	})
	return h
}

// echoHeartbeat answers like executor-registry when task-manager holds
// every claim the Executor reports.
func echoHeartbeat(r fakebroker.Record) (*broker.Reply, error) {
	return fakebroker.JSONReply(200, map[string]any{"connection_state": "connected", "claims_known": true, "claims": claimRefs(r)}), nil
}

// body returns the request body object of a rule-9 envelope.
func body(r fakebroker.Record) map[string]any {
	m := r.JSON()
	b, _ := m["body"].(map[string]any)
	return b
}

func claimRefs(r fakebroker.Record) []map[string]any {
	out := []map[string]any{}
	claims, _ := body(r)["claims"].([]any)
	for _, c := range claims {
		m := c.(map[string]any)
		out = append(out, map[string]any{"task_id": m["task_id"], "attempt": m["attempt"], "claim_seq": m["claim_seq"]})
	}
	return out
}

func (h *harness) setHeartbeat(fn func(req fakebroker.Record) (*broker.Reply, error)) {
	h.hbMu.Lock()
	h.heartbeat = fn
	h.hbMu.Unlock()
}

// dropHeartbeats makes heartbeats get no reply.
func (h *harness) dropHeartbeats() {
	h.setHeartbeat(func(fakebroker.Record) (*broker.Reply, error) { return nil, broker.ErrTimeout })
}

func (h *harness) serveEdge(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	h.edgeMu.Lock()
	h.edgeCalls = append(h.edgeCalls, edgeCall{Path: r.URL.Path, Authorization: r.Header.Get("Authorization"), At: h.ck.Now()})
	envOpen, keyEdge := h.envOpen, h.keyEdge
	h.edgeMu.Unlock()
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "tasks" || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	var status int
	var out any
	switch strings.Join(parts[3:], "/") {
	case "litellm-key":
		status, out = keyEdge(parts[2])
	case "launch-parameters/open":
		status, out = envOpen(parts[2])
	default:
		http.NotFound(w, r)
		return
	}
	writeEdge(w, status, out)
}

func writeEdge(w http.ResponseWriter, status int, out any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(out)
}

func (h *harness) setEnvOpen(fn func(taskID string) (int, any)) {
	h.edgeMu.Lock()
	h.envOpen = fn
	h.edgeMu.Unlock()
}

func (h *harness) setKeyEdge(fn func(taskID string) (int, any)) {
	h.edgeMu.Lock()
	h.keyEdge = fn
	h.edgeMu.Unlock()
}

func (h *harness) edgeRequests(suffix string) []edgeCall {
	h.edgeMu.Lock()
	defer h.edgeMu.Unlock()
	var out []edgeCall
	for _, c := range h.edgeCalls {
		if strings.HasSuffix(c.Path, suffix) {
			out = append(out, c)
		}
	}
	return out
}

// claimTokenReply is task-manager's 200 reply; an empty token derives one
// from the claim.
func claimTokenReply(r fakebroker.Record, token string) *broker.Reply {
	b := body(r)
	if token == "" {
		token = fmt.Sprintf("claim-token-%v-%v-%v", b["task_id"], b["attempt"], b["claim_seq"])
	}
	return fakebroker.JSONReply(200, map[string]any{"claim_token": token, "task_id": b["task_id"], "attempt": b["attempt"], "claim_seq": b["claim_seq"]})
}

// envError is an env-registry open-edge error (integer request_id).
func envError(status int, code, failureReason string) (int, any) {
	st, b := edgeError(status, code, failureReason)
	b.(map[string]any)["request_id"] = 1
	return st, b
}

// edgeError is a model-registry key-edge error: no request_id field (D12
// owner decision 2026-11-09).
func edgeError(status int, code, failureReason string) (int, any) {
	b := map[string]any{"code": code, "message": code}
	if failureReason != "" {
		b["failure_reason"] = failureReason
	}
	return status, b
}

// start runs a new Executor process on the harness's configuration.
func (h *harness) start() {
	h.t.Helper()
	logger := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h.exec = executor.New(executor.Options{
		Config: h.cfg, Dial: h.br.Dial, Kube: h.kube, HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Clock: h.ck, Net: h.net, Logger: logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan error, 1)
	exec := h.exec
	done := h.done
	go func() { done <- exec.Run(ctx) }()
}

// sigterm cancels the Executor's context as SIGTERM does and waits.
func (h *harness) sigterm() error {
	h.t.Helper()
	if h.cancel == nil {
		return nil
	}
	h.cancel()
	select {
	case err := <-h.done:
		h.cancel, h.runErr = nil, err
		return err
	case <-time.After(20 * time.Second):
		h.t.Fatalf("Executor did not exit after SIGTERM")
	}
	return nil
}

// crash stops the Executor as SIGKILL would: no shutdown handling.
func (h *harness) crash() {
	h.t.Helper()
	h.exec.Abort()
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		h.t.Fatalf("Executor did not stop after abort")
	}
	h.cancel()
	h.cancel = nil
}

func (h *harness) stopQuietly() {
	if h.cancel == nil {
		return
	}
	if m, ok := h.ck.(*mclock.Manual); ok {
		defer freeRun(m)() // the shutdown waits on the clock too
	}
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(20 * time.Second):
	}
	h.cancel = nil
}

// freeRun moves a manual clock from deadline to deadline, as idle time
// passes, until the returned stop is called.
func freeRun(m *mclock.Manual) (stop func()) {
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-quit:
				return
			default:
			}
			m.AdvanceToNext()
			time.Sleep(time.Millisecond)
		}
	}()
	return func() { close(quit); <-done }
}

// stepClaimToken runs a manual clock for span from deadline to deadline
// around the claim-token loop of tk, whose waits between requests last
// claimWait. Once tk's pull is recorded the clock never moves while that
// loop is between a wake-up and its next wait (or its end, settled), so the
// loop's reading of the time and its next request share one instant
// however slowly the host runs it. The Executor's other loops must not wait
// exactly claimWait.
func (h *harness) stepClaimToken(m *mclock.Manual, claimWait, span time.Duration, settled func() bool) {
	h.t.Helper()
	end := m.Now().Add(span)
	pulled := false
	for m.Now().Before(end) {
		if !pulled {
			for _, f := range h.taskFetches() {
				pulled = pulled || f.Got > 0
			}
		}
		if pulled {
			eventually(h.t, 10*time.Second, func() bool { return m.Pending(claimWait) > 0 || settled() },
				"claim-token loop waiting or settled")
		}
		m.AdvanceToNext()
		time.Sleep(time.Millisecond)
	}
}

// waitExit waits for Run to return on its own (startup failure).
func (h *harness) waitExit(d time.Duration) (error, bool) {
	select {
	case err := <-h.done:
		h.cancel()
		h.cancel = nil
		return err, true
	case <-time.After(d):
		return nil, false
	}
}

// sim sleeps for a simulated duration.
func (h *harness) sim(d time.Duration) { time.Sleep(h.clk.Real(d)) }

// task is one queue message.
type task struct {
	TaskID   int64
	Attempt  int64
	ClaimSeq int64
	Msg      map[string]any
	Subject  string
	Q        *fakebroker.Queued
}

func (h *harness) newTask(edit ...func(m map[string]any)) task {
	id := h.nextTask.Add(1)
	m := map[string]any{
		"publisher": "task-manager", "publisher_seq": id + 50000, "team_id": teamID, "occurred_at": "2026-11-07T10:00:00.000Z",
		"task_id": id, "flow_id": 2, "source_id": 4, "external_id": fmt.Sprintf("ISSUE-%d", id), "intake_id": id + 7000,
		"attempt": 1, "claim_seq": 1, "required_tag": tag, "payload": map[string]any{"issue": "ISSUE-42"},
		"project_id": nil, "environment_id": nil,
		"execution": map[string]any{
			"execution_revision_id": 5, "required_tag": tag,
			"rendered_prompt":      fmt.Sprintf("Fix the bug of task %d", id),
			"execution_parameters": map[string]any{},
		},
		"resolved_image": "registry.example.com/team-3/runtime:v1", "image_source": "team_default",
		"pending_limit_seconds": 300,
	}
	for _, e := range edit {
		e(m)
	}
	return task{TaskID: id, Attempt: 1, ClaimSeq: 1, Msg: m, Subject: fmt.Sprintf("task-manager.task.%d.%s.2.sub", teamID, tag)}
}

// enqueue puts a task on the Executor's (team, tag) consumer.
func (h *harness) enqueue(tk task) task {
	data, _ := json.Marshal(tk.Msg)
	tk.Q = h.br.Enqueue("TASK_MANAGER_TASK", taskDurable(), tk.Subject, broker.Header{"team": fmt.Sprint(teamID), "Nats-Msg-Id": fmt.Sprintf("task-manager-%v", tk.Msg["publisher_seq"])}, data)
	return tk
}

func (h *harness) enqueueTask(edit ...func(m map[string]any)) task {
	return h.enqueue(h.newTask(edit...))
}

// cancelCommand puts a cancel command on commands-7.
func (h *harness) cancelCommand(tk task, controlID int64) *fakebroker.Queued {
	data, _ := json.Marshal(map[string]any{
		"publisher": "task-manager", "publisher_seq": 90000 + controlID, "team_id": teamID, "executor_id": executorID,
		"control": "cancel", "control_id": controlID, "task_id": tk.TaskID, "attempt": tk.Attempt, "claim_seq": tk.ClaimSeq,
		"occurred_at": "2026-11-07T10:00:00.000Z",
	})
	return h.br.Enqueue("TASK_MANAGER_COMMAND", "commands-7", "task-manager.command.7.sub", broker.Header{"team": "3", "Nats-Msg-Id": fmt.Sprintf("task-manager-%d", 90000+controlID)}, data)
}

// events returns the published events of kind (all kinds for "*").
func (h *harness) events(kind string) []map[string]any {
	var out []map[string]any
	for _, r := range h.br.Published(evPrefix + kind + ".pub") {
		out = append(out, r.JSON())
	}
	return out
}

// taskEvents returns the events of one claim in publish order.
func (h *harness) taskEvents(tk task) []map[string]any {
	var out []map[string]any
	for _, e := range h.events("*") {
		if num(e["task_id"]) == tk.TaskID && num(e["attempt"]) == tk.Attempt && num(e["claim_seq"]) == tk.ClaimSeq {
			out = append(out, e)
		}
	}
	return out
}

// kinds returns the kinds of one claim's events in publish order.
func (h *harness) kinds(tk task) []string {
	var out []string
	for _, e := range h.taskEvents(tk) {
		out = append(out, e["kind"].(string))
	}
	return out
}

func (h *harness) hasKind(tk task, kind string) bool {
	for _, k := range h.kinds(tk) {
		if k == kind {
			return true
		}
	}
	return false
}

func (h *harness) eventOf(tk task, kind string) map[string]any {
	for _, e := range h.taskEvents(tk) {
		if e["kind"] == kind {
			return e
		}
	}
	return nil
}

func num(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case int64:
		return n
	case int:
		return int64(n)
	}
	return -1
}

// pod returns the Pod of a claim.
func (h *harness) pod(tk task) (fakekube.PodRecord, bool) {
	return h.kube.PodByLabel("flowai.task_id", fmt.Sprint(tk.TaskID))
}

// conversation returns the OpenHands conversation started for a task.
func (h *harness) conversation(tk task) string {
	want := fmt.Sprintf("Fix the bug of task %d", tk.TaskID)
	convs := h.oh.Conversations()
	for i, b := range h.oh.StartBodies() {
		if strings.Contains(fmt.Sprint(b["initial_message"]), want) && i < len(convs) {
			return convs[i]
		}
	}
	return ""
}

// waitRunning waits until the claim's agent runs and its stream is open.
func (h *harness) waitRunning(tk task) string {
	h.t.Helper()
	eventually(h.t, 10*time.Second, func() bool {
		c := h.conversation(tk)
		return c != "" && h.oh.Streaming(c) && h.hasKind(tk, "running")
	}, fmt.Sprintf("task %d running with an open OpenHands stream", tk.TaskID))
	return h.conversation(tk)
}

// finish makes OpenHands report a terminal status for a task.
func (h *harness) finish(tk task, status string) {
	h.t.Helper()
	conv := h.waitRunning(tk)
	if err := h.oh.PushTerminal(conv, status); err != nil {
		h.t.Fatalf("push terminal: %v", err)
	}
}

func (h *harness) push(tk task, frame any) {
	h.t.Helper()
	conv := h.waitRunning(tk)
	if err := h.oh.PushEvent(conv, frame); err != nil {
		h.t.Fatalf("push frame: %v", err)
	}
}

// heartbeats returns the recorded heartbeat request bodies.
func (h *harness) heartbeats() []fakebroker.Record {
	return h.br.Requests("executor-registry.executor.7.heartbeat.pub")
}

func (h *harness) waitHeartbeats(n int) {
	h.t.Helper()
	eventually(h.t, 10*time.Second, func() bool { return len(h.heartbeats()) >= n }, fmt.Sprintf("%d heartbeats", n))
}

// waitConnected waits until the Executor connected, created its command
// consumer and sent a heartbeat.
func (h *harness) waitConnected() {
	h.t.Helper()
	eventually(h.t, 10*time.Second, func() bool {
		_, ok := h.br.Consumer("TASK_MANAGER_COMMAND", "commands-7")
		return ok && len(h.heartbeats()) > 0
	}, "connected Executor with its command consumer and a heartbeat")
}

func (h *harness) claimTokenRequests() []fakebroker.Record {
	return h.br.Requests("task-manager.executor.7.claim-token.pub")
}

func (h *harness) checkRequests() []fakebroker.Record {
	return h.br.Requests("model-registry.executor.7.check.pub")
}

// eventually polls cond in wall time.
func eventually(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// never asserts cond stays false for a wall-time duration.
func never(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("unexpected: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// probe calls the Executor's probe handler.
func (h *harness) probe(path string) int {
	rec := httptest.NewRecorder()
	h.exec.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}
