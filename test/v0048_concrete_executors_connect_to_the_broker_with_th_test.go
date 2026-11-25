package k8sopenhands_test

// Requirement: Concrete Executors connect to the broker with their one-time JWT
// (coverage rows 246–254). The K8s Executor receives its JWT through the Helm
// chart: the chart renders the configuration into a Secret and an
// install-config step copies it onto the writable volume with mode 0600.

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
)

// Scenario: First start stores the token (row 246).
func TestFirstStartStoresTheToken(t *testing.T) {
	h := newHarness(t)
	rendered := *h.cfg
	h.installAndLoad(&rendered)

	info, err := os.Stat(h.cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("installed config mode = %v, want 0600", info.Mode().Perm())
	}
	if h.cfg.Broker.Token != h.token {
		t.Fatalf("installed broker.token differs from the chart's token")
	}
	h.start()
	h.waitConnected()
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > 0 }, "first task pull")

	if got := h.br.LastDial().Token; got != h.token {
		t.Fatalf("dialed with token %q, want the installed JWT", got)
	}
	dial := h.br.DialOrders()[0]
	if o := firstOrder(h.heartbeats()); o < dial {
		t.Fatalf("heartbeat (order %d) before the broker connection (order %d)", o, dial)
	}
	if o := h.taskFetches()[0].Order; o < dial {
		t.Fatalf("task pull (order %d) before the broker connection (order %d)", o, dial)
	}
	if len(h.edgeRequests("")) != 0 {
		t.Fatalf("HTTP edge called without a claim: %v", h.edgeRequests(""))
	}
}

// Scenario: Later start reads the stored token (row 247).
func TestLaterStartReadsTheStoredToken(t *testing.T) {
	h := newHarness(t)
	rendered := *h.cfg
	h.installAndLoad(&rendered)
	h.start()
	h.waitConnected()
	if err := h.sigterm(); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second start: no new Secret, the stored file is read again.
	loaded, err := config.Load(h.cfg.Path)
	if err != nil {
		t.Fatalf("reload stored config: %v", err)
	}
	h.cfg = loaded
	dials := h.br.DialCount()
	h.start()
	eventually(t, 10*time.Second, func() bool { return h.br.DialCount() > dials }, "second dial")
	if got := h.br.LastDial().Token; got != h.token {
		t.Fatalf("second start dialed with %q, want the stored JWT", got)
	}
	for _, c := range h.edgeRequests("") {
		if strings.Contains(c.Path, "register") || strings.Contains(c.Path, "executors") {
			t.Fatalf("called a registration endpoint: %s", c.Path)
		}
	}
}

// Scenario: Start without any token (row 248).
func TestStartWithoutAnyToken(t *testing.T) {
	h := newHarness(t)
	rendered := *h.cfg
	rendered.Broker.Token = ""
	src := writeConfigYAML(t, &rendered, h.cfg.Path)
	if _, err := config.Load(src); !errors.Is(err, config.ErrNoToken) {
		t.Fatalf("Load without broker.token = %v, want ErrNoToken", err)
	}

	h.cfg.Broker.Token = ""
	h.start()
	err, exited := h.waitExit(5 * time.Second)
	if !exited || err == nil {
		t.Fatalf("Executor without a token: exited=%v err=%v, want a startup error", exited, err)
	}
	if h.probe("/v1/readyz") != 503 {
		t.Fatalf("readyz without a token is not 503")
	}
	if h.br.DialCount() != 0 {
		t.Fatalf("opened %d broker connections without a token", h.br.DialCount())
	}
	if len(h.taskFetches()) != 0 {
		t.Fatalf("pulled tasks without a token")
	}
}

// Scenario: Revoked token closes the connection (row 249).
func TestRevokedTokenClosesTheConnection(t *testing.T) {
	h := newHarness(t, withFactor(1000), withConfig(func(c *config.Config) { c.ConnectionWait = 120 * time.Second }))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	conv := h.waitRunning(tk)
	pod, _ := h.pod(tk)

	dials := h.br.DialCount()
	h.br.Revoke()
	eventually(t, 10*time.Second, func() bool { return h.oh.Paused(conv) > 0 }, "agent paused after revocation")
	eventually(t, 10*time.Second, func() bool { return h.probe("/v1/readyz") == 503 }, "unhealthy after revocation")
	fetches := len(h.taskFetches())
	if _, ok := h.pod(tk); !ok {
		t.Fatalf("Pod removed right after revocation")
	}

	// Before W: Pod kept, no pulls, no redial.
	h.sim(60 * time.Second)
	if _, ok := h.pod(tk); !ok {
		t.Fatalf("Pod removed before W")
	}
	if len(h.taskFetches()) != fetches {
		t.Fatalf("pulled tasks after revocation")
	}
	if h.br.DialCount() != dials {
		t.Fatalf("redialed with the revoked JWT (%d dials, want %d)", h.br.DialCount(), dials)
	}

	// At W: Pod deleted.
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(tk); return !ok }, "Pod deleted at W")
	if h.br.DialCount() != dials {
		t.Fatalf("redialed with the revoked JWT after W")
	}
	deleted := false
	for _, d := range h.kube.Deletes() {
		if d == pod.Spec.Name {
			deleted = true
		}
	}
	if !deleted {
		t.Fatalf("Pod %s was not deleted at W", pod.Spec.Name)
	}
}

// Scenario: Restart with a reissued token (row 250), K8s analogue: the
// operator runs helm upgrade with a new broker.token while the agent is
// paused after revocation; the old Pod is replaced through SIGTERM.
func TestRestartWithAReissuedToken(t *testing.T) {
	h := newHarness(t, withFactor(400))
	rendered := *h.cfg
	h.installAndLoad(&rendered)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	conv := h.waitRunning(tk)
	pod, _ := h.pod(tk)

	h.br.Revoke()
	eventually(t, 10*time.Second, func() bool { return h.oh.Paused(conv) > 0 }, "agent paused")
	published := len(h.publishedRecords())

	h.sim(3 * time.Minute)
	stopAt := time.Now()
	if err := h.sigterm(); err != nil {
		t.Logf("shutdown returned %v", err)
	}
	if took := time.Since(stopAt); took > h.clk.Real(time.Duration(h.cfg.DrainTimeoutSeconds)*time.Second)/2+time.Second {
		t.Fatalf("shutdown took %v: it drained instead of deleting at once", took)
	}
	if _, ok := h.kube.PodByLabel("flowai.task_id", pod.Spec.Labels["flowai.task_id"]); ok {
		t.Fatalf("paused claim's Pod survived the shutdown")
	}
	if n := len(h.publishedRecords()); n != published {
		t.Fatalf("shutdown without a connection published %d events", n-published)
	}

	// helm upgrade --set broker.token=<new>
	h.br.ResetRevocation()
	newToken := makeJWT(teamID, executorID, tag, "REISSUEDJTI")
	reissued := *h.cfg
	reissued.Broker.Token = newToken
	h.installAndLoad(&reissued)
	resumes := h.oh.Resumed(conv)
	h.start()
	h.waitConnected()
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > 0 && h.br.LastDial().Token == newToken }, "pull with the new JWT")
	h.sim(30 * time.Second)

	for _, e := range h.events("released") {
		if num(e["task_id"]) == tk.TaskID {
			t.Fatalf("published released %v for the dropped claim", e["reason"])
		}
	}
	if h.oh.Resumed(conv) != resumes {
		t.Fatalf("continued the paused agent after the reissue")
	}
	if claims := h.heldClaims(); len(claims) != 0 {
		t.Fatalf("new process holds claims %v", claims)
	}
}

// Scenario: Team display name changes (row 252).
func TestTeamDisplayNameChanges(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	a := h.enqueueTask()
	h.waitRunning(a)
	// task-manager's display-only fields change; the team_id stays 3.
	b := h.enqueueTask(func(m map[string]any) { m["team_name"] = "Renamed Team" })
	h.waitRunning(b)
	for _, r := range h.br.Attempts(">") {
		if r.Header["team"] != "" && r.Header["team"] != "3" {
			t.Fatalf("header team = %q on %s", r.Header["team"], r.Subject)
		}
		if strings.Contains(string(r.Data), "Renamed Team") {
			t.Fatalf("team_name leaked into %s", r.Subject)
		}
	}
	if d := h.br.LastDial(); d.InboxPrefix != "_INBOX.executor-7" {
		t.Fatalf("inbox prefix changed: %q", d.InboxPrefix)
	}
	if len(taskDurable()) == 0 || h.taskFetches()[len(h.taskFetches())-1].Durable != taskDurable() {
		t.Fatalf("task consumer changed after the rename")
	}
}

// Scenario: Executor identity comes from the JWT as integers (row 253).
func TestExecutorIdentityComesFromTheJWTAsIntegers(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	h.finish(tk, "finished")
	h.waitKind(tk, "finished")

	if d := h.br.LastDial(); d.InboxPrefix != "_INBOX.executor-7" {
		t.Fatalf("inbox prefix %q, want _INBOX.executor-7", d.InboxPrefix)
	}
	for _, r := range h.br.Attempts(">") {
		if !strings.HasPrefix(r.Subject, "executor.event.7.") || !strings.HasSuffix(r.Subject, ".pub") {
			t.Fatalf("published on %s", r.Subject)
		}
	}
	for _, r := range h.br.Requests(">") {
		ok := strings.HasPrefix(r.Subject, "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-7.") ||
			(strings.Contains(r.Subject, ".executor.7.") && strings.HasSuffix(r.Subject, ".pub"))
		if !ok {
			t.Fatalf("request on %s", r.Subject)
		}
		if r.Header["team"] != "3" {
			t.Fatalf("request %s without header team=3", r.Subject)
		}
	}
	for _, f := range h.br.Fetches("") {
		if f.Durable != "commands-7" && f.Durable != taskDurable() {
			t.Fatalf("pulled from %s", f.Durable)
		}
	}
	if len(h.br.Fetches("commands-7")) == 0 || len(h.taskFetches()) == 0 {
		t.Fatalf("no pulls from commands-7 and %s", taskDurable())
	}
	for _, r := range h.publishedRecords() {
		e := r.JSON()
		if v, ok := e["team_id"].(float64); !ok || v != 3 {
			t.Fatalf("team_id %#v in %s", e["team_id"], r.Data)
		}
		if v, ok := e["executor_id"].(float64); !ok || v != 7 {
			t.Fatalf("executor_id %#v in %s", e["executor_id"], r.Data)
		}
		if r.Header["team"] != "3" {
			t.Fatalf("event without header team=3: %s", r.Data)
		}
	}
}

// Scenario: Token never appears in logs (row 254).
func TestTokenNeverAppearsInLogs(t *testing.T) {
	h := newHarness(t)
	h.br.DialErr = errors.New("tls: handshake failure")
	h.start()
	eventually(t, 10*time.Second, func() bool { return h.br.DialCount() >= 2 }, "dial retries")
	h.br.DialErr = nil
	h.waitConnected()
	h.stopQuietly()

	parts := strings.Split(h.token, ".")
	logs := h.logs.String()
	if logs == "" {
		t.Fatalf("no log output")
	}
	if p := containsAny(logs, h.token, parts[0], parts[1], parts[2]); p != "" {
		t.Fatalf("log contains JWT part %q", p)
	}
}
