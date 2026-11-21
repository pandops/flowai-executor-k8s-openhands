package k8sopenhands_test

// Requirement: Concrete Executors send heartbeats and survive connection loss (rows 285–296).

import (
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-k8s-openhands/internal/mocks/broker"
)

// replyClaims answers heartbeats with claims_known = true and the given claims.
func replyClaims(tks ...task) func(fakebroker.Record) (*broker.Reply, error) {
	return func(fakebroker.Record) (*broker.Reply, error) {
		claims := []map[string]any{}
		for _, tk := range tks {
			claims = append(claims, map[string]any{"task_id": tk.TaskID, "attempt": tk.Attempt, "claim_seq": tk.ClaimSeq})
		}
		return fakebroker.JSONReply(200, map[string]any{"connection_state": "connected", "claims_known": true, "claims": claims}), nil
	}
}

func claimsUnknown(fakebroker.Record) (*broker.Reply, error) {
	return fakebroker.JSONReply(200, map[string]any{"connection_state": "connected", "claims_known": false}), nil
}

// failedHeartbeats counts heartbeats that got no reply.
func (h *harness) failedHeartbeats() int {
	n := 0
	for _, r := range h.heartbeats() {
		if r.Err != nil {
			n++
		}
	}
	return n
}

// twoRunning starts two running claims.
func twoRunning(h *harness) (task, task, string, string) {
	h.start()
	h.waitConnected()
	a, b := h.enqueueTask(), h.enqueueTask()
	return a, b, h.waitRunning(a), h.waitRunning(b)
}

// Scenario: Three missed heartbeats pause the agents (row 285).
func TestThreeMissedHeartbeatsPauseTheAgents(t *testing.T) {
	h := newHarness(t)
	a, b, ca, cb := twoRunning(h)
	h.dropHeartbeats()
	eventually(t, 10*time.Second, func() bool { return h.failedHeartbeats() >= 3 }, "three missed heartbeats")
	eventually(t, 10*time.Second, func() bool { return h.oh.Paused(ca) > 0 && h.oh.Paused(cb) > 0 }, "both agents paused")
	fetches := len(h.taskFetches())
	queued := h.enqueueTask()
	h.sim(60 * time.Second)
	if len(h.taskFetches()) != fetches || queued.Q.Settled() {
		t.Fatalf("pulled tasks in connection loss")
	}
	for _, tk := range []task{a, b} {
		if _, ok := h.pod(tk); !ok {
			t.Fatalf("Pod of %d removed in connection loss", tk.TaskID)
		}
		if len(h.terminalKinds(tk)) != 0 {
			t.Fatalf("terminal event for %d on our own pause: %v", tk.TaskID, h.kinds(tk))
		}
	}
	if n := len(h.heartbeats()); n < 6 {
		t.Fatalf("heartbeats stopped in connection loss (%d)", n)
	}
	if h.probe("/v1/readyz") != 503 {
		t.Fatalf("ready in connection loss")
	}
}

// Scenario: Connection restored within W (row 286).
func TestConnectionRestoredWithinW(t *testing.T) {
	// A third slot stays free, so pulling can resume next to the two claims.
	h := newHarness(t, withFactor(400), withConfig(func(c *configT) { c.MaxParallelTasks = 3 }))
	a, b, ca, cb := twoRunning(h)
	h.dropHeartbeats()
	eventually(t, 10*time.Second, func() bool { return h.oh.Paused(ca) > 0 && h.oh.Paused(cb) > 0 }, "agents paused")
	h.sim(4*time.Minute - 45*time.Second)
	fetches := len(h.taskFetches())
	h.setHeartbeat(replyClaims(a, b))
	restored := h.waitSelf("executor_connection_restored")
	lost := h.waitSelf("executor_connection_lost")
	eventually(t, 10*time.Second, func() bool { return h.oh.Resumed(ca) > 0 && h.oh.Resumed(cb) > 0 }, "both agents continued")

	lostAt, _ := time.Parse(time.RFC3339Nano, lost["lost_at"].(string))
	restoredAt, _ := time.Parse(time.RFC3339Nano, restored["restored_at"].(string))
	if d := restoredAt.Sub(lostAt); d < 2*time.Minute || d > 5*time.Minute {
		t.Fatalf("lost_at %v restored_at %v", lostAt, restoredAt)
	}
	if restored["lost_at"] != lost["lost_at"] {
		t.Fatalf("restored lost_at %v differs from lost %v", restored["lost_at"], lost["lost_at"])
	}
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > fetches }, "pulling resumed")
	for _, tk := range []task{a, b} {
		if _, ok := h.pod(tk); !ok || len(h.terminalKinds(tk)) != 0 {
			t.Fatalf("claim %d lost its Pod or ended", tk.TaskID)
		}
	}
	eventually(t, 10*time.Second, func() bool { return h.probe("/v1/readyz") == 200 }, "ready again")
}

// Scenario: Reply omits a claim after a late restore (row 287).
func TestReplyOmitsAClaimAfterALateRestore(t *testing.T) {
	h := newHarness(t, withFactor(400))
	a, b, ca, cb := twoRunning(h)
	h.dropHeartbeats()
	eventually(t, 10*time.Second, func() bool { return h.oh.Paused(ca) > 0 && h.oh.Paused(cb) > 0 }, "agents paused")
	h.sim(2 * time.Minute)
	eventsOfB := len(h.taskEvents(b))
	h.setHeartbeat(replyClaims(a))
	h.waitSelf("executor_connection_restored")
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(b); return !ok }, "omitted claim's Pod deleted")
	eventually(t, 10*time.Second, func() bool { return h.oh.Resumed(ca) > 0 }, "listed claim continued")
	h.sim(30 * time.Second)
	if h.oh.Resumed(cb) != 0 {
		t.Fatalf("omitted claim's agent continued")
	}
	if len(h.taskEvents(b)) != eventsOfB {
		t.Fatalf("published events for the omitted claim: %v", h.kinds(b)[eventsOfB:])
	}
	if _, ok := h.pod(a); !ok {
		t.Fatalf("listed claim's Pod deleted")
	}
	for _, c := range h.heldClaims() {
		if num(c["task_id"]) == b.TaskID {
			t.Fatalf("omitted claim still held")
		}
	}
}

// Scenario: Reply without known claims changes no child (row 288).
func TestReplyWithoutKnownClaimsChangesNoChild(t *testing.T) {
	h := newHarness(t, withFactor(1000))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	conv := h.waitRunning(tk)
	h.dropHeartbeats()
	eventually(t, 10*time.Second, func() bool { return h.oh.Paused(conv) > 0 }, "agent paused")
	h.sim(4*time.Minute - 45*time.Second)
	fetches := len(h.taskFetches())
	h.setHeartbeat(claimsUnknown)
	h.sim(8 * time.Minute) // minute 12, past W
	if _, ok := h.pod(tk); !ok {
		t.Fatalf("Pod removed although replies keep arriving")
	}
	if h.oh.Resumed(conv) != 0 {
		t.Fatalf("agent continued on claims_known = false")
	}
	if len(h.events("executor_connection_lost")) != 0 || len(h.events("executor_connection_restored")) != 0 {
		t.Fatalf("connection observations published before claims_known = true")
	}
	if len(h.taskFetches()) != fetches {
		t.Fatalf("pulled before claims_known = true")
	}
	h.setHeartbeat(replyClaims(tk))
	h.waitSelf("executor_connection_restored")
	eventually(t, 10*time.Second, func() bool { return h.oh.Resumed(conv) > 0 }, "agent continued")
	eventually(t, 10*time.Second, func() bool { return len(h.taskFetches()) > fetches }, "pulling resumed")
}

// Scenario: Just-published claim is not dropped by a heartbeat reply (row 289).
func TestJustPublishedClaimIsNotDroppedByAHeartbeatReply(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(r fakebroker.Record) (*broker.Reply, error) {
		<-release
		return claimTokenReply(r, "tok"), nil
	})
	h.start()
	h.waitConnected()
	h.setHeartbeat(replyClaims()) // task-manager does not know the claim yet
	tk := h.enqueueTask()
	eventually(t, 10*time.Second, func() bool {
		return len(h.br.Attempts("task-manager.executor.7.claim-token.pub")) > 0 || h.hasKind(tk, "created")
	}, "created")
	n := len(h.heartbeats())
	eventually(t, 10*time.Second, func() bool { return len(h.heartbeats()) >= n+2 }, "two heartbeats omitting the claim")
	listed := false
	for _, c := range h.heldClaims() {
		if num(c["task_id"]) == tk.TaskID && c["state"] == "created" {
			listed = true
		}
	}
	if !listed {
		t.Fatalf("claim waiting for its token dropped: heartbeat claims %v", h.heldClaims())
	}
	h.setHeartbeat(replyClaims(tk))
	close(release)
	h.waitRunning(tk)
	h.setHeartbeat(replyClaims())
	eventually(t, 10*time.Second, func() bool { _, ok := h.pod(tk); return !ok }, "claim dropped by a reply sent after the acknowledgement")
	if got := h.terminalKinds(tk); len(got) != 0 {
		t.Fatalf("dropped claim published %v", got)
	}
}

// Scenario: Connection not restored within W (row 290).
func TestConnectionNotRestoredWithinW(t *testing.T) {
	h := newHarness(t, withFactor(1000))
	a, b, _, _ := twoRunning(h)
	h.dropHeartbeats()
	h.br.Disconnect()
	eventually(t, 15*time.Second, func() bool { return h.kube.Count() == 0 }, "Pods deleted at W")
	for _, tk := range []task{a, b} {
		if got := h.terminalKinds(tk); len(got) != 0 {
			t.Fatalf("published %v for a claim dropped at W", got)
		}
	}
	h.br.Reconnect()
	h.setHeartbeat(echoHeartbeat)
	h.waitSelf("executor_connection_lost")
	h.waitSelf("executor_connection_restored")
	next := h.enqueueTask()
	h.waitRunning(next)
	for _, tk := range []task{a, b} {
		if len(h.terminalKinds(tk)) != 0 {
			t.Fatalf("published %v after reconnect for a dropped claim", h.terminalKinds(tk))
		}
	}
	for _, c := range h.heldClaims() {
		if num(c["task_id"]) != next.TaskID {
			t.Fatalf("still holds %v", c)
		}
	}
}

// Scenario: Heartbeat carries claims and capacity (row 291).
func TestHeartbeatCarriesClaimsAndCapacity(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) { c.MaxParallelTasks = 4 }))
	release := make(chan struct{})
	defer close(release)
	var createdID int64
	h.br.Respond("task-manager.executor.*.claim-token.pub", func(r fakebroker.Record) (*broker.Reply, error) {
		if num(body(r)["task_id"]) == createdID {
			<-release
		}
		return claimTokenReply(r, ""), nil
	})
	h.setCheck(func(r fakebroker.Record) (*broker.Reply, error) {
		if num(body(r)["task_id"]) == 1002 {
			return fakebroker.JSONReply(200, map[string]any{"result": "temporary", "reason": "budget_exhausted"}), nil
		}
		return fakebroker.JSONReply(200, map[string]any{"result": "ok", "model_alias": "3/m1"}), nil
	})
	h.start()
	h.waitConnected()
	running := h.enqueueTask()
	h.waitRunning(running)
	pending := h.enqueueTask()
	h.waitKind(pending, "pending")
	created := h.newTask()
	createdID = created.TaskID
	h.enqueue(created)
	h.waitKind(created, "created")
	h.waitSlots(3)

	b := h.lastHeartbeat()
	want := map[string]int64{"interval_seconds": 15, "miss_limit": 3, "wait_seconds": 600, "max_parallel_tasks": 4, "slots_in_use": 3}
	for k, v := range want {
		if num(b[k]) != v {
			t.Fatalf("heartbeat %s = %v, want %d", k, b[k], v)
		}
	}
	states := map[int64]string{}
	for _, c := range h.heldClaims() {
		states[num(c["task_id"])] = c["state"].(string)
	}
	if states[running.TaskID] != "running" || states[pending.TaskID] != "pending" || states[created.TaskID] != "created" || len(states) != 3 {
		t.Fatalf("heartbeat claim states %v", states)
	}
	hb := h.heartbeats()
	if strings.Contains(string(hb[len(hb)-1].Data), "claim-token-") {
		t.Fatalf("heartbeat carries a claim token")
	}
	if hb[len(hb)-1].Subject != "executor-registry.executor.7.heartbeat.pub" {
		t.Fatalf("heartbeat subject %s", hb[len(hb)-1].Subject)
	}
}

// Scenario: Configured miss limit decides connection loss (row 292).
func TestConfiguredMissLimitDecidesConnectionLoss(t *testing.T) {
	h := newHarness(t, withConfig(func(c *configT) {
		c.HeartbeatInterval = 10 * time.Second
		c.HeartbeatMissLimit = 5
	}))
	h.start()
	h.waitConnected()
	tk := h.enqueueTask()
	conv := h.waitRunning(tk)
	b := h.lastHeartbeat()
	if num(b["interval_seconds"]) != 10 || num(b["miss_limit"]) != 5 {
		t.Fatalf("heartbeat timing %v", b)
	}
	h.dropHeartbeats()
	eventually(t, 10*time.Second, func() bool { return h.failedHeartbeats() >= 4 }, "four misses")
	if h.oh.Paused(conv) != 0 {
		t.Fatalf("paused after four misses with miss_limit 5")
	}
	eventually(t, 10*time.Second, func() bool { return h.failedHeartbeats() >= 5 }, "fifth miss")
	eventually(t, 10*time.Second, func() bool { return h.oh.Paused(conv) > 0 }, "paused after the fifth miss")
	if n := h.failedHeartbeats(); n > 6 {
		t.Fatalf("paused only after %d misses", n)
	}
}

func lastIP(h *harness) any { return h.lastHeartbeat()["ip"] }

// Scenario: Heartbeat reports the primary interface address (row 293).
func TestHeartbeatReportsThePrimaryInterfaceAddress(t *testing.T) {
	h := newHarness(t)
	h.net.set("eth0", "eth0", map[string][]string{"eth0": {"192.0.2.10/24", "2001:db8:4::21/64"}, "docker0": {"172.17.0.1/16"}})
	h.start()
	h.waitConnected()
	if ip := lastIP(h); ip != "192.0.2.10" {
		t.Fatalf("ip %#v, want 192.0.2.10", ip)
	}
}

// Scenario: IPv6-only primary interface (row 294).
func TestIPv6OnlyPrimaryInterface(t *testing.T) {
	h := newHarness(t)
	h.net.set("", "eth0", map[string][]string{"eth0": {"fe80::1/64", "2001:db8:4:0:0:0:0:21/64"}})
	h.start()
	h.waitConnected()
	if ip := lastIP(h); ip != "2001:db8:4::21" {
		t.Fatalf("ip %#v, want 2001:db8:4::21", ip)
	}
}

// Scenario: Changed address is reported in the next heartbeat (row 295).
func TestChangedAddressIsReportedInTheNextHeartbeat(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.waitConnected()
	if ip := lastIP(h); ip != "10.42.0.9" {
		t.Fatalf("ip %#v", ip)
	}
	h.net.set("eth0", "", map[string][]string{"eth0": {"10.42.0.17/24"}})
	n := len(h.heartbeats())
	h.waitHeartbeats(n + 2)
	if ip := lastIP(h); ip != "10.42.0.17" {
		t.Fatalf("ip %#v after the change", ip)
	}
	if h.br.DialCount() != 1 {
		t.Fatalf("redialed for an address change")
	}
}

// Scenario: No default route still sends the heartbeat (row 296).
func TestNoDefaultRouteStillSendsTheHeartbeat(t *testing.T) {
	h := newHarness(t)
	h.net.set("", "", nil)
	h.start()
	h.waitConnected()
	h.waitHeartbeats(4)
	hb := h.heartbeats()
	for i, r := range hb {
		b := body(r)
		v, present := b["ip"]
		if !present || v != nil {
			t.Fatalf("heartbeat %d ip %#v, want null", i, v)
		}
		if i > 0 {
			if gap := r.At.Sub(hb[i-1].At); gap > 20*time.Second {
				t.Fatalf("heartbeat late by %v", gap)
			}
		}
	}
	if !strings.Contains(h.logs.String(), "heartbeat_ip_unavailable") {
		t.Fatalf("no heartbeat_ip_unavailable warning")
	}
	if w := strings.Count(h.logs.String(), "heartbeat_ip_unavailable"); w > len(h.heartbeats()) {
		t.Fatalf("%d warnings for %d heartbeats", w, len(h.heartbeats()))
	}
	tk := h.enqueueTask()
	h.waitRunning(tk)
	if h.probe("/v1/readyz") != 200 {
		t.Fatalf("not ready without an address")
	}
}
