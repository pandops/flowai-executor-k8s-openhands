package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/cache"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/edges"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/openhands"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
)

const maxLogText = 65536

// imagePullReasons are container waiting reasons of an image that cannot be pulled.
var imagePullReasons = map[string]bool{"ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true, "ErrImageNeverPull": true}

func podName(r platform.ClaimRef) string {
	return fmt.Sprintf("flowai-task-%d-%d-%d", r.TaskID, r.Attempt, r.ClaimSeq)
}

// podLabels are the identity labels plus the LiteLLM data-plane labels.
func (e *Executor) podLabels(c *claim) map[string]string {
	labels := map[string]string{}
	for k, v := range e.cfg.LiteLLMDataPodLabels {
		labels[k] = v
	}
	for k, v := range map[string]string{
		k8sclient.LabelExecutorID: strconv.FormatInt(e.id.ExecutorID, 10), k8sclient.LabelTeamID: strconv.FormatInt(e.id.TeamID, 10),
		k8sclient.LabelTaskID: strconv.FormatInt(c.ref.TaskID, 10), k8sclient.LabelAttempt: strconv.FormatInt(c.ref.Attempt, 10),
		k8sclient.LabelClaimSeq: strconv.FormatInt(c.ref.ClaimSeq, 10), k8sclient.LabelResolvedImageSource: c.msg.ImageSource,
		k8sclient.LabelRuntime: "k8s",
	} {
		labels[k] = v
	}
	return labels
}

func (e *Executor) deletePod(c *claim) {
	c.mu.Lock()
	name := c.podName
	c.mu.Unlock()
	if name == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := e.kube.DeletePod(ctx, e.cfg.Namespace, name); err != nil {
		e.log.Warn("pod_delete_failed", "pod", name, "error", err.Error())
	}
}

// runTask starts the claim's Pod and its OpenHands conversation, publishes
// running and follows the conversation to its terminal event.
func (e *Executor) runTask(c *claim, values map[string]string, llm edges.LLM) {
	env := map[string]string{}
	for k, v := range values {
		env[k] = v
	}
	env["OPENAI_BASE_URL"], env["OPENAI_MODEL"], env["OPENAI_API_KEY"] = llm.BaseURL, llm.Model, llm.APIKey
	if e.cfg.OpenHandsAPIKey != "" {
		env["SESSION_API_KEY"] = e.cfg.OpenHandsAPIKey
	}
	name := podName(c.ref)
	c.mu.Lock()
	if c.ended {
		c.mu.Unlock()
		return
	}
	c.podName = name
	c.mu.Unlock()
	e.persistClaim(c, cache.StatePending)
	_, err := e.kube.CreatePod(c.ctx, k8sclient.PodSpec{
		Name: name, Namespace: e.cfg.Namespace, Image: *c.msg.ResolvedImage, ImagePullPolicy: e.cfg.ImagePullPolicy,
		Env: env, Port: e.cfg.OpenHandsPort, ServiceAccountName: e.cfg.ServiceAccount, Labels: e.podLabels(c),
	})
	if c.ctx.Err() != nil {
		return
	}
	if err != nil {
		e.log.Error("pod_create_failed", "task_id", c.ref.TaskID, "error", err.Error())
		c.mu.Lock()
		c.podName = ""
		c.mu.Unlock()
		e.fail(c, platform.FailurePodFailed, true)
		return
	}
	ip, restarts, reason := e.waitPodReady(c)
	if reason != "" {
		e.fail(c, reason, true)
		return
	}
	if c.ctx.Err() != nil {
		return
	}
	baseURL := fmt.Sprintf("http://%s:%d", ip, e.cfg.OpenHandsPort)
	oh := openhands.NewClient(baseURL, e.cfg.OpenHandsAPIKey, e.http)
	if err := oh.HealthCheck(c.ctx, 2*time.Minute); err != nil {
		if c.ctx.Err() == nil {
			e.fail(c, platform.FailureAgentServerFailed, true)
		}
		return
	}
	res, err := oh.StartConversation(c.ctx, e.conversationConfig(c.msg.Execution.RenderedPrompt, llm.APIKey, llm.BaseURL, llm.Model))
	if err != nil {
		if c.ctx.Err() == nil {
			e.log.Error("conversation_start_failed", "task_id", c.ref.TaskID, "error", err.Error())
			e.fail(c, platform.FailureAgentServerFailed, true)
		}
		return
	}
	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") + openhands.WSPath(res.ID)
	hdr := http.Header{}
	if e.cfg.OpenHandsAPIKey != "" {
		hdr.Set("X-Session-API-Key", e.cfg.OpenHandsAPIKey)
	}
	ws, resp, err := websocket.DefaultDialer.DialContext(c.ctx, wsURL, hdr)
	if resp != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err != nil {
		if c.ctx.Err() == nil {
			e.fail(c, platform.FailureAgentServerFailed, true)
		}
		return
	}
	c.mu.Lock()
	if c.ended {
		c.mu.Unlock()
		_ = ws.Close()
		return
	}
	c.oh, c.convID, c.ws = oh, res.ID, ws
	lossNow := false
	e.mu.Lock()
	lossNow = e.inLoss
	e.mu.Unlock()
	c.mu.Unlock()
	fields := map[string]any{}
	if c.msg.ImageSource == "flow_default" || c.msg.ImageSource == "team_default" {
		fields["image_source"] = c.msg.ImageSource
	}
	if !e.emitFor(c, platform.KindRunning, fields) {
		return
	}
	c.mu.Lock()
	c.state = stateRunning
	c.mu.Unlock()
	e.persistClaim(c, cache.StateRunning)
	if lossNow {
		e.pauseForLoss(c)
	}
	e.follow(c, ws, restarts)
}

// waitPodReady polls the Pod until its agent container runs with an
// address, or returns a failure reason.
func (e *Executor) waitPodReady(c *claim) (string, int32, string) {
	for {
		if !e.sleep(c.ctx, e.cfg.PodPollInterval) {
			return "", 0, ""
		}
		st, err := e.kube.GetPod(c.ctx, e.cfg.Namespace, c.podName)
		if c.ctx.Err() != nil {
			return "", 0, ""
		}
		if err != nil {
			if e.kube.Healthy(c.ctx) {
				return "", 0, platform.FailurePodFailed
			}
			continue
		}
		if r := podFailure(st, -1); r != "" {
			return "", 0, r
		}
		if st.Status == k8sclient.PodStatusRunning && st.PodIP != "" && containersReady(st) {
			return st.PodIP, restartCount(st), ""
		}
	}
}

func containersReady(st *k8sclient.PodState) bool {
	for _, cs := range st.ContainerStatuses {
		if !cs.Ready {
			return false
		}
	}
	return true
}

func restartCount(st *k8sclient.PodState) int32 {
	n := st.RestartCount
	for _, cs := range st.ContainerStatuses {
		if cs.RestartCount > n {
			n = cs.RestartCount
		}
	}
	return n
}

// podFailure maps a Pod state to a failure reason; restarts < 0 skips the
// restart check.
func podFailure(st *k8sclient.PodState, restarts int32) string {
	for _, cs := range st.ContainerStatuses {
		if imagePullReasons[cs.Reason] {
			return platform.FailureImagePullFailed
		}
	}
	switch st.Status {
	case k8sclient.PodStatusFailed, k8sclient.PodStatusSucceeded:
		return platform.FailurePodFailed
	}
	if restarts >= 0 && restartCount(st) > restarts {
		return platform.FailurePodRestartedBeforeFinish
	}
	return ""
}

// follow reads the OpenHands stream and watches the Pod until the claim ends.
func (e *Executor) follow(c *claim, ws *websocket.Conn, restarts int32) {
	frames := make(chan any, 64)
	streamErr := make(chan error, 1)
	go func() {
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				streamErr <- err
				return
			}
			var v any
			if json.Unmarshal(data, &v) != nil {
				continue
			}
			select {
			case frames <- v:
			case <-c.ctx.Done():
				return
			}
		}
	}()
	defer ws.Close()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-streamErr:
			if c.ctx.Err() == nil {
				e.fail(c, platform.FailureAgentServerFailed, true)
			}
			return
		case f := <-frames:
			if e.handleFrame(c, f) {
				return
			}
		case <-e.clk.After(e.cfg.PodPollInterval):
			st, err := e.kube.GetPod(c.ctx, e.cfg.Namespace, c.podName)
			if c.ctx.Err() != nil {
				return
			}
			if err != nil {
				if e.kube.Healthy(c.ctx) {
					e.fail(c, platform.FailurePodFailed, true)
					return
				}
				continue
			}
			if r := podFailure(st, restarts); r != "" {
				e.fail(c, r, true)
				return
			}
		}
	}
}

// frameStatus returns the execution status a frame announces.
func frameStatus(v any) string {
	if m, ok := v.(map[string]any); ok {
		if k, _ := m["key"].(string); k == "execution_status" {
			if s, ok := m["value"].(string); ok {
				return strings.ToLower(strings.TrimSpace(s))
			}
		}
	}
	return conversationStatus(v)
}

// handleFrame publishes the frame's logs and its terminal status; true when
// the claim ended.
func (e *Executor) handleFrame(c *claim, f any) bool {
	if c.toolLogs == nil {
		c.toolLogs = toolCallLogs{}
	}
	for _, l := range c.toolLogs.unpublished(f, publishedLogsFromOpenHands(f)) {
		text := l.content
		if text == "" {
			continue
		}
		if len(text) > maxLogText {
			text = text[:maxLogText]
			for !utf8.ValidString(text) {
				text = text[:len(text)-1]
			}
		}
		e.emitFor(c, platform.KindLog, map[string]any{"stream": l.stream, "text": text})
	}
	if isBudgetExceededError(f) {
		e.fail(c, platform.FailureLLMBudgetExceeded, true)
		return true
	}
	switch status := frameStatus(f); status {
	case "finished":
		e.terminate(c, platform.KindFinished, nil, e.cfg.FinishedCleanupDelay)
		return true
	case "paused":
		c.mu.Lock()
		ours := c.pausedByUs
		c.mu.Unlock()
		if ours {
			return false
		}
		fallthrough
	case "failed", "error", "stuck":
		extra := map[string]any{}
		if code := conversationErrorCode(f); code != "" {
			if len(code) > 256 {
				code = code[:256]
			}
			extra["openhands_error_code"] = code
		}
		e.fail(c, "openhands_"+status, true, extra)
		return true
	}
	return false
}
