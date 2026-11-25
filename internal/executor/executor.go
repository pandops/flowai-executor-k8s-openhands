// Package executor is the K8s OpenHands Executor: it pulls tasks from its
// (team, tag) consumer, claims them with a created event and task-manager's
// claim token, checks them, runs each won claim in one Kubernetes Pod with
// an OpenHands agent server, and publishes every event through the broker.
// It heartbeats to executor-registry, survives connection loss, handles
// cancel commands from its own command consumer, drains on SIGTERM and
// reconciles its recovery cache after a restart.
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/cache"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/clock"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/edges"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/httpapi"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/identity"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/netinfo"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/platform"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/seqfile"
)

// Config is the Executor configuration.
type Config = config.Config

// Options are the Executor's dependencies.
type Options struct {
	Config     *config.Config
	Dial       broker.Dialer
	Kube       k8sclient.Client
	HTTPClient *http.Client
	Clock      clock.Clock
	Net        netinfo.Source
	Logger     *slog.Logger
}

// Executor is one K8s Executor process.
type Executor struct {
	opts  Options
	cfg   *config.Config
	clk   clock.Clock
	log   *slog.Logger
	kube  k8sclient.Client
	http  *http.Client
	edges *edges.Client

	id    identity.Identity
	store *cache.Store
	lock  *cache.LockFile
	seq   *seqfile.Counter

	mu       sync.Mutex
	conn     broker.Conn
	claims   map[string]*claim
	lastHB   time.Time
	misses   int
	inLoss   bool
	lostAt   time.Time
	wExpired bool
	stopping bool
	reqSeq   int64

	live       atomic.Bool
	connected  atomic.Bool
	consumerOK atomic.Bool
	revoked    atomic.Bool
	kubeOK     atomic.Bool
	lossFlag   atomic.Bool
	ready      atomic.Bool

	pubMu   sync.Mutex
	flushMu sync.Mutex

	abortOnce sync.Once
	abortCh   chan struct{}
	runCtx    context.Context
	wg        sync.WaitGroup
}

// New builds an Executor; it opens nothing yet.
func New(opts Options) *Executor {
	e := &Executor{opts: opts, cfg: opts.Config, clk: opts.Clock, log: opts.Logger, kube: opts.Kube, http: opts.HTTPClient,
		claims: map[string]*claim{}, abortCh: make(chan struct{})}
	if e.clk == nil {
		e.clk = clock.Real()
	}
	if e.log == nil {
		e.log = slog.Default()
	}
	e.log = e.log.With("service", platform.ServiceName)
	if e.http == nil {
		e.http = &http.Client{Timeout: 30 * time.Second}
	}
	if e.opts.Net == nil {
		e.opts.Net = netinfo.System()
	}
	e.live.Store(true)
	e.kubeOK.Store(true)
	return e
}

// Live reports whether the process is alive.
func (e *Executor) Live() bool { return e.live.Load() }

// Ready reports whether the Executor is connected with a valid JWT, owns its
// command consumer, is not in connection loss and can supervise Pods.
func (e *Executor) Ready() bool {
	return e.ready.Load() && e.connected.Load() && e.consumerOK.Load() && !e.revoked.Load() &&
		!e.lossFlag.Load() && e.kubeOK.Load()
}

// Handler serves only GET /v1/livez and GET /v1/readyz.
func (e *Executor) Handler() http.Handler {
	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		httpapi.RegisterProbesWithExecutorID(r, platform.ServiceName, func() string {
			if e.id.ExecutorID == 0 {
				return ""
			}
			return strconv.FormatInt(e.id.ExecutorID, 10)
		}, func() (bool, httpapi.ReadinessDeps) {
			return e.Ready(), httpapi.ReadinessDeps{BrokerConnected: e.connected.Load() && !e.revoked.Load(), KubeReachable: e.kubeOK.Load()}
		})
	})
	return r
}

// Abort stops Run at once without any shutdown handling, as SIGKILL would.
// Tests use it to simulate a crash.
func (e *Executor) Abort() { e.abortOnce.Do(func() { close(e.abortCh) }) }

func (e *Executor) aborted() bool {
	select {
	case <-e.abortCh:
		return true
	default:
		return false
	}
}

// sleep waits d on the Executor clock; false when ctx ended first.
func (e *Executor) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-e.clk.After(d):
		return ctx.Err() == nil
	}
}

func (e *Executor) now() time.Time { return e.clk.Now().UTC() }

// Run starts the Executor and blocks until ctx is cancelled (SIGTERM) and
// the shutdown completes, or until startup fails.
func (e *Executor) Run(ctx context.Context) error {
	if err := e.cfg.Validate(); err != nil {
		e.log.Error("executor_config_invalid", "error", err.Error())
		return err
	}
	id, err := identity.FromJWT(e.cfg.Broker.Token)
	if err != nil {
		e.log.Error("executor_token_invalid")
		return err
	}
	e.id = id
	e.log = e.log.With("executor_id", id.ExecutorID, "team_id", id.TeamID)
	e.edges = &edges.Client{EnvRegistryURL: e.cfg.EnvRegistryURL, ModelRegistryURL: e.cfg.ModelRegistryURL, HTTP: e.http}

	lock, err := cache.Lock(e.cfg.CacheDir)
	if err != nil {
		e.log.Error("executor_cache_locked", "error", err.Error())
		return err
	}
	e.lock = lock
	store, err := cache.Open(e.cfg.CacheDir)
	if err != nil {
		_ = lock.Release()
		e.log.Error("executor_cache_unusable", "error", err.Error())
		return err
	}
	e.store = store
	seq, err := seqfile.Open(e.cfg.StatePath())
	if err != nil {
		e.closeLocal()
		e.log.Error("executor_state_file_unusable", "error", err.Error())
		return err
	}
	e.seq = seq
	if stored, _ := store.ExecutorID(); stored != 0 && stored != id.ExecutorID {
		e.log.Warn("executor_cache_executor_changed", "cached_executor_id", stored)
	}
	_ = store.PutExecutorID(id.ExecutorID)
	e.log.Info("executor_starting", "broker_url", e.cfg.Broker.URL, "namespace", e.cfg.Namespace,
		"max_parallel_tasks", e.cfg.MaxParallelTasks, "tag", id.Tag)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.runCtx = runCtx
	go func() {
		select {
		case <-e.abortCh:
			cancel()
		case <-runCtx.Done():
		}
	}()

	restarted, err := e.startupCleanup(runCtx)
	if err != nil {
		e.closeLocal()
		return err
	}
	if !e.dialLoop(ctx, runCtx) {
		return e.finish(ctx, runCtx)
	}
	e.ready.Store(true)
	e.emitSelf(platform.KindExecutorStarted, map[string]any{"executor_type": platform.ExecutorTypeK8sOpenHands})
	e.flushOutbox()
	e.releaseRestarted(restarted)

	loopCtx, stopLoops := context.WithCancel(runCtx)
	e.goLoop(loopCtx, e.heartbeatLoop)
	e.goLoop(loopCtx, e.consumerLoop)
	e.goLoop(loopCtx, e.taskLoop)
	e.goLoop(loopCtx, e.commandLoop)
	e.goLoop(loopCtx, e.kubeLoop)
	e.goLoop(loopCtx, e.connEventsLoop)
	e.goLoop(runCtx, e.outboxLoop)

	select {
	case <-ctx.Done():
	case <-runCtx.Done():
	}
	stopLoops()
	return e.finish(ctx, runCtx)
}

func (e *Executor) goLoop(ctx context.Context, fn func(context.Context)) {
	e.wg.Add(1)
	go func() { defer e.wg.Done(); fn(ctx) }()
}

// finish runs the SIGTERM shutdown, or only closes local state after an abort.
func (e *Executor) finish(ctx context.Context, runCtx context.Context) error {
	if e.aborted() {
		e.ready.Store(false)
		e.closeLocal()
		return nil
	}
	e.shutdown(runCtx)
	e.closeLocal()
	return nil
}

func (e *Executor) closeLocal() {
	e.ready.Store(false)
	e.mu.Lock()
	conn := e.conn
	e.conn = nil
	e.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	e.connected.Store(false)
	if e.seq != nil {
		_ = e.seq.Close()
	}
	if e.store != nil {
		_ = e.store.Close()
	}
	if e.lock != nil {
		_ = e.lock.Release()
	}
}

// dialLoop connects with the stored JWT, retrying failures; a revoked JWT
// is never retried. It returns false when ctx ended first.
func (e *Executor) dialLoop(ctx, runCtx context.Context) bool {
	opts := broker.DialOptions{URL: e.cfg.Broker.URL, Token: e.cfg.Broker.Token, CAFile: e.cfg.Broker.CAFile,
		InboxPrefix: fmt.Sprintf("_INBOX.executor-%d", e.id.ExecutorID), Name: fmt.Sprintf("executor-%d", e.id.ExecutorID)}
	backoff := time.Second
	for {
		if ctx.Err() != nil || runCtx.Err() != nil {
			return false
		}
		conn, err := e.opts.Dial(runCtx, opts)
		if err == nil {
			e.mu.Lock()
			e.conn = conn
			e.mu.Unlock()
			e.connected.Store(true)
			e.mu.Lock()
			e.lastHB = e.now()
			e.mu.Unlock()
			e.log.Info("broker_connected")
			return true
		}
		if errors.Is(err, broker.ErrRevoked) {
			e.revoked.Store(true)
			e.log.Error("broker_token_revoked")
			select {
			case <-ctx.Done():
			case <-runCtx.Done():
			}
			return false
		}
		e.log.Warn("broker_connect_failed", "error", sanitize(err.Error(), e.cfg.Broker.Token))
		select {
		case <-ctx.Done():
			return false
		case <-runCtx.Done():
			return false
		case <-e.clk.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (e *Executor) getConn() broker.Conn {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.conn
}

// canPublish reports whether the broker connection can carry a publish.
func (e *Executor) canPublish() bool {
	c := e.getConn()
	return c != nil && c.Connected() && !e.revoked.Load()
}

// request sends a rule-9 request: headers team and request_id, body
// {"params":{},"body":...}.
func (e *Executor) request(ctx context.Context, svc, op string, body any) (*broker.Reply, error) {
	conn := e.getConn()
	if conn == nil {
		return nil, broker.ErrNotConnected
	}
	data, err := jsonMarshal(map[string]any{"params": map[string]any{}, "body": body})
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.reqSeq++
	rid := e.reqSeq
	e.mu.Unlock()
	hdr := broker.Header{"team": strconv.FormatInt(e.id.TeamID, 10), "request_id": strconv.FormatInt(rid, 10)}
	return conn.Request(ctx, platform.RequestSubject(svc, e.id.ExecutorID, op), hdr, data, e.cfg.Broker.RequestTimeout)
}

// kubeLoop watches the Kubernetes API; an unreachable API stops pulls and
// publishes executor_failed once per outage.
func (e *Executor) kubeLoop(ctx context.Context) {
	for e.sleep(ctx, e.cfg.PollInterval) {
		ok := e.kube.Healthy(ctx)
		was := e.kubeOK.Swap(ok)
		if was && !ok {
			e.log.Error("kubernetes_unreachable")
			e.emitSelf(platform.KindExecutorFailed, map[string]any{"phase": "kubernetes"})
		} else if !was && ok {
			e.log.Info("kubernetes_reachable")
		}
	}
}

// connEventsLoop follows the broker connection's events.
func (e *Executor) connEventsLoop(ctx context.Context) {
	for {
		conn := e.getConn()
		if conn == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-conn.Events():
			if !ok {
				if !e.sleep(ctx, e.cfg.PollInterval) {
					return
				}
				continue
			}
			switch ev.Kind {
			case broker.Revoked:
				e.log.Error("broker_token_revoked")
				e.revoked.Store(true)
				e.connected.Store(false)
				e.enterLoss()
			case broker.Disconnected:
				e.connected.Store(false)
				e.log.Warn("broker_disconnected")
			case broker.Reconnected:
				e.connected.Store(true)
				e.consumerOK.Store(false) // create the command consumer again
				e.log.Info("broker_reconnected")
				e.flushOutbox()
			}
		}
	}
}
