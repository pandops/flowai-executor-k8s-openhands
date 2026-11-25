// Command executor_k8s_openhands runs the K8s OpenHands Executor.
//
//	executor_k8s_openhands --config /var/lib/flowai/executor-k8s/config/config.yaml
//	executor_k8s_openhands install-config --from <rendered> --to <volume path>
//
// The Helm chart renders the configuration (including broker.token) into a
// Secret; the install-config init container copies it onto the writable
// volume with mode 0600, and the Executor reads it from there on every start.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/clock"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/config"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/executor"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/k8sclient"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/logging"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/netinfo"
)

const defaultConfigPath = "/var/lib/flowai/executor-k8s/config/config.yaml"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "install-config" {
		os.Exit(installConfig(os.Args[2:]))
	}
	os.Exit(run(os.Args[1:]))
}

func installConfig(args []string) int {
	fs := flag.NewFlagSet("install-config", flag.ContinueOnError)
	from := fs.String("from", "/etc/flowai/rendered/config.yaml", "rendered configuration")
	to := fs.String("to", defaultConfigPath, "installed configuration on the writable volume")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := config.InstallConfig(*from, *to); err != nil {
		fmt.Fprintln(os.Stderr, "install-config:", err)
		return 1
	}
	return 0
}

func run(args []string) int {
	fs := flag.NewFlagSet("executor_k8s_openhands", flag.ContinueOnError)
	path := fs.String("config", defaultConfigPath, "configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		return 1
	}
	log := logging.New(logging.Level(cfg.LogLevel))
	kube, err := k8sclient.NewInClusterClient()
	if err != nil {
		log.Error("kubernetes_client_unavailable", "error", err.Error())
		return 1
	}
	exec := executor.New(executor.Options{
		Config: cfg, Dial: broker.DialNATS, Kube: kube, HTTPClient: &http.Client{Timeout: 60 * time.Second},
		Clock: clock.Real(), Net: netinfo.System(), Logger: log,
	})
	srv := &http.Server{Addr: cfg.ExecutorAPIBind, Handler: exec.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("probe_listener_failed", "error", err.Error())
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	runErr := exec.Run(ctx)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if runErr != nil {
		return 1
	}
	return 0
}
