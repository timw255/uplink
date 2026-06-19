package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/timw255/uplink/internal/adaptive"
	"github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/connectors"
	"github.com/timw255/uplink/internal/engine"
	"github.com/timw255/uplink/internal/extract"
	"github.com/timw255/uplink/internal/store"
)

// rateAware is the slice of the Aprimo connector the daemon needs to
// drive the engine's adaptive worker pool: read its licensed rate and
// attach the shared telemetry sink the controller samples.
type rateAware interface {
	RateLimit() (rps float64, maxConcurrent int)
	SetRateObserver(obs aprimo.RateObserver)
}

// scriptCompiler adapts an *extract.Runtime to channel.ScriptCompiler.
// Lives here so the channel package stays free of an extract dep —
// only the daemon wiring (which already imports both) bridges the two.
type scriptCompiler struct{ rt *extract.Runtime }

func (c scriptCompiler) Compile(name, path string) (channel.CompiledScript, error) {
	return c.rt.Compile(name, path)
}

// defaultConfigName is the filename `uplink run` looks for next to the
// binary when no --config flag is given.
const defaultConfigName = "uplink.yaml"

// defaultAdaptiveWorkerCap bounds the adaptive worker pool when neither
// engine.workers nor any destination's max_concurrent provides a ceiling.
// The rate limiter is the real speed governor; this just keeps goroutine
// and socket growth sane on an otherwise-uncapped tenant. Not a CPU bound
// — the work is network I/O end to end.
const defaultAdaptiveWorkerCap = 64

// resolveConfigPath returns the config path to load. An explicit --config
// argument is honored as-is. When the flag is empty, the binary's own
// directory is searched for `uplink.yaml`; a clear error fires if that
// file is missing, so the daemon never starts against a config the
// operator didn't intend.
func resolveConfigPath(flag string) (string, error) {
	if flag != "" {
		return flag, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve binary path: %w", err)
	}
	candidate := filepath.Join(filepath.Dir(exe), defaultConfigName)
	if _, err := os.Stat(candidate); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("no config file at %s (pass --config=PATH or place %s next to the binary)",
				candidate, defaultConfigName)
		}
		return "", fmt.Errorf("stat default config: %w", err)
	}
	return candidate, nil
}

// runDaemon is the `uplink run` subcommand and also the default
// behavior when no subcommand is given. It loads config, configures
// the logger, opens the store, starts connectors + engine, blocks
// until SIGTERM/SIGINT, and removes the lockfile on clean exit.
func runDaemon(args []string, bootstrapLogger *slog.Logger) error {
	fset := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fset.String("config", "", "path to YAML config file (default: uplink.yaml next to the binary)")
	logLevel := fset.String("log-level", "", "override logging.level from config (debug|info|warn|error)")
	logFormat := fset.String("log-format", "", "override logging.format from config (text|json)")
	if err := fset.Parse(args); err != nil {
		return err
	}

	resolved, err := resolveConfigPath(*configPath)
	if err != nil {
		return err
	}
	cfg, err := channel.Load(resolved)
	if err != nil {
		return err
	}

	// Resolve effective logger from config + flags (flag wins). Validate
	// flag values now — config values were already validated in Load.
	level := cfg.Logging.Level
	if *logLevel != "" {
		if _, ok := validLogLevel(*logLevel); !ok {
			return fmt.Errorf("--log-level %q must be one of debug, info, warn, error", *logLevel)
		}
		level = *logLevel
	}
	format := cfg.Logging.Format
	if *logFormat != "" {
		switch *logFormat {
		case "text", "json":
			format = *logFormat
		default:
			return fmt.Errorf("--log-format %q must be one of text, json", *logFormat)
		}
	}
	logger := buildLogger(level, format)
	slog.SetDefault(logger)
	_ = bootstrapLogger // intentionally unused after this point

	// 0700: the data dir holds the SQLite DB (paths, record IDs, job
	// payloads) and upload markers/tokens. Chmod too, to tighten a dir a
	// prior version may have created world-readable. (No-op on Windows.)
	if err := os.MkdirAll(cfg.Storage.DataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	_ = os.Chmod(cfg.Storage.DataDir, 0o700)

	if err := acquireLock(cfg.Storage.DataDir); err != nil {
		return err
	}
	defer func() { _ = releaseLock(cfg.Storage.DataDir) }()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.Open(ctx, cfg.Storage.DataDir)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	reg := connector.NewRegistry()
	connectors.Register(reg)

	pool := connectors.NewPool(reg)
	defer func() { _ = pool.Close() }()

	for _, c := range cfg.Connectors {
		if err := pool.Build(ctx, c.Name, c.Type, c.Config, st); err != nil {
			return err
		}
		logger.Info("connector ready", "name", c.Name, "type", c.Type)
	}

	extractRT := extract.NewRuntime(logger)
	channels, err := channel.NewRegistry(cfg.Channels, scriptCompiler{rt: extractRT})
	if err != nil {
		return err
	}

	for _, w := range cfg.Warnings() {
		logger.Warn(w)
	}

	engineOpts := engine.Options{Logger: logger, Workers: cfg.Engine.Workers, MaxAttempts: cfg.Engine.MaxAttempts}
	if cfg.Engine.PollIdle != "" {
		d, err := time.ParseDuration(cfg.Engine.PollIdle)
		if err != nil {
			return fmt.Errorf("engine.poll_idle: %w", err)
		}
		engineOpts.PollIdle = d
	}
	if cfg.Engine.BaseBackoff != "" {
		d, err := time.ParseDuration(cfg.Engine.BaseBackoff)
		if err != nil {
			return fmt.Errorf("engine.base_backoff: %w", err)
		}
		engineOpts.BaseBackoff = d
	}

	// Worker-pool sizing. By default (engine.workers unset) the pool
	// auto-scales to keep the Aprimo rate limiter saturated: attach one
	// shared telemetry sink to every rate-limited destination, sum their
	// licensed RPS as the controller's target, and cap the pool by their
	// in-flight budgets (max_concurrent). Setting engine.workers pins a
	// fixed pool of that size and turns auto-scaling off entirely. With
	// no RPS configured anywhere there's no signal to scale against, so
	// the engine also runs the fixed `workers` pool.
	autoscale := cfg.Engine.Workers <= 0
	metrics := &adaptive.Metrics{}
	var totalRPS float64
	var derivedCap int
	for _, cc := range cfg.Connectors {
		if cc.Type != "aprimo" {
			continue
		}
		conn, ok := pool.Get(cc.Name)
		if !ok {
			continue
		}
		ra, ok := conn.(rateAware)
		if !ok {
			continue
		}
		rps, maxConcurrent := ra.RateLimit()
		if rps > 0 {
			totalRPS += rps
			if autoscale {
				ra.SetRateObserver(metrics)
			}
		}
		derivedCap += maxConcurrent
	}
	if autoscale && totalRPS > 0 {
		maxWorkers := derivedCap
		if maxWorkers <= 0 {
			maxWorkers = defaultAdaptiveWorkerCap
		}
		engineOpts.MaxWorkers = maxWorkers
		engineOpts.TargetRPS = totalRPS
		engineOpts.Metrics = metrics
	}

	eng := engine.New(st, channels, pool, engineOpts)

	var wg sync.WaitGroup
	errCh := make(chan error, 1+len(pool.Sources()))

	wg.Go(func() {
		runLockHeartbeat(ctx, cfg.Storage.DataDir, func(err error) {
			logger.Warn("lockfile heartbeat", "err", err)
		})
	})

	wg.Go(func() {
		if err := eng.Run(ctx); err != nil {
			errCh <- fmt.Errorf("engine: %w", err)
		}
	})

	for name, src := range pool.Sources() {
		wg.Add(1)
		go func(name string, src connector.EventSource) {
			defer wg.Done()
			logger.Info("event source running", "connector", name)
			if err := src.Subscribe(ctx, eng); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("event source %s: %w", name, err)
			}
		}(name, src)
	}

	logger.Info("uplink ready",
		"version", version,
		"pid", os.Getpid(),
		"channels", channels.Names(),
		"data_dir", cfg.Storage.DataDir,
		"log_level", level,
		"log_format", format)

	wg.Wait()
	close(errCh)

	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
		logger.Error("subsystem error", "err", err)
	}
	return firstErr
}

// buildLogger constructs a slog.Logger from a (level, format) pair
// already known to be valid (either passed validation in channel.Load
// or in serve's own flag handling). Output goes to stderr; stdout is
// reserved for the subcommands that emit human-readable status output.
func buildLogger(level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLogLevel(level)}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	default:
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(handler)
}

// parseLogLevel maps the validated level string to slog.Level. The
// empty / unknown case defaults to info; callers should validate
// upstream so unknown never reaches here.
func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// validLogLevel exists so serve's flag handler can validate without
// importing channel.validLogLevels (which is package-private). Kept
// in sync with the YAML validator.
func validLogLevel(s string) (string, bool) {
	switch s {
	case "debug", "info", "warn", "error":
		return s, true
	}
	return "", false
}
