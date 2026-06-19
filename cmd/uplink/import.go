package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/connectors"
	"github.com/timw255/uplink/internal/importer"
	"github.com/timw255/uplink/internal/store"
)

// runImport is the `uplink import` subcommand: a one-shot bulk load of a
// JSONL manifest into Aprimo. It builds the named destination (and
// source, when records carry files) connectors from the config but —
// unlike `run` — opens no data dir, no SQLite store, and no lockfile.
// The only durable output is Aprimo itself plus an optional results
// ledger.
func runImport(args []string, stdout io.Writer) error {
	fset := flag.NewFlagSet("import", flag.ExitOnError)
	configPath := fset.String("config", "", "path to YAML config (default: uplink.yaml next to the binary)")
	manifestPath := fset.String("file", "", "path to the JSONL manifest to import (required)")
	destName := fset.String("destination", "", "aprimo connector name to import into (default: the sole aprimo connector)")
	sourceName := fset.String("source", "", "source connector name supplying asset files (required when any record has a \"file\")")
	dryRun := fset.Bool("dry-run", false, "validate every record (file exists, metadata resolves) without writing")
	stopOnError := fset.Bool("stop-on-error", false, "abort on the first failing record (default: process all, report at end)")
	restart := fset.Bool("restart", false, "ignore the existing ledger and re-process every record from scratch")
	maxWorkers := fset.Int("max-workers", 0, "how many files upload at once (alias for --upload-concurrency; 0 = default 32)")
	uploadConc := fset.Int("upload-concurrency", 0, "how many files upload at once; 0 = default 32 (per-upload throughput auto-tunes)")
	createConc := fset.Int("create-concurrency", 0, "concurrent record writes; the rate limiter paces these (0 = default 16)")
	logLevel := fset.String("log-level", "info", "log verbosity (debug|info|warn|error)")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return fmt.Errorf("--file is required (path to the JSONL manifest)")
	}

	level := "info"
	if v, ok := validLogLevel(*logLevel); ok {
		level = v
	} else {
		return fmt.Errorf("--log-level %q must be one of debug, info, warn, error", *logLevel)
	}
	logger := buildLogger(level, "text")

	resolved, err := resolveConfigPath(*configPath)
	if err != nil {
		return err
	}
	cfg, err := channel.Load(resolved)
	if err != nil {
		return err
	}

	// Resolve the destination connector: explicit name, else the sole
	// aprimo connector in the config.
	dest := *destName
	if dest == "" {
		name, count := soleAprimoConnector(cfg)
		switch count {
		case 0:
			return fmt.Errorf("no aprimo connector defined in config; nothing to import into")
		case 1:
			dest = name
		default:
			return fmt.Errorf("multiple aprimo connectors defined; pass --destination=NAME")
		}
	}
	destSpec, ok := findConnector(cfg, dest)
	if !ok {
		return fmt.Errorf("destination %q is not a defined connector", dest)
	}
	if destSpec.Type != "aprimo" {
		return fmt.Errorf("destination %q has type %q; import targets must be aprimo connectors", dest, destSpec.Type)
	}

	var srcSpec channel.ConnectorSpec
	if *sourceName != "" {
		srcSpec, ok = findConnector(cfg, *sourceName)
		if !ok {
			return fmt.Errorf("source %q is not a defined connector", *sourceName)
		}
		if srcSpec.Type == "aprimo" {
			return fmt.Errorf("source %q is an aprimo connector; Aprimo is destination-only", *sourceName)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	reg := connector.NewRegistry()
	connectors.Register(reg)
	pool := connectors.NewPool(reg)
	defer func() { _ = pool.Close() }()

	// Build with a nil store: no upload markers, no lockfile, no data
	// dir. Init still authenticates and prefetches the Aprimo catalog so
	// field-name resolution (and dry-run validation) works.
	//
	// An import validates against and runs on a single catalog snapshot — it
	// never re-pulls mid-run, so a row referencing something not in the
	// snapshot just errors. The connector's Init otherwise starts the
	// daemon's periodic catalog refresh (shared code path); pin it off here.
	if destSpec.Config != nil {
		destSpec.Config["refresh_interval"] = "0"
	}
	var noStore *store.Store
	logger.Info("connecting", "destination", dest)
	if err := pool.Build(ctx, destSpec.Name, destSpec.Type, destSpec.Config, noStore); err != nil {
		return err
	}
	if *sourceName != "" {
		logger.Info("connecting", "source", *sourceName)
		if err := pool.Build(ctx, srcSpec.Name, srcSpec.Type, srcSpec.Config, noStore); err != nil {
			return err
		}
	}

	destConn, _ := pool.Get(dest)
	destImporter, ok := destConn.(importer.Destination)
	if !ok {
		return fmt.Errorf("destination %q does not support importing", dest)
	}
	var srcConn connector.Connector
	if *sourceName != "" {
		srcConn, _ = pool.Get(*sourceName)
	}

	// Resolve the ledger. A real run always keeps one under the data dir,
	// keyed to (manifest, destination), so re-running the same import
	// resumes where it left off — records already created or updated are
	// not re-uploaded. --restart ignores prior progress. A dry run keeps
	// no ledger (it re-validates everything every time).
	ledgerPath, resume, err := resolveLedger(cfg.Storage.DataDir, *manifestPath, dest, *dryRun, *restart)
	if err != nil {
		return err
	}
	if ledgerPath != "" {
		logger.Info("ledger", "path", ledgerPath, "resume", resume)
	}

	im, err := importer.New(importer.Options{
		ManifestPath:      *manifestPath,
		Dest:              destImporter,
		Source:            srcConn,
		SourceName:        *sourceName,
		DestName:          dest,
		DryRun:            *dryRun,
		StopOnError:       *stopOnError,
		ResultsPath:       ledgerPath,
		Resume:            resume,
		MaxWorkers:        *maxWorkers,
		UploadConcurrency: *uploadConc,
		CreateConcurrency: *createConc,
		StatusWriter:      ttyWriter(stdout),
		Logger:            logger,
	})
	if err != nil {
		return err
	}

	mode := "import"
	if *dryRun {
		mode = "dry-run"
	}
	logger.Info("starting", "mode", mode, "manifest", *manifestPath)

	sum, err := im.Run(ctx)
	if err != nil {
		return err
	}

	printImportSummary(stdout, sum, *dryRun)

	// Non-zero exit when anything didn't pass, so the command composes in
	// scripts and CI.
	if *dryRun && sum.Invalid > 0 {
		return fmt.Errorf("%d of %d record(s) failed validation", sum.Invalid, sum.Total)
	}
	if !*dryRun && sum.Aborted > 0 {
		return fmt.Errorf("run stopped early: %d failed, %d not processed (rerun to resume)", sum.Failed, sum.Aborted)
	}
	if !*dryRun && sum.Failed > 0 {
		return fmt.Errorf("%d of %d record(s) failed to import", sum.Failed, sum.Total)
	}
	return nil
}

// printImportSummary writes a human-readable tally to stdout.
func printImportSummary(w io.Writer, s importer.Summary, dryRun bool) {
	fmt.Fprintln(w)
	if dryRun {
		fmt.Fprintf(w, "Dry run complete in %s\n", s.Elapsed.Round(time.Millisecond))
		fmt.Fprintf(w, "  records:  %d\n", s.Total)
		fmt.Fprintf(w, "  valid:    %d\n", s.Valid)
		fmt.Fprintf(w, "  invalid:  %d\n", s.Invalid)
		if s.Rewritten > 0 {
			fmt.Fprintf(w, "  filenames to be rewritten: %d (see warnings in the log)\n", s.Rewritten)
		}
		if s.Skipped > 0 {
			fmt.Fprintf(w, "  skipped:  %d\n", s.Skipped)
		}
		return
	}
	headline := "Import complete in"
	if s.Aborted > 0 {
		headline = "Import stopped after"
	}
	fmt.Fprintf(w, "%s %s\n", headline, s.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "  records:  %d\n", s.Total)
	fmt.Fprintf(w, "  created:  %d\n", s.Created)
	fmt.Fprintf(w, "  updated:  %d\n", s.Updated)
	fmt.Fprintf(w, "  metadata: %d\n", s.Metadata)
	if s.Skipped > 0 {
		fmt.Fprintf(w, "  skipped:  %d\n", s.Skipped)
	}
	fmt.Fprintf(w, "  failed:   %d\n", s.Failed)
	if s.Aborted > 0 {
		fmt.Fprintf(w, "  not processed: %d (run stopped early — rerun to resume)\n", s.Aborted)
	}
}

// resolveLedger decides where per-record outcomes are tracked and
// whether to resume from prior progress. The location is always derived
// from the data dir — there is no override, the same way the data dir and
// log destination aren't overridable from this command.
//
// A real run persists a ledger at <data_dir>/imports keyed to the
// (absolute manifest path, destination) pair. Resume is on by default so
// a re-run skips records already created/updated/patched — the dedup that
// keeps a crashed or re-issued import from uploading the same file twice.
// --restart turns resume off and starts the ledger fresh. A dry run keeps
// no ledger: it re-validates everything every time.
func resolveLedger(dataDir, manifestPath, dest string, dryRun, restart bool) (path string, resume bool, err error) {
	if dryRun {
		return "", false, nil
	}
	abs, aerr := filepath.Abs(manifestPath)
	if aerr != nil {
		return "", false, fmt.Errorf("resolve manifest path: %w", aerr)
	}
	dir := filepath.Join(dataDir, "imports")
	if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil { // ledger holds upload tokens
		return "", false, fmt.Errorf("create import ledger dir: %w", mkErr)
	}
	path = filepath.Join(dir, ledgerName(abs, dest))
	return path, !restart, nil
}

// ledgerName derives a stable, human-recognizable ledger filename for a
// given manifest+destination so repeated imports of the same manifest
// land on the same ledger and resume cleanly.
func ledgerName(manifestAbs, dest string) string {
	sum := sha256.Sum256([]byte(manifestAbs + "\x00" + dest))
	base := strings.TrimSuffix(filepath.Base(manifestAbs), filepath.Ext(manifestAbs))
	if base == "" {
		base = "manifest"
	}
	return fmt.Sprintf("%s-%s.jsonl", base, hex.EncodeToString(sum[:4]))
}

// ttyWriter returns w for the live status line only when w is an
// interactive terminal. When stdout is piped or redirected to a file,
// carriage-return rewrites would be garbage, so it returns nil and the
// importer falls back to periodic log lines.
//
// IsCygwinTerminal is what makes this correct on Windows: Git Bash / MSYS
// / mintty present as pipes, not Windows consoles, so a stdlib
// ModeCharDevice check would wrongly classify them as non-terminals and
// suppress the status line. go-isatty is already in the dependency tree
// (via the SQLite driver), so this adds no new footprint.
func ttyWriter(w io.Writer) io.Writer {
	f, ok := w.(*os.File)
	if !ok {
		return nil
	}
	if isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd()) {
		return f
	}
	return nil
}

// findConnector returns the connector spec with the given name.
func findConnector(cfg *channel.Config, name string) (channel.ConnectorSpec, bool) {
	for _, c := range cfg.Connectors {
		if c.Name == name {
			return c, true
		}
	}
	return channel.ConnectorSpec{}, false
}

// soleAprimoConnector returns the name of the only aprimo connector in
// the config and how many aprimo connectors exist, so the caller can
// default --destination when unambiguous.
func soleAprimoConnector(cfg *channel.Config) (name string, count int) {
	for _, c := range cfg.Connectors {
		if c.Type == "aprimo" {
			name = c.Name
			count++
		}
	}
	return name, count
}
