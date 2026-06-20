// Command uplink is the Uplink binary. Subcommand layout:
//
//	uplink run [--config=...]                 run the daemon
//	uplink status [--data-dir=...]            summary of jobs + sync_log
//	uplink retry --id|--channel|--all         move failed jobs back to pending
//	uplink inspect {sync|state|upload} ...    print durable state for a key
//	uplink import --file=<manifest.jsonl>     bulk-load a JSONL manifest into Aprimo
//	uplink archive --older-than=<dur>         prune old sync_log rows
//	uplink version                            print version and exit
//
// With no subcommand, the binary runs the daemon — equivalent to `uplink run`.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/timw255/uplink/internal/store"
)

var version = "dev"

func main() {
	// Default logger for the one-shot subcommands: text on stderr, so
	// package-level slog.* calls never pollute their stdout (which carries
	// human output). The daemon overrides this with its configured logger;
	// the value passed to runDaemon is an unused bootstrap handle.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	args := os.Args[1:]
	sub := ""
	if len(args) > 0 && !isFlag(args[0]) {
		sub = args[0]
		args = args[1:]
	}

	var err error
	switch sub {
	case "", "run":
		err = runDaemon(args, logger)
	case "status":
		err = runStatus(args, os.Stdout)
	case "retry":
		err = runRetry(args, os.Stdout, os.Stderr)
	case "inspect":
		err = runInspect(args, os.Stdout)
	case "import":
		err = runImport(args, os.Stdout)
	case "archive":
		err = runArchive(args, os.Stdout)
	case "version", "--version", "-v":
		fmt.Println(version)
		return
	case "help", "--help", "-h":
		printUsage(os.Stdout)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", sub)
		printUsage(os.Stderr)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// isFlag reports whether s starts with "-" — the heuristic that
// distinguishes a global flag (for `run`) from a subcommand name.
func isFlag(s string) bool {
	return len(s) > 0 && s[0] == '-'
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Uplink — storage → Aprimo file sync

Usage:
  uplink <subcommand> [flags]

Subcommands:
  run [--config=PATH]                        run the daemon (default; loads
                                             uplink.yaml next to the binary
                                             when --config is omitted)
  status [--data-dir=PATH]                   print job + sync_log summary
  retry --id=ID | --channel=N | --all        re-enqueue failed jobs
  inspect sync --path=P --channel=C          print latest sync_log entry
  inspect state --connector=N                dump connector state blob
  inspect upload --job=ID                    print in-flight upload marker
  import --file=M.jsonl [--dry-run]          bulk-load a JSONL manifest into Aprimo
  archive --older-than=DUR [--out=D|--discard]  prune old sync_log rows
  version                                    print version and exit
  help                                       print this message`)
}

// openStoreForCLI opens the store from a --data-dir flag. SQLite WAL
// allows concurrent readers, so this is safe even while the daemon is
// writing into the same database file.
func openStoreForCLI(dataDir string) (*store.Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("--data-dir is required")
	}
	return store.Open(context.Background(), dataDir)
}
