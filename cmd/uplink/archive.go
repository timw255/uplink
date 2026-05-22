package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func runArchive(args []string, out io.Writer) error {
	fset := flag.NewFlagSet("archive", flag.ExitOnError)
	dataDir := fset.String("data-dir", "./data", "path to uplink data directory")
	older := fset.String("older-than", "", "duration (e.g. 30d, 720h, 168h)")
	outDir := fset.String("out", "", "directory to write the JSONL archive into (default: <data-dir>/archive)")
	discard := fset.Bool("discard", false, "delete archived rows without writing JSONL (mutually exclusive with --out)")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *older == "" {
		return errors.New("--older-than is required")
	}
	if *discard && *outDir != "" {
		return errors.New("--discard and --out are mutually exclusive")
	}
	dur, err := parseDuration(*older)
	if err != nil {
		return fmt.Errorf("--older-than: %w", err)
	}
	threshold := time.Now().UTC().Add(-dur)

	st, err := openStoreForCLI(*dataDir)
	if err != nil {
		return err
	}
	defer st.Close()

	if *discard {
		count, err := st.ArchiveOlderThan(context.Background(), threshold, io.Discard)
		if err != nil {
			return err
		}
		if count == 0 {
			fmt.Fprintln(out, "no rows older than threshold; nothing pruned")
			return nil
		}
		fmt.Fprintf(out, "pruned %d rows older than %s (no archive written)\n", count, threshold.Format(time.RFC3339))
		return nil
	}

	targetDir := *outDir
	if targetDir == "" {
		targetDir = filepath.Join(*dataDir, "archive")
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return err
	}
	filename := fmt.Sprintf("sync_log-%s.jsonl", time.Now().UTC().Format("2006-01-02T150405Z"))
	target := filepath.Join(targetDir, filename)

	f, err := os.Create(target)
	if err != nil {
		return err
	}
	// closed tracks whether we've already invoked Close so the deferred
	// fallback doesn't double-close. We close explicitly on the happy
	// path so a close error becomes visible to the operator; the defer
	// is a safety net for panic / unexpected return paths.
	var closed bool
	defer func() {
		if !closed {
			_ = f.Close()
			// On a panic-driven exit we can't tell whether the archive
			// completed atomically, so unlink the partial file rather
			// than leave a torn JSONL beside the data dir.
			_ = os.Remove(target)
		}
	}()

	count, archiveErr := st.ArchiveOlderThan(context.Background(), threshold, f)
	closeErr := f.Close()
	closed = true
	if archiveErr != nil {
		_ = os.Remove(target)
		return archiveErr
	}
	if closeErr != nil {
		// Rows were deleted from the DB but the on-disk archive may be
		// truncated or unflushed; remove it so we never leave a
		// silently-corrupted file lying around.
		_ = os.Remove(target)
		return closeErr
	}
	if count == 0 {
		_ = os.Remove(target)
		fmt.Fprintln(out, "no rows older than threshold; nothing archived")
		return nil
	}
	fmt.Fprintf(out, "archived %d rows older than %s to %s\n", count, threshold.Format(time.RFC3339), target)
	return nil
}

// parseDuration extends time.ParseDuration with a "d" suffix for days,
// since operators tend to think in days for retention.
func parseDuration(s string) (time.Duration, error) {
	if len(s) > 1 && s[len(s)-1] == 'd' {
		days := s[:len(s)-1]
		var n int
		if _, err := fmt.Sscanf(days, "%d", &n); err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid days duration %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
