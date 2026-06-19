package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/timw255/uplink/internal/store"
)

func runStatus(args []string, out io.Writer) error {
	fset := flag.NewFlagSet("status", flag.ExitOnError)
	dataDir := fset.String("data-dir", "./data", "path to uplink data directory")
	if err := fset.Parse(args); err != nil {
		return err
	}

	// Daemon state from the lockfile.
	lock, _ := readLock(*dataDir)
	fmt.Fprintln(out, "daemon:")
	switch {
	case lock.Empty:
		fmt.Fprintln(out, "  status: NOT running (no lockfile)")
	case lock.Alive:
		fmt.Fprintf(out, "  status: running (pid %d)\n", lock.PID)
	case lock.Stale:
		fmt.Fprintf(out, "  status: STALE lockfile (pid %d not alive)\n", lock.PID)
	default:
		fmt.Fprintf(out, "  status: lockfile present (pid %d, liveness unknown)\n", lock.PID)
	}

	// Job counts via the store. Jobs moved into SQLite this iteration
	// — there's no filesystem dir to walk anymore.
	stForJobs, err := openStoreForCLI(*dataDir)
	if err != nil {
		return err
	}
	jobCounts := map[string]int{}
	for _, status := range []store.JobStatus{store.StatusPending, store.StatusRunning, store.StatusFailed} {
		list, err := stForJobs.ListJobs(context.Background(), status)
		if err != nil {
			_ = stForJobs.Close()
			return fmt.Errorf("count %s jobs: %w", status, err)
		}
		jobCounts[string(status)] = len(list)
	}
	_ = stForJobs.Close()
	fmt.Fprintln(out, "\njobs:")
	for _, k := range []string{"pending", "running", "failed"} {
		fmt.Fprintf(out, "  %-7s %d\n", k+":", jobCounts[k])
	}

	// In-flight uploads — break down by marker state.
	uploads, err := summarizeUploads(filepath.Join(*dataDir, "uploads"))
	if err != nil {
		return fmt.Errorf("summarize uploads: %w", err)
	}
	fmt.Fprintln(out, "\nin-flight uploads:")
	if len(uploads) == 0 {
		fmt.Fprintln(out, "  (none)")
	} else {
		for _, k := range []string{
			string(store.MarkerUploading),
			string(store.MarkerCommitted),
			string(store.MarkerCreated),
		} {
			fmt.Fprintf(out, "  %-10s %d\n", k+":", uploads[k])
		}
	}

	// sync_log totals per channel + DB size.
	st, err := openStoreForCLI(*dataDir)
	if err != nil {
		return err
	}
	defer st.Close()

	totals, err := st.CountByChannel(context.Background())
	if err != nil {
		return fmt.Errorf("count sync_log: %w", err)
	}
	fmt.Fprintln(out, "\nsync_log totals:")
	if len(totals) == 0 {
		fmt.Fprintln(out, "  (no rows)")
	} else {
		names := make([]string, 0, len(totals))
		for k := range totals {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(out, "  %-30s %d\n", name+":", totals[name])
		}
	}

	if info, err := os.Stat(st.Path()); err == nil {
		fmt.Fprintf(out, "\ndb: %s (%s)\n", st.Path(), humanBytes(info.Size()))
	}

	return nil
}

// summarizeUploads walks data/uploads/ and groups markers by state.
func summarizeUploads(uploadsDir string) (map[string]int, error) {
	out := map[string]int{}
	entries, err := os.ReadDir(uploadsDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return out, nil
		}
		return nil, err
	}
	const suffix = ".session.json"
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), suffix) {
			continue
		}
		// Read the marker file directly rather than opening the Store.
		// `uplink status` runs alongside a live daemon; the daemon holds
		// the SQLite writer, and the marker files are flat JSON, so
		// bypassing the Store keeps this command read-only and lock-free.
		body, err := os.ReadFile(filepath.Join(uploadsDir, e.Name()))
		if err != nil {
			continue
		}
		state := extractState(body)
		if state == "" {
			state = "unknown"
		}
		out[state]++
	}
	return out, nil
}

// extractState peeks at the "state" field of a marker file's JSON
// without unmarshaling the whole thing. Cheap; not strict — falls back
// to "unknown" on any parse error.
func extractState(body []byte) string {
	const key = `"state"`
	i := strings.Index(string(body), key)
	if i < 0 {
		return ""
	}
	rest := string(body)[i+len(key):]
	colon := strings.IndexByte(rest, ':')
	if colon < 0 {
		return ""
	}
	rest = rest[colon+1:]
	openQ := strings.IndexByte(rest, '"')
	if openQ < 0 {
		return ""
	}
	rest = rest[openQ+1:]
	before, _, ok := strings.Cut(rest, "\"")
	if !ok {
		return ""
	}
	return before
}

// humanBytes formats n as a short decimal-prefix string.
func humanBytes(n int64) string {
	const (
		_  = iota
		KB = 1 << (10 * iota)
		MB
		GB
		TB
	)
	switch {
	case n >= TB:
		return fmt.Sprintf("%.2f TB", float64(n)/float64(TB))
	case n >= GB:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(GB))
	case n >= MB:
		return fmt.Sprintf("%.2f MB", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.2f KB", float64(n)/float64(KB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
