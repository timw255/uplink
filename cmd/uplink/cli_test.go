package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/store"
)

// setupTestStore creates a fresh data dir + opens a store. Subcommands
// that take --data-dir get pointed at the returned path.
func setupTestStore(t *testing.T) (dataDir string) {
	t.Helper()
	dataDir = t.TempDir()
	s, err := store.Open(context.Background(), dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return dataDir
}

// writeFailedJob seeds a failed-status job in SQLite. Pre-launch
// migration moved the job queue out of `data/jobs/{pending,running,
// failed}/` into a single `jobs` table; this helper now writes
// directly via SQL to match the new layout.
func writeFailedJob(t *testing.T, dataDir, jobID, channel, sourcePath, lastErr string) {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	if _, err := s.EnqueueJob(ctx, store.Job{
		ID:              jobID,
		ChannelName:     channel,
		Kind:            "OnCreate",
		SourceConnector: "fs-in",
		SourcePath:      sourcePath,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Claim then fail to land it in failed status with the right
	// attempts + last_error.
	if _, err := s.ClaimNextJob(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.FailJob(ctx, jobID, 5, lastErr); err != nil {
		t.Fatalf("fail: %v", err)
	}
}

func TestCLI_Status_EmptyDataDir(t *testing.T) {
	dataDir := setupTestStore(t)
	var out bytes.Buffer
	if err := runStatus([]string{"--data-dir=" + dataDir}, &out); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	s := out.String()
	for _, want := range []string{
		"daemon:",
		"NOT running",
		"pending: 0",
		"failed: 0",
		"(none)",
		"(no rows)",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("status output missing %q\nfull:\n%s", want, s)
		}
	}
}

func TestCLI_Status_WithJobsAndSyncLog(t *testing.T) {
	dataDir := setupTestStore(t)

	// Seed: 1 failed job, 1 marker in state=committed, 2 sync_log rows.
	writeFailedJob(t, dataDir, "01HZJOB1A", "ch1", "a.txt", "boom")
	markerBody := `{"job_id":"01HZJOB99","state":"committed","upload_token":"t","segments_total":4,"segments_done":[0,1,2,3],"channel":"ch1","source_connector":"fs-in","source_path":"big.bin","filename":"big.bin","updated":"2026-05-17T00:00:00Z"}`
	if err := os.WriteFile(filepath.Join(dataDir, "uploads", "01HZJOB99.session.json"), []byte(markerBody), 0o644); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
	st, err := store.Open(context.Background(), dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	for _, p := range []string{"a.txt", "b.txt"} {
		if err := st.InsertSyncLog(context.Background(), store.SyncLogEntry{
			ChannelName: "ch1", SourceConnector: "fs-in", SourcePath: p,
			SourceVersion: "v", DestID: "rec-" + p, Kind: store.SyncCreate,
		}); err != nil {
			t.Fatalf("seed sync_log %s: %v", p, err)
		}
	}
	_ = st.Close()

	var out bytes.Buffer
	if err := runStatus([]string{"--data-dir=" + dataDir}, &out); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	s := out.String()
	if !strings.Contains(s, "failed: 1") {
		t.Fatalf("missing failed count\nfull:\n%s", s)
	}
	if !strings.Contains(s, "committed: 1") {
		t.Fatalf("missing committed marker count\nfull:\n%s", s)
	}
	if !regexp.MustCompile(`(?m)^\s+ch1:\s+2$`).MatchString(s) {
		t.Fatalf("missing 'ch1: 2' channel row\nfull:\n%s", s)
	}
}

func TestCLI_Retry_ByID(t *testing.T) {
	dataDir := setupTestStore(t)
	writeFailedJob(t, dataDir, "01HZJOBABC", "ch1", "a.txt", "boom")

	var out, errOut bytes.Buffer
	if err := runRetry([]string{"--data-dir=" + dataDir, "--id=01HZJOBABC"}, &out, &errOut); err != nil {
		t.Fatalf("runRetry: %v", err)
	}
	if !strings.Contains(out.String(), "retried 1 job(s)") {
		t.Fatalf("unexpected output: %q", out.String())
	}

	// Row should now be in pending status with attempts+last_error cleared.
	s, err := store.Open(context.Background(), dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	pending, err := s.ListJobs(context.Background(), store.StatusPending)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "01HZJOBABC" {
		t.Fatalf("expected one pending job 01HZJOBABC, got %+v", pending)
	}
	if pending[0].Attempts != 0 || pending[0].LastError != "" {
		t.Fatalf("retry should clear attempts+last_error, got %+v", pending[0])
	}
	failed, _ := s.ListJobs(context.Background(), store.StatusFailed)
	if len(failed) != 0 {
		t.Fatalf("expected no failed jobs after retry, got %d", len(failed))
	}
}

func TestCLI_Retry_All(t *testing.T) {
	dataDir := setupTestStore(t)
	writeFailedJob(t, dataDir, "01HZAAA", "ch", "a.txt", "err1")
	writeFailedJob(t, dataDir, "01HZBBB", "ch", "b.txt", "err2")
	writeFailedJob(t, dataDir, "01HZCCC", "ch", "c.txt", "err3")

	var out, errOut bytes.Buffer
	if err := runRetry([]string{"--data-dir=" + dataDir, "--all"}, &out, &errOut); err != nil {
		t.Fatalf("runRetry: %v", err)
	}
	if !strings.Contains(out.String(), "retried 3 job(s)") {
		t.Fatalf("expected 'retried 3 job(s)', got: %s", out.String())
	}
	s, err := store.Open(context.Background(), dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	pending, err := s.ListJobs(context.Background(), store.StatusPending)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	got := make(map[string]bool, len(pending))
	for _, j := range pending {
		got[j.ID] = true
	}
	for _, id := range []string{"01HZAAA", "01HZBBB", "01HZCCC"} {
		if !got[id] {
			t.Fatalf("%s should be pending; have: %v", id, got)
		}
	}
}

func TestCLI_Retry_MissingID(t *testing.T) {
	dataDir := setupTestStore(t)
	var out, errOut bytes.Buffer
	err := runRetry([]string{"--data-dir=" + dataDir, "--id=does-not-exist"}, &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), `no failed job with id "does-not-exist"`) {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

// Operator-edited BOM tolerance was a localization quirk of the
// previous filesystem-backed job format (Notepad on Windows would
// prepend a UTF-8 BOM when saving). Jobs now live in SQLite — there
// is no operator-edited text file to BOM — so the regression is
// retired with the format. stripBOM is kept in
// internal/store/atomicwrite.go for upload-marker tolerance.

func TestCLI_Archive_RoundTrip(t *testing.T) {
	dataDir := setupTestStore(t)

	st, err := store.Open(context.Background(), dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	ctx := context.Background()
	oldTS := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339Nano)
	for i := range 3 {
		if _, err := st.DB().ExecContext(ctx, `
            INSERT INTO sync_log
                (ts, channel_name, source_connector, source_path,
                 source_version, dest_id, kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
			oldTS, "ch", "src", "old-"+strconv.Itoa(i), "v", "rec-"+strconv.Itoa(i), "create",
		); err != nil {
			t.Fatalf("seed old: %v", err)
		}
	}
	for i := range 2 {
		if err := st.InsertSyncLog(ctx, store.SyncLogEntry{
			ChannelName: "ch", SourceConnector: "src", SourcePath: "new-" + strconv.Itoa(i),
			SourceVersion: "v", DestID: "new-" + strconv.Itoa(i), Kind: store.SyncCreate,
		}); err != nil {
			t.Fatalf("seed new: %v", err)
		}
	}
	_ = st.Close()

	var out bytes.Buffer
	if err := runArchive([]string{"--data-dir=" + dataDir, "--older-than=1h"}, &out); err != nil {
		t.Fatalf("runArchive: %v", err)
	}
	if !strings.Contains(out.String(), "archived 3 rows") {
		t.Fatalf("unexpected output: %q", out.String())
	}

	// JSONL file exists with 3 lines.
	matches, _ := filepath.Glob(filepath.Join(dataDir, "archive", "sync_log-*.jsonl"))
	if len(matches) != 1 {
		t.Fatalf("expected one archive file, got %v", matches)
	}
	body, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if got := strings.Count(string(body), "\n"); got != 3 {
		t.Fatalf("archive file has %d lines, want 3", got)
	}

	// Remaining sync_log: 2 rows.
	st2, _ := store.Open(ctx, dataDir)
	defer st2.Close()
	var remaining int
	if err := st2.DB().QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("remaining = %d, want 2", remaining)
	}
}

// seedOldSyncRows inserts n rows backdated 48h so they're guaranteed
// older than any `--older-than=1h` threshold the test passes in.
func seedOldSyncRows(t *testing.T, dataDir string, n int) {
	t.Helper()
	st, err := store.Open(context.Background(), dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()
	oldTS := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339Nano)
	for i := range n {
		if _, err := st.DB().ExecContext(ctx, `
            INSERT INTO sync_log
                (ts, channel_name, source_connector, source_path,
                 source_version, dest_id, kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
			oldTS, "ch", "src", "old-"+strconv.Itoa(i), "v", "rec-"+strconv.Itoa(i), "create",
		); err != nil {
			t.Fatalf("seed old row: %v", err)
		}
	}
}

func TestCLI_Archive_DiscardSkipsFile(t *testing.T) {
	dataDir := setupTestStore(t)
	seedOldSyncRows(t, dataDir, 3)

	var out bytes.Buffer
	if err := runArchive([]string{"--data-dir=" + dataDir, "--older-than=1h", "--discard"}, &out); err != nil {
		t.Fatalf("runArchive: %v", err)
	}
	if !strings.Contains(out.String(), "pruned 3 rows") {
		t.Fatalf("expected 'pruned 3 rows' in output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "no archive written") {
		t.Fatalf("expected discard-mode marker in output, got %q", out.String())
	}

	// No JSONL file should exist anywhere under <data-dir>/archive/.
	matches, _ := filepath.Glob(filepath.Join(dataDir, "archive", "*.jsonl"))
	if len(matches) != 0 {
		t.Fatalf("expected no archive files in discard mode, got %v", matches)
	}

	// Rows are actually gone from the live DB.
	st, _ := store.Open(context.Background(), dataDir)
	defer st.Close()
	var remaining int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining rows = %d, want 0 (all pruned)", remaining)
	}
}

func TestCLI_Archive_DiscardAndOutAreMutuallyExclusive(t *testing.T) {
	dataDir := setupTestStore(t)
	seedOldSyncRows(t, dataDir, 1)
	outDir := filepath.Join(t.TempDir(), "archive-out")

	var out bytes.Buffer
	err := runArchive([]string{
		"--data-dir=" + dataDir,
		"--older-than=1h",
		"--discard",
		"--out=" + outDir,
	}, &out)
	if err == nil {
		t.Fatal("expected --discard + --out to error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("error %q does not explain the rule", err)
	}

	// The store row must NOT have been touched — failure must be detected
	// before any deletion happens.
	st, _ := store.Open(context.Background(), dataDir)
	defer st.Close()
	var remaining int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want 1 (no rows should have been pruned)", remaining)
	}
}

func TestCLI_Archive_DiscardWithNoMatchingRows(t *testing.T) {
	// Seed no old rows; pruning finds nothing. Output should report it,
	// and no file should be created (discard mode wouldn't create one
	// anyway, but the message wording is part of the UX contract).
	dataDir := setupTestStore(t)

	var out bytes.Buffer
	if err := runArchive([]string{"--data-dir=" + dataDir, "--older-than=1h", "--discard"}, &out); err != nil {
		t.Fatalf("runArchive: %v", err)
	}
	if !strings.Contains(out.String(), "nothing pruned") {
		t.Fatalf("expected 'nothing pruned' message, got %q", out.String())
	}
}

func TestResolveConfigPath_ExplicitFlagWins(t *testing.T) {
	got, err := resolveConfigPath("/explicit/path.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "/explicit/path.yaml" {
		t.Errorf("got %q, want explicit flag value", got)
	}
}

func TestResolveConfigPath_MissingDefaultErrors(t *testing.T) {
	// No --config and no uplink.yaml next to the test binary's location.
	// os.Executable() during a `go test` run points at a temp test binary
	// in t.TempDir()-style temp dir, where uplink.yaml is not present.
	_, err := resolveConfigPath("")
	if err == nil {
		t.Fatal("expected error when no default config exists")
	}
	if !strings.Contains(err.Error(), "no config file at") {
		t.Errorf("error %q does not explain how to fix it", err)
	}
	if !strings.Contains(err.Error(), defaultConfigName) {
		t.Errorf("error %q does not name the expected filename", err)
	}
}

func TestCLI_InspectUpload(t *testing.T) {
	dataDir := setupTestStore(t)
	markerBody := `{"job_id":"01HZINSPECT","state":"committed","upload_token":"tok","segments_total":4,"segments_done":[0,1,2,3],"channel":"ch","source_connector":"src","source_path":"x.bin","filename":"x.bin","updated":"2026-05-17T00:00:00Z"}`
	if err := os.WriteFile(filepath.Join(dataDir, "uploads", "01HZINSPECT.session.json"),
		[]byte(markerBody), 0o644); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	var out bytes.Buffer
	if err := runInspect([]string{"upload", "--data-dir=" + dataDir, "--job=01HZINSPECT"}, &out); err != nil {
		t.Fatalf("runInspect: %v", err)
	}
	// Output should be the JSON, decodable back to a map.
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("inspect output is not valid JSON: %v\noutput: %s", err, out.String())
	}
	if got["job_id"] != "01HZINSPECT" || got["state"] != "committed" {
		t.Fatalf("inspect missed fields: %+v", got)
	}
}
