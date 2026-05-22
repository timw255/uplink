package store

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestLookupLatestBatch_OverChunkSize confirms the >500-path chunking
// in LookupLatestBatch correctly aggregates results from multiple
// query chunks.
func TestLookupLatestBatch_OverChunkSize(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	const n = 1500
	paths := make([]string, n)
	for i := range paths {
		paths[i] = "p/" + itoaTest(i)
		if err := s.InsertSyncLog(ctx, SyncLogEntry{
			ChannelName:     "ch",
			SourceConnector: "src",
			SourcePath:      paths[i],
			SourceVersion:   "v" + itoaTest(i),
			DestID:  "rec-" + itoaTest(i),
			Kind:            SyncCreate,
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	got, err := s.LookupLatestBatch(ctx, "ch", paths)
	if err != nil {
		t.Fatalf("LookupLatestBatch: %v", err)
	}
	if len(got) != n {
		t.Fatalf("expected %d results, got %d", n, len(got))
	}
	for i, p := range paths {
		entry, ok := got[p]
		if !ok {
			t.Fatalf("missing result for %s", p)
		}
		if entry.DestID != "rec-"+itoaTest(i) {
			t.Fatalf("path %s: got record id %q", p, entry.DestID)
		}
	}
}

// TestLookupLatestBatch_PartialMissing confirms missing paths are
// absent from the result map rather than producing nil entries.
func TestLookupLatestBatch_PartialMissing(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName: "ch", SourceConnector: "src", SourcePath: "known",
		SourceVersion: "v1", DestID: "rec-1", Kind: SyncCreate,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := s.LookupLatestBatch(ctx, "ch", []string{"known", "missing-1", "missing-2"})
	if err != nil {
		t.Fatalf("LookupLatestBatch: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d: %+v", len(got), got)
	}
	if _, ok := got["missing-1"]; ok {
		t.Fatal("missing path should not be in result map")
	}
}

// TestLookupLatestBatch_ReturnsMostRecentPerPath verifies the GROUP BY
// + MAX(id) query returns only the latest row per path, not all rows.
func TestLookupLatestBatch_ReturnsMostRecentPerPath(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for _, v := range []string{"v1", "v2", "v3"} {
		if err := s.InsertSyncLog(ctx, SyncLogEntry{
			ChannelName: "ch", SourceConnector: "src", SourcePath: "foo",
			SourceVersion: v, DestID: "rec-X", Kind: SyncUpdate,
		}); err != nil {
			t.Fatalf("seed %s: %v", v, err)
		}
	}
	got, err := s.LookupLatestBatch(ctx, "ch", []string{"foo"})
	if err != nil {
		t.Fatalf("LookupLatestBatch: %v", err)
	}
	if got["foo"].SourceVersion != "v3" {
		t.Fatalf("latest = %q, want v3", got["foo"].SourceVersion)
	}
}

// TestArchiveOlderThan_ChunkedDeletion confirms ArchiveOlderThan
// correctly processes more than chunkSize (1000) rows by iterating
// multiple chunks. Each chunk is its own transaction; partial failure
// in one chunk would leave earlier chunks committed.
func TestArchiveOlderThan_ChunkedDeletion(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Seed 1500 old rows + 500 new ones.
	oldTS := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	for i := 0; i < 1500; i++ {
		if _, err := s.DB().ExecContext(ctx, `
            INSERT INTO sync_log
                (ts, channel_name, source_connector, source_path,
                 source_version, dest_id, kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
			oldTS, "ch", "src", "old/"+itoaTest(i), "v", "rec-old-"+itoaTest(i), "create",
		); err != nil {
			t.Fatalf("seed old %d: %v", i, err)
		}
	}
	for i := 0; i < 500; i++ {
		if err := s.InsertSyncLog(ctx, SyncLogEntry{
			ChannelName: "ch", SourceConnector: "src", SourcePath: "new/" + itoaTest(i),
			SourceVersion: "v", DestID: "rec-new-" + itoaTest(i), Kind: SyncCreate,
		}); err != nil {
			t.Fatalf("seed new %d: %v", i, err)
		}
	}

	threshold := time.Now().UTC().Add(-1 * time.Hour)
	var buf bytes.Buffer
	count, err := s.ArchiveOlderThan(ctx, threshold, &buf)
	if err != nil {
		t.Fatalf("ArchiveOlderThan: %v", err)
	}
	if count != 1500 {
		t.Fatalf("archived %d, want 1500", count)
	}

	// JSONL should have exactly 1500 lines.
	lines := strings.Count(buf.String(), "\n")
	if lines != 1500 {
		t.Fatalf("jsonl lines = %d, want 1500", lines)
	}

	// 500 new rows remain.
	var remaining int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 500 {
		t.Fatalf("remaining = %d, want 500", remaining)
	}
}

// TestEngineIdempotencyPattern documents the LookupLatest +
// InsertSyncLog sequence the engine uses to avoid duplicate rows when
// a worker crashes between InsertSyncLog and DeleteMarker. The exact
// sequence: query for the latest row; if it matches the row we're
// about to insert (channel+source_path+source_version+dest_id),
// skip the insert.
func TestEngineIdempotencyPattern(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	entry := SyncLogEntry{
		ChannelName:     "ch",
		SourceConnector: "src",
		SourcePath:      "x.bin",
		SourceVersion:   "v1",
		DestID:  "rec-1",
		Kind:            SyncCreate,
	}
	if err := s.InsertSyncLog(ctx, entry); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// Simulate the post-crash re-attempt: the worker finishes the same
	// job again and reaches the InsertSyncLog step. The engine does
	// LookupLatest first; if the latest matches, it skips.
	latest, err := s.LookupLatest(ctx, "ch", "x.bin")
	if err != nil {
		t.Fatalf("LookupLatest: %v", err)
	}
	needsInsert := latest == nil ||
		latest.SourceVersion != entry.SourceVersion ||
		latest.DestID != entry.DestID
	if needsInsert {
		t.Fatal("idempotency check says insert needed, but row is already there")
	}

	// Confirm only one row exists.
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sync_log WHERE source_path=?`, "x.bin").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}

	// A genuine update (different version) should NOT be skipped.
	entry2 := entry
	entry2.SourceVersion = "v2"
	latest, _ = s.LookupLatest(ctx, "ch", "x.bin")
	needsInsert = latest == nil ||
		latest.SourceVersion != entry2.SourceVersion ||
		latest.DestID != entry2.DestID
	if !needsInsert {
		t.Fatal("idempotency check should NOT skip insert when SourceVersion differs")
	}
}

// TestInsertSyncLog_PopulatesStemAndDir confirms InsertSyncLog
// computes source_stem and source_dir from source_path and writes
// them into the new columns.
func TestInsertSyncLog_PopulatesStemAndDir(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	cases := []struct {
		path     string
		wantStem string
		wantDir  string
	}{
		// Nested asset: dir is the directory, stem strips final ext.
		{"photos/sunset.jpg", "photos/sunset", "photos"},
		// Top-level file: dir is empty string.
		{"README.md", "README", ""},
		// No extension at all: stem == path, dir as expected.
		{"docs/CHANGELOG", "docs/CHANGELOG", "docs"},
		// Multi-segment directory.
		{"a/b/c/file.txt", "a/b/c/file", "a/b/c"},
	}
	for _, tc := range cases {
		if err := s.InsertSyncLog(ctx, SyncLogEntry{
			ChannelName: "ch", SourceConnector: "src", SourcePath: tc.path,
			SourceVersion: "v1", DestID: "rec-" + tc.path, Kind: SyncCreate,
		}); err != nil {
			t.Fatalf("InsertSyncLog %q: %v", tc.path, err)
		}
		var gotStem, gotDir sql.NullString
		if err := s.DB().QueryRowContext(ctx,
			`SELECT source_stem, source_dir FROM sync_log WHERE source_path = ?`, tc.path,
		).Scan(&gotStem, &gotDir); err != nil {
			t.Fatalf("select %q: %v", tc.path, err)
		}
		if !gotStem.Valid || gotStem.String != tc.wantStem {
			t.Errorf("path %q: stem = %v, want %q", tc.path, gotStem, tc.wantStem)
		}
		if !gotDir.Valid || gotDir.String != tc.wantDir {
			t.Errorf("path %q: dir = %v, want %q", tc.path, gotDir, tc.wantDir)
		}
	}
}

// TestLookupByStem_ReturnsLatestMatch confirms LookupByStem returns
// the most recent row matching (channel, stem, dir) and ignores
// older rows + rows from other channels.
func TestLookupByStem_ReturnsLatestMatch(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Three inserts of the same source path on the target channel,
	// plus one on a different channel that must not contaminate.
	for _, v := range []string{"v1", "v2", "v3"} {
		if err := s.InsertSyncLog(ctx, SyncLogEntry{
			ChannelName: "ch", SourceConnector: "src", SourcePath: "photos/sunset.jpg",
			SourceVersion: v, DestID: "rec-" + v, Kind: SyncUpdate,
		}); err != nil {
			t.Fatalf("seed %s: %v", v, err)
		}
	}
	if err := s.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName: "other-ch", SourceConnector: "src", SourcePath: "photos/sunset.jpg",
		SourceVersion: "wrong", DestID: "rec-wrong", Kind: SyncCreate,
	}); err != nil {
		t.Fatalf("seed other channel: %v", err)
	}

	got, err := s.LookupByStem(ctx, "ch", "photos/sunset", "photos")
	if err != nil {
		t.Fatalf("LookupByStem: %v", err)
	}
	if got == nil {
		t.Fatal("expected a row, got nil")
	}
	if got.SourceVersion != "v3" || got.DestID != "rec-v3" {
		t.Fatalf("latest = %+v, want v3/rec-v3", got)
	}
	if got.ChannelName != "ch" {
		t.Fatalf("channel = %q, want ch", got.ChannelName)
	}
}

// TestLookupByStem_TopLevelEmptyDir confirms top-level files with
// dir == "" round-trip correctly.
func TestLookupByStem_TopLevelEmptyDir(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName: "ch", SourceConnector: "src", SourcePath: "README.md",
		SourceVersion: "v1", DestID: "rec-readme", Kind: SyncCreate,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := s.LookupByStem(ctx, "ch", "README", "")
	if err != nil {
		t.Fatalf("LookupByStem: %v", err)
	}
	if got == nil || got.DestID != "rec-readme" {
		t.Fatalf("expected rec-readme, got %+v", got)
	}
}

// TestLookupByStem_NoMatchReturnsNil confirms the "no match" path
// returns (nil, nil) without surfacing sql.ErrNoRows.
func TestLookupByStem_NoMatchReturnsNil(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName: "ch", SourceConnector: "src", SourcePath: "photos/sunset.jpg",
		SourceVersion: "v1", DestID: "rec-1", Kind: SyncCreate,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Wrong channel.
	got, err := s.LookupByStem(ctx, "other-ch", "photos/sunset", "photos")
	if err != nil {
		t.Fatalf("LookupByStem wrong channel: %v", err)
	}
	if got != nil {
		t.Fatalf("wrong channel: expected nil, got %+v", got)
	}

	// Wrong stem.
	got, err = s.LookupByStem(ctx, "ch", "photos/sunrise", "photos")
	if err != nil {
		t.Fatalf("LookupByStem wrong stem: %v", err)
	}
	if got != nil {
		t.Fatalf("wrong stem: expected nil, got %+v", got)
	}

	// Wrong dir.
	got, err = s.LookupByStem(ctx, "ch", "photos/sunset", "videos")
	if err != nil {
		t.Fatalf("LookupByStem wrong dir: %v", err)
	}
	if got != nil {
		t.Fatalf("wrong dir: expected nil, got %+v", got)
	}
}

// TestLookupByStem_IgnoresLegacyRows confirms rows with NULL
// source_stem / source_dir (pre-companion-feature legacy rows) are
// invisible to LookupByStem. They naturally won't match the WHERE
// clause since NULL != any value.
func TestLookupByStem_IgnoresLegacyRows(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Bypass InsertSyncLog so we don't populate the new columns —
	// simulates a row written by an older binary.
	if _, err := s.DB().ExecContext(ctx, `
        INSERT INTO sync_log
            (ts, channel_name, source_connector, source_path,
             source_version, dest_id, kind)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		time.Now().UTC().Format(syncLogTSLayout),
		"ch", "src", "photos/sunset.jpg", "v1", "rec-legacy", "create",
	); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	got, err := s.LookupByStem(ctx, "ch", "photos/sunset", "photos")
	if err != nil {
		t.Fatalf("LookupByStem: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for legacy NULL row, got %+v", got)
	}
}

// TestStemCompositeIndexExists verifies the composite index used by
// LookupByStem is actually present in the schema. PRAGMA index_list
// is the canonical introspection.
func TestStemCompositeIndexExists(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	rows, err := s.DB().QueryContext(ctx, `PRAGMA index_list('sync_log')`)
	if err != nil {
		t.Fatalf("PRAGMA index_list: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var (
			seq     int
			name    string
			unique  int
			origin  string
			partial int
		)
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name == "idx_sync_log_stem" {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if !found {
		t.Fatal("idx_sync_log_stem not present in sync_log indexes")
	}

	// Confirm the index covers the expected columns in the expected
	// order. PRAGMA index_info returns one row per indexed column.
	colRows, err := s.DB().QueryContext(ctx, `PRAGMA index_info('idx_sync_log_stem')`)
	if err != nil {
		t.Fatalf("PRAGMA index_info: %v", err)
	}
	defer colRows.Close()
	var cols []string
	for colRows.Next() {
		var (
			seqno int
			cid   int
			name  string
		)
		if err := colRows.Scan(&seqno, &cid, &name); err != nil {
			t.Fatalf("index_info scan: %v", err)
		}
		cols = append(cols, name)
	}
	wantPrefix := []string{"channel_name", "source_stem", "source_dir"}
	if len(cols) < len(wantPrefix) {
		t.Fatalf("index columns = %v, want prefix %v", cols, wantPrefix)
	}
	for i, w := range wantPrefix {
		if cols[i] != w {
			t.Fatalf("index col[%d] = %q, want %q (full: %v)", i, cols[i], w, cols)
		}
	}
}

// TestMigrationIdempotent confirms running Open twice against the
// same data dir is a no-op on the second run — no errors, columns
// and index still present. Covers both the fresh-DB path on first
// open and the ALTER-TABLE-IF-MISSING / CREATE-INDEX-IF-NOT-EXISTS
// path on second open.
func TestMigrationIdempotent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := s1.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName: "ch", SourceConnector: "src", SourcePath: "x/y.jpg",
		SourceVersion: "v1", DestID: "rec-1", Kind: SyncCreate,
	}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_ = s1.Close()

	s2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	// Columns still there: a SELECT touching them must succeed.
	var stem, dir2 sql.NullString
	if err := s2.DB().QueryRowContext(ctx,
		`SELECT source_stem, source_dir FROM sync_log WHERE source_path = ?`, "x/y.jpg",
	).Scan(&stem, &dir2); err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if stem.String != "x/y" || dir2.String != "x" {
		t.Fatalf("after reopen got stem=%v dir=%v, want x/y / x", stem, dir2)
	}

	// LookupByStem still works.
	got, err := s2.LookupByStem(ctx, "ch", "x/y", "x")
	if err != nil {
		t.Fatalf("LookupByStem after reopen: %v", err)
	}
	if got == nil || got.DestID != "rec-1" {
		t.Fatalf("LookupByStem after reopen = %+v, want rec-1", got)
	}
}

// TestMigrationFromLegacyDB simulates upgrading a database created
// before source_stem / source_dir existed: it drops the columns
// after first Open, reopens, and confirms ensureSchema re-adds them
// via the ALTER TABLE fallback. Also confirms the index gets
// (re-)created so LookupByStem works.
func TestMigrationFromLegacyDB(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	// Drop the new columns and index to simulate a pre-feature DB.
	// SQLite added DROP COLUMN in 3.35; modernc.org/sqlite is current.
	for _, stmt := range []string{
		`DROP INDEX IF EXISTS idx_sync_log_stem`,
		`ALTER TABLE sync_log DROP COLUMN source_stem`,
		`ALTER TABLE sync_log DROP COLUMN source_dir`,
	} {
		if _, err := s1.DB().ExecContext(ctx, stmt); err != nil {
			t.Fatalf("simulate legacy (%s): %v", stmt, err)
		}
	}
	// Insert a legacy-style row before reopen.
	if _, err := s1.DB().ExecContext(ctx, `
        INSERT INTO sync_log
            (ts, channel_name, source_connector, source_path,
             source_version, dest_id, kind)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		time.Now().UTC().Format(syncLogTSLayout),
		"ch", "src", "legacy/file.bin", "v1", "rec-legacy", "create",
	); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	_ = s1.Close()

	// Reopen; ensureSchema must add the columns + index back.
	s2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen on legacy DB: %v", err)
	}
	defer s2.Close()

	// Insert a new row through InsertSyncLog — proves the columns
	// exist now.
	if err := s2.InsertSyncLog(ctx, SyncLogEntry{
		ChannelName: "ch", SourceConnector: "src", SourcePath: "fresh/x.jpg",
		SourceVersion: "v1", DestID: "rec-fresh", Kind: SyncCreate,
	}); err != nil {
		t.Fatalf("InsertSyncLog after migration: %v", err)
	}
	got, err := s2.LookupByStem(ctx, "ch", "fresh/x", "fresh")
	if err != nil {
		t.Fatalf("LookupByStem after migration: %v", err)
	}
	if got == nil || got.DestID != "rec-fresh" {
		t.Fatalf("LookupByStem after migration = %+v, want rec-fresh", got)
	}

	// And the legacy row is invisible to LookupByStem (NULL columns).
	got, err = s2.LookupByStem(ctx, "ch", "legacy/file", "legacy")
	if err != nil {
		t.Fatalf("LookupByStem legacy: %v", err)
	}
	if got != nil {
		t.Fatalf("legacy row should be invisible to LookupByStem, got %+v", got)
	}
}

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
