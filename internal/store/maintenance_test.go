package store

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestArchiveOlderThanRemovesRowsAndStreamsJSONL(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Seed: 5 rows, the first 3 carrying old timestamps, the last 2
	// freshly inserted.
	oldTS := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	for i, ch := range []string{"a", "b", "c"} {
		_, err := s.DB().ExecContext(ctx, `
            INSERT INTO sync_log
                (ts, channel_name, source_connector, source_path,
                 source_version, dest_id, kind, file_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			oldTS, ch, "fs-in", "old"+ch, "v", "rec-old-"+ch, "create", int64(100+i),
		)
		if err != nil {
			t.Fatalf("seed old %s: %v", ch, err)
		}
	}
	for _, ch := range []string{"d", "e"} {
		if err := s.InsertSyncLog(ctx, SyncLogEntry{
			ChannelName:     ch,
			SourceConnector: "fs-in",
			SourcePath:      "new" + ch,
			SourceVersion:   "v",
			DestID:          "rec-new-" + ch,
			Kind:            SyncCreate,
		}); err != nil {
			t.Fatalf("seed new %s: %v", ch, err)
		}
	}

	threshold := time.Now().UTC().Add(-1 * time.Hour)
	var buf bytes.Buffer
	count, err := s.ArchiveOlderThan(ctx, threshold, &buf)
	if err != nil {
		t.Fatalf("ArchiveOlderThan: %v", err)
	}
	if count != 3 {
		t.Fatalf("archived %d rows, want 3", count)
	}

	// JSONL output should have exactly 3 lines, all matching the old rows.
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 jsonl lines, got %d:\n%s", len(lines), buf.String())
	}
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("bad jsonl line %q: %v", line, err)
		}
		if !strings.HasPrefix(row["dest_id"].(string), "rec-old-") {
			t.Fatalf("unexpected row in archive: %+v", row)
		}
	}

	// SQLite should have exactly the 2 new rows left.
	var remaining int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&remaining); err != nil {
		t.Fatalf("count sync_log: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("expected 2 rows left after archive, got %d", remaining)
	}
}

func TestArchiveOlderThanEmptyIsNoop(t *testing.T) {
	s := openTestStore(t)
	var buf bytes.Buffer
	count, err := s.ArchiveOlderThan(context.Background(), time.Now(), &buf)
	if err != nil {
		t.Fatalf("ArchiveOlderThan: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows archived, got %d", count)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer, got %q", buf.String())
	}
}
