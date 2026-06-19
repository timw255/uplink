package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

// SyncKind names the operation that produced a sync_log row.
type SyncKind string

const (
	SyncCreate SyncKind = "create"
	SyncUpdate SyncKind = "update"
)

// SyncLogEntry is one row in sync_log. Every successful destination
// Write produces exactly one row.
type SyncLogEntry struct {
	ID              int64
	TS              time.Time
	ChannelName     string
	SourceConnector string
	SourcePath      string
	SourceVersion   string
	DestID          string
	Kind            SyncKind
	FileSize        sql.NullInt64
	FileHash        sql.NullString
}

// syncLogTSLayout is the fixed-width RFC3339-style format used for the
// sync_log.ts column. Fixed-width matters: the table stores ts as TEXT
// and the archive threshold is compared with `WHERE ts < ?` — a
// lexicographic compare. RFC3339Nano strips trailing zeros, producing
// variable-width strings that lex-compare incorrectly across precisions
// (e.g. ".123Z" vs ".123456789Z" sorts wrong because 'Z' > '4'). Always
// rendering nine fractional digits keeps lex order identical to time
// order. parseTime accepts this format via its time.RFC3339Nano entry.
const syncLogTSLayout = "2006-01-02T15:04:05.000000000Z"

// InsertSyncLog appends a row. Called once per successful destination
// Write, after Records.Create (or Records.Update) returns.
func (s *Store) InsertSyncLog(ctx context.Context, e SyncLogEntry) error {
	// Explicit ts in the format archive thresholds use, so lex compare
	// matches time order. The schema DEFAULT (strftime millisecond) is
	// kept as a last-resort fallback for rows inserted out-of-band.
	ts := time.Now().UTC().Format(syncLogTSLayout)
	stem, dir := splitSourcePath(e.SourcePath)
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO sync_log
            (ts, channel_name, source_connector, source_path, source_version,
             dest_id, kind, file_size, file_hash,
             source_stem, source_dir)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ts, e.ChannelName, e.SourceConnector, e.SourcePath, e.SourceVersion,
		e.DestID, string(e.Kind), e.FileSize, e.FileHash,
		stem, dir,
	)
	if err != nil {
		return fmt.Errorf("store: insert sync_log: %w", err)
	}
	return nil
}

// splitSourcePath derives (stem, dir) from a forward-slash source
// path. stem is the full path with its final extension stripped
// (e.g. "photos/sunset.jpg" -> "photos/sunset"); dir is the directory
// portion (e.g. "photos/sunset.jpg" -> "photos", top-level -> "").
//
// Always uses package "path" (forward slashes) — never path/filepath,
// because source connectors normalize to forward-slash paths regardless
// of host OS, and filepath would mangle them on Windows.
//
// Quirks deliberately not papered over:
//   - Hidden files ("/.gitignore") have ext == ".gitignore" per
//     path.Ext, leaving stem == "<dir>/" pre-trim. Operators are
//     unlikely to have hidden files as primary assets; we accept this.
//   - Files with no extension yield stem == srcPath, ext == "".
func splitSourcePath(srcPath string) (stem, dir string) {
	d, _ := path.Split(srcPath)
	dir = strings.TrimSuffix(d, "/")
	ext := path.Ext(srcPath)
	stem = strings.TrimSuffix(srcPath, ext)
	return stem, dir
}

// LookupLatest returns the most recent sync_log row for (channel,
// source_path), or nil if there is none. The DESC index makes this a
// near-constant-time read regardless of table size.
func (s *Store) LookupLatest(ctx context.Context, channel, sourcePath string) (*SyncLogEntry, error) {
	row := s.db.QueryRowContext(ctx, `
        SELECT id, ts, channel_name, source_connector, source_path,
               source_version, dest_id, kind, file_size, file_hash
        FROM sync_log
        WHERE channel_name = ? AND source_path = ?
        ORDER BY id DESC
        LIMIT 1`, channel, sourcePath)
	return scanSyncLog(row.Scan)
}

// LookupByStem returns the latest sync_log row for this channel whose
// (source_stem, source_dir) matches, or nil if no row matches. This
// is the inverse lookup the companion-file dispatcher uses: given a
// sidecar event (e.g. "photos/sunset.xmp"), find the parent asset
// ("photos/sunset.jpg") by comparing on the extension-stripped path.
//
// Legacy rows inserted before this feature shipped have NULL in
// source_stem and source_dir; they naturally don't match this WHERE
// clause, which is correct — they pre-date companions and could
// never be the parent of one.
func (s *Store) LookupByStem(ctx context.Context, channel, stem, dir string) (*SyncLogEntry, error) {
	row := s.db.QueryRowContext(ctx, `
        SELECT id, ts, channel_name, source_connector, source_path,
               source_version, dest_id, kind, file_size, file_hash
        FROM sync_log
        WHERE channel_name = ? AND source_stem = ? AND source_dir = ?
        ORDER BY id DESC
        LIMIT 1`, channel, stem, dir)
	return scanSyncLog(row.Scan)
}

// LookupLatestBatch does one indexed query per chunk of paths and
// returns the latest sync_log row per path. Missing paths are absent
// from the map. The map is keyed by source_path. Chunked at 500 paths
// per IN(...) query so the SQL stays inside SQLite's bound-variable
// limit on every platform.
func (s *Store) LookupLatestBatch(ctx context.Context, channel string, paths []string) (map[string]*SyncLogEntry, error) {
	out := make(map[string]*SyncLogEntry, len(paths))
	const chunk = 500
	for i := 0; i < len(paths); i += chunk {
		end := min(i+chunk, len(paths))
		batch := paths[i:end]
		if err := s.lookupBatchChunk(ctx, channel, batch, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) lookupBatchChunk(ctx context.Context, channel string, paths []string, out map[string]*SyncLogEntry) error {
	if len(paths) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(paths))
	placeholders = placeholders[:len(placeholders)-1]
	args := make([]any, 0, len(paths)+1)
	args = append(args, channel)
	for _, p := range paths {
		args = append(args, p)
	}
	// "Latest per path" via subquery: pick the row with max(id) per
	// source_path scoped to channel, then return all its columns.
	query := `
        SELECT s.id, s.ts, s.channel_name, s.source_connector, s.source_path,
               s.source_version, s.dest_id, s.kind, s.file_size, s.file_hash
        FROM sync_log s
        JOIN (
            SELECT source_path, MAX(id) AS max_id
            FROM sync_log
            WHERE channel_name = ? AND source_path IN (` + placeholders + `)
            GROUP BY source_path
        ) latest ON latest.max_id = s.id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("store: batch lookup sync_log: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		entry, err := scanSyncLog(rows.Scan)
		if err != nil {
			return err
		}
		out[entry.SourcePath] = entry
	}
	return rows.Err()
}

// scanSyncLog reads one row from any scanner shape (Row or Rows).
func scanSyncLog(scan func(...any) error) (*SyncLogEntry, error) {
	var e SyncLogEntry
	var tsStr, kindStr string
	err := scan(
		&e.ID, &tsStr, &e.ChannelName, &e.SourceConnector,
		&e.SourcePath, &e.SourceVersion, &e.DestID, &kindStr,
		&e.FileSize, &e.FileHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: scan sync_log: %w", err)
	}
	e.TS = parseTime(tsStr)
	e.Kind = SyncKind(kindStr)
	return &e, nil
}

// CountByChannel returns a map of channel_name → total row count.
// Used by `uplink status` and similar.
func (s *Store) CountByChannel(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT channel_name, COUNT(*) FROM sync_log GROUP BY channel_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var ch string
		var n int64
		if err := rows.Scan(&ch, &n); err != nil {
			return nil, err
		}
		out[ch] = n
	}
	return out, rows.Err()
}

// parseTime tries the formats we write into TEXT columns.
func parseTime(s string) time.Time {
	for _, layout := range []string{
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
