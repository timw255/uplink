package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// ArchiveOlderThan streams sync_log rows older than threshold as NDJSON
// to w, then deletes them from the table. Done in chunks of 1000 inside
// per-chunk transactions so the daemon's concurrent inserts queue
// briefly but never fail.
//
// Returns the number of rows archived.
func (s *Store) ArchiveOlderThan(ctx context.Context, threshold time.Time, w io.Writer) (int, error) {
	const chunkSize = 1000
	// Match InsertSyncLog's fixed-width layout so the lex compare in
	// `WHERE ts < ?` matches actual time order. See syncLogTSLayout.
	thresholdStr := threshold.UTC().Format(syncLogTSLayout)
	enc := json.NewEncoder(w)
	total := 0

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}

		n, err := s.archiveChunk(ctx, thresholdStr, chunkSize, enc)
		if err != nil {
			return total, err
		}
		total += n
		if n < chunkSize {
			return total, nil
		}
	}
}

func (s *Store) archiveChunk(ctx context.Context, threshold string, limit int, enc *json.Encoder) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: archive begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
        SELECT id, ts, channel_name, source_connector, source_path,
               source_version, dest_id, kind, file_size, file_hash
        FROM sync_log
        WHERE ts < ?
        ORDER BY id
        LIMIT ?`, threshold, limit)
	if err != nil {
		return 0, fmt.Errorf("store: archive select: %w", err)
	}

	ids := make([]int64, 0, limit)
	for rows.Next() {
		e, err := scanSyncLog(rows.Scan)
		if err != nil {
			_ = rows.Close()
			return 0, err
		}
		if e == nil {
			continue
		}
		ids = append(ids, e.ID)
		row := archiveRow{
			ID:              e.ID,
			TS:              e.TS,
			Channel:         e.ChannelName,
			SourceConnector: e.SourceConnector,
			SourcePath:      e.SourcePath,
			SourceVersion:   e.SourceVersion,
			DestID:          e.DestID,
			Kind:            string(e.Kind),
		}
		if e.FileSize.Valid {
			v := e.FileSize.Int64
			row.FileSize = &v
		}
		if e.FileHash.Valid {
			v := e.FileHash.String
			row.FileHash = &v
		}
		if err := enc.Encode(row); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("store: archive encode: %w", err)
		}
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	// Bulk DELETE the rows we just exported. ROWID IN (?, ?, ...).
	placeholders := make([]byte, 0, len(ids)*2)
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id)
	}
	delQuery := "DELETE FROM sync_log WHERE id IN (" + string(placeholders) + ")"
	if _, err := tx.ExecContext(ctx, delQuery, args...); err != nil {
		return 0, fmt.Errorf("store: archive delete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: archive commit: %w", err)
	}
	return len(ids), nil
}

// archiveRow is the on-disk shape of an archived sync_log row.
type archiveRow struct {
	ID              int64     `json:"id"`
	TS              time.Time `json:"ts"`
	Channel         string    `json:"channel"`
	SourceConnector string    `json:"source_connector"`
	SourcePath      string    `json:"source_path"`
	SourceVersion   string    `json:"source_version"`
	DestID          string    `json:"dest_id"`
	Kind            string    `json:"kind"`
	FileSize        *int64    `json:"file_size,omitempty"`
	FileHash        *string   `json:"file_hash,omitempty"`
}
