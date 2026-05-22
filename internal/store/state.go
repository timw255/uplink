package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// connector_state is the per-(scope, path) row each source connector
// diffs against. State storage moved from one JSON blob per connector
// to a SQLite table in this iteration so polling cost can be O(Δ)
// instead of O(N). See internal/store/store.go for the schema.

// LoadState returns the previously-saved listing for a scope. A scope
// is typically a connector name; nested watchers (see
// internal/connector/watcher.go) append `#<prefix>` to partition state.
//
// Missing-scope returns an empty map and no error — treated as a
// fresh scope.
//
// Implements connector.StateStore.
func (s *Store) LoadState(ctx context.Context, scope string) (map[string]connector.StateEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT path, size, mtime, hash, metadata FROM connector_state WHERE scope = ?`, scope)
	if err != nil {
		return nil, fmt.Errorf("store: load state %q: %w", scope, err)
	}
	defer rows.Close()

	out := make(map[string]connector.StateEntry)
	for rows.Next() {
		var (
			path     string
			size     int64
			mtimeStr string
			hash     sql.NullString
			metadata []byte
		)
		if err := rows.Scan(&path, &size, &mtimeStr, &hash, &metadata); err != nil {
			return nil, fmt.Errorf("store: scan state %q: %w", scope, err)
		}
		t, err := time.Parse(time.RFC3339Nano, mtimeStr)
		if err != nil {
			// Old data or a clock-skewed write; treat as zero rather
			// than failing the scan. The next save will overwrite.
			t = time.Time{}
		}
		entry := connector.StateEntry{
			Path:    path,
			Size:    size,
			ModTime: t,
			Hash:    hash.String,
		}
		if len(metadata) > 0 {
			entry.Metadata = metadata
		}
		out[path] = entry
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate state %q: %w", scope, err)
	}
	return out, nil
}

// SaveState atomically replaces the listing for a scope. It bumps the
// scope's generation, upserts every entry in `state` with the new
// generation, then sweeps everything below the new generation as
// deletions. One transaction; one fsync (in WAL mode).
//
// This is the transitional API — kept compatible with the prior JSON-
// blob signature so the existing scan() implementations in each source
// connector still work. Streaming Walk (P3.6) introduces
// ApplyStateDelta + SweepStateBelowGeneration for the batched diff
// path that doesn't require holding the full state in memory.
//
// Implements connector.StateStore.
func (s *Store) SaveState(ctx context.Context, scope string, state map[string]connector.StateEntry) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin save state %q: %w", scope, err)
	}
	defer func() { _ = tx.Rollback() }()

	gen, err := nextGenerationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if err := upsertStateTx(ctx, tx, scope, state, gen); err != nil {
		return err
	}
	if err := sweepBelowGenerationTx(ctx, tx, scope, gen); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit save state %q: %w", scope, err)
	}
	return nil
}

// --- New delta API (used by streaming Walk in P3.6) ---------------------

// NextGeneration bumps the scope's generation counter and returns the
// new value. Callers tag every observed entry with this generation
// during their scan; anything still at a lower generation afterwards
// is a deletion.
func (s *Store) NextGeneration(ctx context.Context, scope string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: begin next-gen %q: %w", scope, err)
	}
	defer func() { _ = tx.Rollback() }()
	gen, err := nextGenerationTx(ctx, tx, scope)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: commit next-gen %q: %w", scope, err)
	}
	return gen, nil
}

// LoadStateFor bulk-loads the existing rows for a set of paths.
// Returned map keys are paths; absent paths are not in the map.
func (s *Store) LoadStateFor(ctx context.Context, scope string, paths []string) (map[string]connector.StateEntry, error) {
	if len(paths) == 0 {
		return map[string]connector.StateEntry{}, nil
	}
	// SQLite has a default 999 host-parameter limit; chunk if needed.
	const chunk = 500
	out := make(map[string]connector.StateEntry, len(paths))
	for i := 0; i < len(paths); i += chunk {
		end := i + chunk
		if end > len(paths) {
			end = len(paths)
		}
		group := paths[i:end]
		q := `SELECT path, size, mtime, hash, metadata FROM connector_state WHERE scope = ? AND path IN (?` +
			repeatComma(len(group)-1) + `)`
		args := make([]any, 0, 1+len(group))
		args = append(args, scope)
		for _, p := range group {
			args = append(args, p)
		}
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("store: load-for state %q: %w", scope, err)
		}
		for rows.Next() {
			var (
				path     string
				size     int64
				mtimeStr string
				hash     sql.NullString
				metadata []byte
			)
			if err := rows.Scan(&path, &size, &mtimeStr, &hash, &metadata); err != nil {
				rows.Close()
				return nil, fmt.Errorf("store: scan state %q: %w", scope, err)
			}
			t, _ := time.Parse(time.RFC3339Nano, mtimeStr)
			out[path] = connector.StateEntry{
				Path: path, Size: size, ModTime: t, Hash: hash.String, Metadata: metadata,
			}
		}
		rows.Close()
	}
	return out, nil
}

// ApplyStateDelta upserts the given entries with the supplied
// generation. Used by streaming scans: tag every observed entry with
// the current scan's generation, then call SweepStateBelowGeneration
// at the end to handle deletions.
func (s *Store) ApplyStateDelta(ctx context.Context, scope string, upserts []connector.StateEntry, generation int64) error {
	if len(upserts) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin apply-delta %q: %w", scope, err)
	}
	defer func() { _ = tx.Rollback() }()

	m := make(map[string]connector.StateEntry, len(upserts))
	for _, e := range upserts {
		m[e.Path] = e
	}
	if err := upsertStateTx(ctx, tx, scope, m, generation); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit apply-delta %q: %w", scope, err)
	}
	return nil
}

// SweepStateBelowGeneration deletes (and returns the paths of) all
// rows for the scope whose generation is below the supplied value.
// Call at the end of a scan to surface deletions.
func (s *Store) SweepStateBelowGeneration(ctx context.Context, scope string, generation int64) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin sweep %q: %w", scope, err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT path FROM connector_state WHERE scope = ? AND generation < ?`,
		scope, generation)
	if err != nil {
		return nil, fmt.Errorf("store: sweep-list %q: %w", scope, err)
	}
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: scan sweep %q: %w", scope, err)
		}
		paths = append(paths, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate sweep %q: %w", scope, err)
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM connector_state WHERE scope = ? AND generation < ?`,
		scope, generation); err != nil {
		return nil, fmt.Errorf("store: sweep-delete %q: %w", scope, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit sweep %q: %w", scope, err)
	}
	return paths, nil
}

// --- internal transaction helpers ---------------------------------------

// nextGenerationTx atomically bumps the scope's generation counter.
// Uses INSERT ... ON CONFLICT to handle first-call init, and
// UPDATE ... RETURNING to retrieve the new value in one round-trip.
func nextGenerationTx(ctx context.Context, tx *sql.Tx, scope string) (int64, error) {
	// First write: INSERT or UPDATE. modernc.org/sqlite supports
	// `INSERT ... ON CONFLICT DO UPDATE SET ... RETURNING`.
	var gen int64
	err := tx.QueryRowContext(ctx, `
        INSERT INTO state_generations (scope, generation) VALUES (?, 1)
        ON CONFLICT(scope) DO UPDATE SET generation = generation + 1
        RETURNING generation`, scope).Scan(&gen)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("store: next-gen %q: no row returned", scope)
		}
		return 0, fmt.Errorf("store: next-gen %q: %w", scope, err)
	}
	return gen, nil
}

// upsertStateTx writes every entry in state with the given generation.
// Existing rows have their size/mtime/hash/metadata/generation
// overwritten; new rows are inserted.
func upsertStateTx(ctx context.Context, tx *sql.Tx, scope string, state map[string]connector.StateEntry, generation int64) error {
	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO connector_state (scope, path, size, mtime, hash, metadata, generation)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scope, path) DO UPDATE SET
            size = excluded.size,
            mtime = excluded.mtime,
            hash = excluded.hash,
            metadata = excluded.metadata,
            generation = excluded.generation`)
	if err != nil {
		return fmt.Errorf("store: prepare upsert state %q: %w", scope, err)
	}
	defer stmt.Close()

	for _, e := range state {
		mtime := e.ModTime.UTC().Format(time.RFC3339Nano)
		var hash any
		if e.Hash != "" {
			hash = e.Hash
		}
		var metadata any
		if len(e.Metadata) > 0 {
			metadata = e.Metadata
		}
		if _, err := stmt.ExecContext(ctx, scope, e.Path, e.Size, mtime, hash, metadata, generation); err != nil {
			return fmt.Errorf("store: upsert state %q path %q: %w", scope, e.Path, err)
		}
	}
	return nil
}

// sweepBelowGenerationTx is the in-transaction equivalent of
// SweepStateBelowGeneration. It does not return the deleted paths —
// callers using SaveState don't need them (the diff happened in the
// in-memory map).
func sweepBelowGenerationTx(ctx context.Context, tx *sql.Tx, scope string, generation int64) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM connector_state WHERE scope = ? AND generation < ?`,
		scope, generation); err != nil {
		return fmt.Errorf("store: sweep state %q: %w", scope, err)
	}
	return nil
}

// repeatComma returns ",?" repeated n times — for building IN clauses
// with a variable number of placeholders.
func repeatComma(n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, 0, n*2)
	for i := 0; i < n; i++ {
		out = append(out, ',', '?')
	}
	return string(out)
}
