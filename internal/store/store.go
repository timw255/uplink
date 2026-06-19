// Package store is the durable layer for Uplink.
//
// Storage is split by role:
//
//   - SQLite (uplink.db) holds the persisted system-of-record: every
//     successful sync gets one row in `sync_log` ((channel, source_path)
//     → dest_id, version, kind, size, hash). Used both as the
//     create-vs-update lookup table at dispatch time and as the audit
//     trail for diagnostics. ONE table, schema set up idempotently via
//     CREATE TABLE IF NOT EXISTS on every Open — no migrations.
//
//   - Files under data/ hold work-in-progress state: the job queue
//     (data/jobs/{pending,running,failed}/), connector listings
//     (data/state/<name>.json), and in-flight upload markers
//     (data/uploads/<job_id>.session.json).
//
// SQLite tells us what completed; the file tree tells us what was in
// flight. Together they let us resume exactly where we left off.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite" // pure-Go driver, registers "sqlite"
)

// Store is the handle every other subsystem uses to persist state.
type Store struct {
	db      *sql.DB
	dbPath  string
	dataDir string

	// claimMu serializes ClaimNextJob across worker goroutines in the
	// same process. The on-disk atomic-rename remains the durable
	// claim primitive, but the mutex eliminates an in-process race
	// window we've seen on Windows where two goroutines that both
	// ReadDir before either rename can each see the same pending file.
	claimMu sync.Mutex
}

// Open opens (or creates) the SQLite database at dataDir/uplink.db and
// ensures the file-tree subdirectories exist. The sync_log schema is
// created idempotently — no migration framework.
//
// Callers must Close the returned Store.
func Open(ctx context.Context, dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("store: empty dataDir")
	}
	if err := ensureDirs(dataDir); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(dataDir, "uplink.db")
	dsn := "file:" + dbPath + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite: %w", err)
	}

	// modernc.org/sqlite is safe with multiple connections, but WAL
	// still serializes writes. Cap connections to keep contention
	// predictable.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)

	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA auto_vacuum = INCREMENTAL",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("store: %s: %w", pragma, err)
		}
	}

	s := &Store{db: db, dbPath: dbPath, dataDir: dataDir}
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.recoverRunningJobs(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: recover running jobs: %w", err)
	}
	return s, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Path returns the on-disk path of the SQLite database file.
func (s *Store) Path() string { return s.dbPath }

// DataDir returns the root data directory; subsystem helpers derive
// jobs/state/uploads paths from it.
func (s *Store) DataDir() string { return s.dataDir }

// DB exposes the raw *sql.DB for tests and advanced callers. Production
// code should prefer the typed methods on Store.
func (s *Store) DB() *sql.DB { return s.db }

// ensureSchema creates the sync_log table and its lookup index. Both
// statements are idempotent via IF NOT EXISTS; running them on every
// startup means we never need a separate migration step. If a column
// ever needs to be added, it goes here as an additional idempotent
// ALTER TABLE ... ADD COLUMN next to the create — strictly additive.
func (s *Store) ensureSchema(ctx context.Context) error {
	stmts := []string{
		// Fresh-DB shape: source_stem and source_dir are included in the
		// CREATE so brand-new databases get them at creation time. For
		// pre-existing databases the ALTER TABLE fallbacks below add the
		// columns in place. Rows inserted before this feature shipped
		// have NULL in both — that's expected; the companion-file
		// lookup naturally won't match legacy rows, which is fine since
		// they pre-date companions.
		`CREATE TABLE IF NOT EXISTS sync_log (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            ts                TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            channel_name      TEXT NOT NULL,
            source_connector  TEXT NOT NULL,
            source_path       TEXT NOT NULL,
            source_version    TEXT NOT NULL,
            dest_id  TEXT NOT NULL,
            kind              TEXT NOT NULL,
            file_size         INTEGER,
            file_hash         TEXT,
            source_stem       TEXT,
            source_dir        TEXT
        )`,
		`CREATE INDEX IF NOT EXISTS idx_sync_log_lookup
            ON sync_log(channel_name, source_path, id DESC)`,

		// connector_state holds the per-(scope, path) last-known view
		// each source connector diffs against. scope is the connector
		// name optionally suffixed with a watcher prefix (see
		// internal/connector/watcher.go) — e.g., "fs-in" or
		// "fs-in#images/hot". generation is bumped on every scan so a
		// SELECT WHERE generation < N reveals deletions.
		`CREATE TABLE IF NOT EXISTS connector_state (
            scope      TEXT NOT NULL,
            path       TEXT NOT NULL,
            size       INTEGER NOT NULL,
            mtime      TEXT NOT NULL,
            hash       TEXT,
            metadata   BLOB,
            generation INTEGER NOT NULL,
            PRIMARY KEY (scope, path)
        ) WITHOUT ROWID`,
		`CREATE INDEX IF NOT EXISTS idx_connector_state_scope
            ON connector_state(scope, generation)`,

		// state_generations is a tiny counter table: one row per
		// scope. NextGeneration UPDATE-RETURNINGs from here.
		`CREATE TABLE IF NOT EXISTS state_generations (
            scope      TEXT PRIMARY KEY,
            generation INTEGER NOT NULL
        ) WITHOUT ROWID`,

		// jobs is the persisted work queue. Atomic-rename across
		// pending/running/failed filesystem directories is gone —
		// claim is now a single UPDATE...RETURNING (modernc.org/sqlite
		// supports this), so the cross-process race window the old
		// implementation needed claimMu to guard collapses to SQLite's
		// transaction isolation. At scale this also avoids hitting the
		// "N files in one directory" cliff (NTFS / ext4 misbehave past
		// tens of thousands).
		`CREATE TABLE IF NOT EXISTS jobs (
            id                TEXT PRIMARY KEY,
            channel_name      TEXT NOT NULL,
            kind              TEXT NOT NULL,
            source_connector  TEXT NOT NULL,
            source_path       TEXT NOT NULL,
            source_version    TEXT,
            dest_id  TEXT,
            payload           BLOB,
            status            TEXT NOT NULL,
            attempts          INTEGER NOT NULL DEFAULT 0,
            next_run_at       TEXT NOT NULL,
            created_at        TEXT NOT NULL,
            last_error        TEXT
        )`,
		// The claim path's hot index. Partial index on pending+failed-
		// retry rows would be ideal, but SQLite's partial-index parser
		// is fussy; this non-partial index is plenty fast given table
		// size in steady state.
		`CREATE INDEX IF NOT EXISTS idx_jobs_claim
            ON jobs(status, next_run_at)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("store: ensure schema: %w", err)
		}
	}

	// Additive ALTERs for databases created before source_stem /
	// source_dir existed. SQLite's ADD COLUMN is not natively
	// idempotent (duplicate column name is an error), so we gate on
	// PRAGMA table_info. Fresh DBs already have the columns from the
	// CREATE TABLE above and will no-op here.
	if err := s.addSyncLogColumnIfMissing(ctx, "source_stem"); err != nil {
		return err
	}
	if err := s.addSyncLogColumnIfMissing(ctx, "source_dir"); err != nil {
		return err
	}
	// Rename aprimo_record_id → dest_id on databases created before
	// the destination-agnostic field rename. SQLite's RENAME COLUMN
	// has no IF EXISTS form, so we probe with PRAGMA table_info first
	// and skip when the new name is already there. Fresh DBs hit the
	// skip branch on first boot because dest_id exists from CREATE.
	if err := s.renameLegacyAprimoRecordID(ctx, "sync_log"); err != nil {
		return err
	}
	if err := s.renameLegacyAprimoRecordID(ctx, "jobs"); err != nil {
		return err
	}
	// Composite index powering LookupByStem: the companion-file
	// dispatcher routes events keyed by (channel, basename-stem,
	// directory) to the parent asset's sync_log row. Created after
	// the ALTERs so the columns are guaranteed to exist on stale DBs.
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_sync_log_stem
            ON sync_log(channel_name, source_stem, source_dir, id DESC)`); err != nil {
		return fmt.Errorf("store: ensure stem index: %w", err)
	}
	return nil
}

// addSyncLogColumnIfMissing adds a TEXT column to sync_log only if it
// doesn't already exist. PRAGMA table_info is the idempotency probe;
// SQLite's ALTER TABLE ADD COLUMN itself is not idempotent.
func (s *Store) addSyncLogColumnIfMissing(ctx context.Context, col string) error {
	has, err := s.syncLogHasColumn(ctx, col)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE sync_log ADD COLUMN `+col+` TEXT`); err != nil {
		return fmt.Errorf("store: add column %s: %w", col, err)
	}
	return nil
}

// renameLegacyAprimoRecordID renames the pre-rename `aprimo_record_id`
// column to the destination-agnostic `dest_id` if (and only if) the
// table still has the old column and not the new one. Fresh DBs and
// already-migrated DBs both no-op. The two tables this applies to
// (sync_log, jobs) are passed in by name; column-rename is otherwise
// identical between them.
func (s *Store) renameLegacyAprimoRecordID(ctx context.Context, table string) error {
	hasOld, err := s.tableHasColumn(ctx, table, "aprimo_record_id")
	if err != nil {
		return err
	}
	if !hasOld {
		return nil
	}
	hasNew, err := s.tableHasColumn(ctx, table, "dest_id")
	if err != nil {
		return err
	}
	if hasNew {
		// Should not happen in practice — both columns coexisting is
		// only possible from manual schema tinkering. Refuse rather
		// than drop data.
		return fmt.Errorf("store: %s has both aprimo_record_id and dest_id; manual cleanup required", table)
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE `+table+` RENAME COLUMN aprimo_record_id TO dest_id`); err != nil {
		return fmt.Errorf("store: rename %s.aprimo_record_id: %w", table, err)
	}
	return nil
}

// syncLogHasColumn reports whether sync_log has a column with the
// given name. Uses PRAGMA table_info, which is the standard SQLite
// introspection probe.
func (s *Store) syncLogHasColumn(ctx context.Context, col string) (bool, error) {
	return s.tableHasColumn(ctx, "sync_log", col)
}

// tableHasColumn reports whether the given table has a column with the
// given name. PRAGMA table_info is the standard introspection probe;
// the table name is interpolated (not parameter-bound) because PRAGMA
// statements do not accept SQL parameters — callers MUST pass a
// hardcoded table name, never user input.
func (s *Store) tableHasColumn(ctx context.Context, table, col string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, fmt.Errorf("store: pragma table_info(%s): %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, fmt.Errorf("store: scan pragma: %w", err)
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
}

// ensureDirs creates the file-tree subdirectories the store needs.
// State and jobs moved into SQLite this iteration; only upload markers
// remain on disk (they're file-handle-shaped because crash-resume must
// survive across daemon restarts and the file system gives us atomic
// rename + stable inodes).
func ensureDirs(dataDir string) error {
	dirs := []string{
		filepath.Join(dataDir, "uploads"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o700); err != nil { // holds upload tokens/markers
			return fmt.Errorf("store: create %s: %w", d, err)
		}
	}
	return nil
}
