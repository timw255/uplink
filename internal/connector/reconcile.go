package connector

import (
	"context"
	"time"
)

// StateEntry is the per-path record connectors persist so they can diff
// and synthesize events on the next scan or reconcile. It is the type
// shared between the connector layer and the durable store.
type StateEntry struct {
	Path     string
	Size     int64
	ModTime  time.Time
	Hash     string
	Metadata []byte
}

// StateStore is the persistence contract Reconcile (and poll-based
// EventSources) use to remember what they last saw. Backed by the
// `connector_state` SQLite table — one row per (scope, path) where
// scope is either a connector name or a connector#watcher key.
type StateStore interface {
	// LoadState returns the previously-saved listing for a scope.
	// Legacy convenience for callers that diff the full set in memory.
	LoadState(ctx context.Context, scope string) (map[string]StateEntry, error)

	// SaveState atomically replaces the listing for a scope. Legacy
	// convenience matching LoadState.
	SaveState(ctx context.Context, scope string, state map[string]StateEntry) error

	// --- streaming-scan API (P3.6) ------------------------------------

	// NextGeneration bumps the scope's generation counter and returns
	// the new value. Streaming scans tag every observed entry with
	// this generation; the post-scan sweep treats anything still at a
	// lower generation as a deletion.
	NextGeneration(ctx context.Context, scope string) (int64, error)

	// LoadStateFor bulk-loads existing rows for a set of paths.
	LoadStateFor(ctx context.Context, scope string, paths []string) (map[string]StateEntry, error)

	// ApplyStateDelta upserts the given entries with the supplied
	// generation. Streaming scans call this per batch.
	ApplyStateDelta(ctx context.Context, scope string, upserts []StateEntry, generation int64) error

	// SweepStateBelowGeneration deletes and returns the paths of rows
	// whose generation is below the supplied value. Called once at
	// the end of a scan to surface deletions.
	SweepStateBelowGeneration(ctx context.Context, scope string, generation int64) ([]string, error)
}

// ReconcileProgress is reported during a long-running Reconcile so the
// caller can surface throughput, current file, and error count without
// blocking the scan.
type ReconcileProgress struct {
	// Connector is the instance name being reconciled.
	Connector string

	// Scanned is the number of entries enumerated so far.
	Scanned int

	// Processed is the number of entries the connector has diffed
	// against last-known state.
	Processed int

	// Current is the path the connector is working on right now.
	Current string

	// Bytes is the cumulative byte count of processed entries (if
	// the connector tracks size).
	Bytes int64

	// Errors is the count of per-entry errors so far.
	Errors int
}

// ReconcileResult is the terminal summary of a Reconcile call.
type ReconcileResult struct {
	// Connector is the instance name that was reconciled.
	Connector string

	// Total is the number of entries enumerated.
	Total int

	// New / Modified / Deleted / Unchanged classify each entry vs.
	// the last-known state held in the store. A connector that does
	// not track last-known state may report Total only and leave the
	// detailed counts at zero.
	New       int
	Modified  int
	Deleted   int
	Unchanged int

	// Errors is the count of per-entry errors observed.
	Errors int

	// Duration is wall-clock time for the scan.
	Duration time.Duration
}

// ProgressFunc receives reconcile progress notifications. Implementations
// MUST be non-blocking and safe to call from any goroutine.
type ProgressFunc func(ReconcileProgress)

// The Connector interface includes a Reconcile method (see connector.go).
// Connectors that have nothing meaningful to scan — destination-only
// adapters, for instance — return ErrUnsupported. Engine code branches
// on errors.Is(err, ErrUnsupported) and falls through.
