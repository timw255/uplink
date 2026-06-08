// Package connector defines the core abstractions every source and destination
// in Uplink implements. The same interface is used by every backend —
// local filesystem, S3, Azure Blob, Backblaze B2, and Aprimo.
//
// Path semantics are opaque: a "path" is a connector-specific string. Aprimo
// record IDs and S3 prefixes share no semantics, and no POSIX hierarchy is
// imposed at this layer.
//
// Metadata is a free-form map. There is no canonical schema here; channel
// transforms are responsible for mapping fields between connector vocabularies.
package connector

import (
	"context"
	"io"
	"time"
)

// Entry is the universal file abstraction. It is what flows across the
// boundary between connectors.
type Entry struct {
	// Path is connector-specific and opaque. It must uniquely identify the
	// entry within the connector. Treat it as a string; do not parse it.
	Path string

	// Size is the entry size in bytes if known. -1 if unknown.
	Size int64

	// ModTime is the last-modified time if the connector tracks it.
	ModTime time.Time

	// Hash is an optional content hash. Format is connector-specific
	// (e.g. "sha256:...", "md5:...", "etag:..."). Empty if unavailable.
	Hash string

	// Metadata carries connector-specific fields without normalization.
	// Transforms map between vocabularies; the core does not.
	Metadata map[string]any
}

// EventKind enumerates the change events a connector can emit.
type EventKind string

const (
	EventCreate EventKind = "OnCreate"
	EventUpdate EventKind = "OnUpdate"
	EventDelete EventKind = "OnDelete"
)

// Event represents a change observed on a source connector. Poll-based
// connectors synthesize Events by diffing the current backend listing
// against last-known state.
type Event struct {
	// Connector is the connector instance name (matches config).
	Connector string

	// Kind is the change kind.
	Kind EventKind

	// Entry is the post-change entry. For EventDelete, only Path is
	// guaranteed to be populated.
	Entry Entry

	// Observed is when the connector detected the event (not when it
	// happened upstream, which may be unknown).
	Observed time.Time
}

// Connector is the universal source/destination contract. Implementations
// may support only a subset of operations; unsupported calls should return
// ErrUnsupported.
type Connector interface {
	// Name returns the configured instance name.
	Name() string

	// Init prepares the connector (auth handshakes, client construction,
	// poll-state warmup). It must be safe to call once at startup.
	Init(ctx context.Context) error

	// Close releases any held resources. Idempotent.
	Close() error

	// List enumerates entries under the given opaque prefix. The prefix
	// semantics are connector-specific; "" means "the connector's root".
	List(ctx context.Context, prefix string) ([]Entry, error)

	// Stat returns metadata for a single entry without reading bytes.
	Stat(ctx context.Context, path string) (Entry, error)

	// Read opens an entry for reading. The caller closes the returned
	// reader. Implementations should support context cancellation.
	Read(ctx context.Context, path string) (io.ReadCloser, error)

	// OpenRange returns a reader over bytes [start, start+length) of the
	// entry at path. Implementations typically issue a ranged GET against
	// their backend (S3 Range header, Azure range option, B2 range
	// header, local file seek+limit). Returns ErrUnsupported if the
	// connector cannot serve ranged reads — SegmentSourceFor falls back
	// to a single full Read when that happens.
	OpenRange(ctx context.Context, path string, start, length int64) (io.ReadCloser, error)

	// Write creates or replaces an entry. body is a SegmentSource so
	// destinations that upload in segments (Aprimo) can pull byte
	// ranges in parallel without local staging. Trivial destinations
	// can just `io.Copy(dest, body.Open(ctx, 0, body.Size()))`. The
	// returned Entry reflects the final state on the destination
	// (final path, server-assigned id, ...).
	Write(ctx context.Context, path string, body SegmentSource, meta map[string]any) (Entry, error)

	// Delete removes an entry. No-op or error semantics for missing
	// entries are connector-specific.
	Delete(ctx context.Context, path string) error

	// Move renames or relocates an entry. May return ErrUnsupported for
	// connectors with no native move (callers can fall back to copy+delete).
	Move(ctx context.Context, from, to string) error

	// Reconcile walks the connector's full state, classifies each entry
	// against last-known state in `state`, persists the new picture, and
	// returns a tally. Connectors that have nothing to scan (destination-
	// only adapters, for example) return ErrUnsupported.
	//
	// onProgress may be nil; if set, the connector must call it
	// periodically (not on every entry) so the caller can surface
	// throughput without overwhelming the channel.
	Reconcile(ctx context.Context, state StateStore, onProgress ProgressFunc) (ReconcileResult, error)

	// Walk enumerates entries under prefix, calling yield for each one.
	// Implementations MUST stream — they MUST NOT buffer the full
	// result set. yield's error halts the walk; ctx cancellation does
	// the same. Empty prefix walks the entire connector root.
	//
	// Walk replaces the in-memory List-then-diff loop the scan path
	// used to do; it's what makes memory cost during a scan bounded
	// by batch size rather than corpus size. List is still present
	// for callers that genuinely want one directory's worth of
	// entries (the companion presync sweep, the post-create sweep).
	Walk(ctx context.Context, prefix string, yield func(Entry) error) error
}

// EventSource is implemented by connectors that can emit change events.
// In Uplink every source is poll-based: the connector runs an internal
// loop, lists the backend, diffs against last-known state in the store,
// and synthesizes Events for the differences.
type EventSource interface {
	// Subscribe begins event delivery. The connector calls handler.Handle
	// for each event until ctx is cancelled. Subscribe is expected to
	// block until ctx is done; callers run it in a goroutine.
	Subscribe(ctx context.Context, handler EventHandler) error
}

// EventHandler receives events from an EventSource. The engine implements
// this and turns each event into a channel-matched job.
type EventHandler interface {
	Handle(ctx context.Context, e Event) error
}

// HandlerFunc adapts a plain function to EventHandler.
type HandlerFunc func(ctx context.Context, e Event) error

// Handle implements EventHandler.
func (f HandlerFunc) Handle(ctx context.Context, e Event) error { return f(ctx, e) }

// EventBatchHandler is the optional interface a handler may implement
// to receive a whole poll cycle's events at once. Poll-based
// EventSources should check for this and prefer HandleBatch when
// available — it lets the engine do bulk sync_log lookups (one query
// per poll, not one per event) and bulk job-file writes.
//
// All events in a single HandleBatch call MUST share the same source
// connector name (passed as `sourceConnector`).
type EventBatchHandler interface {
	HandleBatch(ctx context.Context, sourceConnector string, events []Event) error
}

// EventSourceFactory is the optional interface a Connector implements
// to expose itself as an EventSource. The Pool checks for this after
// Init and stores the returned EventSource for the engine to run.
// Implementations typically wrap their poll loop and use `state` as
// the per-connector diff store.
type EventSourceFactory interface {
	NewEventSource(state StateStore) EventSource
}

// MetadataWriter is the optional interface a destination implements to
// support metadata-only updates against an existing record. Used by
// the companion-job worker to PATCH fields on a parent asset's Aprimo
// record without re-uploading bytes — there's nothing new to upload,
// only metadata derived from a companion file the script just read.
//
// Implementations:
//
//   - recordID identifies the destination record to PATCH. For Aprimo,
//     this is the Aprimo record id stored on the parent's sync_log row.
//   - meta carries the same `dest_fields` (or destination-specific
//     equivalent) the engine populates for regular Write calls; the
//     destination resolves and PATCHes them. Other meta keys are
//     ignored.
//
// Destinations that don't support metadata-only writes don't implement
// this interface; the engine treats that as a configuration error if a
// companion job ever lands against such a destination.
type MetadataWriter interface {
	WriteMetadata(ctx context.Context, recordID string, meta map[string]any) error
}
