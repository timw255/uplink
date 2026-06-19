package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// StubSource is a minimal in-memory Connector usable as the source of
// an engine test. Each entry in Files is exposed via List/Stat/Read/
// OpenRange. The destination side (Write/Delete/Move) is unsupported.
type StubSource struct {
	NameStr string
	// Files maps source path to raw bytes. The entry's Hash is set to
	// a deterministic fingerprint of the bytes; tests can override via
	// HashOverride.
	Files map[string][]byte
	// HashOverride, when non-nil, replaces the default content fingerprint
	// for entries that match a key. Useful for "same bytes, different
	// version" or "different bytes, same version" tests.
	HashOverride map[string]string
}

// Name implements connector.Connector.
func (s *StubSource) Name() string { return s.NameStr }

// Init / Close are no-ops.
func (s *StubSource) Init(context.Context) error { return nil }
func (s *StubSource) Close() error               { return nil }

// List returns one entry per file, with deterministic ordering.
func (s *StubSource) List(_ context.Context, _ string) ([]connector.Entry, error) {
	out := make([]connector.Entry, 0, len(s.Files))
	for path, data := range s.Files {
		out = append(out, s.entry(path, data))
	}
	return out, nil
}

// Stat returns metadata for one path.
func (s *StubSource) Stat(_ context.Context, path string) (connector.Entry, error) {
	data, ok := s.Files[path]
	if !ok {
		return connector.Entry{}, connector.ErrNotFound
	}
	return s.entry(path, data), nil
}

// Read returns the entire object.
func (s *StubSource) Read(_ context.Context, path string) (io.ReadCloser, error) {
	data, ok := s.Files[path]
	if !ok {
		return nil, connector.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// OpenRange returns the requested byte range.
func (s *StubSource) OpenRange(_ context.Context, path string, start, length int64) (io.ReadCloser, error) {
	data, ok := s.Files[path]
	if !ok {
		return nil, connector.ErrNotFound
	}
	if start >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := min(start+length, int64(len(data)))
	return io.NopCloser(bytes.NewReader(data[start:end])), nil
}

// Write / Delete / Move are not supported on a source-only stub.
func (s *StubSource) Write(_ context.Context, _ string, _ connector.SegmentSource, _ map[string]any) (connector.Entry, error) {
	return connector.Entry{}, connector.ErrUnsupported
}
func (s *StubSource) Delete(_ context.Context, _ string) error  { return connector.ErrUnsupported }
func (s *StubSource) Move(_ context.Context, _, _ string) error { return connector.ErrUnsupported }

// Reconcile is unused by these tests but the interface requires it.
func (s *StubSource) Reconcile(_ context.Context, _ connector.StateStore, _ connector.ProgressFunc) (connector.ReconcileResult, error) {
	return connector.ReconcileResult{Connector: s.NameStr}, connector.ErrUnsupported
}

// Walk streams entries — yields each file once. The interface
// requires it; engine tests don't exercise streaming-scan paths but
// the signature has to match.
func (s *StubSource) Walk(_ context.Context, _ string, yield func(connector.Entry) error) error {
	for path, data := range s.Files {
		if err := yield(s.entry(path, data)); err != nil {
			return err
		}
	}
	return nil
}

func (s *StubSource) entry(path string, data []byte) connector.Entry {
	hash := fmt.Sprintf("h-%d-%x", len(data), simpleSum(data))
	if s.HashOverride != nil {
		if v, ok := s.HashOverride[path]; ok {
			hash = v
		}
	}
	return connector.Entry{
		Path:    path,
		Size:    int64(len(data)),
		ModTime: time.Now().UTC(),
		Hash:    hash,
	}
}

// simpleSum is a cheap, deterministic byte fingerprint for test
// stability. Not cryptographic.
func simpleSum(b []byte) uint64 {
	var sum uint64 = 1469598103934665603 // FNV-1a 64 offset basis
	for _, c := range b {
		sum ^= uint64(c)
		sum *= 1099511628211 // FNV-1a 64 prime
	}
	return sum
}

// StubDestination is an in-memory Connector usable as the destination
// of an engine test. Write records each call so tests can assert on
// ordering, payloads, and retry behavior.
type StubDestination struct {
	NameStr string

	// FailNTimes, if > 0, makes the first N Write calls fail.
	// Subsequent calls succeed. Used for retry/backoff tests.
	FailNTimes int32

	// ResponseRecordID is the Entry.Path returned on success. Defaults
	// to a generated "rec-" id per call when empty.
	ResponseRecordID string

	mu         sync.Mutex
	failed     atomic.Int32
	writes     []StubWriteCall
	metadataMu sync.Mutex
	metadata   []StubMetadataCall
	idSeq      int
}

// WriteMetadata implements connector.MetadataWriter so the companion
// job path can be exercised against a stub destination. Captures the
// recordID + meta for assertion.
func (d *StubDestination) WriteMetadata(_ context.Context, recordID string, meta map[string]any) error {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadata = append(d.metadata, StubMetadataCall{
		RecordID: recordID,
		Meta:     copyMeta(meta),
	})
	return nil
}

// MetadataWrites returns a snapshot of all recorded WriteMetadata calls.
func (d *StubDestination) MetadataWrites() []StubMetadataCall {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	out := make([]StubMetadataCall, len(d.metadata))
	copy(out, d.metadata)
	return out
}

// StubWriteCall captures one Write invocation for assertion.
type StubWriteCall struct {
	Path       string
	Size       int64
	Meta       map[string]any
	BodyHashed string // first 16 hex of fingerprint over body bytes
}

// StubMetadataCall captures one WriteMetadata invocation for assertion.
// Companion-job tests use this to verify which record received which
// fields without standing up a fake Aprimo server.
type StubMetadataCall struct {
	RecordID string
	Meta     map[string]any
}

// Name implements connector.Connector.
func (d *StubDestination) Name() string { return d.NameStr }

// Init / Close are no-ops.
func (d *StubDestination) Init(context.Context) error { return nil }
func (d *StubDestination) Close() error               { return nil }

// Write records the call. If FailNTimes is positive and we haven't
// hit that count yet, returns a synthetic transient error so the
// engine retries.
func (d *StubDestination) Write(ctx context.Context, p string, body connector.SegmentSource, meta map[string]any) (connector.Entry, error) {
	if cur := d.failed.Add(1); cur <= d.FailNTimes {
		return connector.Entry{}, fmt.Errorf("stub destination: synthetic failure %d/%d", cur, d.FailNTimes)
	}

	// Drain the source so tests can assert on body content.
	var fingerprint string
	if body != nil && body.Size() > 0 {
		rc, err := body.Open(ctx, 0, body.Size())
		if err != nil {
			return connector.Entry{}, fmt.Errorf("stub destination: open source: %w", err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		fingerprint = fmt.Sprintf("%016x", simpleSum(data))
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	call := StubWriteCall{
		Path:       p,
		Size:       body.Size(),
		Meta:       copyMeta(meta),
		BodyHashed: fingerprint,
	}
	d.writes = append(d.writes, call)

	recordID := d.ResponseRecordID
	if recordID == "" {
		d.idSeq++
		recordID = fmt.Sprintf("rec-%s-%d", d.NameStr, d.idSeq)
	}
	return connector.Entry{
		Path: recordID,
		Size: body.Size(),
	}, nil
}

// Writes returns a snapshot of all recorded Write calls.
func (d *StubDestination) Writes() []StubWriteCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]StubWriteCall, len(d.writes))
	copy(out, d.writes)
	return out
}

// Source-side methods are unsupported on a destination-only stub.
func (d *StubDestination) List(_ context.Context, _ string) ([]connector.Entry, error) {
	return nil, connector.ErrUnsupported
}
func (d *StubDestination) Stat(_ context.Context, _ string) (connector.Entry, error) {
	return connector.Entry{}, connector.ErrUnsupported
}
func (d *StubDestination) Read(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, connector.ErrUnsupported
}
func (d *StubDestination) OpenRange(_ context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
	return nil, connector.ErrUnsupported
}
func (d *StubDestination) Delete(_ context.Context, _ string) error  { return connector.ErrUnsupported }
func (d *StubDestination) Move(_ context.Context, _, _ string) error { return connector.ErrUnsupported }
func (d *StubDestination) Reconcile(_ context.Context, _ connector.StateStore, _ connector.ProgressFunc) (connector.ReconcileResult, error) {
	return connector.ReconcileResult{Connector: d.NameStr}, connector.ErrUnsupported
}

// Walk is unsupported on a destination-only stub. Matches the
// production Aprimo destination's behavior.
func (d *StubDestination) Walk(_ context.Context, _ string, _ func(connector.Entry) error) error {
	return connector.ErrUnsupported
}

// stubConnectors is the engine's Connectors interface backed by a
// simple name map. Returned by NewStubConnectors.
type stubConnectors struct {
	byName map[string]connector.Connector
}

// NewStubConnectors builds a Connectors registry from the given
// connectors keyed by their Name().
func NewStubConnectors(conns ...connector.Connector) Connectors {
	m := make(map[string]connector.Connector, len(conns))
	for _, c := range conns {
		m[c.Name()] = c
	}
	return &stubConnectors{byName: m}
}

func (s *stubConnectors) Get(name string) (connector.Connector, bool) {
	c, ok := s.byName[name]
	return c, ok
}

func copyMeta(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	maps.Copy(out, in)
	return out
}
