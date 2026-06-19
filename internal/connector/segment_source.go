package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// SegmentSource is the read side of streaming-through uploads. A
// destination connector consumes a SegmentSource so it can pull byte
// ranges directly from the source on demand — no local download
// staging, no buffering of the full file in memory.
//
// For destinations like Aprimo that upload in segments in parallel,
// multiple goroutines call Open concurrently with non-overlapping
// (start, length) ranges; each call returns an independent reader.
// Implementations MUST be safe for concurrent Open calls.
type SegmentSource interface {
	// Size is the total byte length of the source.
	Size() int64

	// Open returns a reader over bytes [start, start+length). The
	// caller closes the reader. May be called concurrently with
	// non-overlapping ranges.
	Open(ctx context.Context, start, length int64) (io.ReadCloser, error)
}

// SegmentSourceFor wraps a (Connector, Entry) as a SegmentSource. If
// the connector implements OpenRange, the SegmentSource delegates to
// it (the streaming path). Otherwise it falls back to a single Read +
// in-memory slicing — slower and bounded by file size, but correct
// for connectors without native range support.
func SegmentSourceFor(src Connector, entry Entry) SegmentSource {
	return &connectorSource{
		conn:  src,
		entry: entry,
	}
}

// SequentialReads reports whether the underlying source prefers sequential,
// one-at-a-time reads (spinning media). Sources opt in by implementing the
// same method; the rest read in parallel.
func (s *connectorSource) SequentialReads() bool {
	if seq, ok := s.conn.(interface{ SequentialReads() bool }); ok {
		return seq.SequentialReads()
	}
	return false
}

// PresignGetURL returns a short-lived authenticated GET URL for this entry,
// so a destination can have storage fetch the bytes server-side. Sources
// with credentials (object stores) implement it; others return an error and
// the destination falls back to streaming the bytes through.
func (s *connectorSource) PresignGetURL(ctx context.Context, ttl time.Duration) (string, error) {
	if p, ok := s.conn.(interface {
		PresignGetURL(context.Context, string, time.Duration) (string, error)
	}); ok {
		return p.PresignGetURL(ctx, s.entry.Path, ttl)
	}
	return "", errors.New("source does not support presigned URLs")
}

type connectorSource struct {
	conn  Connector
	entry Entry

	// fallback is populated on first Open if OpenRange is unsupported.
	once     sync.Once
	fallback []byte
	loadErr  error
}

func (s *connectorSource) Size() int64 {
	return s.entry.Size
}

func (s *connectorSource) Open(ctx context.Context, start, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if s.entry.Size > 0 && start+length > s.entry.Size {
		length = s.entry.Size - start
		if length <= 0 {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
	}

	rc, err := s.conn.OpenRange(ctx, s.entry.Path, start, length)
	if err == nil {
		return rc, nil
	}
	if !errors.Is(err, ErrUnsupported) {
		return nil, err
	}

	if err := s.loadFallback(ctx); err != nil {
		return nil, err
	}
	// Defensive bounds: if a connector under-delivered (returned fewer
	// bytes than Size() advertised), start may be past the loaded
	// slice. Slicing past length panics — return EOF instead so the
	// uploader sees an empty segment and surfaces the size mismatch
	// at commit time rather than crashing the worker.
	if start >= int64(len(s.fallback)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := start + length
	if end > int64(len(s.fallback)) {
		end = int64(len(s.fallback))
	}
	return io.NopCloser(bytes.NewReader(s.fallback[start:end])), nil
}

// maxFallbackBytes caps the size of objects we'll load into memory via
// the OpenRange-less fallback path. Any production storage connector
// should implement OpenRange; this limit exists to keep a misbehaving
// connector from OOMing a worker. 256 MiB is generous for the
// localfs/test cases that hit this path and refuses anything larger.
const maxFallbackBytes = 256 * 1024 * 1024

// loadFallback reads the entire source once and caches it in memory.
// Used only for connectors without OpenRange — explicit bandaid, not
// the production path. Refuses sources with unknown or excessive size
// so a misbehaving connector can't blow up memory.
func (s *connectorSource) loadFallback(ctx context.Context) error {
	s.once.Do(func() {
		if s.entry.Size <= 0 {
			s.loadErr = fmt.Errorf("segment source fallback: unknown size for %q (connector should implement OpenRange)", s.entry.Path)
			return
		}
		if s.entry.Size > maxFallbackBytes {
			s.loadErr = fmt.Errorf("segment source fallback: %q is %d bytes, exceeds %d-byte fallback cap (connector should implement OpenRange)",
				s.entry.Path, s.entry.Size, maxFallbackBytes)
			return
		}
		rc, err := s.conn.Read(ctx, s.entry.Path)
		if err != nil {
			s.loadErr = fmt.Errorf("segment source fallback read: %w", err)
			return
		}
		defer rc.Close()
		// LimitReader with one extra byte lets us detect "lied about size"
		// from the connector and fail loud rather than truncating silently.
		body, err := io.ReadAll(io.LimitReader(rc, s.entry.Size+1))
		if err != nil {
			s.loadErr = fmt.Errorf("segment source fallback drain: %w", err)
			return
		}
		if int64(len(body)) > s.entry.Size {
			s.loadErr = fmt.Errorf("segment source fallback: %q returned more bytes than Size() advertised", s.entry.Path)
			return
		}
		s.fallback = body
	})
	return s.loadErr
}

// ReaderSource wraps a known-size byte slice (or any io.ReaderAt-able
// thing) as a SegmentSource. Useful in tests.
type ReaderSource struct {
	Data []byte
}

// Size implements SegmentSource.
func (r *ReaderSource) Size() int64 { return int64(len(r.Data)) }

// Open implements SegmentSource.
func (r *ReaderSource) Open(_ context.Context, start, length int64) (io.ReadCloser, error) {
	if start > int64(len(r.Data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := start + length
	if end > int64(len(r.Data)) {
		end = int64(len(r.Data))
	}
	return io.NopCloser(bytes.NewReader(r.Data[start:end])), nil
}
