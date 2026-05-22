package connector

import (
	"bytes"
	"context"
	"io"
	"testing"
)

// underDeliveryConn is a Connector that LIES about its entry size in
// Stat but only returns a partial byte slice on Read. It also refuses
// OpenRange (forcing the fallback path). This drives the
// segment_source bounds-check defense.
type underDeliveryConn struct {
	Connector
	advertised int64
	body       []byte
}

func (u *underDeliveryConn) Name() string { return "under" }
func (u *underDeliveryConn) Read(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(u.body)), nil
}
func (u *underDeliveryConn) OpenRange(_ context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
	return nil, ErrUnsupported
}

// TestSegmentSource_FallbackHandlesUnderDelivery proves we no longer
// panic when a connector advertises Size=N but Read returns < N bytes.
// Before the bounds fix, requesting a range whose start falls past the
// actually-loaded slice triggered "slice bounds out of range" on the
// worker goroutine. With the fix, we return EOF for that range so the
// uploader fails cleanly at commit time with a size mismatch instead.
func TestSegmentSource_FallbackHandlesUnderDelivery(t *testing.T) {
	// Advertise 1000 bytes; actually return 500.
	conn := &underDeliveryConn{
		advertised: 1000,
		body:       bytes.Repeat([]byte("x"), 500),
	}
	entry := Entry{Path: "x.bin", Size: 1000}
	src := SegmentSourceFor(conn, entry)

	// Range that starts past the actually-loaded bytes — would have
	// panicked before the bounds check.
	rc, err := src.Open(context.Background(), 600, 200)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if len(body) != 0 {
		t.Fatalf("expected empty reader for past-end range, got %d bytes", len(body))
	}

	// Range that's fully within the actually-loaded bytes should work.
	rc2, err := src.Open(context.Background(), 100, 50)
	if err != nil {
		t.Fatalf("Open mid-range: %v", err)
	}
	defer rc2.Close()
	body2, _ := io.ReadAll(rc2)
	if len(body2) != 50 {
		t.Fatalf("expected 50 bytes in-range, got %d", len(body2))
	}

	// Range that straddles the actually-loaded boundary should clamp
	// to what's available.
	rc3, err := src.Open(context.Background(), 400, 200)
	if err != nil {
		t.Fatalf("Open straddling: %v", err)
	}
	defer rc3.Close()
	body3, _ := io.ReadAll(rc3)
	if len(body3) != 100 {
		t.Fatalf("expected 100 bytes after clamp, got %d", len(body3))
	}
}
