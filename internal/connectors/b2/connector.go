// Package b2 is the Backblaze B2 source connector. Read-only:
// List/Stat/Read/OpenRange against the bucket+prefix scope, and a
// poll-based EventSource that diffs the current listing against
// last-known state to emit OnCreate/OnUpdate/OnDelete events.
//
// SDK: github.com/Backblaze/blazer/b2.
package b2

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Backblaze/blazer/b2"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/ignore"
)

// Connector is one configured B2 instance.
type Connector struct {
	name          string
	cfg           *Config
	client        *b2.Client
	bucket        *b2.Bucket
	ignoreMatcher *ignore.Matcher
}

// Factory builds a B2 connector from its raw YAML config block.
func Factory(name string, raw map[string]any) (connector.Connector, error) {
	cfg, err := loadConfig(name, raw)
	if err != nil {
		return nil, err
	}
	return &Connector{name: name, cfg: cfg}, nil
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return c.name }

// Init authenticates with B2 and pins the bucket handle for the rest
// of the connector's lifetime.
func (c *Connector) Init(ctx context.Context) error {
	client, err := b2.NewClient(ctx, c.cfg.KeyID, c.cfg.ApplicationKey, b2.UserAgent("aprimo-uplink"))
	if err != nil {
		return fmt.Errorf("b2[%s]: authenticate: %w", c.name, err)
	}
	bucket, err := client.Bucket(ctx, c.cfg.Bucket)
	if err != nil {
		return fmt.Errorf("b2[%s]: open bucket %q: %w", c.name, c.cfg.Bucket, err)
	}
	c.client = client
	c.bucket = bucket

	m, err := connector.LoadIgnoreMatcher(ctx, c)
	if err != nil {
		return fmt.Errorf("b2[%s]: load ignore: %w", c.name, err)
	}
	c.ignoreMatcher = m
	return nil
}

// Close is a no-op; the SDK manages its own resources.
func (c *Connector) Close() error { return nil }

// List enumerates objects under cfg.Prefix + prefix. Each Entry
// carries the name relative to the connector's prefix, size, upload
// timestamp, and SHA1 as Hash. Paths ignored by .uplinkignore (and
// the .uplinkignore file itself) are excluded — ignored paths are
// invisible to Uplink. The ignore check happens before the per-object
// Attrs call so ignored objects don't incur a network round-trip.
func (c *Connector) List(ctx context.Context, prefix string) ([]connector.Entry, error) {
	full := c.fullPrefix(prefix)
	iter := c.bucket.List(ctx, b2.ListPrefix(full))
	var out []connector.Entry
	for iter.Next() {
		obj := iter.Object()
		relPath := c.relKey(obj.Name())
		if !connector.IsEventEligible(relPath, c.ignoreMatcher) {
			continue
		}
		attrs, err := obj.Attrs(ctx)
		if err != nil {
			return nil, fmt.Errorf("b2[%s]: attrs for %s: %w", c.name, obj.Name(), err)
		}
		out = append(out, c.entryFromAttrs(obj.Name(), attrs))
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("b2[%s]: list: %w", c.name, err)
	}
	return out, nil
}

// Walk streams entries under cfg.Prefix + prefix. Memory cost is
// O(1) — yields per object without buffering. obj.Attrs(ctx) is
// still one network call per object, so callers with very large
// prefixes should partition with watchers to keep wall-clock down.
// Paths ignored by .uplinkignore (and the .uplinkignore file itself)
// are silently skipped before the Attrs call.
func (c *Connector) Walk(ctx context.Context, prefix string, yield func(connector.Entry) error) error {
	full := c.fullPrefix(prefix)
	iter := c.bucket.List(ctx, b2.ListPrefix(full))
	for iter.Next() {
		obj := iter.Object()
		relPath := c.relKey(obj.Name())
		if !connector.IsEventEligible(relPath, c.ignoreMatcher) {
			continue
		}
		attrs, err := obj.Attrs(ctx)
		if err != nil {
			return fmt.Errorf("b2[%s]: attrs for %s: %w", c.name, obj.Name(), err)
		}
		if err := yield(c.entryFromAttrs(obj.Name(), attrs)); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("b2[%s]: list: %w", c.name, err)
	}
	return nil
}

// Stat returns the entry for a single name. Returns ErrNotFound if
// the object doesn't exist.
func (c *Connector) Stat(ctx context.Context, path string) (connector.Entry, error) {
	name := c.fullKey(path)
	obj := c.bucket.Object(name)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if b2.IsNotExist(err) {
			return connector.Entry{}, connector.ErrNotFound
		}
		return connector.Entry{}, fmt.Errorf("b2[%s]: stat %s: %w", c.name, name, err)
	}
	return c.entryFromAttrs(name, attrs), nil
}

// Read opens the whole object for streaming. Paths matching
// .uplinkignore are treated as not present — the .uplinkignore file
// itself is still readable so LoadIgnoreMatcher can bootstrap during
// Init.
func (c *Connector) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	r := c.bucket.Object(c.fullKey(path)).NewReader(ctx)
	return r, nil
}

// OpenRange returns bytes [start, start+length) of the object via
// B2's range-reader. The Aprimo uploader calls this once per parallel
// segment worker and streams the body straight into its segment POST.
// Paths matching .uplinkignore are treated as not present.
func (c *Connector) OpenRange(ctx context.Context, path string, start, length int64) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	if length <= 0 {
		return nil, fmt.Errorf("b2[%s]: OpenRange length must be > 0", c.name)
	}
	r := c.bucket.Object(c.fullKey(path)).NewRangeReader(ctx, start, length)
	return r, nil
}

// Write / Delete / Move return ErrUnsupported. B2 is a source-only
// connector — channels flow storage → Aprimo, never the other way.
func (c *Connector) Write(_ context.Context, _ string, _ connector.SegmentSource, _ map[string]any) (connector.Entry, error) {
	return connector.Entry{}, connector.ErrUnsupported
}
func (c *Connector) Delete(_ context.Context, _ string) error  { return connector.ErrUnsupported }
func (c *Connector) Move(_ context.Context, _, _ string) error { return connector.ErrUnsupported }

// Reconcile walks the bucket+prefix, diffs against persisted state,
// and emits events for the differences.
func (c *Connector) Reconcile(
	ctx context.Context,
	state connector.StateStore,
	onProgress connector.ProgressFunc,
) (connector.ReconcileResult, error) {
	return c.scanWatcher(ctx, connector.WatcherSpec{Prefix: ""}, nil, state, nil, onProgress)
}

// PollInterval reports the configured poll cadence.
func (c *Connector) PollInterval() time.Duration { return c.cfg.PollInterval }

// fullPrefix joins the connector's configured prefix with an extra
// listing prefix.
func (c *Connector) fullPrefix(extra string) string {
	if extra == "" {
		return c.cfg.Prefix
	}
	if c.cfg.Prefix == "" {
		return strings.TrimPrefix(extra, "/")
	}
	return strings.TrimRight(c.cfg.Prefix, "/") + "/" + strings.TrimLeft(extra, "/")
}

// fullKey converts a connector-relative path into an absolute B2
// object name.
func (c *Connector) fullKey(relPath string) string {
	if c.cfg.Prefix == "" {
		return relPath
	}
	return strings.TrimRight(c.cfg.Prefix, "/") + "/" + strings.TrimLeft(relPath, "/")
}

// relKey converts an absolute B2 name into a connector-relative path
// by stripping the configured prefix.
func (c *Connector) relKey(name string) string {
	if c.cfg.Prefix == "" {
		return name
	}
	return strings.TrimPrefix(name, strings.TrimRight(c.cfg.Prefix, "/")+"/")
}

// entryFromAttrs turns a B2 object's attributes into an Entry.
// Hash is the file's SHA1 (which B2 returns on the listing and on
// Attrs); it's a true content hash for files under the large-file
// threshold and a B2-assigned token otherwise — either way it changes
// when the bytes change, so it's a valid version identifier for
// change detection.
func (c *Connector) entryFromAttrs(name string, attrs *b2.Attrs) connector.Entry {
	return connector.Entry{
		Path:    c.relKey(name),
		Size:    attrs.Size,
		ModTime: attrs.UploadTimestamp.UTC(),
		Hash:    attrs.SHA1,
	}
}
