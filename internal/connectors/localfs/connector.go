// Package localfs is the local-filesystem source connector. Connector
// paths are relative to a configured root; absolute paths are rejected.
// The poll-based event source diffs the current tree against last-known
// state in the store and synthesizes OnCreate/OnUpdate/OnDelete.
package localfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/ignore"
)

// Connector is a localfs connector instance.
type Connector struct {
	name          string
	cfg           Config
	ignoreMatcher *ignore.Matcher
}

// Config is the YAML connector block for localfs.
type Config struct {
	Root         string                  `yaml:"root"`
	PollInterval time.Duration           `yaml:"poll_interval"`
	Watchers     []connector.WatcherSpec `yaml:"watchers"`
	// Sequential makes the importer read one file at a time, front-to-back,
	// instead of issuing parallel ranged reads. Set it when the root lives
	// on spinning media — a local HDD or a NAS/storage server — where
	// concurrent seeks tank throughput. Default false (parallel), the right
	// choice for SSD/NVMe. Only affects bulk upload reads.
	Sequential bool `yaml:"sequential_reads"`
}

const defaultPollInterval = 2 * time.Second

// New constructs a Connector. The factory wraps this.
func New(name string, cfg Config) (*Connector, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("localfs[%s]: root is required", name)
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	abs, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("localfs[%s]: resolve root: %w", name, err)
	}
	cfg.Root = abs
	return &Connector{name: name, cfg: cfg}, nil
}

// Factory adapts the raw map[string]any config block from YAML.
func Factory(name string, raw map[string]any) (connector.Connector, error) {
	cfg := Config{}
	if v, ok := raw["root"].(string); ok {
		cfg.Root = v
	}
	if v, ok := raw["poll_interval"].(string); ok {
		d, err := connector.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("localfs[%s]: poll_interval: %w", name, err)
		}
		cfg.PollInterval = d
	}
	if v, ok := raw["sequential_reads"].(bool); ok {
		cfg.Sequential = v
	}
	watchers, err := connector.ParseWatchersYAML("localfs", name, raw["watchers"])
	if err != nil {
		return nil, err
	}
	cfg.Watchers = watchers
	return New(name, cfg)
}

// SequentialReads reports whether reads should be serialized front-to-back
// (spinning media). Consumed by the bulk uploader via the SegmentSource.
func (c *Connector) SequentialReads() bool { return c.cfg.Sequential }

// Name implements connector.Connector.
func (c *Connector) Name() string { return c.name }

// PollInterval reports the configured poll cadence, exported for the
// event source.
func (c *Connector) PollInterval() time.Duration { return c.cfg.PollInterval }

// Init ensures the root directory exists and loads .uplinkignore (if
// present) once for the lifetime of the connector. Edits to the ignore
// file take effect on the next daemon restart.
func (c *Connector) Init(ctx context.Context) error {
	if err := os.MkdirAll(c.cfg.Root, 0o755); err != nil {
		return err
	}
	m, err := connector.LoadIgnoreMatcher(ctx, c)
	if err != nil {
		return fmt.Errorf("localfs[%s]: load ignore: %w", c.name, err)
	}
	c.ignoreMatcher = m
	return nil
}

// Close is a no-op for localfs.
func (c *Connector) Close() error { return nil }

// List walks the tree under prefix and returns one Entry per regular
// file. Paths ignored by .uplinkignore (and the .uplinkignore file
// itself) are excluded — ignored paths are invisible to Uplink.
func (c *Connector) List(_ context.Context, prefix string) ([]connector.Entry, error) {
	base, err := c.resolve(prefix)
	if err != nil {
		return nil, err
	}
	var out []connector.Entry
	err = filepath.WalkDir(base, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) && path == base {
				return io.EOF
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(c.cfg.Root, path)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		if !connector.IsEventEligible(relSlash, c.ignoreMatcher) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, connector.Entry{
			Path:    relSlash,
			Size:    info.Size(),
			ModTime: info.ModTime().UTC(),
			Hash:    fsVersionToken(info.Size(), info.ModTime()),
		})
		return nil
	})
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	return out, err
}

// Walk streams entries under prefix, invoking yield for each regular
// file. Memory cost is O(directory depth), not O(corpus size) — the
// streaming-scan path uses this instead of List to keep scan loops
// bounded even on million-asset trees. yield returning a non-nil
// error halts the walk; ctx cancellation halts via WalkDir's normal
// error propagation. Paths ignored by .uplinkignore (and the
// .uplinkignore file itself) are silently skipped.
func (c *Connector) Walk(ctx context.Context, prefix string, yield func(connector.Entry) error) error {
	base, err := c.resolve(prefix)
	if err != nil {
		return err
	}
	walkErr := filepath.WalkDir(base, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) && path == base {
				return io.EOF
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(c.cfg.Root, path)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		if !connector.IsEventEligible(relSlash, c.ignoreMatcher) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return yield(connector.Entry{
			Path:    relSlash,
			Size:    info.Size(),
			ModTime: info.ModTime().UTC(),
			Hash:    fsVersionToken(info.Size(), info.ModTime()),
		})
	})
	if errors.Is(walkErr, io.EOF) {
		return nil
	}
	return walkErr
}

// Stat returns an Entry for a single relative path.
func (c *Connector) Stat(_ context.Context, path string) (connector.Entry, error) {
	abs, err := c.resolve(path)
	if err != nil {
		return connector.Entry{}, err
	}
	info, err := os.Stat(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return connector.Entry{}, connector.ErrNotFound
	}
	if err != nil {
		return connector.Entry{}, err
	}
	return connector.Entry{
		Path:    filepath.ToSlash(path),
		Size:    info.Size(),
		ModTime: info.ModTime().UTC(),
		Hash:    fsVersionToken(info.Size(), info.ModTime()),
	}, nil
}

// fsVersionToken is a stable identifier that changes whenever a file's
// observable state changes. We don't checksum the bytes (would be
// expensive on every poll); size+nanosecond-mtime is enough to detect
// any real change and gives each version a distinct sync_log row.
func fsVersionToken(size int64, mtime time.Time) string {
	return fmt.Sprintf("%d-%d", size, mtime.UTC().UnixNano())
}

// Read opens a file for streaming. Paths matching .uplinkignore are
// treated as not present — the .uplinkignore file itself is still
// readable so LoadIgnoreMatcher can bootstrap during Init.
func (c *Connector) Read(_ context.Context, path string) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	abs, err := c.resolve(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, connector.ErrNotFound
	}
	return f, err
}

// OpenRange returns a reader over [start, start+length) of the file
// at path. File handle is seeked once and capped by io.LimitedReader.
// Paths matching .uplinkignore are treated as not present.
func (c *Connector) OpenRange(_ context.Context, path string, start, length int64) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	abs, err := c.resolve(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, connector.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if start > 0 {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return &limitedFile{f: f, lr: io.LimitReader(f, length)}, nil
}

// limitedFile bundles a file handle with a length-capping reader so
// Close still closes the underlying os.File.
type limitedFile struct {
	f  *os.File
	lr io.Reader
}

func (l *limitedFile) Read(p []byte) (int, error) { return l.lr.Read(p) }
func (l *limitedFile) Close() error               { return l.f.Close() }

// Write streams body into root/path, creating parent dirs as needed.
// Writes are atomic-on-rename: <path>.tmp then os.Rename.
// In normal operation localfs is a source, not a destination — the
// engine never calls Write on it. The implementation is retained for
// test use where a SegmentSource needs to be round-tripped through the
// filesystem.
func (c *Connector) Write(ctx context.Context, path string, body connector.SegmentSource, _ map[string]any) (connector.Entry, error) {
	abs, err := c.resolve(path)
	if err != nil {
		return connector.Entry{}, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return connector.Entry{}, err
	}
	tmp := abs + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return connector.Entry{}, err
	}
	rc, err := body.Open(ctx, 0, body.Size())
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return connector.Entry{}, err
	}
	written, copyErr := io.Copy(f, rc)
	_ = rc.Close()
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return connector.Entry{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return connector.Entry{}, closeErr
	}
	if err := os.Rename(tmp, abs); err != nil {
		_ = os.Remove(tmp)
		return connector.Entry{}, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return connector.Entry{}, err
	}
	return connector.Entry{
		Path:    filepath.ToSlash(path),
		Size:    written,
		ModTime: info.ModTime().UTC(),
	}, nil
}

// Delete removes a file. Missing files return ErrNotFound.
func (c *Connector) Delete(_ context.Context, path string) error {
	abs, err := c.resolve(path)
	if err != nil {
		return err
	}
	if err := os.Remove(abs); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return connector.ErrNotFound
		}
		return err
	}
	return nil
}

// Move renames a file inside the root.
func (c *Connector) Move(_ context.Context, from, to string) error {
	src, err := c.resolve(from)
	if err != nil {
		return err
	}
	dst, err := c.resolve(to)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.Rename(src, dst)
}

// resolve joins prefix to the root and verifies the result does not escape
// the root via traversal.
func (c *Connector) resolve(rel string) (string, error) {
	cleaned := filepath.Clean(filepath.FromSlash(rel))
	if cleaned == "." {
		return c.cfg.Root, nil
	}
	if filepath.IsAbs(cleaned) || strings.HasPrefix(cleaned, "..") {
		return "", fmt.Errorf("localfs[%s]: path %q escapes root", c.name, rel)
	}
	return filepath.Join(c.cfg.Root, cleaned), nil
}

// Root returns the resolved absolute root, exported for the event source.
func (c *Connector) Root() string { return c.cfg.Root }

// Reconcile walks the root, classifies each file against the persisted
// last-known state, persists the new picture, and returns a tally.
// Uses the shared streaming-scan helper so memory cost during a scan
// is bounded by batch size rather than corpus size.
func (c *Connector) Reconcile(
	ctx context.Context,
	state connector.StateStore,
	onProgress connector.ProgressFunc,
) (connector.ReconcileResult, error) {
	return c.scanWatcher(ctx, connector.WatcherSpec{Prefix: ""}, nil, state, nil, onProgress)
}

// scanWatcher is a thin adapter over connector.ScanWatcher that
// passes the localfs-specific ignore matcher.
func (c *Connector) scanWatcher(
	ctx context.Context,
	watcher connector.WatcherSpec,
	subPrefixes []string,
	state connector.StateStore,
	onEvent connector.EventHandler,
	onProgress connector.ProgressFunc,
) (connector.ReconcileResult, error) {
	return connector.ScanWatcher(ctx, c, watcher, subPrefixes, state, c.ignoreMatcher, onEvent, onProgress)
}
