package connector

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/timw255/uplink/internal/ignore"
)

// maxIgnoreFileSize caps how many bytes LoadIgnoreMatcher will read
// from a connector's .uplinkignore. Real-world ignore files are at
// most kilobytes; the cap prevents a pathologically large blob from
// being slurped into memory at Init.
const maxIgnoreFileSize = 1 << 20 // 1 MiB

// LoadIgnoreMatcher reads .uplinkignore from a source connector's root
// and returns a compiled Matcher. The file is fetched via the
// connector's own Read method, so any source that implements the
// Connector interface uses the same code path — local filesystem,
// S3, Azure Blob, B2.
//
// Once loaded, the matcher is consulted directly by each connector's
// List/Walk/Read/OpenRange so ignored files are truly hidden from
// Uplink — companion scripts cannot reach them either. Companion
// files that need to remain reachable (XMP sidecars, JSON descriptors)
// should be declared via the channel's `companions:` block, not via
// .uplinkignore.
//
// Returns (nil, nil) when no .uplinkignore exists at the root, so
// callers can treat absence as "no filter." Any non-ErrNotFound read
// failure (auth, transport, malformed transport response) propagates
// as an error and should fail Init — a daemon shouldn't silently sync
// everything when the ignore file is supposed to be there but can't be
// read.
//
// The file body is capped at maxIgnoreFileSize; anything beyond is
// truncated silently. Patterns past the cap are ignored.
//
// Only the root .uplinkignore is loaded. Nested ignore files in
// subdirectories are not discovered in this version. Patterns like
// `**/*.tmp` written in the root file still match everywhere.
func LoadIgnoreMatcher(ctx context.Context, src Connector) (*ignore.Matcher, error) {
	rc, err := src.Read(ctx, ignore.IgnoreFilename)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", ignore.IgnoreFilename, err)
	}
	defer rc.Close()

	body, err := io.ReadAll(io.LimitReader(rc, maxIgnoreFileSize))
	if err != nil {
		return nil, fmt.Errorf("read %s body: %w", ignore.IgnoreFilename, err)
	}

	// rootPath is empty because source connectors emit RELATIVE paths
	// (entry.Path is relative to the configured root/prefix), and the
	// Matcher's relPath conversion only triggers when an absolute path
	// is passed in. Keeping rootPath empty avoids OS-specific path
	// handling on Windows.
	m := ignore.NewMatcher("")
	m.AddPatternsFromContent(string(body), "")
	return m, nil
}

// IsEventEligible reports whether an entry's path should be visible
// to Uplink. Two reasons to skip:
//
//   - The path is the .uplinkignore file itself, which is connector
//     metadata, not content to sync.
//   - The path matches an active ignore pattern.
//
// Source connectors apply this predicate inside their List and Walk
// methods so ignored paths never surface to the engine, the scan loop,
// or companion scripts. Read also rejects ignored paths — but it must
// use matcher.ShouldIgnore directly, not this helper, because
// LoadIgnoreMatcher reads the .uplinkignore file itself via Read
// during Init.
func IsEventEligible(path string, matcher *ignore.Matcher) bool {
	if path == ignore.IgnoreFilename {
		return false
	}
	if matcher != nil && matcher.ShouldIgnore(path) {
		return false
	}
	return true
}
