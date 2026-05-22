package connector

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// WatcherSpec describes one disjoint coverage unit inside a source
// connector. Operators can configure multiple watchers per connector
// to give different subtrees different polling cadences:
//
//	root: ./incoming
//	poll_interval: 1h           # the default empty-prefix watcher
//	watchers:
//	  - prefix: images/
//	    poll_interval: 5m
//	  - prefix: images/hot/
//	    poll_interval: 10s
//
// Paths belong to the watcher with the LONGEST matching prefix — so
// `images/hot/x.jpg` is owned by the 10s watcher, not the 5m or 1h
// one. Coverage is disjoint by construction; no file is scanned twice
// across watchers, and identical prefixes within a connector are a
// config error.
//
// State partitioning: each watcher has its own scope key
// (`connector#prefix`) in connector_state so the SQLite delta + sweep
// happen independently. sync_log keys on the connector name (not the
// scope) so a file moving between watcher prefixes shows up as an
// Update on the same Aprimo record.
type WatcherSpec struct {
	// Prefix is the slash-relative path inside the connector root
	// that this watcher owns. Empty means "everything not claimed by
	// a more-specific watcher" — the catch-all root watcher.
	Prefix string

	// PollInterval is how often this watcher's scan loop runs.
	PollInterval time.Duration
}

// ScopeKey returns the per-watcher state-table scope key. Empty
// prefix → just the connector name (backwards-compatible with the
// pre-watcher state rows).
func (w WatcherSpec) ScopeKey(connectorName string) string {
	if w.Prefix == "" {
		return connectorName
	}
	return connectorName + "#" + w.Prefix
}

// NormalizeWatchers trims, validates, and sorts the supplied specs.
// Returned slice is sorted by prefix length descending so callers
// can match longest-first. Errors on duplicate prefixes or
// non-positive poll intervals.
func NormalizeWatchers(specs []WatcherSpec) ([]WatcherSpec, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make([]WatcherSpec, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for _, w := range specs {
		p := strings.TrimSpace(w.Prefix)
		p = strings.TrimPrefix(p, "/")
		p = strings.TrimSuffix(p, "/")
		if _, dup := seen[p]; dup {
			return nil, fmt.Errorf("watchers: duplicate prefix %q", w.Prefix)
		}
		seen[p] = struct{}{}
		if w.PollInterval <= 0 {
			return nil, fmt.Errorf("watchers: prefix %q has non-positive poll_interval", w.Prefix)
		}
		out = append(out, WatcherSpec{Prefix: p, PollInterval: w.PollInterval})
	}
	sort.Slice(out, func(i, j int) bool {
		return len(out[i].Prefix) > len(out[j].Prefix)
	})
	return out, nil
}

// SubwatcherPrefixes returns the prefixes of all watchers in `all`
// that are strictly more specific than `w` — those whose subtrees
// the caller must skip when walking `w`'s coverage. Returned slice is
// stable (insertion order from `all`, which is already longest-first).
func SubwatcherPrefixes(all []WatcherSpec, w WatcherSpec) []string {
	var subs []string
	for _, other := range all {
		if other.Prefix == w.Prefix {
			continue
		}
		// `other` is more specific than `w` iff it is under `w`'s
		// prefix AND its prefix is strictly longer.
		if len(other.Prefix) <= len(w.Prefix) {
			continue
		}
		if w.Prefix == "" || pathIsUnder(other.Prefix, w.Prefix) {
			subs = append(subs, other.Prefix)
		}
	}
	return subs
}

// pathIsUnder reports whether `path` is under `prefix`. Both are
// slash-separated. Empty prefix matches anything.
func pathIsUnder(path, prefix string) bool {
	if prefix == "" {
		return true
	}
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if len(path) == len(prefix) {
		return true
	}
	return path[len(prefix)] == '/'
}

// PathIsUnderAnyPrefix returns true when path is under any of the
// supplied prefixes. Used by the scan helper to skip entries that
// belong to a more-specific sibling watcher.
func PathIsUnderAnyPrefix(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if pathIsUnder(path, p) {
			return true
		}
	}
	return false
}

// ParseWatchersYAML decodes the optional `watchers:` list out of a
// raw YAML mapping. Shape:
//
//	watchers:
//	  - prefix: images/
//	    poll_interval: 5m
//	  - prefix: images/hot/
//	    poll_interval: 10s
//
// Returns nil when the entry is absent or empty. connectorType +
// connectorName are used only to construct readable error messages.
// The returned slice has NOT been normalized (caller composes with
// the default watcher and calls NormalizeWatchers).
func ParseWatchersYAML(connectorType, connectorName string, raw any) ([]WatcherSpec, error) {
	if raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, errWatcher(connectorType, connectorName, -1, "expected a list of mappings")
	}
	if len(list) == 0 {
		return nil, nil
	}
	out := make([]WatcherSpec, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, errWatcher(connectorType, connectorName, i, "expected mapping")
		}
		w := WatcherSpec{}
		if v, ok := m["prefix"].(string); ok {
			w.Prefix = v
		}
		if v, ok := m["poll_interval"].(string); ok {
			d, err := ParseDuration(v)
			if err != nil {
				return nil, errWatcher(connectorType, connectorName, i, "poll_interval: "+err.Error())
			}
			w.PollInterval = d
		}
		out = append(out, w)
	}
	return out, nil
}

func errWatcher(connectorType, connectorName string, idx int, msg string) error {
	if idx < 0 {
		return fmt.Errorf("%s[%s]: watchers: %s", connectorType, connectorName, msg)
	}
	return fmt.Errorf("%s[%s]: watchers[%d]: %s", connectorType, connectorName, idx, msg)
}
