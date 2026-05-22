package localfs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// EventSource is the poll-based EventSource for a localfs connector.
// It diffs the current tree against last-known state held in the
// store and synthesizes OnCreate / OnUpdate / OnDelete events.
//
// Each watcher (see WatcherSpec) gets its own poll loop. The default
// watcher uses the connector's `poll_interval`; nested watchers use
// their own per-prefix intervals. State is partitioned by watcher
// scope so the loops don't interfere.
type EventSource struct {
	conn     *Connector
	state    connector.StateStore
	watchers []connector.WatcherSpec
}

// NewEventSource binds an existing localfs Connector to a state store.
func NewEventSource(c *Connector, s connector.StateStore) *EventSource {
	return &EventSource{conn: c, state: s, watchers: c.allWatchers()}
}

// NewEventSource implements connector.EventSourceFactory.
func (c *Connector) NewEventSource(state connector.StateStore) connector.EventSource {
	return NewEventSource(c, state)
}

// allWatchers returns the full set of watcher specs for this
// connector — the explicit per-prefix list from YAML PLUS an implicit
// empty-prefix watcher using the connector's `poll_interval` (so
// existing configs without a `watchers:` block keep working). The
// returned slice is sorted longest-prefix-first.
func (c *Connector) allWatchers() []connector.WatcherSpec {
	specs := make([]connector.WatcherSpec, 0, 1+len(c.cfg.Watchers))
	specs = append(specs, connector.WatcherSpec{Prefix: "", PollInterval: c.cfg.PollInterval})
	specs = append(specs, c.cfg.Watchers...)
	normalized, err := connector.NormalizeWatchers(specs)
	if err != nil {
		// Should have been caught at config load; on the off chance
		// it slips through, fall back to the single default watcher.
		slog.Warn("localfs: watcher normalization failed; falling back to default-only",
			"connector", c.name, "err", err)
		return []connector.WatcherSpec{{Prefix: "", PollInterval: c.cfg.PollInterval}}
	}
	return normalized
}

// Subscribe blocks until ctx is cancelled. It spawns one poll loop
// per watcher, each running at its own cadence. Each loop's scan is
// scoped to its watcher's prefix and excludes subtrees owned by
// more-specific watchers.
func (s *EventSource) Subscribe(ctx context.Context, handler connector.EventHandler) error {
	var wg sync.WaitGroup
	for _, w := range s.watchers {
		w := w
		subPrefixes := connector.SubwatcherPrefixes(s.watchers, w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runWatcherLoop(ctx, w, subPrefixes, handler)
		}()
	}
	wg.Wait()
	return nil
}

func (s *EventSource) runWatcherLoop(
	ctx context.Context,
	w connector.WatcherSpec,
	subPrefixes []string,
	handler connector.EventHandler,
) {
	// Warm-up tick so existing files surface as OnCreate on a fresh
	// state store without waiting for the first interval.
	if _, err := s.conn.scanWatcher(ctx, w, subPrefixes, s.state, handler, nil); err != nil {
		slog.Warn("localfs scan failed",
			"connector", s.conn.Name(), "prefix", w.Prefix, "err", err)
	}

	t := time.NewTicker(w.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.conn.scanWatcher(ctx, w, subPrefixes, s.state, handler, nil); err != nil {
				// Transient errors don't tear down the loop; the next
				// tick retries. Operators see WARN if it's sustained.
				slog.Warn("localfs scan failed",
					"connector", s.conn.Name(), "prefix", w.Prefix, "err", err)
			}
		}
	}
}
