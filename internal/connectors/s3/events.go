package s3

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// EventSource is the poll-based source for an s3 connector. Spawns
// one poll loop per configured watcher; each loop has its own
// cadence and its own state-table scope key so they're independent.
type EventSource struct {
	conn     *Connector
	state    connector.StateStore
	watchers []connector.WatcherSpec
}

func NewEventSource(c *Connector, s connector.StateStore) *EventSource {
	return &EventSource{conn: c, state: s, watchers: c.allWatchers()}
}

func (c *Connector) NewEventSource(state connector.StateStore) connector.EventSource {
	return NewEventSource(c, state)
}

// allWatchers composes the implicit default (empty-prefix) watcher
// with any explicitly configured `watchers:` block.
func (c *Connector) allWatchers() []connector.WatcherSpec {
	specs := make([]connector.WatcherSpec, 0, 1+len(c.cfg.Watchers))
	specs = append(specs, connector.WatcherSpec{Prefix: "", PollInterval: c.cfg.PollInterval})
	specs = append(specs, c.cfg.Watchers...)
	normalized, err := connector.NormalizeWatchers(specs)
	if err != nil {
		slog.Warn("s3: watcher normalization failed; falling back to default-only",
			"connector", c.name, "err", err)
		return []connector.WatcherSpec{{Prefix: "", PollInterval: c.cfg.PollInterval}}
	}
	return normalized
}

// Subscribe spawns one poll loop per watcher and blocks until ctx
// is cancelled. Each loop scans its prefix subtree at its own
// poll_interval, skipping descendants owned by more-specific watchers.
func (s *EventSource) Subscribe(ctx context.Context, handler connector.EventHandler) error {
	var wg sync.WaitGroup
	for _, w := range s.watchers {
		subPrefixes := connector.SubwatcherPrefixes(s.watchers, w)
		wg.Go(func() {
			s.runWatcherLoop(ctx, w, subPrefixes, handler)
		})
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
	if _, err := s.conn.scanWatcher(ctx, w, subPrefixes, s.state, handler, nil); err != nil {
		slog.Warn("s3 scan failed",
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
				slog.Warn("s3 scan failed",
					"connector", s.conn.Name(), "prefix", w.Prefix, "err", err)
			}
		}
	}
}
