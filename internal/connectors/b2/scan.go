package b2

import (
	"context"

	"github.com/timw255/uplink/internal/connector"
)

// scanWatcher is a thin adapter over the shared streaming-scan helper
// that threads in the b2-specific ignore matcher.
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
