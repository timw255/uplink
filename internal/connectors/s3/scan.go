package s3

import (
	"context"

	"github.com/timw255/uplink/internal/connector"
)

// scanWatcher is a thin adapter over the shared streaming-scan helper
// that threads in the s3-specific ignore matcher. Per-watcher scope
// keys partition state in connector_state so nested watchers don't
// step on each other.
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
