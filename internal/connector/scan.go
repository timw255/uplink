package connector

import (
	"context"
	"time"

	"github.com/timw255/uplink/internal/ignore"
)

// ScanWatcher runs one batched-streaming-diff cycle for one watcher.
// It is the shared implementation every source connector uses for its
// scan loop — the four near-identical per-connector scan() functions
// that lived in localfs/s3/azblob/b2 collapse to one here.
//
// Memory cost is bounded by `batchSize` regardless of how many
// entries the watcher's subtree contains, because Walk is streaming
// and the diff happens batch-by-batch against SQLite.
//
// `subPrefixes` is the set of more-specific watcher prefixes whose
// subtrees this scan must skip — paths under one of those prefixes
// belong to that other watcher's scope.
//
// `ignoreMatcher` may be nil. When non-nil, .uplinkignore-matched
// paths are skipped (no event emitted, no state row written). The
// .uplinkignore file itself is also skipped.
//
// `onEvent` may be nil for the reconcile-only case (no event
// emission); when set, it receives one Event per Create / Update /
// Delete classified during the diff. Implementations that satisfy
// EventBatchHandler receive whole-batch dispatches for efficiency.
func ScanWatcher(
	ctx context.Context,
	conn Connector,
	watcher WatcherSpec,
	subPrefixes []string,
	state StateStore,
	ignoreMatcher *ignore.Matcher,
	onEvent EventHandler,
	onProgress ProgressFunc,
) (ReconcileResult, error) {
	const batchSize = 500

	connName := conn.Name()
	scope := watcher.ScopeKey(connName)
	res := ReconcileResult{Connector: connName}
	start := time.Now()

	gen, err := state.NextGeneration(ctx, scope)
	if err != nil {
		return res, err
	}

	batch := make([]Entry, 0, batchSize)
	now := time.Now().UTC()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		paths := make([]string, len(batch))
		for i, e := range batch {
			paths[i] = e.Path
		}
		prior, err := state.LoadStateFor(ctx, scope, paths)
		if err != nil {
			return err
		}
		upserts := make([]StateEntry, 0, len(batch))
		var events []Event
		for _, e := range batch {
			upserts = append(upserts, StateEntry{
				Path: e.Path, Size: e.Size, ModTime: e.ModTime, Hash: e.Hash,
			})
			prev, hadPrev := prior[e.Path]
			var kind EventKind
			switch {
			case !hadPrev:
				res.New++
				kind = EventCreate
			case prev.Hash != e.Hash ||
				prev.Size != e.Size ||
				!prev.ModTime.Equal(e.ModTime):
				res.Modified++
				kind = EventUpdate
			default:
				res.Unchanged++
			}
			if kind != "" && onEvent != nil {
				events = append(events, Event{
					Connector: connName,
					Kind:      kind,
					Entry:     e,
					Observed:  now,
				})
			}
		}
		if err := state.ApplyStateDelta(ctx, scope, upserts, gen); err != nil {
			return err
		}
		if err := dispatchEvents(ctx, onEvent, connName, events); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	walkErr := conn.Walk(ctx, watcher.Prefix, func(e Entry) error {
		if PathIsUnderAnyPrefix(e.Path, subPrefixes) {
			return nil
		}
		if !IsEventEligible(e.Path, ignoreMatcher) {
			return nil
		}
		res.Total++
		batch = append(batch, e)
		if onProgress != nil {
			onProgress(ReconcileProgress{
				Connector: connName,
				Scanned:   res.Total,
				Processed: res.Total,
				Current:   e.Path,
				Bytes:     e.Size,
				Errors:    res.Errors,
			})
		}
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	})
	if walkErr != nil {
		res.Duration = time.Since(start)
		return res, walkErr
	}
	if err := flush(); err != nil {
		res.Duration = time.Since(start)
		return res, err
	}

	// Delete sweep: any row not bumped to this generation is gone.
	deletedPaths, err := state.SweepStateBelowGeneration(ctx, scope, gen)
	if err != nil {
		res.Duration = time.Since(start)
		return res, err
	}
	if len(deletedPaths) > 0 {
		res.Deleted = len(deletedPaths)
		if onEvent != nil {
			events := make([]Event, len(deletedPaths))
			for i, p := range deletedPaths {
				events[i] = Event{
					Connector: connName,
					Kind:      EventDelete,
					Entry:     Entry{Path: p},
					Observed:  now,
				}
			}
			if err := dispatchEvents(ctx, onEvent, connName, events); err != nil {
				res.Duration = time.Since(start)
				return res, err
			}
		}
	}

	res.Duration = time.Since(start)
	return res, nil
}

// dispatchEvents hands events to the handler, preferring batch
// delivery when the handler implements EventBatchHandler.
func dispatchEvents(ctx context.Context, h EventHandler, connName string, events []Event) error {
	if h == nil || len(events) == 0 {
		return nil
	}
	if bh, ok := h.(EventBatchHandler); ok {
		return bh.HandleBatch(ctx, connName, events)
	}
	for _, ev := range events {
		if err := h.Handle(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}
