// Package engine wires events into jobs and executes them. It is the
// runtime heart of Uplink.
//
// Flow:
//
//  1. A connector emits an Event via its EventSource.
//  2. The Dispatcher matches the event against channels, consults the
//     sync_log to dedup or attach the existing Aprimo record id, then
//     enqueues one job per matched channel into the file-based queue.
//  3. A pool of Workers claim jobs (atomic rename pending → running),
//     run the source → destination transfer, insert a sync_log row on
//     success, and delete the job file.
//
// Mid-flight failures retry with exponential backoff. Permanent
// failures leave the job in data/jobs/failed/ for the operator.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// Connectors gives the engine name-keyed access to running connectors.
type Connectors interface {
	Get(name string) (connector.Connector, bool)
}

// StoreAPI is the slice of *store.Store the engine touches. Tests
// inject a decorator that wraps a real store to drive specific
// failure branches (e.g. InsertSyncLog returning a synthetic error)
// without needing to corrupt a real database.
type StoreAPI interface {
	LookupLatestBatch(ctx context.Context, channel string, paths []string) (map[string]*store.SyncLogEntry, error)
	LookupLatest(ctx context.Context, channel, sourcePath string) (*store.SyncLogEntry, error)
	LookupByStem(ctx context.Context, channel, stem, dir string) (*store.SyncLogEntry, error)
	InsertSyncLog(ctx context.Context, entry store.SyncLogEntry) error
	EnqueueJobs(ctx context.Context, jobs []store.Job) ([]string, error)
	ClaimNextJob(ctx context.Context) (*store.Job, error)
	CompleteJob(ctx context.Context, jobID string) error
	FailJob(ctx context.Context, jobID string, attempts int, reason string) error
	RetryJob(ctx context.Context, jobID string, attempts int, reason string, delay time.Duration) error
	DeleteMarker(jobID string) error
}

// Engine owns workers and a dispatcher. Construct with New, then call
// Run to block until ctx is cancelled.
type Engine struct {
	store      StoreAPI
	channels   *channel.Registry
	connectors Connectors
	logger     *slog.Logger

	workers     int
	pollIdle    time.Duration
	maxAttempts int
	baseBackoff time.Duration
}

// Options configures the engine. Zero values use sensible defaults.
type Options struct {
	Workers     int
	PollIdle    time.Duration
	MaxAttempts int
	BaseBackoff time.Duration
	Logger      *slog.Logger
}

// New constructs an Engine. Workers are not started until Run is called.
func New(s *store.Store, ch *channel.Registry, c Connectors, opts Options) *Engine {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.PollIdle <= 0 {
		opts.PollIdle = 500 * time.Millisecond
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = 2 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Engine{
		store:       s,
		channels:    ch,
		connectors:  c,
		logger:      opts.Logger,
		workers:     opts.Workers,
		pollIdle:    opts.PollIdle,
		maxAttempts: opts.MaxAttempts,
		baseBackoff: opts.BaseBackoff,
	}
}

// Dispatch is the per-event entry point. It is the EventHandler given
// to every running EventSource. For batches, prefer DispatchBatch —
// this one wraps it for the single-event case.
func (e *Engine) Dispatch(ctx context.Context, ev connector.Event) error {
	return e.DispatchBatch(ctx, ev.Connector, []connector.Event{ev})
}

// Handle implements connector.EventHandler.
func (e *Engine) Handle(ctx context.Context, ev connector.Event) error {
	return e.Dispatch(ctx, ev)
}

// HandleBatch implements connector.EventBatchHandler. Poll-based
// EventSources hand a whole cycle's events to this method so the
// engine can do one sync_log lookup per channel instead of one per
// event. Storage connectors should prefer this path.
func (e *Engine) HandleBatch(ctx context.Context, sourceConnector string, events []connector.Event) error {
	return e.DispatchBatch(ctx, sourceConnector, events)
}

// DispatchBatch is the batched entry point. The storage poll loop
// hands an entire poll cycle's worth of events to this one method so
// the engine can do ONE bulk sync_log lookup per channel and ONE
// batch of job file writes per channel — independent of file count.
//
// sourceConnector is the connector that produced these events; all
// events in the batch must share the same source connector. This
// matches how EventSources naturally emit (one source per poll loop).
//
// Events are pre-classified: anything matching a declared companion
// pattern on a channel listening to this source connector takes the
// companion route (PATCH metadata onto the parent asset's record).
// Everything else takes the existing asset route.
func (e *Engine) DispatchBatch(ctx context.Context, sourceConnector string, events []connector.Event) error {
	if len(events) == 0 {
		return nil
	}
	matched := e.channels.ChannelsForSource(sourceConnector)
	if len(matched) == 0 {
		return nil
	}

	// Pre-classify: pull companion events out of the asset flow. A path
	// claimed as a companion by ANY channel is treated as a companion
	// (one routed event per matching channel) and removed from the
	// asset flow. The asset flow still fans out across channels for
	// non-companion paths via the per-channel loop below.
	//
	// If the same path would ALSO have matched a channel's asset
	// filter, the asset route is suppressed silently from the
	// dispatcher's point of view — but we emit a warning so operators
	// can spot contradictory configurations (a companion declared on
	// one channel whose path the other channel was expecting as an
	// asset). The companion declaration is treated as the
	// authoritative intent; this warning just makes the trade visible.
	var (
		assetEvents     = make([]connector.Event, 0, len(events))
		companionEvents []companionRoutedEvent
	)
	for _, ev := range events {
		matches := e.channels.MatchCompanions(sourceConnector, ev.Entry.Path)
		if len(matches) > 0 {
			e.warnShadowedAssetMatch(sourceConnector, ev, matches)
			for _, m := range matches {
				companionEvents = append(companionEvents, companionRoutedEvent{event: ev, match: m})
			}
			continue
		}
		assetEvents = append(assetEvents, ev)
	}

	var dispatchErrs []error
	if err := e.dispatchCompanions(ctx, companionEvents); err != nil {
		dispatchErrs = append(dispatchErrs, err)
	}

	if len(assetEvents) == 0 {
		return errors.Join(dispatchErrs...)
	}
	events = assetEvents

	// Per-channel failures don't abort the batch — they're collected
	// and joined at the end. A transient sync_log lookup failure on one
	// channel must not drop events for the other channels in matched[].
	var channelErrs []error
	for _, ch := range matched {
		// First pass: filter by event kind + CEL + drop deletes. Collect
		// the matching events for this channel before the sync_log lookup
		// so we only do one bulk query per channel.
		matchedEvents := make([]connector.Event, 0, len(events))
		for _, ev := range events {
			if ev.Kind == connector.EventDelete {
				continue // deletes never propagate to Aprimo
			}
			ok, err := ch.Match(ev)
			if err != nil {
				e.logger.Warn("filter eval failed",
					"channel", ch.Spec.Name, "err", err)
				continue
			}
			if !ok {
				continue
			}
			matchedEvents = append(matchedEvents, ev)
		}
		if len(matchedEvents) == 0 {
			continue
		}

		// Bulk lookup: one sync_log query per channel for the whole batch.
		paths := make([]string, len(matchedEvents))
		for i, ev := range matchedEvents {
			paths[i] = ev.Entry.Path
		}
		existing, err := e.store.LookupLatestBatch(ctx, ch.Spec.Name, paths)
		if err != nil {
			channelErrs = append(channelErrs,
				fmt.Errorf("engine: bulk sync_log lookup for %q: %w", ch.Spec.Name, err))
			continue
		}

		// Build the jobs we need to enqueue. Skip events already synced
		// at this version; attach dest_id for the Update flow.
		jobs := make([]store.Job, 0, len(matchedEvents))
		for _, ev := range matchedEvents {
			prior := existing[ev.Entry.Path]
			if prior != nil && prior.SourceVersion == ev.Entry.Hash && ev.Entry.Hash != "" {
				// Same content as last sync; nothing to do.
				continue
			}
			payload, err := json.Marshal(eventPayload{
				Kind:     string(ev.Kind),
				Path:     ev.Entry.Path,
				Size:     ev.Entry.Size,
				Hash:     ev.Entry.Hash,
				Metadata: ev.Entry.Metadata,
			})
			if err != nil {
				return fmt.Errorf("engine: marshal payload: %w", err)
			}
			j := store.Job{
				ChannelName:     ch.Spec.Name,
				Kind:            string(ev.Kind),
				SourceConnector: sourceConnector,
				SourcePath:      ev.Entry.Path,
				SourceVersion:   ev.Entry.Hash,
				Payload:         payload,
			}
			if prior != nil {
				j.DestID = prior.DestID
			}
			jobs = append(jobs, j)
		}

		if len(jobs) == 0 {
			continue
		}
		ids, err := e.store.EnqueueJobs(ctx, jobs)
		if err != nil {
			channelErrs = append(channelErrs,
				fmt.Errorf("engine: enqueue for %q: %w", ch.Spec.Name, err))
			continue
		}
		e.logger.Info("jobs enqueued",
			"channel", ch.Spec.Name, "count", len(ids))
	}
	return errors.Join(append(dispatchErrs, channelErrs...)...)
}

// Run starts the worker pool and blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < e.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			e.workerLoop(ctx, id)
		}(i)
	}
	wg.Wait()
	return nil
}

func (e *Engine) workerLoop(ctx context.Context, id int) {
	log := e.logger.With("worker", id)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := e.store.ClaimNextJob(ctx)
		switch {
		case errors.Is(err, store.ErrNoJob):
			select {
			case <-ctx.Done():
				return
			case <-time.After(e.pollIdle):
			}
			continue
		case err != nil:
			log.Error("claim failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(e.pollIdle):
			}
			continue
		}

		e.runJob(ctx, log, job)
	}
}

func (e *Engine) runJob(ctx context.Context, log *slog.Logger, job *store.Job) {
	log = log.With("job", job.ID, "channel", job.ChannelName, "path", job.SourcePath)

	// Recover from panics inside any job-execution call. A panic here
	// previously killed the daemon; for an appliance that may run
	// unsupervised we'd rather mark the offending job failed and keep
	// processing the queue. Panics are treated as terminal (not
	// retried) because they indicate a programmer-class bug or
	// corrupted data, not a transient fault.
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err := fmt.Errorf("panic: %v", r)
		log.Error("job panicked",
			"err", err,
			"stack", string(debug.Stack()))
		if ferr := e.store.FailJob(ctx, job.ID, job.Attempts, err.Error()); ferr != nil {
			log.Error("FailJob after panic failed", "err", ferr)
		}
	}()

	ch := e.channels.Lookup(job.ChannelName)
	if ch == nil {
		e.terminate(ctx, log, job, fmt.Errorf("channel %q no longer exists", job.ChannelName))
		return
	}

	src, ok := e.connectors.Get(ch.Spec.Source)
	if !ok {
		e.terminate(ctx, log, job, fmt.Errorf("source connector %q not running", ch.Spec.Source))
		return
	}
	dst, ok := e.connectors.Get(ch.Spec.Destination)
	if !ok {
		e.terminate(ctx, log, job, fmt.Errorf("destination connector %q not running", ch.Spec.Destination))
		return
	}

	// Capture "was this a fresh asset Create?" before execute can
	// mutate anything. Used after finalize to drive the post-Create
	// sweep that catches companions arriving during the Create RPC.
	isAssetCreate := job.Kind != CompanionJobKind && job.DestID == ""

	result, presyncedPaths, err := e.execute(ctx, job, ch, src, dst)
	if err == nil {
		if finalizeErr := e.finalize(ctx, job, result); finalizeErr != nil {
			// Audit-of-record write (or marker cleanup) failed AFTER the
			// destination already succeeded. Route through the retry path
			// so the next attempt can re-try the audit step. The on-disk
			// marker (state=created with dest_id) makes the next
			// destination call a no-op — no double-upload, no double-create.
			err = finalizeErr
		} else {
			log.Info("job done", "dest", result.destID)
			if isAssetCreate {
				// On retry attempts (Attempts > 1; ClaimNextJob
				// increments Attempts at claim time, so the FIRST
				// run lands here with Attempts == 1), the
				// destination's Write may have short-circuited
				// (marker in "created" state from a previous attempt
				// that finalized halfway) — in which case presync's
				// fields were discarded, and any new companion paths
				// we processed on THIS attempt also got their fields
				// silently dropped. Clearing the dedup set forces
				// the sweep to dispatch ALL matching companions.
				// Cost: idempotent PATCHes for companions that DID
				// land via Write. Benefit: nothing silently lost
				// when finalize fails between attempts.
				dedup := presyncedPaths
				if job.Attempts > 1 {
					dedup = nil
				}
				e.sweepLateArrivingCompanions(ctx, job, ch, dedup)
			}
			return
		}
	}

	if job.Attempts >= e.maxAttempts {
		if errFail := e.store.FailJob(ctx, job.ID, job.Attempts, err.Error()); errFail != nil {
			log.Error("mark failed failed", "err", errFail)
		}
		log.Error("job permanently failed", "attempts", job.Attempts, "err", err)
		return
	}

	delay := backoff(e.baseBackoff, job.Attempts)
	if errRetry := e.store.RetryJob(ctx, job.ID, job.Attempts, err.Error(), delay); errRetry != nil {
		log.Error("schedule retry failed", "err", errRetry)
	}
	log.Warn("job retrying", "attempts", job.Attempts, "delay", delay, "err", err)
}

// finalize is the post-execute audit step: write sync_log (idempotent),
// remove the upload marker, and remove the job file. Any error here
// routes through the retry path. The marker is the resume point — its
// state=created snapshot is what lets the retry skip Aprimo entirely.
//
// Companion jobs skip the sync_log + marker steps: there's no record
// creation or version bump to audit, and no upload marker was ever
// staged. Only the job file is cleaned up. The parent asset's sync_log
// row remains authoritative.
func (e *Engine) finalize(ctx context.Context, job *store.Job, result jobResult) error {
	if job.Kind == CompanionJobKind {
		if err := e.store.CompleteJob(ctx, job.ID); err != nil {
			return fmt.Errorf("complete companion job: %w", err)
		}
		return nil
	}
	kind := store.SyncCreate
	if job.DestID != "" {
		kind = store.SyncUpdate
	}
	entry := store.SyncLogEntry{
		ChannelName:     job.ChannelName,
		SourceConnector: job.SourceConnector,
		SourcePath:      job.SourcePath,
		SourceVersion:   job.SourceVersion,
		DestID:  result.destID,
		Kind:            kind,
	}
	// Idempotent insert: if the latest sync_log row for this
	// (channel, source_path) already matches this attempt
	// (same source_version + dest_id), a prior run
	// crashed AFTER InsertSyncLog but BEFORE the marker was
	// deleted. Don't write a duplicate row.
	latest, err := e.store.LookupLatest(ctx, job.ChannelName, job.SourcePath)
	if err != nil {
		return fmt.Errorf("sync_log idempotency lookup: %w", err)
	}
	needsInsert := latest == nil ||
		latest.SourceVersion != entry.SourceVersion ||
		latest.DestID != entry.DestID
	if needsInsert {
		if err := e.store.InsertSyncLog(ctx, entry); err != nil {
			return fmt.Errorf("insert sync_log: %w", err)
		}
	}
	if err := e.store.DeleteMarker(job.ID); err != nil {
		return fmt.Errorf("delete marker: %w", err)
	}
	if err := e.store.CompleteJob(ctx, job.ID); err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	return nil
}

type jobResult struct {
	destID string
}

// execute drives a single attempt at the source → destination
// transfer. Resume after a crash is handled inside the destination
// connector's Write (driven by the upload marker), not here.
//
// For asset Create jobs, execute runs a companion presync first: it
// lists the parent's directory, runs every declared companion script
// against present companion files, and folds the merged field list
// into the record creation payload. That collapses what would be N+1
// API calls (one Create plus one PATCH per companion) into a single
// Create + fields call. Companions that arrive AFTER the Create
// completes fire their own events and route through the companion
// job path as PATCHes.
//
// Asset Update jobs do not presync companions: the existing record
// already has whatever metadata was set previously, and companion
// changes propagate through their own events. Re-running companion
// scripts on every asset content update would waste API calls.
func (e *Engine) execute(
	ctx context.Context,
	job *store.Job,
	ch *channel.Channel,
	src connector.Connector,
	dst connector.Connector,
) (jobResult, []string, error) {
	switch job.Kind {
	case CompanionJobKind:
		res, err := e.executeCompanion(ctx, job)
		return res, nil, err
	case string(connector.EventCreate), string(connector.EventUpdate):
		srcEntry, err := src.Stat(ctx, job.SourcePath)
		if err != nil {
			return jobResult{}, nil, fmt.Errorf("stat: %w", err)
		}
		segSrc := connector.SegmentSourceFor(src, srcEntry)

		var (
			companionFields []any
			presyncedPaths  []string
		)
		if job.DestID == "" {
			companionFields, presyncedPaths, err = e.presyncCompanions(ctx, ch, src, job.SourcePath)
			if err != nil {
				return jobResult{}, nil, fmt.Errorf("companion presync: %w", err)
			}
		}

		meta := map[string]any{
			"_job_id":           job.ID,
			"_channel":          job.ChannelName,
			"_source_connector": job.SourceConnector,
			"_source_version":   job.SourceVersion,
		}
		if job.DestID != "" {
			meta["dest_id"] = job.DestID
		}
		if len(companionFields) > 0 {
			meta["dest_fields"] = companionFields
		}
		out, err := dst.Write(ctx, job.SourcePath, segSrc, meta)
		if err != nil {
			return jobResult{}, nil, fmt.Errorf("push: %w", err)
		}
		return jobResult{destID: out.Path}, presyncedPaths, nil

	default:
		return jobResult{}, nil, fmt.Errorf("unsupported event kind %q", job.Kind)
	}
}

func (e *Engine) terminate(ctx context.Context, log *slog.Logger, job *store.Job, cause error) {
	log.Error("job terminated", "err", cause)
	if err := e.store.FailJob(ctx, job.ID, job.Attempts, cause.Error()); err != nil {
		log.Error("FailJob failed", "err", err)
	}
}

type eventPayload struct {
	Kind     string         `json:"kind"`
	Path     string         `json:"path"`
	Size     int64          `json:"size"`
	Hash     string         `json:"hash,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// backoff returns a delay for the given attempt number using a doubling
// schedule capped at channel.MaxRetryBackoff, with ±25% random jitter
// applied to the final value. Jitter desynchronizes a burst of
// simultaneously-failing jobs so they don't retry in lockstep and create
// a second thundering herd against the same backend.
func backoff(base time.Duration, attempts int) time.Duration {
	d := base
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= channel.MaxRetryBackoff {
			d = channel.MaxRetryBackoff
			break
		}
	}
	return applyJitter(d)
}

// applyJitter returns d scaled by a random factor in [0.75, 1.25]. Split
// out so tests can verify the jitter window without needing to seed RNG.
func applyJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// rand/v2 is automatically seeded per program start; no global lock,
	// no manual Seed call needed.
	factor := 0.75 + rand.Float64()*0.5
	return time.Duration(float64(d) * factor)
}
