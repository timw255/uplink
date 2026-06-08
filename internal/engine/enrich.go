package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/extract"
	"github.com/timw255/uplink/internal/store"
)

// EnrichDeleteJobKind is the value stored in store.Job.Kind for jobs
// that run a channel's enrich scripts against a source-side DELETE.
// Create/Update enrich runs inline in the asset job (its fields fold
// into the same Write call), so it needs no distinct kind; only the
// delete case — which has no content transfer and PATCHes an existing
// record — gets its own job kind, dispatched like a companion.
const EnrichDeleteJobKind = "EnrichDelete"

// enrichDeletePayload is the JSON body carried on an EnrichDeleteJobKind
// job. The worker re-runs the channel's enrich scripts against this
// asset path with the delete signal and PATCHes the result.
type enrichDeletePayload struct {
	// AssetPath is the source-side path of the (now-deleted) asset.
	AssetPath string `json:"asset_path"`
}

// runEnrichers runs every enrich script declared on ch against one
// asset lifecycle event and returns the merged field list. Returns
// (nil, nil) when the channel declares no enrichers — callers can fold
// the result into a Create/Update Write or a delete PATCH without a
// guard.
//
// recordID is empty during a Create (the record doesn't exist yet) and
// populated for Update/Delete; a script can branch on it but typically
// just emits field values. A single script error aborts the whole set:
// partial enrichment is worse than a retryable failure that re-runs the
// lot.
func (e *Engine) runEnrichers(
	ctx context.Context,
	ch *channel.Channel,
	assetPath string,
	size int64,
	hash string,
	recordID string,
	kind connector.EventKind,
	deleted bool,
) ([]any, error) {
	if !ch.HasEnrichers() {
		return nil, nil
	}
	ext := strings.TrimPrefix(path.Ext(assetPath), ".")
	var merged []any
	for _, en := range ch.Enrichers {
		script, ok := en.Script.(*extract.Script)
		if !ok {
			return nil, fmt.Errorf("enrich compiled script is %T, want *extract.Script", en.Script)
		}
		fields, err := script.RunAsset(ctx, extract.AssetScriptInput{
			Channel:       ch.Spec.Name,
			Asset:         extract.AssetInfo{Path: assetPath, Size: size, Hash: hash},
			AssetRecordID: recordID,
			Extension:     ext,
			Event:         extract.AssetEvent{Kind: string(kind), Deleted: deleted},
		})
		if err != nil {
			return nil, fmt.Errorf("enrich script %q: %w", script.Name(), err)
		}
		merged = append(merged, fields...)
	}
	return merged, nil
}

// dispatchEnrichDeletes turns source-side DELETE events into
// EnrichDelete jobs for every channel that (a) declares enrich scripts
// and (b) names OnDelete in its trigger. The job PATCHes the asset's
// existing record — Uplink never deletes the destination record, but a
// script may want to flip a field ("Archived") in response.
//
// A delete whose asset was never synced (no sync_log row, or a row with
// no dest_id) is dropped: there's no record to PATCH.
func (e *Engine) dispatchEnrichDeletes(ctx context.Context, sourceConnector string, events []connector.Event) error {
	if len(events) == 0 {
		return nil
	}
	var errs []error
	for _, ch := range e.channels.ChannelsForSource(sourceConnector) {
		if !ch.HasEnrichers() || !ch.FiresOn(connector.EventDelete) {
			continue
		}
		paths := make([]string, len(events))
		for i, ev := range events {
			paths[i] = ev.Entry.Path
		}
		existing, err := e.store.LookupLatestBatch(ctx, ch.Spec.Name, paths)
		if err != nil {
			errs = append(errs, fmt.Errorf("enrich-delete lookup (%s): %w", ch.Spec.Name, err))
			continue
		}
		var jobs []store.Job
		for _, ev := range events {
			prior := existing[ev.Entry.Path]
			if prior == nil || prior.DestID == "" {
				e.logger.Debug("enrich-delete dropped: asset never synced",
					"channel", ch.Spec.Name, "path", ev.Entry.Path)
				continue
			}
			payload, err := json.Marshal(enrichDeletePayload{AssetPath: ev.Entry.Path})
			if err != nil {
				errs = append(errs, fmt.Errorf("enrich-delete marshal: %w", err))
				continue
			}
			jobs = append(jobs, store.Job{
				ChannelName:     ch.Spec.Name,
				Kind:            EnrichDeleteJobKind,
				SourceConnector: sourceConnector,
				SourcePath:      ev.Entry.Path,
				DestID:          prior.DestID,
				Payload:         payload,
			})
		}
		if len(jobs) == 0 {
			continue
		}
		ids, err := e.store.EnqueueJobs(ctx, jobs)
		if err != nil {
			errs = append(errs, fmt.Errorf("enrich-delete enqueue (%s): %w", ch.Spec.Name, err))
			continue
		}
		e.logger.Info("enrich-delete jobs enqueued", "channel", ch.Spec.Name, "count", len(ids))
	}
	return errors.Join(errs...)
}

// executeEnrichDelete runs one EnrichDeleteJobKind job: re-run the
// channel's enrich scripts with the delete signal and PATCH the merged
// fields onto the asset's existing record via MetadataWriter. The
// record itself is never deleted — same contract as a companion delete.
// An empty field set is a legitimate no-op (WriteMetadata short-circuits
// when there are no fields), so a script that returns {} on delete costs
// no API call.
func (e *Engine) executeEnrichDelete(ctx context.Context, job *store.Job) (jobResult, error) {
	if job.DestID == "" {
		return jobResult{}, fmt.Errorf("enrich-delete job missing DestID")
	}

	var pl enrichDeletePayload
	if err := json.Unmarshal(job.Payload, &pl); err != nil {
		return jobResult{}, fmt.Errorf("enrich-delete payload: %w", err)
	}

	ch := e.channels.Lookup(job.ChannelName)
	if ch == nil {
		return jobResult{}, fmt.Errorf("channel %q no longer exists", job.ChannelName)
	}
	dst, ok := e.connectors.Get(ch.Spec.Destination)
	if !ok {
		return jobResult{}, fmt.Errorf("destination connector %q not running", ch.Spec.Destination)
	}
	mw, ok := dst.(connector.MetadataWriter)
	if !ok {
		return jobResult{}, fmt.Errorf("destination %q (%T) does not support metadata-only writes", ch.Spec.Destination, dst)
	}

	// Asset is gone, so size/hash are unknown — the script reacts to the
	// path and the delete signal, not the (vanished) bytes.
	fields, err := e.runEnrichers(ctx, ch, pl.AssetPath, 0, "", job.DestID, connector.EventDelete, true)
	if err != nil {
		return jobResult{}, err
	}

	meta := map[string]any{
		"_job_id":           job.ID,
		"_channel":          job.ChannelName,
		"_source_connector": job.SourceConnector,
	}
	if len(fields) > 0 {
		meta["dest_fields"] = fields
	}
	if err := mw.WriteMetadata(ctx, job.DestID, meta); err != nil {
		return jobResult{}, fmt.Errorf("enrich-delete WriteMetadata: %w", err)
	}
	return jobResult{destID: job.DestID}, nil
}
