package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/extract"
	"github.com/timw255/uplink/internal/store"
)

// CompanionJobKind is the value stored in store.Job.Kind for jobs that
// run a companion script against an already-existing Aprimo record.
// Distinct from connector.Event* kinds so the worker switch picks the
// companion branch cleanly.
const CompanionJobKind = "Companion"

// companionPayload is the JSON-marshaled body carried on a
// CompanionJobKind job. The dispatcher fills it in at enqueue time;
// the worker re-applies the pattern's inverse resolver to rebuild
// MatchInfo for the script.
type companionPayload struct {
	// ParentAssetPath is the source-side path of the parent asset,
	// recorded so the worker can populate uplink.asset.path and so
	// debug logs identify what the companion belongs to without an
	// extra sync_log read.
	ParentAssetPath string `json:"parent_asset_path"`

	// CompanionPath is the source-side path of the companion file
	// itself. Same as Job.SourcePath; duplicated in the payload so the
	// worker has a self-contained record.
	CompanionPath string `json:"companion_path"`

	// PatternRaw is the original pattern text from the companion
	// declaration. The worker uses it to find the matching *Companion
	// in the channel registry (and to apply the inverse resolver
	// against CompanionPath to rebuild MatchInfo).
	PatternRaw string `json:"pattern_raw"`

	// Deleted is true when the source event was EventDelete — the
	// script runs with uplink.file.deleted = true and no content.
	Deleted bool `json:"deleted,omitempty"`
}

// dispatchCompanions handles the subset of events the caller has
// already classified as companion-matched. For each event:
//
//  1. Look up the parent asset's sync_log row via (channel, stem, dir).
//  2. If found, enqueue a CompanionJobKind job carrying the parent's
//     record id, the pattern reference, and the deleted flag.
//  3. If not found, drop the event silently — the parent's eventual
//     Create flow runs a directory sweep (Phase 7) that re-emits
//     present companions, so the sidecar-first case still gets handled.
//
// Errors from sync_log lookup or enqueue are collected; one bad lookup
// doesn't stop the batch.
func (e *Engine) dispatchCompanions(ctx context.Context, classified []companionRoutedEvent) error {
	if len(classified) == 0 {
		return nil
	}
	var errs []error
	jobs := make([]store.Job, 0, len(classified))
	for _, c := range classified {
		stem, dir := companionParentStemDir(c.match.Match)
		parent, err := e.store.LookupByStem(ctx, c.match.Channel.Spec.Name, stem, dir)
		if err != nil {
			errs = append(errs, fmt.Errorf("companion lookup parent (%s): %w", c.event.Entry.Path, err))
			continue
		}
		if parent == nil {
			e.logger.Debug("companion event dropped: parent not yet synced",
				"channel", c.match.Channel.Spec.Name,
				"path", c.event.Entry.Path,
				"stem", stem,
				"dir", dir)
			continue
		}
		payload, err := json.Marshal(companionPayload{
			ParentAssetPath: parent.SourcePath,
			CompanionPath:   c.event.Entry.Path,
			PatternRaw:      c.match.Companion.Pattern.Raw(),
			Deleted:         c.event.Kind == connector.EventDelete,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("companion marshal payload: %w", err))
			continue
		}
		jobs = append(jobs, store.Job{
			ChannelName:     c.match.Channel.Spec.Name,
			Kind:            CompanionJobKind,
			SourceConnector: c.event.Connector,
			SourcePath:      c.event.Entry.Path,
			SourceVersion:   c.event.Entry.Hash,
			DestID:  parent.DestID,
			Payload:         payload,
		})
	}
	if len(jobs) == 0 {
		return errors.Join(errs...)
	}
	ids, err := e.store.EnqueueJobs(ctx, jobs)
	if err != nil {
		errs = append(errs, fmt.Errorf("companion enqueue: %w", err))
	} else {
		e.logger.Info("companion jobs enqueued", "count", len(ids))
	}
	return errors.Join(errs...)
}

// companionRoutedEvent pairs a source event with its successful
// channel-registry match. Filled in by DispatchBatch's pre-pass.
type companionRoutedEvent struct {
	event connector.Event
	match *channel.CompanionMatch
}

// warnShadowedAssetMatch logs a WARN-level entry when a companion
// classification suppressed an asset match that an OTHER (or same)
// channel's filter would have accepted. Operators should investigate
// these — they generally indicate contradictory or stale config (e.g.,
// a path declared as a companion whose extension is also matched by
// the channel's asset filter). The companion route always wins; this
// is purely diagnostic.
func (e *Engine) warnShadowedAssetMatch(sourceConnector string, ev connector.Event, companionMatches []*channel.CompanionMatch) {
	var shadowed []string
	for _, ch := range e.channels.ChannelsForSource(sourceConnector) {
		ok, err := ch.Match(ev)
		if err != nil || !ok {
			continue
		}
		shadowed = append(shadowed, ch.Spec.Name)
	}
	if len(shadowed) == 0 {
		return
	}
	companions := make([]string, len(companionMatches))
	for i, m := range companionMatches {
		companions[i] = m.Channel.Spec.Name
	}
	e.logger.Warn("companion classification suppressed an asset match; check for contradictory channel config",
		"path", ev.Entry.Path,
		"kind", string(ev.Kind),
		"source", sourceConnector,
		"companion_channels", companions,
		"shadowed_asset_channels", shadowed)
}

// companionParentStemDir derives the (stem, dir) lookup key for the
// parent asset's sync_log row from a companion pattern match.
//
// The companion match captured `Basename` (the parent's basename
// without extension) and `Dir` (the parent's directory). The parent
// asset's `source_stem` column in sync_log is the FULL path with
// final extension stripped — `photos/sunset.jpg` → `photos/sunset`.
// So we recompose: dir + "/" + basename, or just basename when dir
// is empty (top-level files).
func companionParentStemDir(m *channel.Match) (stem, dir string) {
	if m.Dir == "" {
		return m.Basename, ""
	}
	return m.Dir + "/" + m.Basename, m.Dir
}

// executeCompanion runs one CompanionJobKind job: read the companion
// file, run the declared script with companion-shaped input, PATCH the
// returned fields onto the parent's Aprimo record via the
// destination's MetadataWriter interface.
//
// Failures route through the normal job retry path. An empty field
// list from the script is a legitimate no-op outcome — WriteMetadata
// itself short-circuits when fields are empty, so the script's
// "do nothing" return doesn't generate API calls.
func (e *Engine) executeCompanion(ctx context.Context, job *store.Job) (jobResult, error) {
	if job.DestID == "" {
		return jobResult{}, fmt.Errorf("companion job missing DestID")
	}

	var pl companionPayload
	if err := json.Unmarshal(job.Payload, &pl); err != nil {
		return jobResult{}, fmt.Errorf("companion payload: %w", err)
	}

	ch := e.channels.Lookup(job.ChannelName)
	if ch == nil {
		return jobResult{}, fmt.Errorf("channel %q no longer exists", job.ChannelName)
	}

	// Find the *Companion declaration that produced this job. Pattern
	// raw is the stable key. A config edit between enqueue and execute
	// that removed the pattern lands here as a hard failure — the job
	// will exhaust retries and end up in failed/.
	var companion *channel.Companion
	for _, co := range ch.Companions {
		if co.Pattern.Raw() == pl.PatternRaw {
			companion = co
			break
		}
	}
	if companion == nil {
		return jobResult{}, fmt.Errorf("no companion declaration with pattern %q on channel %q", pl.PatternRaw, job.ChannelName)
	}

	script, ok := companion.Script.(*extract.Script)
	if !ok {
		return jobResult{}, fmt.Errorf("companion %q compiled script is %T, want *extract.Script", pl.PatternRaw, companion.Script)
	}

	src, ok := e.connectors.Get(job.SourceConnector)
	if !ok {
		return jobResult{}, fmt.Errorf("source connector %q not running", job.SourceConnector)
	}
	dst, ok := e.connectors.Get(ch.Spec.Destination)
	if !ok {
		return jobResult{}, fmt.Errorf("destination connector %q not running", ch.Spec.Destination)
	}
	mw, ok := dst.(connector.MetadataWriter)
	if !ok {
		return jobResult{}, fmt.Errorf("destination %q (%T) does not support metadata-only writes", ch.Spec.Destination, dst)
	}

	// Rebuild the match info so the script's uplink.match reflects what
	// originally caused the dispatch.
	m := companion.Pattern.Match(pl.CompanionPath)
	if m == nil {
		return jobResult{}, fmt.Errorf("companion %q pattern no longer matches path %q", pl.PatternRaw, pl.CompanionPath)
	}

	file := extract.CompanionFile{Path: pl.CompanionPath}
	if pl.Deleted {
		file.Deleted = true
	} else {
		body, readErr := readAllCompanion(ctx, src, pl.CompanionPath)
		if readErr != nil {
			if errors.Is(readErr, connector.ErrNotFound) {
				// File vanished between the event firing and this job
				// claiming it. Race against a fast deletion, an
				// .uplinkignore update mid-flight, or a cloud
				// connector returning a stale listing. Run the script
				// as if it were a delete so the operator at least
				// sees a consistent terminal state, but warn so a
				// "metadata vanished and I don't know why" report has
				// a breadcrumb to follow.
				e.logger.Warn("companion job: file unreadable at execute time; running script with deleted=true",
					"channel", job.ChannelName,
					"path", pl.CompanionPath,
					"parent", pl.ParentAssetPath,
					"reason", "ErrNotFound")
				file.Deleted = true
			} else {
				return jobResult{}, fmt.Errorf("read companion %s: %w", pl.CompanionPath, readErr)
			}
		} else {
			file.Content = body
		}
	}

	in := extract.CompanionInput{
		Channel:       job.ChannelName,
		Asset:         extract.AssetInfo{Path: pl.ParentAssetPath},
		AssetRecordID: job.DestID,
		File:          file,
		Match: extract.MatchInfo{
			Pattern:   m.Pattern,
			Basename:  m.Basename,
			Extension: m.Extension,
			Vars:      m.Vars,
			Wildcards: m.Wildcards,
		},
	}
	fields, err := script.RunCompanion(ctx, in)
	if err != nil {
		return jobResult{}, fmt.Errorf("companion script %q: %w", pl.PatternRaw, err)
	}

	meta := map[string]any{
		"_job_id":           job.ID,
		"_channel":          job.ChannelName,
		"_source_connector": job.SourceConnector,
		"_source_version":   job.SourceVersion,
	}
	if len(fields) > 0 {
		meta["dest_fields"] = fields
	}
	if err := mw.WriteMetadata(ctx, job.DestID, meta); err != nil {
		return jobResult{}, fmt.Errorf("companion WriteMetadata: %w", err)
	}
	return jobResult{destID: job.DestID}, nil
}

// readAllCompanion pulls the companion's full bytes into memory. The
// memory ceiling is bounded by the script runtime's per-file cap (the
// script can't see more than maxScriptFileBytes anyway), so we don't
// stream — the script needs the whole content as a single Lua string.
func readAllCompanion(ctx context.Context, src connector.Connector, p string) ([]byte, error) {
	rc, err := src.Read(ctx, p)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(io.LimitReader(rc, maxCompanionBytes))
}

// maxCompanionBytes caps how many bytes we will read for a companion
// file before truncating. Held here as a package-local constant to
// avoid coupling the engine to internal extract constants.
const maxCompanionBytes int64 = 64 << 20 // 64 MiB

// stemAndDirFromPath produces the same (stem, dir) decomposition the
// store's InsertSyncLog records, exposed here for the completion sweep
// and for tests that need to verify dispatch routing without reaching
// into the store package.
//
// path → strip last extension to get stem; everything before the last
// `/` is the dir. Top-level files get dir = "".
func stemAndDirFromPath(p string) (stem, dir string) {
	d, _ := path.Split(p)
	d = strings.TrimSuffix(d, "/")
	ext := path.Ext(p)
	return strings.TrimSuffix(p, ext), d
}

// presyncCompanions runs before an asset Create. It lists the parent's
// directory via the source connector, finds every file matching one of
// THIS channel's companion patterns, executes each declared script
// against the file's content, and returns the merged field list that
// the worker folds directly into the destination's Create payload.
//
// The optimization vs a post-Create sweep: companion metadata lands on
// the record in the SAME API call that creates it. No separate PATCH
// per companion. For an asset with five language-caption companions,
// that's six API calls collapsed into one.
//
// Errors from individual companion scripts propagate — a malformed
// script blocks the parent's Create. That's the tradeoff for the
// efficient single-call create: an irrecoverable script error means
// the asset can't be safely created with partial metadata. Retry runs
// the same path and lands the same outcome.
//
// Companions that arrive AFTER the Create completes fire their own
// events normally and route through dispatchCompanions, which finds
// the now-existing parent in sync_log and enqueues a PATCH job.
func (e *Engine) presyncCompanions(ctx context.Context, ch *channel.Channel, src connector.Connector, parentPath string) ([]any, []string, error) {
	if len(ch.Companions) == 0 {
		return nil, nil, nil
	}
	_, parentDir := stemAndDirFromPath(parentPath)
	entries, err := src.List(ctx, parentDir)
	if err != nil {
		return nil, nil, fmt.Errorf("companion presync list %q: %w", parentDir, err)
	}

	var (
		merged    []any
		processed []string
	)
	for _, entry := range entries {
		if entry.Path == parentPath {
			continue
		}
		m := ch.MatchCompanion(entry.Path)
		if m == nil {
			continue
		}
		// List is recursive across all source connectors — the call
		// above returns entries from sibling subdirectories too.
		// Companion patterns match the basename only, so a file at
		// e.g. photos/year-2026/month-05/sunset.xmp would match the
		// pattern even though it's nowhere near photos/sunset.jpg.
		// Constrain matches to entries actually in the parent's
		// directory.
		if m.Match.Dir != parentDir {
			continue
		}
		body, err := readAllCompanion(ctx, src, entry.Path)
		if err != nil {
			if errors.Is(err, connector.ErrNotFound) {
				// Vanished between List and Read. Rare in practice
				// (operator deleted the file mid-scan, or a cloud
				// connector returned a stale prefix listing) but the
				// silent skip would make debugging "why didn't this
				// metadata land?" hard, so leave a breadcrumb.
				e.logger.Info("companion presync: file vanished between list and read; skipping",
					"channel", ch.Spec.Name,
					"path", entry.Path,
					"parent", parentPath)
				continue
			}
			return nil, nil, fmt.Errorf("companion presync read %s: %w", entry.Path, err)
		}
		script, ok := m.Companion.Script.(*extract.Script)
		if !ok {
			return nil, nil, fmt.Errorf("companion %q compiled script is %T, want *extract.Script", m.Companion.Pattern.Raw(), m.Companion.Script)
		}
		fields, err := script.RunCompanion(ctx, extract.CompanionInput{
			Channel: ch.Spec.Name,
			Asset:   extract.AssetInfo{Path: parentPath},
			// AssetRecordID is intentionally left empty — the record
			// doesn't exist yet. A presync script that needs the id
			// can check for "" and skip the field write; in practice
			// presync scripts only emit field values and never
			// reference the record id directly.
			File: extract.CompanionFile{Path: entry.Path, Content: body},
			Match: extract.MatchInfo{
				Pattern:   m.Match.Pattern,
				Basename:  m.Match.Basename,
				Extension: m.Match.Extension,
				Vars:      m.Match.Vars,
				Wildcards: m.Match.Wildcards,
			},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("companion presync script %q against %s: %w", m.Companion.Pattern.Raw(), entry.Path, err)
		}
		merged = append(merged, fields...)
		processed = append(processed, entry.Path)
	}
	return merged, processed, nil
}

// sweepLateArrivingCompanions handles a narrow race window: between
// presyncCompanions' List call and the parent's Create RPC returning,
// a NEW companion file might appear on disk, fire a scan event, and
// be silently dropped by dispatchCompanions because the parent's
// sync_log row didn't exist yet at dispatch time. The scan still
// records the file in connector_state, so no future scan re-emits it —
// without this sweep the companion would be orphaned forever.
//
// The sweep re-lists the parent's directory after Create finalize
// (sync_log row now exists) and dispatches companion events for any
// matching file NOT in presync's processed set. The common case (no
// race) costs one extra List call against the source — no wasted
// scripts, no wasted PATCHes. Only the race case triggers actual
// dispatch + script execution + PATCH for the late-arrivers.
//
// CONTRACT — best-effort safety net. The sweep is fire-and-forget
// after the parent's job has already succeeded. Failures here
// (cancellation mid-sweep, transient List error, EnqueueJobs
// transaction failure) are logged at WARN and otherwise ignored.
// Recovery in those cases is via normal channel-event flow: any
// subsequent touch of the orphaned companion fires its own scan
// event, which finds the parent's sync_log row (already inserted by
// finalize) and routes through dispatchCompanions normally. We do
// NOT block the parent's job completion on sweep success — coupling
// Create durability to companion dispatch would make Create flaky
// for reasons unrelated to the asset itself.
func (e *Engine) sweepLateArrivingCompanions(ctx context.Context, job *store.Job, ch *channel.Channel, presyncedPaths []string) {
	if ch == nil || len(ch.Companions) == 0 {
		return
	}
	src, ok := e.connectors.Get(job.SourceConnector)
	if !ok {
		e.logger.Warn("post-create sweep skipped: source connector not running",
			"channel", ch.Spec.Name, "connector", job.SourceConnector)
		return
	}
	_, parentDir := stemAndDirFromPath(job.SourcePath)
	entries, err := src.List(ctx, parentDir)
	if err != nil {
		e.logger.Warn("post-create sweep list failed",
			"channel", ch.Spec.Name, "dir", parentDir, "err", err)
		return
	}
	presyncedSet := make(map[string]struct{}, len(presyncedPaths))
	for _, p := range presyncedPaths {
		presyncedSet[p] = struct{}{}
	}
	var routed []companionRoutedEvent
	for _, entry := range entries {
		if entry.Path == job.SourcePath {
			continue
		}
		if _, done := presyncedSet[entry.Path]; done {
			continue
		}
		m := ch.MatchCompanion(entry.Path)
		if m == nil {
			continue
		}
		if m.Match.Dir != parentDir {
			continue
		}
		routed = append(routed, companionRoutedEvent{
			event: connector.Event{
				Connector: job.SourceConnector,
				Kind:      connector.EventCreate,
				Entry:     entry,
			},
			match: m,
		})
	}
	if len(routed) == 0 {
		return
	}
	e.logger.Info("post-create sweep dispatching late-arriving companions",
		"channel", ch.Spec.Name, "count", len(routed))
	if err := e.dispatchCompanions(ctx, routed); err != nil {
		e.logger.Warn("post-create sweep dispatch errors",
			"channel", ch.Spec.Name, "err", err)
	}
}
