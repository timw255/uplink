// Package aprimo is the Aprimo DAM connector. Channels are
// unidirectional storage → Aprimo, so this connector only implements
// the destination side: Write uploads bytes (segmented) and creates or
// updates a record, Delete removes one, Stat fetches metadata.
//
// Crash safety is via the upload marker state machine
// (data/uploads/<job_id>.session.json). On every claim the connector
// reads the marker and resumes from whichever of the three states
// (uploading | committed | created) the previous attempt left off in.
// The result is exactly-once Aprimo record creation per
// (channel, source_path, source_version) regardless of where a crash
// falls. See internal/store/markers.go for the contract.
package aprimo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// markerStore is the slice of *store.Store the connector calls. Held as
// an interface so tests can wrap the real store with a decorator that
// fails one method — see failingDeleteMarkerStore in connector_test.go.
// Pool passes the real *store.Store at startup.
type markerStore interface {
	LoadMarker(jobID string) (*store.UploadMarker, error)
	SaveMarker(m *store.UploadMarker) error
	DeleteMarker(jobID string) error
}

// Connector is one configured Aprimo instance.
type Connector struct {
	name  string
	cfg   *Config
	api   *aprimo.Client
	store markerStore // set by Pool via UseStore

	// blob uploads file bytes straight to Aprimo's Azure Blob storage via
	// the SAS the upload API returns, bypassing the rate-limited Aprimo
	// upload service. nil disables the direct path (used by tests that
	// exercise the segmented uploader). directUpload gates it on top.
	blob         blobUploader
	directUpload bool

	// resolver is updated atomically by the background refresh
	// goroutine so callers always see a consistent catalog snapshot.
	resolver atomic.Pointer[resolver]

	// refresh goroutine lifecycle. Cancelled by Close.
	refreshCancel context.CancelFunc
	refreshDone   chan struct{}
}

// Factory adapts the raw YAML block. It is the value registered with
// the global connector registry.
func Factory(name string, raw map[string]any) (connector.Connector, error) {
	cfg, err := loadConfig(name, raw)
	if err != nil {
		return nil, err
	}
	api, err := aprimo.New(aprimo.Config{
		Environment:   cfg.Environment,
		ClientID:      cfg.ClientID,
		ClientSecret:  cfg.ClientSecret,
		HTTPTimeout:   cfg.HTTPTimeout,
		MaxConcurrent: cfg.MaxConcurrent,
		RPS:           cfg.RPS,
	})
	if err != nil {
		return nil, err
	}
	return &Connector{
		name:         name,
		cfg:          cfg,
		api:          api,
		blob:         newBlockUploader(cfg.DirectUploadConcurrency, directBlockSize),
		directUpload: cfg.DirectUpload,
	}, nil
}

// NewForTest constructs a Connector with a pre-built Aprimo client
// and store. Test-only — production callers go through Factory.
func NewForTest(name string, client *aprimo.Client, st *store.Store) *Connector {
	return &Connector{
		name:  name,
		cfg:   &Config{DefaultStatus: "draft"},
		api:   client,
		store: st,
	}
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return c.name }

// Init prefetches the tenant's catalogs (field definitions, languages,
// classifications, option items, users, user groups) so name→id
// resolution for companion-script-supplied metadata is a pure in-memory
// lookup at write time. Authentication happens here on the first API
// call (the SDK's token provider is lazy).
//
// When cfg.RefreshInterval > 0 (default 1h), a background goroutine
// is launched that periodically rebuilds the catalogs so new fields
// added in Aprimo become visible without a daemon restart. Refresh
// errors are logged at WARN and do NOT replace the working catalog
// — the old maps stay in place until a successful refresh.
//
// If the connector has no `default_language` configured, prefetch
// still runs but the resolver's default-language fallback is unset
// — any companion-script field entry that omits `language` will fail
// loudly.
func (c *Connector) Init(ctx context.Context) error {
	res, err := buildResolver(ctx, c.api, c.cfg.DefaultLanguage)
	if err != nil {
		return fmt.Errorf("aprimo[%s]: %w", c.name, err)
	}
	c.resolver.Store(res)

	if c.cfg.RefreshInterval > 0 {
		refreshCtx, cancel := context.WithCancel(context.Background())
		c.refreshCancel = cancel
		c.refreshDone = make(chan struct{})
		go c.runRefreshLoop(refreshCtx)
	}
	return nil
}

// runRefreshLoop rebuilds the catalogs on a ticker. Failed refreshes
// log a warning but do not replace the resolver — companion scripts
// keep using the last-good catalog snapshot.
func (c *Connector) runRefreshLoop(ctx context.Context) {
	defer close(c.refreshDone)
	t := time.NewTicker(c.cfg.RefreshInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fresh, err := buildResolver(ctx, c.api, c.cfg.DefaultLanguage)
			if err != nil {
				slog.Warn("aprimo: catalog refresh failed; keeping prior snapshot",
					"connector", c.name, "err", err)
				continue
			}
			c.resolver.Store(fresh)
			slog.Debug("aprimo: catalog refreshed", "connector", c.name)
		}
	}
}

// Close releases resources, including stopping the background
// catalog-refresh goroutine if one is running. Bounded wait so a
// stuck refresh doesn't hang shutdown.
func (c *Connector) Close() error {
	if c.refreshCancel != nil {
		c.refreshCancel()
		select {
		case <-c.refreshDone:
		case <-time.After(5 * time.Second):
			slog.Warn("aprimo: refresh goroutine did not exit within 5s", "connector", c.name)
		}
	}
	// Stop the direct-upload worker pool (and its controller/watcher) so
	// the connector doesn't leak goroutines when rebuilt within a process.
	if closer, ok := c.blob.(interface{ Close() }); ok {
		closer.Close()
	}
	return nil
}

// UseStore implements connector.StoreAware. Pool calls this so the
// connector can read/write upload markers for crash-safe resume. We
// type-assert to the narrow markerStore interface rather than the
// concrete *store.Store so tests can pass a decorator. The Pool always
// hands us a real *store.Store, which satisfies markerStore.
func (c *Connector) UseStore(s any) {
	if ms, ok := s.(markerStore); ok {
		c.store = ms
	}
}

// --- Connector destination side ---

// Write uploads bytes and either creates a new record or updates an
// existing one. It is the entry point for the marker state machine.
//
// Meta keys the engine populates:
//   - "_job_id"            (string) — required for marker tracking
//   - "dest_id"   (string) — non-empty drives the Update flow
//   - "dest_fields"      ([]any)  — list of {id, value} entries from
//     companion scripts; wrapped into Aprimo's addOrUpdate payload at
//     upload time
//   - "aprimo_segment_size", "aprimo_parallel_segments" — uploader tuning
//
// The marker is created (state=uploading) before any byte is sent, and
// is deleted by the engine after sync_log is written. Every crash window
// leaves the marker in a state from which the next claim drives exactly
// the right next-step operation without re-uploading bytes or creating
// duplicate records.
func (c *Connector) Write(
	ctx context.Context,
	srcPath string,
	body connector.SegmentSource,
	meta map[string]any,
) (connector.Entry, error) {
	filename := c.filenameFor(srcPath)

	jobID, _ := meta["_job_id"].(string)
	preferredRecordID, _ := meta["dest_id"].(string)

	var marker *store.UploadMarker
	if c.store != nil && jobID != "" {
		m, err := c.store.LoadMarker(jobID)
		if err != nil {
			return connector.Entry{}, fmt.Errorf("aprimo[%s]: load marker: %w", c.name, err)
		}
		marker = m
	}

	// Already finished an earlier attempt — short-circuit straight to
	// engine, which will insert sync_log (idempotently) and delete the
	// marker.
	if marker != nil && marker.State == store.MarkerCreated {
		return c.entryFor(marker.DestID, body.Size(), filename), nil
	}

	// If we've already committed the upload, skip straight to the
	// record step using the retained token.
	if marker != nil && marker.State == store.MarkerCommitted {
		recordID, err := c.applyRecord(ctx, marker.UploadToken, filename, preferredRecordID, meta)
		if err != nil {
			if errors.Is(err, aprimo.ErrUploadTokenMissing) {
				c.resetUploadForRetry(jobID, "token purged between commit and create on resume")
				return connector.Entry{}, fmt.Errorf("aprimo[%s]: upload token purged for %s; will re-upload on retry: %w", c.name, filename, err)
			}
			return connector.Entry{}, fmt.Errorf("aprimo[%s]: apply record (resumed) %s: %w", c.name, filename, err)
		}
		c.transitionToCreated(marker, recordID)
		return c.entryFor(recordID, body.Size(), filename), nil
	}

	// Otherwise: do the upload (fresh or resumed). The chosen path owns
	// the marker — it loads/creates one if needed, drives the on-disk
	// transitions, and returns the committed marker so we can advance it
	// to "created" after the record write.
	token, postUploadMarker, err := c.uploadBytes(ctx, body, filename, marker, jobID, srcPath, meta)
	if err != nil {
		if errors.Is(err, aprimo.ErrUploadTokenMissing) {
			// A resumed segmented session referenced a token Aprimo had
			// already purged. Drop the marker so the next attempt sets
			// up a fresh session from scratch.
			c.resetUploadForRetry(jobID, "segmented session purged on resume")
			return connector.Entry{}, fmt.Errorf("aprimo[%s]: segmented upload session purged for %s; will re-upload on retry: %w", c.name, filename, err)
		}
		return connector.Entry{}, fmt.Errorf("aprimo[%s]: upload %s: %w", c.name, filename, err)
	}

	recordID, err := c.applyRecord(ctx, token, filename, preferredRecordID, meta)
	if err != nil {
		if errors.Is(err, aprimo.ErrUploadTokenMissing) {
			c.resetUploadForRetry(jobID, "token purged between commit and create")
			return connector.Entry{}, fmt.Errorf("aprimo[%s]: upload token purged for %s; will re-upload on retry: %w", c.name, filename, err)
		}
		return connector.Entry{}, fmt.Errorf("aprimo[%s]: apply record %s: %w", c.name, filename, err)
	}

	if postUploadMarker != nil {
		c.transitionToCreated(postUploadMarker, recordID)
	}
	return c.entryFor(recordID, body.Size(), filename), nil
}

// filenameFor derives the Aprimo filename from a source path: the base
// name, defaulted when empty, with characters Aprimo forbids
// (< > : " / \ | ? *) replaced — or Aprimo rejects the upload/record.
func (c *Connector) filenameFor(srcPath string) string {
	filename := path.Base(srcPath)
	if filename == "" || filename == "." || filename == "/" {
		filename = "upload"
	}
	return aprimo.SanitizeFilename(filename)
}

// UploadOnly streams a file's bytes to Aprimo storage (direct-to-blob
// when enabled) and returns the upload token WITHOUT creating a record.
// It is the upload half of the import pipeline's two stages: uploads run
// ahead, in the background, decoupled from the rate-limited record
// creation so a slow upload never throttles the create rate. No marker is
// kept — the import path is stateless, and a record that never gets
// created just re-uploads on a later run (tokens are cleaned up server-
// side after a few days anyway).
func (c *Connector) UploadOnly(
	ctx context.Context,
	srcPath string,
	body connector.SegmentSource,
	meta map[string]any,
) (string, error) {
	filename := c.filenameFor(srcPath)
	token, _, err := c.uploadBytes(ctx, body, filename, nil, "", srcPath, meta)
	if err != nil {
		return "", fmt.Errorf("aprimo[%s]: upload %s: %w", c.name, filename, err)
	}
	return token, nil
}

// CreateFromToken creates (or, with meta["dest_id"], updates) a record
// from an already-uploaded token — the create half of the pipeline. The
// returned Entry carries the record id.
func (c *Connector) CreateFromToken(
	ctx context.Context,
	srcPath, token string,
	meta map[string]any,
) (connector.Entry, error) {
	filename := c.filenameFor(srcPath)
	preferredRecordID, _ := meta["dest_id"].(string)
	recordID, err := c.applyRecord(ctx, token, filename, preferredRecordID, meta)
	if err != nil {
		return connector.Entry{}, fmt.Errorf("aprimo[%s]: create record %s: %w", c.name, filename, err)
	}
	return c.entryFor(recordID, 0, filename), nil
}

// resetUploadForRetry drops the upload marker so the next worker claim
// runs a fresh upload + record-create. Called when Aprimo has purged
// the upload behind a stale token — there is no way to recover the
// token, so the only forward path is to restart.
//
// DeleteMarker errors are logged, not returned: the caller is already
// surfacing a retryable error, and a stale marker is self-healing — the
// next attempt re-trips the same detection.
func (c *Connector) resetUploadForRetry(jobID, reason string) {
	if c.store == nil || jobID == "" {
		return
	}
	if err := c.store.DeleteMarker(jobID); err != nil {
		slog.Warn("aprimo: reset upload state for retry: delete marker failed",
			"connector", c.name, "job_id", jobID, "reason", reason, "err", err)
	}
}

func (c *Connector) entryFor(recordID string, size int64, filename string) connector.Entry {
	return connector.Entry{
		Path:    recordID,
		Size:    size,
		ModTime: time.Now().UTC(),
		Metadata: map[string]any{
			"dest_id":  recordID,
			"filename": filename,
		},
	}
}

// uploadBytes gets the file's bytes to Aprimo and returns a usable upload
// token. It prefers the direct-to-blob path (bytes go straight to Azure
// storage, off the rate-limited Aprimo API); without it — disabled by
// config or a missing blob uploader in tests — it falls back to the
// segmented upload service. Both honor the same marker contract.
func (c *Connector) uploadBytes(
	ctx context.Context,
	body connector.SegmentSource,
	filename string,
	prior *store.UploadMarker,
	jobID, srcPath string,
	meta map[string]any,
) (string, *store.UploadMarker, error) {
	if c.directUpload && c.blob != nil {
		return c.uploadDirect(ctx, body, filename, prior, jobID, srcPath, meta)
	}
	return c.upload(ctx, body, filename, prior, jobID, srcPath, meta)
}

// uploadDirect mints a SAS via the Aprimo API (one rate-limited call),
// streams the bytes straight to blob storage out-of-band, and returns the
// upload token. It honors the same marker states as the segmented path:
// uploading → committed(token) → (the caller advances to) created. A
// crash mid-upload is not block-resumable, so resume re-mints and
// re-streams from scratch; the marker's real value is the committed→created
// idempotency in Write, which keeps a retry from creating a duplicate
// record once the token is in hand.
func (c *Connector) uploadDirect(
	ctx context.Context,
	body connector.SegmentSource,
	filename string,
	prior *store.UploadMarker,
	jobID, srcPath string,
	meta map[string]any,
) (string, *store.UploadMarker, error) {
	du, err := c.api.Uploader.CreateDirectUpload(ctx, filename)
	if err != nil {
		return "", nil, err
	}

	// Defense in depth: the SAS URL decides where the file's bytes go.
	// Only ever stream them to an Azure Blob endpoint; if the upload
	// response points anywhere else (a tampered/MITM'd response, a
	// misconfiguration), fall back to the segmented upload through Aprimo's
	// own API rather than send customer bytes to an unexpected host.
	if !isAzureBlobURL(du.SASURL) {
		slog.Warn("aprimo: direct-upload SAS is not an Azure blob URL; using segmented upload instead",
			"connector", c.name)
		return c.upload(ctx, body, filename, prior, jobID, srcPath, meta)
	}

	// Stage the marker in "uploading" for crash visibility, mirroring the
	// segmented path's setup (minus the segment bookkeeping, which doesn't
	// apply to a single streamed blob).
	var current *store.UploadMarker
	if c.store != nil && jobID != "" {
		if prior != nil {
			cp := *prior
			current = &cp
			current.State = store.MarkerUploading
		} else {
			current = &store.UploadMarker{
				JobID:           jobID,
				Channel:         stringFromMeta(meta, "_channel"),
				SourceConnector: stringFromMeta(meta, "_source_connector"),
				SourcePath:      srcPath,
				SourceVersion:   stringFromMeta(meta, "_source_version"),
				Filename:        filename,
				State:           store.MarkerUploading,
			}
		}
		current.UploadPath = ""
		current.SegmentsDone = nil
		if err := c.store.SaveMarker(current); err != nil {
			return "", current, fmt.Errorf("save uploading marker: %w", err)
		}
	}

	if err := c.blob.Upload(ctx, du.SASURL, body, filename); err != nil {
		// Blob errors can echo SAS / presigned source URLs (the Azure SDK
		// dumps response bodies, and net errors carry the request URL).
		// Strip URL query strings so the embedded credentials don't reach
		// logs or the persisted job error.
		return "", current, fmt.Errorf("direct blob upload: %s", redactURLQueries(err.Error()))
	}

	// Blob exists now, so the token is usable; record it for committed→
	// created resume.
	if current != nil {
		current.State = store.MarkerCommitted
		current.UploadToken = du.Token
		if err := c.store.SaveMarker(current); err != nil {
			return "", current, fmt.Errorf("save committed marker: %w", err)
		}
	}
	return du.Token, current, nil
}

// upload runs the segmented upload, persisting progress to the marker
// (when available) so a crash can resume. Returns the final upload
// token, the on-disk marker (in state=committed), and any error.
//
// When the store or jobID isn't available (tests, lightweight flows),
// upload runs without persistence and returns a nil marker — the
// caller still gets the token for the immediate Records.Create step.
func (c *Connector) upload(
	ctx context.Context,
	body connector.SegmentSource,
	filename string,
	prior *store.UploadMarker,
	jobID, srcPath string,
	meta map[string]any,
) (string, *store.UploadMarker, error) {
	opts := uploadOptsFromMeta(meta)
	if opts == nil {
		opts = &aprimo.UploadOptions{}
	}

	// Resume from a partial upload if we have one.
	if prior != nil && prior.State == store.MarkerUploading && prior.UploadPath != "" {
		opts.Resume = &aprimo.ResumeOptions{
			UploadPath:        prior.UploadPath,
			CommittedSegments: append([]int(nil), prior.SegmentsDone...),
		}
	}

	// In production (store + job ID present), set up the marker
	// state machine. Without a store we just run the upload, no
	// persistence.
	var (
		current *store.UploadMarker
		muSave  sync.Mutex
	)
	if c.store != nil && jobID != "" {
		if prior != nil {
			// Mutate a copy so we never alias the caller's pointer.
			cp := *prior
			current = &cp
			current.State = store.MarkerUploading
		} else {
			current = &store.UploadMarker{
				JobID:           jobID,
				Channel:         stringFromMeta(meta, "_channel"),
				SourceConnector: stringFromMeta(meta, "_source_connector"),
				SourcePath:      srcPath,
				SourceVersion:   stringFromMeta(meta, "_source_version"),
				Filename:        filename,
				State:           store.MarkerUploading,
			}
		}

		saveLocked := func() error {
			return c.store.SaveMarker(current)
		}

		opts.OnSetupComplete = func(uploadPath string) {
			muSave.Lock()
			defer muSave.Unlock()
			current.UploadPath = uploadPath
			_ = saveLocked()
		}
		opts.OnSegmentCommit = func(index int) {
			muSave.Lock()
			defer muSave.Unlock()
			current.SegmentsDone = appendUnique(current.SegmentsDone, index)
			_ = saveLocked()
		}
		// If we're resuming with an existing UploadPath, the setup-complete
		// callback won't fire — save the marker as-is up front so the
		// "uploading" state is visible.
		if current.UploadPath != "" {
			func() {
				muSave.Lock()
				defer muSave.Unlock()
				_ = saveLocked()
			}()
		}
	}

	result, err := c.api.Uploader.UploadFromSource(ctx, body, filename, opts)
	if err != nil {
		return "", current, err
	}

	// Upload committed — transition to "committed" state with the
	// final token.
	if current != nil {
		err := func() error {
			muSave.Lock()
			defer muSave.Unlock()
			current.State = store.MarkerCommitted
			current.UploadToken = result.Token
			return c.store.SaveMarker(current)
		}()
		if err != nil {
			return "", current, fmt.Errorf("save committed marker: %w", err)
		}
	}
	return result.Token, current, nil
}

func (c *Connector) applyRecord(
	ctx context.Context,
	uploadToken, filename, preferredRecordID string,
	meta map[string]any,
) (string, error) {
	if preferredRecordID != "" {
		if err := c.updateRecord(ctx, preferredRecordID, uploadToken, filename, meta); err != nil {
			return "", err
		}
		return preferredRecordID, nil
	}
	return c.createRecord(ctx, uploadToken, filename, meta)
}

func (c *Connector) transitionToCreated(marker *store.UploadMarker, recordID string) {
	if c.store == nil || marker == nil {
		return
	}
	marker.State = store.MarkerCreated
	marker.DestID = recordID
	_ = c.store.SaveMarker(marker)
}

func (c *Connector) createRecord(
	ctx context.Context,
	uploadToken, filename string,
	meta map[string]any,
) (string, error) {
	req := aprimo.CreateRequest{
		Status: statusFromMeta(meta, c.cfg.DefaultStatus),
		Files:  aprimo.NewFilesFromUpload(uploadToken, filename),
	}
	fields, err := c.fieldsFromMeta(meta)
	if err != nil {
		return "", err
	}
	if len(fields) > 0 {
		req.Fields = fields
	}
	resp, err := c.api.Records.Create(ctx, req, false)
	if err != nil {
		return "", fmt.Errorf("aprimo[%s]: create record for %s: %w", c.name, filename, err)
	}
	// The bulk importer defers filing so it can batch all new records into
	// the collection in chunks (and track them in its ledger) rather than
	// one call per record. The streaming daemon files per-record here.
	deferred, _ := meta["defer_collection_add"].(bool)
	if c.cfg.DefaultCollection != "" && !deferred {
		c.fileIntoDefaultCollection(ctx, resp.ID)
	}
	return resp.ID, nil
}

// fileIntoDefaultCollection adds a freshly-created record to the
// configured default collection. Errors are logged, not returned:
// Records.Create is not idempotent, so propagating the error would
// cause the engine retry to create a duplicate record. The warning log
// carries the record id + collection id for manual re-filing.
func (c *Connector) fileIntoDefaultCollection(ctx context.Context, recordID string) {
	err := c.api.Collections.UpdateRecords(ctx, c.cfg.DefaultCollection, aprimo.UpdateCollectionRequest{
		Records: &aprimo.CollectionRecordActions{
			AddOrUpdate: []aprimo.IDRef{{ID: recordID}},
		},
	})
	if err != nil {
		slog.Warn("aprimo: file new record into default collection failed",
			"connector", c.name,
			"record_id", recordID,
			"collection_id", c.cfg.DefaultCollection,
			"err", err)
	}
}

func (c *Connector) updateRecord(
	ctx context.Context,
	recordID, uploadToken, filename string,
	meta map[string]any,
) error {
	// The importer resolves master files up front in batched search calls
	// and passes the result here, letting the update skip the per-record
	// MasterFile GET. Empty → versionedFilesForUpdate does the lookup.
	prefetchedMaster, _ := meta["dest_master_file_id"].(string)
	files, err := c.versionedFilesForUpdate(ctx, recordID, uploadToken, filename, prefetchedMaster)
	if err != nil {
		return fmt.Errorf("aprimo[%s]: update record %s: %w", c.name, recordID, err)
	}
	req := aprimo.UpdateRequest{Files: files}
	if s := statusFromMeta(meta, ""); s != "" {
		req.Status = s
	}
	fields, err := c.fieldsFromMeta(meta)
	if err != nil {
		return err
	}
	if len(fields) > 0 {
		req.Fields = fields
	}
	if err := c.api.Records.Update(ctx, recordID, req, false); err != nil {
		return fmt.Errorf("aprimo[%s]: update record %s: %w", c.name, recordID, err)
	}
	return nil
}

// versionedFilesForUpdate builds the Files payload for an Update by
// looking up the current master file on the record so the new upload
// lands as a *version* on the existing master, not a sibling file.
//
// The lookup-at-update-time approach (rather than caching the master
// file id on Create) is what keeps the daemon correct when a user has
// changed the record's master via the Aprimo UI between syncs: we
// always target whatever Aprimo currently considers master.
//
// If the record has no master (an oddly-shaped record, or one whose
// master was removed), we fall back to the Create-shape payload so the
// upload becomes a new file on the record.
func (c *Connector) versionedFilesForUpdate(
	ctx context.Context,
	recordID, uploadToken, filename, prefetchedMasterID string,
) (*aprimo.Files, error) {
	// Resolved up front in a batched search — no per-record GET needed.
	if prefetchedMasterID != "" {
		return aprimo.NewVersionFilesUpdate(uploadToken, filename, prefetchedMasterID), nil
	}
	master, err := c.api.Records.MasterFile(ctx, recordID)
	if err != nil {
		if errors.Is(err, aprimo.ErrNotFound) {
			return aprimo.NewFilesFromUpload(uploadToken, filename), nil
		}
		return nil, fmt.Errorf("fetch master file: %w", err)
	}
	if master.ID == "" {
		return aprimo.NewFilesFromUpload(uploadToken, filename), nil
	}
	return aprimo.NewVersionFilesUpdate(uploadToken, filename, master.ID), nil
}

// ResolveMasterFiles batch-resolves the current master file id for many
// records in a few search calls, so the importer's update path can skip the
// per-record MasterFile GET. Delegates to the SDK; see
// aprimo.Records.ResolveMasterFiles. The importer calls this; the streaming
// daemon doesn't (it resolves at write time to stay correct across UI edits).
func (c *Connector) ResolveMasterFiles(ctx context.Context, recordIDs []string) (map[string]string, error) {
	return c.api.Records.ResolveMasterFiles(ctx, recordIDs)
}

// DefaultCollection is the configured collection new records are filed into
// (empty when none is set). The importer reads it to decide whether to batch
// filing; with the defer_collection_add meta flag set, createRecord skips its
// per-record filing and leaves it to the importer's batched path.
func (c *Connector) DefaultCollection() string {
	return c.cfg.DefaultCollection
}

// AddRecordsToCollection files the given records into the default collection
// in chunks, instead of one call per record. No-op when no default
// collection is configured or no ids are given.
func (c *Connector) AddRecordsToCollection(ctx context.Context, recordIDs []string) error {
	if c.cfg.DefaultCollection == "" || len(recordIDs) == 0 {
		return nil
	}
	return c.api.Collections.AddRecords(ctx, c.cfg.DefaultCollection, recordIDs)
}

// WriteMetadata PATCHes fields on an existing record without touching
// its binary. Used by the companion-job worker: a companion file
// changed and the script that ran against it produced an updated field
// set for the parent asset's record. There are no bytes to upload, so
// the marker state machine doesn't apply — this is one API call.
//
// recordID must be non-empty; an empty meta["dest_fields"] (or one
// that resolves to no fields) is a legitimate no-op outcome that
// returns nil without hitting the API. The script driving this call
// signals "no metadata to write right now" by returning an empty list,
// which lands here as an absent or empty dest_fields slot.
func (c *Connector) WriteMetadata(ctx context.Context, recordID string, meta map[string]any) error {
	if recordID == "" {
		return fmt.Errorf("aprimo[%s]: WriteMetadata: recordID is empty", c.name)
	}
	fields, err := c.fieldsFromMeta(meta)
	if err != nil {
		return err
	}
	status := statusFromMeta(meta, "")
	// Nothing to write: no resolved fields and no status change. The
	// script (or import line) signalled a no-op.
	if len(fields) == 0 && status == "" {
		return nil
	}
	req := aprimo.UpdateRequest{}
	if len(fields) > 0 {
		req.Fields = fields
	}
	if status != "" {
		req.Status = status
	}
	if err := c.api.Records.Update(ctx, recordID, req, false); err != nil {
		return fmt.Errorf("aprimo[%s]: update record metadata %s: %w", c.name, recordID, err)
	}
	return nil
}

// ValidateFields runs the same field-name/value resolution that Write
// and WriteMetadata perform, returning the first error WITHOUT making
// any API call. The import command's dry-run uses it to validate a
// record's metadata against the live (prefetched) Aprimo catalog —
// catching unknown field names, bad classification paths, missing
// languages, and per-type value mismatches before anything is written.
func (c *Connector) ValidateFields(meta map[string]any) error {
	_, err := c.fieldsFromMeta(meta)
	return err
}

// RateLimit reports the sustained request rate (RPS) and in-flight
// request cap configured on this connector. A bulk-import driver anchors
// its adaptive worker pool to these so it pushes the Aprimo client's
// limiter exactly as hard as the tenant allows — no harder. maxConcurrent
// is 0 when uncapped.
func (c *Connector) RateLimit() (rps float64, maxConcurrent int) {
	return c.cfg.RPS, c.cfg.MaxConcurrent
}

// SetRateObserver attaches a rate-limiter telemetry sink to the
// underlying Aprimo client so the import scheduler can watch how close
// it is running to the configured RPS. The daemon never calls this.
func (c *Connector) SetRateObserver(obs aprimo.RateObserver) {
	c.api.SetRateObserver(obs)
}

// statusFromMeta returns the per-record status override carried in
// meta["dest_status"] (set by the import command), falling back to the
// supplied default when absent or empty.
func statusFromMeta(meta map[string]any, fallback string) string {
	if meta != nil {
		if s, ok := meta["dest_status"].(string); ok && s != "" {
			return s
		}
	}
	return fallback
}

// Delete permanently removes a record.
func (c *Connector) Delete(ctx context.Context, recordID string) error {
	if err := c.api.Records.Delete(ctx, recordID, false); err != nil {
		return fmt.Errorf("aprimo[%s]: delete %s: %w", c.name, recordID, err)
	}
	return nil
}

// Stat returns metadata for a single record by id.
func (c *Connector) Stat(ctx context.Context, recordID string) (connector.Entry, error) {
	raw, err := c.api.Records.GetByID(ctx, recordID, "")
	if err != nil {
		return connector.Entry{}, fmt.Errorf("aprimo[%s]: stat %s: %w", c.name, recordID, err)
	}
	return connector.Entry{
		Path:     recordID,
		Size:     -1,
		ModTime:  time.Now().UTC(),
		Metadata: map[string]any{"aprimo_record": json.RawMessage(raw)},
	}, nil
}

// Read returns ErrUnsupported. Aprimo is destination-only.
func (c *Connector) Read(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, connector.ErrUnsupported
}

// OpenRange returns ErrUnsupported. Aprimo is destination-only.
func (c *Connector) OpenRange(_ context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
	return nil, connector.ErrUnsupported
}

// List returns ErrUnsupported. Aprimo records are not browsed through
// Uplink; consumers query Aprimo directly.
func (c *Connector) List(_ context.Context, _ string) ([]connector.Entry, error) {
	return nil, connector.ErrUnsupported
}

// Walk returns ErrUnsupported. Aprimo is destination-only — nothing
// to stream from.
func (c *Connector) Walk(_ context.Context, _ string, _ func(connector.Entry) error) error {
	return connector.ErrUnsupported
}

// Move returns ErrUnsupported. Aprimo records don't have file-path
// identities to rename; modify fields via Update instead.
func (c *Connector) Move(_ context.Context, _, _ string) error {
	return connector.ErrUnsupported
}

// Reconcile returns ErrUnsupported. The destination side of a channel
// does not reconcile against persisted state — sync_log is authoritative.
func (c *Connector) Reconcile(
	_ context.Context,
	_ connector.StateStore,
	_ connector.ProgressFunc,
) (connector.ReconcileResult, error) {
	return connector.ReconcileResult{Connector: c.name}, connector.ErrUnsupported
}

// CurrentMarker returns the current marker state for jobID, used by
// the engine after Write returns to decide what to write to sync_log
// and whether the marker still needs deleting.
func (c *Connector) CurrentMarker(jobID string) (*store.UploadMarker, error) {
	if c.store == nil || jobID == "" {
		return nil, errors.New("aprimo connector: store or jobID missing")
	}
	return c.store.LoadMarker(jobID)
}

// --- helpers ---

func stringFromMeta(meta map[string]any, key string) string {
	if meta == nil {
		return ""
	}
	if v, ok := meta[key].(string); ok {
		return v
	}
	return ""
}

func uploadOptsFromMeta(meta map[string]any) *aprimo.UploadOptions {
	if meta == nil {
		return nil
	}
	opts := &aprimo.UploadOptions{}
	any := false
	if v, ok := meta["aprimo_segment_size"].(int); ok && v > 0 {
		opts.SegmentSize = int64(v)
		any = true
	}
	if v, ok := meta["aprimo_parallel_segments"].(int); ok && v > 0 {
		opts.ParallelSegments = v
		any = true
	}
	if !any {
		return nil
	}
	return opts
}

// fieldsFromMeta extracts the channel-supplied fields block, resolves
// companion-script-supplied `name` references to Aprimo field `id`s,
// and wraps values in the localized payload Aprimo expects. The
// resolver (populated at Init) holds the name→id and culture→id maps.
//
// Two shapes are accepted in meta["dest_fields"]:
//
//  1. []any — flat list of {name, value, language?} entries from
//     companion scripts. Resolved + wrapped here.
//  2. anything else (typically a map) — power-user escape hatch.
//     Passed through unchanged. Must already match Aprimo's API shape
//     (id + localizedValues + addOrUpdate/remove envelope).
//
// Returns (nil, nil) when there are no fields to write.
func (c *Connector) fieldsFromMeta(meta map[string]any) (json.RawMessage, error) {
	if meta == nil {
		return nil, nil
	}
	v, ok := meta["dest_fields"]
	if !ok {
		return nil, nil
	}
	switch typed := v.(type) {
	case []any:
		if len(typed) == 0 {
			return nil, nil
		}
		res := c.resolver.Load()
		if res == nil {
			return nil, fmt.Errorf("aprimo[%s]: resolver not initialized; Init must run before companion scripts fire", c.name)
		}
		resolved, err := res.resolveFieldEntries(typed)
		if err != nil {
			return nil, fmt.Errorf("aprimo[%s]: resolve companion-script fields: %w", c.name, err)
		}
		data, err := json.Marshal(map[string]any{"addOrUpdate": resolved})
		if err != nil {
			return nil, fmt.Errorf("aprimo[%s]: marshal field payload: %w", c.name, err)
		}
		return data, nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("aprimo[%s]: marshal pre-shaped field payload: %w", c.name, err)
		}
		return data, nil
	}
}

func appendUnique(s []int, v int) []int {
	if slices.Contains(s, v) {
		return s
	}
	s = append(s, v)
	sort.Ints(s)
	return s
}
