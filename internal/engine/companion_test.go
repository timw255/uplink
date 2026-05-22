package engine

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/extract"
	"github.com/timw255/uplink/internal/store"
)

// recordingLogger captures stderr-written log output so tests can
// assert that specific WARN entries were emitted. slog's text handler
// produces deterministic key=value lines we can match on.
type recordingLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{}
}

func (r *recordingLogger) Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(r, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (r *recordingLogger) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *recordingLogger) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

// extractCompiler adapts an *extract.Runtime to channel.ScriptCompiler.
// Mirrors the daemon's production adapter (cmd/uplink/run.go) for use
// inside engine tests.
type extractCompiler struct{ rt *extract.Runtime }

func (c extractCompiler) Compile(name, path string) (channel.CompiledScript, error) {
	return c.rt.Compile(name, path)
}

// writeScript drops a Lua source file into a temp dir and returns its
// absolute path. Companion scripts in the engine tests are short
// inline strings — this keeps the test self-contained.
func writeScript(t *testing.T, src string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "script.lua")
	if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return p
}

// companionTestRig builds the minimum harness needed to exercise the
// companion dispatch + execute path: a real store, a registry compiled
// with one or more companion declarations, the engine, and a stub
// source/destination pair. Tests drive it by calling Dispatch /
// runJob-style methods directly.
type companionTestRig struct {
	t      *testing.T
	ctx    context.Context
	store  *store.Store
	reg    *channel.Registry
	src    *StubSource
	dst    *StubDestination
	engine *Engine
}

func newCompanionRig(t *testing.T, specs []channel.ChannelSpec, files map[string][]byte) *companionTestRig {
	return newCompanionRigWithLogger(t, specs, files, slog.New(slog.DiscardHandler))
}

func newCompanionRigWithLogger(t *testing.T, specs []channel.ChannelSpec, files map[string][]byte, logger *slog.Logger) *companionTestRig {
	t.Helper()
	ctx := context.Background()
	dataDir := t.TempDir()

	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	rt := extract.NewRuntime(slog.New(slog.DiscardHandler))
	reg, err := channel.NewRegistry(specs, extractCompiler{rt: rt})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	src := &StubSource{NameStr: "src", Files: files}
	dst := &StubDestination{NameStr: "dst"}

	eng := New(st, reg, NewStubConnectors(src, dst), Options{
		Workers:     1,
		PollIdle:    10 * time.Millisecond,
		MaxAttempts: 2,
		BaseBackoff: 5 * time.Millisecond,
		Logger:      logger,
	})
	return &companionTestRig{t: t, ctx: ctx, store: st, reg: reg, src: src, dst: dst, engine: eng}
}

// runOnce drives one synthetic batch of events through the dispatcher,
// then runs the worker loop until the job queue drains. Returns when
// no claimable job remains for a 50ms quiet window.
func (r *companionTestRig) runOnce(events []connector.Event) {
	r.t.Helper()
	if err := r.engine.DispatchBatch(r.ctx, "src", events); err != nil {
		r.t.Fatalf("DispatchBatch: %v", err)
	}
	idle := time.Now()
	for {
		job, err := r.store.ClaimNextJob(r.ctx)
		if err == store.ErrNoJob {
			if time.Since(idle) > 50*time.Millisecond {
				return
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err != nil {
			r.t.Fatalf("ClaimNextJob: %v", err)
		}
		idle = time.Now()
		log := slog.New(slog.DiscardHandler)
		r.engine.runJob(r.ctx, log, job)
	}
}

// emit synthesizes a Create event for one source path. The hash is
// derived from the file's bytes so the engine sees a stable identifier.
func (r *companionTestRig) emit(kind connector.EventKind, path string) connector.Event {
	r.t.Helper()
	entry, err := r.src.Stat(r.ctx, path)
	if err != nil {
		r.t.Fatalf("Stat(%s): %v", path, err)
	}
	return connector.Event{Connector: "src", Kind: kind, Entry: entry}
}

// channelWithCompanion builds a single-channel spec that fires on
// OnCreate/OnUpdate/OnDelete and declares one companion pattern.
func channelWithCompanion(filter, pattern, scriptPath string) channel.ChannelSpec {
	return channel.ChannelSpec{
		Name:        "ch",
		Source:      "src",
		Destination: "dst",
		Trigger:     channel.TriggerSpec{Events: []string{"OnCreate", "OnUpdate", "OnDelete"}, Filter: filter},
		Companions: []channel.CompanionSpec{
			{Pattern: pattern, Script: scriptPath},
		},
	}
}

// TestE2E_Companion_PresyncOnCreate: asset and companion both present
// at scan time. The asset event arrives at the worker; presync lists
// the dir, finds the companion, runs the script, and folds the
// returned field into the Create call. No separate WriteMetadata.
func TestE2E_Companion_PresyncOnCreate(t *testing.T) {
	scriptPath := writeScript(t, `
		return { { name = "xmp_body", value = uplink.file.content } }
	`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg": []byte("JPEG-bytes"),
			"photos/sunset.xmp": []byte("<xmp>data</xmp>"),
		},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})

	writes := rig.dst.Writes()
	if len(writes) != 1 {
		t.Fatalf("expected exactly 1 Write (asset Create), got %d: %+v", len(writes), writes)
	}
	if writes[0].Path != "photos/sunset.jpg" {
		t.Errorf("Write.Path = %q", writes[0].Path)
	}
	fields, ok := writes[0].Meta["dest_fields"].([]any)
	if !ok || len(fields) == 0 {
		t.Fatalf("Write meta missing dest_fields: %+v", writes[0].Meta)
	}
	entry := fields[0].(map[string]any)
	if entry["name"] != "xmp_body" {
		t.Errorf("field name = %v", entry["name"])
	}
	if entry["value"] != "<xmp>data</xmp>" {
		t.Errorf("field value = %v", entry["value"])
	}

	if mw := rig.dst.MetadataWrites(); len(mw) != 0 {
		t.Errorf("expected 0 WriteMetadata calls (presync folded fields), got %d", len(mw))
	}
}

// TestE2E_Companion_LateArrivalPatches: asset is synced first, then
// the companion arrives. Verifies the dispatcher routes the
// companion event as a metadata-only PATCH against the existing
// record id.
func TestE2E_Companion_LateArrivalPatches(t *testing.T) {
	scriptPath := writeScript(t, `
		return { { name = "xmp_body", value = uplink.file.content } }
	`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg": []byte("JPEG-bytes"),
		},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})

	writes := rig.dst.Writes()
	if len(writes) != 1 {
		t.Fatalf("expected 1 Write on first pass, got %d", len(writes))
	}
	syncRow, err := rig.store.LookupLatest(rig.ctx, "ch", "photos/sunset.jpg")
	if err != nil || syncRow == nil {
		t.Fatalf("LookupLatest after Create: %+v err=%v", syncRow, err)
	}
	createdRecordID := syncRow.DestID
	if createdRecordID == "" {
		t.Fatal("sync_log row missing dest_id")
	}

	// Now drop the companion in and emit its event.
	rig.src.Files["photos/sunset.xmp"] = []byte("<xmp>after</xmp>")
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.xmp")})

	mw := rig.dst.MetadataWrites()
	if len(mw) != 1 {
		t.Fatalf("expected 1 WriteMetadata call after companion arrival, got %d", len(mw))
	}
	if mw[0].RecordID != createdRecordID {
		t.Errorf("PATCH targeted %q, want the asset's record id %q", mw[0].RecordID, createdRecordID)
	}
	fields, ok := mw[0].Meta["dest_fields"].([]any)
	if !ok || len(fields) != 1 {
		t.Fatalf("WriteMetadata fields shape wrong: %+v", mw[0].Meta)
	}
	if got := fields[0].(map[string]any)["value"]; got != "<xmp>after</xmp>" {
		t.Errorf("companion field value = %v", got)
	}
}

// TestE2E_Companion_BeforeAsset: companion arrives BEFORE the asset
// exists in sync_log. The companion event should be dropped silently
// (no PATCH target). When the asset's Create later fires, presync
// picks up the companion and folds it into the Create call. Exercises
// the sidecar-first arrival case.
func TestE2E_Companion_BeforeAsset(t *testing.T) {
	scriptPath := writeScript(t, `
		return { { name = "xmp_body", value = uplink.file.content } }
	`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.xmp": []byte("<xmp>early</xmp>"),
		},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.xmp")})

	if w := rig.dst.Writes(); len(w) != 0 {
		t.Errorf("expected 0 Writes (companion alone is no-op), got %d", len(w))
	}
	if mw := rig.dst.MetadataWrites(); len(mw) != 0 {
		t.Errorf("expected 0 WriteMetadata calls (no parent), got %d", len(mw))
	}

	// Now the asset arrives.
	rig.src.Files["photos/sunset.jpg"] = []byte("JPEG-bytes")
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})

	writes := rig.dst.Writes()
	if len(writes) != 1 {
		t.Fatalf("expected 1 Write after asset arrives, got %d", len(writes))
	}
	fields, ok := writes[0].Meta["dest_fields"].([]any)
	if !ok || len(fields) == 0 {
		t.Fatalf("expected presync to fold companion fields into Create: %+v", writes[0].Meta)
	}
	if got := fields[0].(map[string]any)["value"]; got != "<xmp>early</xmp>" {
		t.Errorf("presync field value = %v", got)
	}
	if mw := rig.dst.MetadataWrites(); len(mw) != 0 {
		t.Errorf("presync should NOT fire a follow-on WriteMetadata, got %d", len(mw))
	}
}

// TestE2E_Companion_DeletePatchesWithDeletedFlag verifies that a
// companion-delete event runs the script with file.deleted=true. The
// script returns a clearing field; the engine PATCHes it.
func TestE2E_Companion_DeletePatchesWithDeletedFlag(t *testing.T) {
	scriptPath := writeScript(t, `
		if uplink.file.deleted then
			return { { name = "xmp_body", value = "" } }
		end
		return { { name = "xmp_body", value = uplink.file.content } }
	`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg": []byte("JPEG-bytes"),
			"photos/sunset.xmp": []byte("<xmp>present</xmp>"),
		},
	)
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})

	syncRow, err := rig.store.LookupLatest(rig.ctx, "ch", "photos/sunset.jpg")
	if err != nil || syncRow == nil {
		t.Fatalf("LookupLatest after Create: %+v err=%v", syncRow, err)
	}
	createdRecordID := syncRow.DestID

	// Now simulate a companion delete. The source no longer has the
	// file but we synthesize a delete event with the path.
	delete(rig.src.Files, "photos/sunset.xmp")
	deleteEvent := connector.Event{
		Connector: "src",
		Kind:      connector.EventDelete,
		Entry:     connector.Entry{Path: "photos/sunset.xmp"},
	}
	rig.runOnce([]connector.Event{deleteEvent})

	mw := rig.dst.MetadataWrites()
	if len(mw) != 1 {
		t.Fatalf("expected 1 WriteMetadata call from delete, got %d", len(mw))
	}
	if mw[0].RecordID != createdRecordID {
		t.Errorf("delete PATCH targeted %q, want %q", mw[0].RecordID, createdRecordID)
	}
	fields := mw[0].Meta["dest_fields"].([]any)
	if got := fields[0].(map[string]any)["value"]; got != "" {
		t.Errorf("clear field value = %q, want empty string", got)
	}
}

// TestE2E_Companion_PresyncIgnoresOtherDirectories regresses a bug
// where presync's underlying List(parentDir) returns the entire
// subtree (List is recursive on every source connector). A file in a
// SIBLING subdirectory whose filename happens to match the pattern
// must NOT be folded into the parent's Create. The bug would
// erroneously attach a "wrong-folder" XMP's metadata to an asset that
// happened to share a basename.
func TestE2E_Companion_PresyncIgnoresOtherDirectories(t *testing.T) {
	scriptPath := writeScript(t, `
		return { { name = "xmp_body", value = uplink.file.content } }
	`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg":         []byte("JPEG"),
			"photos/sunset.xmp":         []byte("right-dir"),
			"photos/other/sunset.xmp":   []byte("wrong-dir"),
			"photos/deeper/sub/sunset.xmp": []byte("also-wrong"),
		},
	)
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})

	writes := rig.dst.Writes()
	if len(writes) != 1 {
		t.Fatalf("expected 1 Write, got %d", len(writes))
	}
	fields, ok := writes[0].Meta["dest_fields"].([]any)
	if !ok {
		t.Fatalf("missing dest_fields in meta: %+v", writes[0].Meta)
	}
	if len(fields) != 1 {
		t.Fatalf("expected exactly 1 companion field (same-dir only), got %d: %+v", len(fields), fields)
	}
	if got := fields[0].(map[string]any)["value"]; got != "right-dir" {
		t.Errorf("presync picked wrong companion: value = %q", got)
	}
}

// TestE2E_Companion_LateArrivalMultiChannelPatches verifies that when
// two channels both declare the same companion pattern, a
// late-arriving companion fires a PATCH on BOTH channels' records —
// not just the first-matching channel. Regresses the
// Registry.MatchCompanions all-matches contract.
func TestE2E_Companion_LateArrivalMultiChannelPatches(t *testing.T) {
	scriptPath := writeScript(t, `
		return { { name = "xmp_body", value = uplink.file.content } }
	`)
	specs := []channel.ChannelSpec{
		{
			Name:        "ch-a",
			Source:      "src",
			Destination: "dst",
			Trigger:     channel.TriggerSpec{Events: []string{"OnCreate", "OnUpdate"}, Filter: "path.endsWith('.jpg')"},
			Companions:  []channel.CompanionSpec{{Pattern: "${basename}.xmp", Script: scriptPath}},
		},
		{
			Name:        "ch-b",
			Source:      "src",
			Destination: "dst",
			Trigger:     channel.TriggerSpec{Events: []string{"OnCreate", "OnUpdate"}, Filter: "path.endsWith('.jpg')"},
			Companions:  []channel.CompanionSpec{{Pattern: "${basename}.xmp", Script: scriptPath}},
		},
	}
	rig := newCompanionRig(t, specs, map[string][]byte{
		"photos/sunset.jpg": []byte("JPEG"),
	})

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})
	if len(rig.dst.Writes()) != 2 {
		t.Fatalf("expected 2 Writes (asset Create per channel), got %d", len(rig.dst.Writes()))
	}
	recA, err := rig.store.LookupLatest(rig.ctx, "ch-a", "photos/sunset.jpg")
	if err != nil || recA == nil {
		t.Fatalf("LookupLatest ch-a: %+v err=%v", recA, err)
	}
	recB, err := rig.store.LookupLatest(rig.ctx, "ch-b", "photos/sunset.jpg")
	if err != nil || recB == nil {
		t.Fatalf("LookupLatest ch-b: %+v err=%v", recB, err)
	}

	// Companion arrives now. Both channels claim it; both should PATCH.
	rig.src.Files["photos/sunset.xmp"] = []byte("<xmp>data</xmp>")
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.xmp")})

	mw := rig.dst.MetadataWrites()
	if len(mw) != 2 {
		t.Fatalf("expected 2 PATCH calls (one per matching channel), got %d", len(mw))
	}
	got := map[string]bool{}
	for _, call := range mw {
		got[call.RecordID] = true
	}
	if !got[recA.DestID] || !got[recB.DestID] {
		t.Errorf("expected PATCHes against both record ids %q and %q, got %v",
			recA.DestID, recB.DestID, got)
	}
}

// TestE2E_Companion_ShadowingEmitsWarning asserts that when a path is
// claimed as a companion AND would have matched a different channel's
// asset filter, the dispatcher logs a WARN with both channel names so
// the operator can spot the contradictory config. The companion route
// still wins — this is purely diagnostic.
func TestE2E_Companion_ShadowingEmitsWarning(t *testing.T) {
	scriptPath := writeScript(t, `return {}`)
	specs := []channel.ChannelSpec{
		{
			Name:        "companion-ch",
			Source:      "src",
			Destination: "dst",
			Trigger:     channel.TriggerSpec{Events: []string{"OnCreate"}, Filter: "path.endsWith('.jpg')"},
			Companions:  []channel.CompanionSpec{{Pattern: "${basename}.xmp", Script: scriptPath}},
		},
		{
			// This channel's filter would have accepted *.xmp as an asset —
			// a config the operator probably did not intend.
			Name:        "asset-ch",
			Source:      "src",
			Destination: "dst",
			Trigger:     channel.TriggerSpec{Events: []string{"OnCreate"}, Filter: "path.endsWith('.xmp')"},
		},
	}
	rec := newRecordingLogger()
	rig := newCompanionRigWithLogger(t, specs, map[string][]byte{
		"photos/sunset.jpg": []byte("JPEG"),
		"photos/sunset.xmp": []byte("<xmp>data</xmp>"),
	}, rec.Logger())

	// First Create the JPG so a parent exists in sync_log.
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})

	// Now the XMP arrives. It's claimed as a companion by companion-ch
	// AND would be an asset on asset-ch — the warning should fire.
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.xmp")})

	logs := rec.String()
	if !strings.Contains(logs, "companion classification suppressed an asset match") {
		t.Fatalf("expected shadowing WARN, got logs:\n%s", logs)
	}
	if !strings.Contains(logs, "asset-ch") {
		t.Errorf("WARN should name the shadowed asset channel, got:\n%s", logs)
	}
	if !strings.Contains(logs, "companion-ch") {
		t.Errorf("WARN should name the companion channel, got:\n%s", logs)
	}
	if !strings.Contains(logs, `level=WARN`) {
		t.Errorf("expected WARN level, got:\n%s", logs)
	}
}

// TestE2E_Companion_NoWarningWhenNoShadowing confirms the shadowing
// warning does NOT fire in the common case where the companion path
// only matches its own companion channel (no other channel claims
// the path as an asset).
func TestE2E_Companion_NoWarningWhenNoShadowing(t *testing.T) {
	scriptPath := writeScript(t, `return {}`)
	rec := newRecordingLogger()
	rig := newCompanionRigWithLogger(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg": []byte("JPEG"),
			"photos/sunset.xmp": []byte("<xmp>data</xmp>"),
		}, rec.Logger())

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.xmp")})

	if strings.Contains(rec.String(), "suppressed an asset match") {
		t.Errorf("did not expect shadowing WARN in clean case, got:\n%s", rec.String())
	}
}

// TestE2E_Companion_SweepCatchesLateArriver regresses the race-window
// fix: presync's List captures the parent's directory at one moment,
// then the Create RPC runs. If a companion file appears on disk DURING
// the Create RPC, its own scan event would be dispatched and silently
// dropped (parent not yet in sync_log). The connector_state would
// record the companion so no future scan re-emits it — leaving the
// metadata orphaned.
//
// We simulate the race by:
//  1. Running a Create against a source with NO companion present
//     (so presync's processed-paths set is empty).
//  2. Dropping the companion file in afterward.
//  3. Invoking sweepLateArrivingCompanions directly with the empty
//     presynced-paths set, which is what the production runJob path
//     does after finalize for an asset Create.
//
// The sweep should locate the new companion, dispatch it through
// the normal companion job path, and produce a PATCH against the
// parent's record.
func TestE2E_Companion_SweepCatchesLateArriver(t *testing.T) {
	scriptPath := writeScript(t, `
		return { { name = "xmp_body", value = uplink.file.content } }
	`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg": []byte("JPEG"),
		},
	)

	// Phase 1: presync sees nothing (no companion on disk yet).
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})
	if len(rig.dst.MetadataWrites()) != 0 {
		t.Fatalf("expected 0 PATCHes after Create with no companion, got %d", len(rig.dst.MetadataWrites()))
	}

	syncRow, err := rig.store.LookupLatest(rig.ctx, "ch", "photos/sunset.jpg")
	if err != nil || syncRow == nil {
		t.Fatalf("parent not in sync_log: %+v err=%v", syncRow, err)
	}

	// Phase 2: late-arriving companion. The scan event for it would
	// have been dropped (parent existed by then in this test, but we
	// simulate the "presync missed it" condition by invoking the sweep
	// directly with an empty presynced-paths set — same effect as a
	// real race where the file landed after presync's List).
	rig.src.Files["photos/sunset.xmp"] = []byte("<xmp>late</xmp>")
	ch := rig.reg.Lookup("ch")
	syntheticJob := &store.Job{
		ChannelName:     "ch",
		Kind:            string(connector.EventCreate),
		SourceConnector: "src",
		SourcePath:      "photos/sunset.jpg",
	}
	rig.engine.sweepLateArrivingCompanions(rig.ctx, syntheticJob, ch, nil)

	// Drain whatever the sweep enqueued.
	rig.runOnce(nil)

	mw := rig.dst.MetadataWrites()
	if len(mw) != 1 {
		t.Fatalf("expected 1 PATCH from sweep, got %d", len(mw))
	}
	if mw[0].RecordID != syncRow.DestID {
		t.Errorf("PATCH targeted %q, want %q", mw[0].RecordID, syncRow.DestID)
	}
	fields := mw[0].Meta["dest_fields"].([]any)
	if got := fields[0].(map[string]any)["value"]; got != "<xmp>late</xmp>" {
		t.Errorf("sweep recovered wrong content: %v", got)
	}
}

// TestE2E_Companion_SweepSkipsPresyncedPaths confirms that when
// presync already processed a path, the sweep does NOT re-dispatch
// it (dedup correctness). The sweep's job is to catch ONLY late
// arrivers, never to duplicate work presync already did.
func TestE2E_Companion_SweepSkipsPresyncedPaths(t *testing.T) {
	scriptPath := writeScript(t, `return { { name = "xmp_body", value = uplink.file.content } }`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg": []byte("JPEG"),
			"photos/sunset.xmp": []byte("<xmp>already-presynced</xmp>"),
		},
	)

	// presync handled the companion in the Create — one Write, zero PATCHes.
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})
	if got := len(rig.dst.MetadataWrites()); got != 0 {
		t.Fatalf("expected 0 PATCHes when presync handled everything, got %d", got)
	}

	// Now invoke the sweep manually with the same path marked as
	// already-processed. The companion is still on disk; sweep must
	// see it but skip it.
	ch := rig.reg.Lookup("ch")
	syntheticJob := &store.Job{
		ChannelName:     "ch",
		Kind:            string(connector.EventCreate),
		SourceConnector: "src",
		SourcePath:      "photos/sunset.jpg",
	}
	rig.engine.sweepLateArrivingCompanions(rig.ctx, syntheticJob, ch, []string{"photos/sunset.xmp"})
	rig.runOnce(nil)

	if got := len(rig.dst.MetadataWrites()); got != 0 {
		t.Fatalf("dedup failed: sweep enqueued a PATCH for an already-presynced path (count=%d)", got)
	}
}

// TestE2E_Companion_RetryClearsDedup regresses the Option-B fix:
// when an asset Create job runs on a RETRY (Attempts > 1 after
// ClaimNextJob's atomic increment), the destination's Write may have
// short-circuited on a marker left in "created" state by a prior
// attempt that finalized halfway — discarding any fields presync
// gathered. To recover, runJob must clear the sweep's dedup set on
// retries so the sweep dispatches ALL matching companions and any
// new ones get their fields PATCHed in.
//
// This test simulates a retry by enqueuing a job with Attempts=1
// (ClaimNextJob will bump it to 2) and asserting the sweep produces
// a PATCH for the present companion even though presync also
// processed it. The duplicate PATCH is correct B behavior — same
// field values applied to Aprimo, idempotent.
func TestE2E_Companion_RetryClearsDedup(t *testing.T) {
	scriptPath := writeScript(t, `return { { name = "xmp_body", value = uplink.file.content } }`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion("path.endsWith('.jpg')", "${basename}.xmp", scriptPath)},
		map[string][]byte{
			"photos/sunset.jpg": []byte("JPEG"),
			"photos/sunset.xmp": []byte("<xmp>data</xmp>"),
		},
	)

	// Enqueue an asset Create with Attempts=1 already on the row.
	// ClaimNextJob's atomic increment brings it to 2 when the worker
	// picks it up — that's the "second-attempt retry" state.
	_, err := rig.store.EnqueueJobs(rig.ctx, []store.Job{{
		ChannelName:     "ch",
		Kind:            string(connector.EventCreate),
		SourceConnector: "src",
		SourcePath:      "photos/sunset.jpg",
		SourceVersion:   "v1",
		Attempts:        1,
	}})
	if err != nil {
		t.Fatalf("EnqueueJobs: %v", err)
	}

	rig.runOnce(nil)

	if got := len(rig.dst.Writes()); got != 1 {
		t.Fatalf("expected 1 Write for the Create itself, got %d", got)
	}
	mw := rig.dst.MetadataWrites()
	if len(mw) != 1 {
		t.Fatalf("expected 1 PATCH from the dedup-cleared sweep on retry, got %d", len(mw))
	}
	fields := mw[0].Meta["dest_fields"].([]any)
	if got := fields[0].(map[string]any)["value"]; got != "<xmp>data</xmp>" {
		t.Errorf("PATCH carried wrong field value: %v", got)
	}
}

// TestE2E_Companion_PerLanguageCaptions verifies the multilingual
// case: one declared pattern with a ${lang} named capture; multiple
// caption files at presync time, each producing its own locale field.
func TestE2E_Companion_PerLanguageCaptions(t *testing.T) {
	// Script reads vars.lang and emits one entry per invocation.
	scriptPath := writeScript(t, `
		return { { name = "caption", value = uplink.file.content, language = uplink.match.vars.lang } }
	`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithCompanion(
			"path.endsWith('.jpg')",
			"${basename}.caption.${lang}.txt",
			scriptPath,
		)},
		map[string][]byte{
			"photos/sunset.jpg":              []byte("JPEG"),
			"photos/sunset.caption.en.txt":   []byte("Sunset"),
			"photos/sunset.caption.fr.txt":   []byte("Coucher"),
		},
	)
	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "photos/sunset.jpg")})

	writes := rig.dst.Writes()
	if len(writes) != 1 {
		t.Fatalf("expected 1 Write, got %d", len(writes))
	}
	fields, _ := writes[0].Meta["dest_fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("expected 2 caption fields (en + fr), got %d: %+v", len(fields), fields)
	}
	seen := map[string]string{}
	for _, f := range fields {
		m := f.(map[string]any)
		seen[m["language"].(string)] = m["value"].(string)
	}
	if seen["en"] != "Sunset" || seen["fr"] != "Coucher" {
		t.Errorf("locale-keyed values wrong: %+v", seen)
	}
}
