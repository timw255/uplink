package engine

import (
	"slices"
	"testing"

	"github.com/timw255/uplink/internal/channel"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// channelWithEnrich builds a single-channel spec firing on
// Create/Update/Delete with one enrich script and no companions.
func channelWithEnrich(filter, scriptPath string) channel.ChannelSpec {
	return channel.ChannelSpec{
		Name:        "ch",
		Source:      "src",
		Destination: "dst",
		Trigger:     channel.TriggerSpec{Events: []string{"OnCreate", "OnUpdate", "OnDelete"}, Filter: filter},
		Enrich:      []channel.EnrichSpec{{Script: scriptPath}},
	}
}

// pathTagsScript derives a TextList of the asset's directory segments,
// echoes the event kind, and flips a Status field to "Archived" on
// delete. Exercises every branch an enrich script cares about.
const pathTagsScript = `
	if uplink.event.deleted then
		return { { name = "Status", value = "Archived" } }
	end
	local segs = {}
	for s in uplink.asset.path:gmatch("[^/]+") do table.insert(segs, s) end
	table.remove(segs)  -- drop filename
	return {
		{ name = "Path Tags", value = segs },
		{ name = "Event",     value = uplink.event.kind },
	}
`

// TestE2E_Enrich_FoldsFieldsOnCreate: an enrich script with no
// companion file runs at Create and folds its derived fields straight
// into the Create Write — no separate PATCH.
func TestE2E_Enrich_FoldsFieldsOnCreate(t *testing.T) {
	scriptPath := writeScript(t, pathTagsScript)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithEnrich("path.endsWith('.png')", scriptPath)},
		map[string][]byte{
			"emea/spring-2026/hero-banner.png": []byte("PNG"),
		},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "emea/spring-2026/hero-banner.png")})

	writes := rig.dst.Writes()
	if len(writes) != 1 {
		t.Fatalf("expected 1 Write (asset Create), got %d", len(writes))
	}
	if mw := rig.dst.MetadataWrites(); len(mw) != 0 {
		t.Fatalf("enrich should fold into Create, not PATCH; got %d PATCHes", len(mw))
	}

	fields, ok := writes[0].Meta["dest_fields"].([]any)
	if !ok || len(fields) != 2 {
		t.Fatalf("expected 2 enrich fields folded into Create, got %+v", writes[0].Meta["dest_fields"])
	}
	byName := fieldsByName(t, fields)

	tags, ok := byName["Path Tags"].([]any)
	if !ok {
		t.Fatalf("Path Tags should be a list, got %T: %v", byName["Path Tags"], byName["Path Tags"])
	}
	if len(tags) != 2 || tags[0] != "emea" || tags[1] != "spring-2026" {
		t.Errorf("Path Tags = %v, want [emea spring-2026]", tags)
	}
	if byName["Event"] != "OnCreate" {
		t.Errorf("Event = %v, want OnCreate", byName["Event"])
	}
}

// TestE2E_Enrich_RunsOnUpdate: a content change re-runs the enrich
// script and folds the fields into the Update Write, with the event
// kind reported as OnUpdate.
func TestE2E_Enrich_RunsOnUpdate(t *testing.T) {
	scriptPath := writeScript(t, pathTagsScript)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithEnrich("path.endsWith('.png')", scriptPath)},
		map[string][]byte{
			"a/b/image.png": []byte("v1"),
		},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "a/b/image.png")})

	// Change the bytes so the dispatcher sees a new version (not deduped).
	rig.src.Files["a/b/image.png"] = []byte("v2-different-bytes")
	rig.runOnce([]connector.Event{rig.emit(connector.EventUpdate, "a/b/image.png")})

	writes := rig.dst.Writes()
	if len(writes) != 2 {
		t.Fatalf("expected 2 Writes (Create + Update), got %d", len(writes))
	}
	fields, _ := writes[1].Meta["dest_fields"].([]any)
	byName := fieldsByName(t, fields)
	if byName["Event"] != "OnUpdate" {
		t.Errorf("update Event = %v, want OnUpdate", byName["Event"])
	}
	if tags, _ := byName["Path Tags"].([]any); len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("update Path Tags = %v, want [a b]", byName["Path Tags"])
	}
}

// TestE2E_Enrich_DeletePatchesRecord: a source-side delete on a channel
// that declares enrich + OnDelete runs the script with
// uplink.event.deleted=true and PATCHes the existing record. The record
// is never deleted.
func TestE2E_Enrich_DeletePatchesRecord(t *testing.T) {
	scriptPath := writeScript(t, pathTagsScript)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithEnrich("path.endsWith('.png')", scriptPath)},
		map[string][]byte{
			"a/b/image.png": []byte("PNG"),
		},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "a/b/image.png")})
	syncRow, err := rig.store.LookupLatest(rig.ctx, "ch", "a/b/image.png")
	if err != nil || syncRow == nil {
		t.Fatalf("LookupLatest after Create: %+v err=%v", syncRow, err)
	}
	recordID := syncRow.DestID
	if recordID == "" {
		t.Fatal("sync_log row missing dest_id")
	}

	// Source deletes the file; synthesize the delete event (only Path
	// is populated on a delete entry, matching the scan engine).
	delete(rig.src.Files, "a/b/image.png")
	rig.runOnce([]connector.Event{{
		Connector: "src",
		Kind:      connector.EventDelete,
		Entry:     connector.Entry{Path: "a/b/image.png"},
	}})

	mw := rig.dst.MetadataWrites()
	if len(mw) != 1 {
		t.Fatalf("expected 1 PATCH from enrich-delete, got %d", len(mw))
	}
	if mw[0].RecordID != recordID {
		t.Errorf("PATCH targeted %q, want the asset's record id %q", mw[0].RecordID, recordID)
	}
	fields, _ := mw[0].Meta["dest_fields"].([]any)
	byName := fieldsByName(t, fields)
	if byName["Status"] != "Archived" {
		t.Errorf("delete Status = %v, want Archived", byName["Status"])
	}
}

// TestE2E_Enrich_DeleteDroppedWithoutEnricher confirms the legacy
// behavior is preserved: a channel with NO enrich scripts drops source
// deletes entirely (no PATCH, no record mutation).
func TestE2E_Enrich_DeleteDroppedWithoutEnricher(t *testing.T) {
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{{
			Name:        "ch",
			Source:      "src",
			Destination: "dst",
			Trigger:     channel.TriggerSpec{Events: []string{"OnCreate", "OnUpdate", "OnDelete"}},
		}},
		map[string][]byte{"a/b/image.png": []byte("PNG")},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "a/b/image.png")})
	delete(rig.src.Files, "a/b/image.png")
	rig.runOnce([]connector.Event{{
		Connector: "src",
		Kind:      connector.EventDelete,
		Entry:     connector.Entry{Path: "a/b/image.png"},
	}})

	if mw := rig.dst.MetadataWrites(); len(mw) != 0 {
		t.Fatalf("delete on a channel without enrichers must be a no-op, got %d PATCHes", len(mw))
	}
}

// TestE2E_Enrich_DeleteDroppedWhenNeverSynced confirms a delete for an
// asset that was never synced (no record to PATCH) is dropped without
// error, even on an enricher channel.
func TestE2E_Enrich_DeleteDroppedWhenNeverSynced(t *testing.T) {
	scriptPath := writeScript(t, pathTagsScript)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{channelWithEnrich("path.endsWith('.png')", scriptPath)},
		map[string][]byte{},
	)

	rig.runOnce([]connector.Event{{
		Connector: "src",
		Kind:      connector.EventDelete,
		Entry:     connector.Entry{Path: "never/synced.png"},
	}})

	if mw := rig.dst.MetadataWrites(); len(mw) != 0 {
		t.Fatalf("delete of never-synced asset must be a no-op, got %d PATCHes", len(mw))
	}
	if _, err := rig.store.ClaimNextJob(rig.ctx); err != store.ErrNoJob {
		t.Fatalf("expected no enqueued job for never-synced delete, err=%v", err)
	}
}

// TestE2E_Enrich_MergesWithCompanionFields verifies enrich fields and
// companion presync fields land in the SAME Create call together.
func TestE2E_Enrich_MergesWithCompanionFields(t *testing.T) {
	enrichPath := writeScript(t, `return { { name = "Source", value = "enrich" } }`)
	companionPath := writeScript(t, `return { { name = "Source", value = "companion" } }`)
	rig := newCompanionRig(t,
		[]channel.ChannelSpec{{
			Name:        "ch",
			Source:      "src",
			Destination: "dst",
			Trigger:     channel.TriggerSpec{Events: []string{"OnCreate"}, Filter: "path.endsWith('.png')"},
			Companions:  []channel.CompanionSpec{{Pattern: "${basename}.json", Script: companionPath}},
			Enrich:      []channel.EnrichSpec{{Script: enrichPath}},
		}},
		map[string][]byte{
			"a/image.png":  []byte("PNG"),
			"a/image.json": []byte("{}"),
		},
	)

	rig.runOnce([]connector.Event{rig.emit(connector.EventCreate, "a/image.png")})

	writes := rig.dst.Writes()
	if len(writes) != 1 {
		t.Fatalf("expected 1 Write, got %d", len(writes))
	}
	fields, _ := writes[0].Meta["dest_fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("expected companion + enrich fields folded together (2), got %d: %+v", len(fields), fields)
	}
	var sources []string
	for _, f := range fields {
		sources = append(sources, f.(map[string]any)["value"].(string))
	}
	if !slices.Contains(sources, "enrich") || !slices.Contains(sources, "companion") {
		t.Errorf("expected both enrich and companion fields, got %v", sources)
	}
}

// fieldsByName flattens a dest_fields slice into a name->value map for
// assertion convenience. Fails the test on a malformed entry.
func fieldsByName(t *testing.T, fields []any) map[string]any {
	t.Helper()
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		m, ok := f.(map[string]any)
		if !ok {
			t.Fatalf("field entry is %T, want map: %v", f, f)
		}
		out[m["name"].(string)] = m["value"]
	}
	return out
}
