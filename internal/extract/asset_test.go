package extract

import (
	"context"
	"log/slog"
	"testing"
)

func runAsset(t *testing.T, script *Script, in AssetScriptInput) ([]any, error) {
	t.Helper()
	return script.RunAsset(context.Background(), in)
}

// TestAsset_APISurface exercises the uplink globals exposed in enrich
// mode: asset and event must be populated; file and match (companion-only)
// must NOT exist; the dangerous stdlib must be unreachable.
func TestAsset_APISurface(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		assert(type(uplink) == "table", "uplink missing")
		assert(type(uplink.asset) == "table", "uplink.asset missing")
		assert(type(uplink.event) == "table", "uplink.event missing")
		assert(uplink.file == nil, "uplink.file must NOT exist in enrich mode")
		assert(uplink.match == nil, "uplink.match must NOT exist in enrich mode")
		assert(uplink.asset.path == "emea/spring-2026/hero-banner.png", "asset.path = " .. tostring(uplink.asset.path))
		assert(uplink.asset.extension == "png", "asset.extension = " .. tostring(uplink.asset.extension))
		assert(uplink.asset.record_id == "rec-7", "asset.record_id = " .. tostring(uplink.asset.record_id))
		assert(uplink.event.kind == "OnCreate", "event.kind = " .. tostring(uplink.event.kind))
		assert(uplink.event.deleted == false, "event.deleted should be false")
		assert(os == nil and io == nil and require == nil, "sandbox leak")
		return {}
	`
	script := compileString(t, rt, "surface", src)
	if _, err := runAsset(t, script, AssetScriptInput{
		Channel:       "test",
		Asset:         AssetInfo{Path: "emea/spring-2026/hero-banner.png", Size: 10},
		AssetRecordID: "rec-7",
		Extension:     "png",
		Event:         AssetEvent{Kind: "OnCreate", Deleted: false},
	}); err != nil {
		t.Fatalf("RunAsset: %v", err)
	}
}

// TestAsset_PathTagsToTextList is the canonical use case: split the
// asset path into directory segments and return them as a TextList.
func TestAsset_PathTagsToTextList(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		local segs = {}
		for s in uplink.asset.path:gmatch("[^/]+") do table.insert(segs, s) end
		table.remove(segs)  -- drop filename
		return { { name = "Path Tags", value = segs } }
	`
	script := compileString(t, rt, "pathtags", src)
	out, err := runAsset(t, script, AssetScriptInput{
		Channel: "test",
		Asset:   AssetInfo{Path: "emea/spring-2026/hero-banner.png"},
		Event:   AssetEvent{Kind: "OnCreate"},
	})
	if err != nil {
		t.Fatalf("RunAsset: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 field, got %d", len(out))
	}
	entry := out[0].(map[string]any)
	if entry["name"] != "Path Tags" {
		t.Errorf("name = %v", entry["name"])
	}
	tags, ok := entry["value"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "emea" || tags[1] != "spring-2026" {
		t.Errorf("value = %v, want [emea spring-2026]", entry["value"])
	}
}

// TestAsset_DeletedSignal verifies the deleted flag reaches the script
// so it can branch (e.g. mark archived) on a source-side delete.
func TestAsset_DeletedSignal(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		if uplink.event.deleted then
			return { { name = "Status", value = "Archived" } }
		end
		return {}
	`
	script := compileString(t, rt, "archive", src)
	out, err := runAsset(t, script, AssetScriptInput{
		Channel: "test",
		Asset:   AssetInfo{Path: "a/b/image.png"},
		Event:   AssetEvent{Kind: "OnDelete", Deleted: true},
	})
	if err != nil {
		t.Fatalf("RunAsset: %v", err)
	}
	if len(out) != 1 || out[0].(map[string]any)["value"] != "Archived" {
		t.Fatalf("expected Archived field on delete, got %+v", out)
	}
}
