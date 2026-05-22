package extract

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func compileString(t *testing.T, rt *Runtime, name, src string) *Script {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".lua")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}
	s, err := rt.Compile(name, path)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return s
}

// runCompanion drives a script in companion mode with a fixed asset
// pointing at folder/report.jpg + a configurable companion file +
// match info. Test helpers — production code wires these via the
// engine's companion job execution path.
func runCompanion(t *testing.T, script *Script, file CompanionFile, match MatchInfo) ([]any, error) {
	t.Helper()
	in := CompanionInput{
		Channel: "test",
		Asset: AssetInfo{
			Path:    "folder/report.jpg",
			Size:    123,
			Hash:    "abc",
			ModTime: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		AssetRecordID: "record-42",
		File:          file,
		Match:         match,
	}
	return script.RunCompanion(context.Background(), in)
}

// TestCompanion_APISurface exercises the uplink globals exposed in
// companion mode: asset, file, match must be populated; read_file and
// list_files must NOT exist. The same script also verifies the sandbox
// isolation contract — dangerous stdlib modules must be unreachable.
func TestCompanion_APISurface(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		assert(type(uplink) == "table", "uplink missing")
		assert(type(uplink.asset) == "table", "uplink.asset missing")
		assert(type(uplink.file) == "table", "uplink.file missing")
		assert(type(uplink.match) == "table", "uplink.match missing")
		assert(uplink.read_file == nil, "read_file must NOT exist in companion mode")
		assert(uplink.list_files == nil, "list_files must NOT exist in companion mode")
		assert(uplink.asset.path == "folder/report.jpg", "asset.path = " .. tostring(uplink.asset.path))
		assert(uplink.asset.record_id == "record-42", "asset.record_id = " .. tostring(uplink.asset.record_id))
		assert(uplink.asset.extension == "jpg", "asset.extension = " .. tostring(uplink.asset.extension))
		assert(uplink.file.path == "folder/report.xmp", "file.path = " .. tostring(uplink.file.path))
		assert(uplink.file.deleted == false, "file.deleted should be false")
		assert(uplink.file.content == "<xmp>hi</xmp>", "file.content = " .. tostring(uplink.file.content))
		assert(uplink.match.pattern == "${basename}.xmp", "match.pattern = " .. tostring(uplink.match.pattern))
		assert(uplink.match.basename == "report", "match.basename = " .. tostring(uplink.match.basename))
		assert(uplink.match.extension == "jpg", "match.extension = " .. tostring(uplink.match.extension))

		-- Sandbox isolation: dangerous globals must be nil.
		assert(os == nil, "os must be nil")
		assert(io == nil, "io must be nil")
		assert(debug == nil, "debug must be nil")
		assert(package == nil, "package must be nil")
		assert(require == nil, "require must be nil")
		assert(loadfile == nil, "loadfile must be nil")
		assert(dofile == nil, "dofile must be nil")
		assert(load == nil, "load must be nil")
		assert(loadstring == nil, "loadstring must be nil")
		assert(collectgarbage == nil, "collectgarbage must be nil")
		assert(newproxy == nil, "newproxy must be nil")
		assert(print == nil, "print must be nil")

		-- Safe stdlib subset must work.
		assert(string.upper("hi") == "HI", "string.upper broken")
		assert(table.concat({"a","b"}, "-") == "a-b", "table.concat broken")
		assert(math.floor(2.7) == 2, "math.floor broken")
		return {}
	`
	s := compileString(t, rt, "companion-api", src)
	_, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.xmp", Content: []byte("<xmp>hi</xmp>")},
		MatchInfo{Pattern: "${basename}.xmp", Basename: "report", Extension: "jpg"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestCompanion_DeletedSignal sends the script a deleted companion;
// verifies file.deleted is true and content is nil.
func TestCompanion_DeletedSignal(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		assert(uplink.file.deleted == true, "file.deleted should be true")
		assert(uplink.file.content == nil, "file.content should be nil for deleted")
		return {}
	`
	s := compileString(t, rt, "companion-deleted", src)
	_, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.xmp", Deleted: true},
		MatchInfo{Pattern: "${basename}.xmp", Basename: "report", Extension: "jpg"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestCompanion_VarsAndWildcards confirms named captures land in
// match.vars and positional wildcards land in match.wildcards in
// pattern order.
func TestCompanion_VarsAndWildcards(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		assert(uplink.match.vars.lang == "en", "vars.lang = " .. tostring(uplink.match.vars.lang))
		assert(uplink.match.wildcards[1] == "v2", "wildcards[1] = " .. tostring(uplink.match.wildcards[1]))
		return {}
	`
	s := compileString(t, rt, "companion-captures", src)
	_, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.en.v2.json"},
		MatchInfo{
			Pattern:   "${basename}.${lang}.*.json",
			Basename:  "report",
			Extension: "jpg",
			Vars:      map[string]string{"lang": "en"},
			Wildcards: []string{"v2"},
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestCompanion_ReturnsFieldList confirms the companion path threads a
// returned field list back through luaToGoList — the script's table
// shape mirrors the field-entry contract the Aprimo destination
// resolver expects.
func TestCompanion_ReturnsFieldList(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		return {
			{ name = "caption", value = uplink.file.content, language = uplink.match.vars.lang },
		}
	`
	s := compileString(t, rt, "companion-out", src)
	out, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.en.txt", Content: []byte("Hello")},
		MatchInfo{Pattern: "${basename}.${lang}.txt", Basename: "report", Vars: map[string]string{"lang": "en"}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(out))
	}
	m := out[0].(map[string]any)
	if m["name"] != "caption" {
		t.Errorf("name = %v", m["name"])
	}
	if m["value"] != "Hello" {
		t.Errorf("value = %v", m["value"])
	}
	if m["language"] != "en" {
		t.Errorf("language = %v", m["language"])
	}
}

// TestCompanion_Parsers covers parse_json / parse_xml / parse_csv via
// companion mode. The parsers are pure and shared across all script
// modes; this is the one place we test them end-to-end.
func TestCompanion_Parsers(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `
		local j = uplink.parse_json('{"hello":"world"}')
		assert(j.hello == "world", "parse_json broken")

		local rows = uplink.parse_csv("a,b\n1,2\n", { header_row = true })
		assert(rows[1].a == "1", "parse_csv broken")

		local x = uplink.parse_xml('<root><leaf>v</leaf></root>')
		assert(x.root.leaf == "v", "parse_xml broken")
		return {}
	`
	s := compileString(t, rt, "companion-parsers", src)
	_, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.xmp", Content: []byte("")},
		MatchInfo{Pattern: "${basename}.xmp", Basename: "report"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestCompanion_FailRaises confirms uplink.fail surfaces as an error
// from RunCompanion.
func TestCompanion_FailRaises(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `uplink.fail("nope")`
	s := compileString(t, rt, "companion-fail", src)
	_, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.xmp", Content: []byte("")},
		MatchInfo{Pattern: "${basename}.xmp", Basename: "report"})
	if err == nil {
		t.Fatal("expected error from uplink.fail")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %q does not carry the fail reason", err.Error())
	}
}

// TestCompanion_TimeoutFires verifies the wall-clock cap interrupts a
// runaway script. Script burns CPU in a tight loop until the runtime
// cancels it.
func TestCompanion_TimeoutFires(t *testing.T) {
	rt := NewRuntime(slog.Default()).WithTimeout(100 * time.Millisecond)
	src := `while true do end`
	s := compileString(t, rt, "companion-timeout", src)
	_, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.xmp", Content: []byte("")},
		MatchInfo{Pattern: "${basename}.xmp", Basename: "report"})
	if err == nil {
		t.Fatal("expected timeout error from runaway loop")
	}
	if !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "cancel") {
		t.Errorf("error %q does not look like a timeout", err.Error())
	}
}

// TestCompanion_RejectsKeyedTableReturn verifies that a script
// returning a keyed table (instead of an array of field entries)
// surfaces a clear error rather than silently misbehaving.
func TestCompanion_RejectsKeyedTableReturn(t *testing.T) {
	rt := NewRuntime(slog.Default())
	src := `return { name = "hello", value = "world" }`
	s := compileString(t, rt, "companion-keyed", src)
	_, err := runCompanion(t, s,
		CompanionFile{Path: "folder/report.xmp", Content: []byte("")},
		MatchInfo{Pattern: "${basename}.xmp", Basename: "report"})
	if err == nil {
		t.Fatal("expected error for keyed-table return")
	}
	if !strings.Contains(err.Error(), "sequence") {
		t.Errorf("error %q should mention the required sequence shape", err.Error())
	}
}
