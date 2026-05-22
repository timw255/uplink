package channel

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCompiledScript / fakeScriptCompiler let the registry tests exercise
// companion script compilation without pulling in the Lua runtime.
type fakeCompiledScript struct{ name string }

func (f fakeCompiledScript) Name() string { return f.name }

type fakeScriptCompiler struct{}

func (fakeScriptCompiler) Compile(name, path string) (CompiledScript, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("fake compile %q: %w", name, err)
	}
	return fakeCompiledScript{name: name}, nil
}

// writeCompanionYAML builds a minimal YAML that declares a companion
// alongside one channel, after dropping the named script files into the
// directory holding the test binary so the relative-path resolver inside
// Load can find them.
func writeCompanionYAML(t *testing.T, companionsBlock string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	exeDir := filepath.Dir(exe)
	scripts := []string{"scripts/xmp.lua", "scripts/captions.lua", "scripts/json.lua"}
	for _, rel := range scripts {
		dst := filepath.Join(exeDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatalf("mkdir for fake script: %v", err)
		}
		if err := os.WriteFile(dst, []byte("-- noop"), 0o600); err != nil {
			t.Fatalf("write fake script: %v", err)
		}
		t.Cleanup(func() { _ = os.Remove(dst) })
	}
	body := `
logging:
  level: info
storage:
  data_dir: "./data"
connectors:
  - name: fs-in
    type: localfs
    config:
      root: "./_test/in"
  - name: aprimo-prod
    type: aprimo
    config:
      environment: acme
      client_id: id
      client_secret: sec
channels:
  - name: photos
    source: fs-in
    destination: aprimo-prod
    trigger:
      event: OnCreate
    companions:
` + companionsBlock
	return writeTempYAML(t, body)
}

func TestLoadConfig_CompanionsParse(t *testing.T) {
	yamlPath := writeCompanionYAML(t, `      - pattern: "${basename}.xmp"
        script: scripts/xmp.lua
      - pattern: "${basename}.caption.${lang}.txt"
        script: scripts/captions.lua
`)
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Channels) != 1 {
		t.Fatalf("channels: %d", len(cfg.Channels))
	}
	got := cfg.Channels[0].Companions
	if len(got) != 2 {
		t.Fatalf("companions: %d, want 2", len(got))
	}
	if got[0].Pattern != "${basename}.xmp" {
		t.Errorf("got[0].Pattern = %q", got[0].Pattern)
	}
	if !filepath.IsAbs(got[0].Script) {
		t.Errorf("got[0].Script not absolute: %q", got[0].Script)
	}
}

func TestLoadConfig_CompanionMissingScript(t *testing.T) {
	yamlPath := writeCompanionYAML(t, `      - pattern: "${basename}.xmp"
        script: scripts/does-not-exist.lua
`)
	_, err := Load(yamlPath)
	if err == nil {
		t.Fatal("expected error for missing companion script")
	}
	if !strings.Contains(err.Error(), "does-not-exist.lua") {
		t.Errorf("error %q does not mention the missing script", err.Error())
	}
}

func TestLoadConfig_CompanionBadPattern(t *testing.T) {
	yamlPath := writeCompanionYAML(t, `      - pattern: "no-basename-here.xmp"
        script: scripts/xmp.lua
`)
	_, err := Load(yamlPath)
	if err == nil {
		t.Fatal("expected error for pattern missing ${basename}")
	}
	if !strings.Contains(err.Error(), "${basename}") {
		t.Errorf("error %q does not mention ${basename}", err.Error())
	}
}

func TestLoadConfig_CompanionDuplicatePattern(t *testing.T) {
	yamlPath := writeCompanionYAML(t, `      - pattern: "${basename}.xmp"
        script: scripts/xmp.lua
      - pattern: "${basename}.xmp"
        script: scripts/json.lua
`)
	_, err := Load(yamlPath)
	if err == nil {
		t.Fatal("expected error for duplicate companion pattern")
	}
	if !strings.Contains(err.Error(), "duplicate companion pattern") {
		t.Errorf("error %q does not flag duplicate", err.Error())
	}
}

func TestRegistry_CompiledCompanions(t *testing.T) {
	yamlPath := writeCompanionYAML(t, `      - pattern: "${basename}.xmp"
        script: scripts/xmp.lua
`)
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg, err := NewRegistry(cfg.Channels, fakeScriptCompiler{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	ch := reg.Lookup("photos")
	if ch == nil {
		t.Fatal("channel not in registry")
	}
	if len(ch.Companions) != 1 {
		t.Fatalf("Companions: %d, want 1", len(ch.Companions))
	}
	co := ch.Companions[0]
	if co.Pattern == nil || co.Pattern.Raw() != "${basename}.xmp" {
		t.Errorf("Pattern not preserved: %+v", co.Pattern)
	}
	if co.Script == nil || co.Script.Name() != "${basename}.xmp" {
		t.Errorf("Script not compiled: %+v", co.Script)
	}
	// Sanity: the compiled pattern actually matches.
	if co.Pattern.Match("dir/foo.xmp") == nil {
		t.Error("compiled pattern doesn't match the canonical example")
	}
}

func TestChannel_MatchCompanion(t *testing.T) {
	yamlPath := writeCompanionYAML(t, `      - pattern: "${basename}.xmp"
        script: scripts/xmp.lua
      - pattern: "${basename}.caption.${lang}.txt"
        script: scripts/captions.lua
`)
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg, err := NewRegistry(cfg.Channels, fakeScriptCompiler{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	ch := reg.Lookup("photos")

	// XMP pattern matches; the captions pattern doesn't.
	m := ch.MatchCompanion("dir/sunset.xmp")
	if m == nil {
		t.Fatal("expected match on first pattern")
	}
	if m.Match.Basename != "sunset" {
		t.Errorf("Basename = %q", m.Match.Basename)
	}
	if m.Companion.Pattern.Raw() != "${basename}.xmp" {
		t.Errorf("Companion.Pattern = %q", m.Companion.Pattern.Raw())
	}

	// Caption pattern matches; the xmp pattern doesn't.
	m = ch.MatchCompanion("dir/sunset.caption.en.txt")
	if m == nil {
		t.Fatal("expected match on second pattern")
	}
	if got := m.Match.Vars["lang"]; got != "en" {
		t.Errorf("Vars[lang] = %q", got)
	}
	if m.Companion.Pattern.Raw() != "${basename}.caption.${lang}.txt" {
		t.Errorf("Companion.Pattern = %q", m.Companion.Pattern.Raw())
	}

	// Neither pattern matches a plain asset.
	if got := ch.MatchCompanion("dir/sunset.jpg"); got != nil {
		t.Errorf("expected nil match, got %+v", got)
	}
}

func TestRegistry_MatchCompanion(t *testing.T) {
	yamlPath := writeCompanionYAML(t, `      - pattern: "${basename}.xmp"
        script: scripts/xmp.lua
`)
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg, err := NewRegistry(cfg.Channels, fakeScriptCompiler{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	matches := reg.MatchCompanions("fs-in", "photos/sunset.xmp")
	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 match, got %d", len(matches))
	}
	m := matches[0]
	if m.Channel.Spec.Name != "photos" {
		t.Errorf("Channel = %q", m.Channel.Spec.Name)
	}
	if m.Match.Basename != "sunset" || m.Match.Dir != "photos" {
		t.Errorf("Match = %+v", m.Match)
	}

	// Wrong source connector — no match even though the path looks
	// companion-shaped.
	if got := reg.MatchCompanions("other-source", "photos/sunset.xmp"); len(got) != 0 {
		t.Errorf("expected zero matches for unconfigured source, got %d", len(got))
	}
}

func TestRegistry_CompanionWithoutCompilerErrors(t *testing.T) {
	// If a channel declares a companion but the engine forgot to pass
	// a compiler, NewRegistry must refuse to silently drop the script.
	yamlPath := writeCompanionYAML(t, `      - pattern: "${basename}.xmp"
        script: scripts/xmp.lua
`)
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	_, err = NewRegistry(cfg.Channels, nil)
	if err == nil {
		t.Fatal("expected NewRegistry to fail when compiler is nil and companions exist")
	}
	if !strings.Contains(err.Error(), "no script compiler") {
		t.Errorf("error %q does not flag the missing compiler", err.Error())
	}
}

func TestCompilePattern_Valid(t *testing.T) {
	cases := []string{
		"${basename}.xmp",
		"${basename}.${extension}.metadata.json",
		"${basename}.caption.${lang}.txt",
		"${basename}.metadata.*.json",
		"${basename}.${extension}.metadata.*.json",
		"${basename}.${lang}.${region}.json",
		"${basename}_${lang}.json",
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			p, err := CompilePattern(raw)
			if err != nil {
				t.Fatalf("CompilePattern(%q): %v", raw, err)
			}
			if p.Raw() != raw {
				t.Fatalf("Raw() = %q, want %q", p.Raw(), raw)
			}
		})
	}
}

func TestCompilePattern_Invalid(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"", "empty"},
		{"foo.bar", "${basename}"},                                      // no basename
		{"${basename}/${name}.txt", "must not contain '/'"},             // multi-segment
		{"${basename}${basename}.txt", "appears more than once"},        // duplicate basename
		{"${basename}.${extension}.${extension}", "appears more than once"},
		{"${basename}.${lang}.${lang}.txt", "appears more than once"},
		{"${basename}.${basename}.txt", "appears more than once"},       // duplicate also caught
		{"${basename}.${1bad}.txt", "invalid var name"},                 // ident starts with digit
		{"${basename}.${bad-name}.txt", "invalid var name"},             // ident has dash
		{"${basename}.${}.txt", "empty ${}"},
		{"${basename}.${unterminated.txt", "unterminated"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			_, err := CompilePattern(tc.raw)
			if err == nil {
				t.Fatalf("CompilePattern(%q): expected error containing %q, got nil", tc.raw, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompilePattern(%q): error %q does not contain %q", tc.raw, err.Error(), tc.want)
			}
		})
	}
}

func TestPatternMatch_Basic(t *testing.T) {
	p, err := CompilePattern("${basename}.xmp")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	m := p.Match("photos/sunset.xmp")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Basename != "sunset" {
		t.Errorf("Basename = %q, want %q", m.Basename, "sunset")
	}
	if m.Dir != "photos" {
		t.Errorf("Dir = %q, want %q", m.Dir, "photos")
	}
	if m.Path != "photos/sunset.xmp" {
		t.Errorf("Path = %q", m.Path)
	}
	if m.Extension != "" {
		t.Errorf("Extension = %q, want empty (pattern has no ${extension})", m.Extension)
	}
	if len(m.Vars) != 0 {
		t.Errorf("Vars = %v, want empty", m.Vars)
	}
	if len(m.Wildcards) != 0 {
		t.Errorf("Wildcards = %v, want empty", m.Wildcards)
	}
	if m.Pattern != "${basename}.xmp" {
		t.Errorf("Pattern = %q", m.Pattern)
	}
}

func TestPatternMatch_BasenameWithDots(t *testing.T) {
	// Greedy ${basename} must consume internal dots so that
	// `my.weird.photo.xmp` recovers basename = `my.weird.photo`,
	// matching the asset `my.weird.photo.jpg`.
	p, _ := CompilePattern("${basename}.xmp")
	m := p.Match("dir/my.weird.photo.xmp")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Basename != "my.weird.photo" {
		t.Errorf("Basename = %q, want %q", m.Basename, "my.weird.photo")
	}
}

func TestPatternMatch_WithExtension(t *testing.T) {
	p, err := CompilePattern("${basename}.${extension}.metadata.json")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	m := p.Match("photos/sunset.jpg.metadata.json")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Basename != "sunset" {
		t.Errorf("Basename = %q", m.Basename)
	}
	if m.Extension != "jpg" {
		t.Errorf("Extension = %q", m.Extension)
	}
}

func TestPatternMatch_WithExtensionAndDottedBasename(t *testing.T) {
	p, _ := CompilePattern("${basename}.${extension}.metadata.json")
	m := p.Match("photos/my.weird.photo.jpg.metadata.json")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Basename != "my.weird.photo" {
		t.Errorf("Basename = %q, want %q", m.Basename, "my.weird.photo")
	}
	if m.Extension != "jpg" {
		t.Errorf("Extension = %q, want %q", m.Extension, "jpg")
	}
}

func TestPatternMatch_NamedCapture(t *testing.T) {
	p, err := CompilePattern("${basename}.caption.${lang}.txt")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	m := p.Match("photos/sunset.caption.en.txt")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Basename != "sunset" {
		t.Errorf("Basename = %q", m.Basename)
	}
	if got := m.Vars["lang"]; got != "en" {
		t.Errorf("Vars[lang] = %q, want %q", got, "en")
	}
}

func TestPatternMatch_Wildcards(t *testing.T) {
	p, err := CompilePattern("${basename}.metadata.*.*.json")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	m := p.Match("photos/sunset.metadata.en.v2.json")
	if m == nil {
		t.Fatal("expected match")
	}
	if got := m.Wildcards; len(got) != 2 || got[0] != "en" || got[1] != "v2" {
		t.Errorf("Wildcards = %v, want [en v2]", got)
	}
}

func TestPatternMatch_MixedNamedAndWildcard(t *testing.T) {
	p, _ := CompilePattern("${basename}.${lang}.*.json")
	m := p.Match("dir/sunset.en.v3.json")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Vars["lang"] != "en" {
		t.Errorf("Vars[lang] = %q", m.Vars["lang"])
	}
	if len(m.Wildcards) != 1 || m.Wildcards[0] != "v3" {
		t.Errorf("Wildcards = %v", m.Wildcards)
	}
}

func TestPatternMatch_NoMatch(t *testing.T) {
	p, _ := CompilePattern("${basename}.xmp")
	if p.Match("photos/sunset.jpg") != nil {
		t.Error("expected nil match for non-companion path")
	}
	// extension separator missing
	if p.Match("photos/sunsetxmp") != nil {
		t.Error("expected nil match when literal '.' is absent")
	}
}

func TestPatternMatch_NamedCaptureRejectsDots(t *testing.T) {
	// Named captures (and wildcards) match [^/.]+, so a dotted middle
	// shouldn't match a single-segment slot.
	p, _ := CompilePattern("${basename}.caption.${lang}.txt")
	if p.Match("dir/sunset.caption.en.US.txt") != nil {
		t.Error("expected nil: dotted lang value should not match a named capture")
	}
}

func TestPatternMatch_NoDirectory(t *testing.T) {
	p, _ := CompilePattern("${basename}.xmp")
	m := p.Match("sunset.xmp")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Dir != "" {
		t.Errorf("Dir = %q, want empty for top-level file", m.Dir)
	}
	if m.Basename != "sunset" {
		t.Errorf("Basename = %q", m.Basename)
	}
}

func TestPatternHasExtensionVar(t *testing.T) {
	with, _ := CompilePattern("${basename}.${extension}.json")
	if !with.HasExtensionVar() {
		t.Error("HasExtensionVar() = false, want true")
	}
	without, _ := CompilePattern("${basename}.json")
	if without.HasExtensionVar() {
		t.Error("HasExtensionVar() = true, want false")
	}
}

func TestParseSegments_BareDollar(t *testing.T) {
	// A `$` not followed by `{` is a literal — useful in scenarios
	// like a `$price` placeholder used by the upstream system. The
	// pattern as a whole still requires ${basename} to be valid.
	p, err := CompilePattern("${basename}$tag.xmp")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	m := p.Match("foo$tag.xmp")
	if m == nil {
		t.Fatal("expected match")
	}
	if m.Basename != "foo" {
		t.Errorf("Basename = %q, want %q", m.Basename, "foo")
	}
}
