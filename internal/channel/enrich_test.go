package channel

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeEnrichYAML builds a minimal single-channel YAML declaring an
// enrich block, dropping the referenced script files next to the test
// binary so Load's relative-path resolver can find them.
func writeEnrichYAML(t *testing.T, enrichBlock string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	exeDir := filepath.Dir(exe)
	for _, rel := range []string{"scripts/path-tags.lua", "scripts/other.lua"} {
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
      events: [OnCreate, OnDelete]
    enrich:
` + enrichBlock
	return writeTempYAML(t, body)
}

func TestLoadConfig_EnrichParse(t *testing.T) {
	yamlPath := writeEnrichYAML(t, `      - script: scripts/path-tags.lua
`)
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Channels[0].Enrich
	if len(got) != 1 {
		t.Fatalf("enrich: %d, want 1", len(got))
	}
	if !filepath.IsAbs(got[0].Script) {
		t.Errorf("enrich script not resolved to absolute path: %q", got[0].Script)
	}
	if !strings.HasSuffix(filepath.ToSlash(got[0].Script), "scripts/path-tags.lua") {
		t.Errorf("enrich script = %q", got[0].Script)
	}
}

func TestLoadConfig_EnrichMissingScriptField(t *testing.T) {
	yamlPath := writeEnrichYAML(t, `      - script: ""
`)
	_, err := Load(yamlPath)
	if err == nil || !strings.Contains(err.Error(), "script is required") {
		t.Fatalf("expected 'script is required' error, got %v", err)
	}
}

func TestLoadConfig_EnrichScriptNotFound(t *testing.T) {
	yamlPath := writeEnrichYAML(t, `      - script: scripts/does-not-exist.lua
`)
	_, err := Load(yamlPath)
	if err == nil || !strings.Contains(err.Error(), "does-not-exist.lua") {
		t.Fatalf("expected missing-script error, got %v", err)
	}
}

func TestLoadConfig_EnrichDuplicateScript(t *testing.T) {
	yamlPath := writeEnrichYAML(t, `      - script: scripts/path-tags.lua
      - script: scripts/path-tags.lua
`)
	_, err := Load(yamlPath)
	if err == nil || !strings.Contains(err.Error(), "duplicate enrich script") {
		t.Fatalf("expected duplicate-script error, got %v", err)
	}
}

func TestRegistry_CompiledEnrichers(t *testing.T) {
	yamlPath := writeEnrichYAML(t, `      - script: scripts/path-tags.lua
      - script: scripts/other.lua
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
		t.Fatal("channel photos not found")
	}
	if !ch.HasEnrichers() || len(ch.Enrichers) != 2 {
		t.Fatalf("Enrichers: %d (HasEnrichers=%v), want 2", len(ch.Enrichers), ch.HasEnrichers())
	}
	if !ch.FiresOn("OnDelete") {
		t.Error("FiresOn(OnDelete) = false, want true")
	}
	if ch.FiresOn("OnUpdate") {
		t.Error("FiresOn(OnUpdate) = true, want false (not in trigger)")
	}
}

func TestRegistry_EnrichWithoutCompilerErrors(t *testing.T) {
	yamlPath := writeEnrichYAML(t, `      - script: scripts/path-tags.lua
`)
	cfg, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := NewRegistry(cfg.Channels, nil); err == nil ||
		!strings.Contains(err.Error(), "no script compiler") {
		t.Fatalf("expected compiler-required error, got %v", err)
	}
}
