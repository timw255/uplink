package channel

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/timw255/uplink/internal/connector"
)

const sampleYAML = `
logging:
  level: info
  format: text
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
  - name: smoke
    source: fs-in
    destination: aprimo-prod
    trigger:
      event: OnCreate
      filter: 'size > 0'
`

func writeTempYAML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write tmp yaml: %v", err)
	}
	return p
}

func TestLoadAndValidate(t *testing.T) {
	cfg, err := Load(writeTempYAML(t, sampleYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Channels) != 1 || cfg.Channels[0].Name != "smoke" {
		t.Fatalf("unexpected channels: %+v", cfg.Channels)
	}
	if cfg.Logging.Level != "info" || cfg.Logging.Format != "text" {
		t.Fatalf("logging defaults wrong: %+v", cfg.Logging)
	}
}

func TestValidationCatchesUnknownSource(t *testing.T) {
	bad := strings.Replace(sampleYAML, "source: fs-in", "source: nope", 1)
	_, err := Load(writeTempYAML(t, bad))
	if err == nil || !strings.Contains(err.Error(), "not a defined connector") {
		t.Fatalf("expected unknown-source error, got %v", err)
	}
}

func TestRegistryAndFilterMatch(t *testing.T) {
	cfg, err := Load(writeTempYAML(t, sampleYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg, err := NewRegistry(cfg.Channels, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	chans := reg.ChannelsForSource("fs-in")
	if len(chans) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(chans))
	}

	matchEvent := connector.Event{
		Connector: "fs-in",
		Kind:      connector.EventCreate,
		Entry:     connector.Entry{Path: "a.txt", Size: 42},
	}
	ok, err := chans[0].Match(matchEvent)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !ok {
		t.Fatalf("expected match")
	}

	emptyEvent := matchEvent
	emptyEvent.Entry.Size = 0
	ok, err = chans[0].Match(emptyEvent)
	if err != nil {
		t.Fatalf("Match empty: %v", err)
	}
	if ok {
		t.Fatalf("expected non-match for size==0")
	}

	wrongKind := matchEvent
	wrongKind.Kind = connector.EventDelete
	ok, _ = chans[0].Match(wrongKind)
	if ok {
		t.Fatalf("expected non-match for wrong kind")
	}
}

func TestValidationRejectsNonAprimoDestination(t *testing.T) {
	bad := strings.Replace(sampleYAML, "destination: aprimo-prod", "destination: fs-in", 1)
	_, err := Load(writeTempYAML(t, bad))
	if err == nil || !strings.Contains(err.Error(), "channels must end at aprimo") {
		t.Fatalf("expected aprimo-destination error, got %v", err)
	}
}

func TestValidationRejectsAprimoSource(t *testing.T) {
	// Two-aprimo config — both ends type aprimo.
	yaml := `
storage:
  data_dir: "./data"
connectors:
  - name: aprimo-a
    type: aprimo
    config:
      environment: e
      client_id: id
      client_secret: sec
  - name: aprimo-b
    type: aprimo
    config:
      environment: e
      client_id: id
      client_secret: sec
channels:
  - name: bad
    source: aprimo-a
    destination: aprimo-b
    trigger:
      event: OnCreate
`
	_, err := Load(writeTempYAML(t, yaml))
	if err == nil || !strings.Contains(err.Error(), "Aprimo is destination-only") {
		t.Fatalf("expected aprimo-source error, got %v", err)
	}
}

func TestFilterCompileError(t *testing.T) {
	_, err := CompileFilter("this is not cel ::")
	if err == nil {
		t.Fatal("expected compile error")
	}
}

func TestWarnings_HealthyConfigHasNone(t *testing.T) {
	cfg, err := Load(writeTempYAML(t, sampleYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Warnings(); len(got) != 0 {
		t.Errorf("healthy config produced warnings: %v", got)
	}
}

func TestWarnings_ZeroChannels(t *testing.T) {
	// Strip the entire channels block; replace with an empty list. The
	// connectors stay so this isn't also an unreferenced-connector test.
	yml := strings.SplitN(sampleYAML, "channels:", 2)[0] + "channels: []\n"
	cfg, err := Load(writeTempYAML(t, yml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Warnings()
	if !containsSubstr(got, "no channels configured") {
		t.Fatalf("expected zero-channels warning, got %v", got)
	}
}

func TestWarnings_UnreferencedConnector(t *testing.T) {
	// Add a second localfs source that no channel uses.
	yml := strings.Replace(sampleYAML,
		"  - name: fs-in",
		"  - name: orphan\n    type: localfs\n    config:\n      root: \"./_test/orphan\"\n  - name: fs-in",
		1)
	cfg, err := Load(writeTempYAML(t, yml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Warnings()
	if !containsSubstr(got, `"orphan"`) {
		t.Fatalf("expected unreferenced-connector warning naming \"orphan\", got %v", got)
	}
}

func TestWarnings_OnDeleteOnlyChannel(t *testing.T) {
	yml := strings.Replace(sampleYAML, "event: OnCreate", "event: OnDelete", 1)
	// Also drop the size>0 filter since it isn't relevant here.
	yml = strings.Replace(yml, "      filter: 'size > 0'\n", "", 1)
	cfg, err := Load(writeTempYAML(t, yml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Warnings()
	if !containsSubstr(got, "OnDelete") {
		t.Fatalf("expected OnDelete-only warning, got %v", got)
	}
	if !containsSubstr(got, "smoke") {
		t.Fatalf("warning should name the offending channel, got %v", got)
	}
}

// Multi-kind channels including OnDelete must NOT trigger the warning.
func TestWarnings_OnDeleteWithOtherKindsIsFine(t *testing.T) {
	yml := strings.Replace(sampleYAML,
		"event: OnCreate",
		"events: [OnCreate, OnDelete]",
		1)
	yml = strings.Replace(yml, "      filter: 'size > 0'\n", "", 1)
	cfg, err := Load(writeTempYAML(t, yml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Warnings(); containsSubstr(got, "OnDelete") {
		t.Errorf("multi-kind channel including OnDelete should NOT warn, got %v", got)
	}
}

func containsSubstr(xs []string, sub string) bool {
	for _, s := range xs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func TestEmptyFilterAlwaysMatches(t *testing.T) {
	f, err := CompileFilter("")
	if err != nil {
		t.Fatalf("CompileFilter: %v", err)
	}
	ok, err := f.Matches(connector.Event{Kind: connector.EventCreate})
	if err != nil {
		t.Fatalf("Matches: %v", err)
	}
	if !ok {
		t.Fatal("empty filter should match")
	}
}

// TestTrigger_MultiEventChannelMatchesAllListed exercises the headline
// reason for adding TriggerSpec.Events: one channel firing on both
// OnCreate and OnUpdate, so a customer doesn't need two channels (and
// two separate sync_log streams) to track create-then-update on the
// same file.
func TestTrigger_MultiEventChannelMatchesAllListed(t *testing.T) {
	yml := strings.Replace(sampleYAML,
		"event: OnCreate",
		"events: [OnCreate, OnUpdate]",
		1)
	// Drop the redundant filter so this test only varies the events list.
	yml = strings.Replace(yml, "      filter: 'size > 0'\n", "", 1)

	cfg, err := Load(writeTempYAML(t, yml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg, err := NewRegistry(cfg.Channels, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	ch := reg.Lookup("smoke")
	if ch == nil {
		t.Fatal("channel not registered")
	}

	for _, kind := range []connector.EventKind{connector.EventCreate, connector.EventUpdate} {
		ok, err := ch.Match(connector.Event{Connector: "fs-in", Kind: kind})
		if err != nil {
			t.Fatalf("Match(%s): %v", kind, err)
		}
		if !ok {
			t.Errorf("expected channel to match %s", kind)
		}
	}

	// A kind not in the list must NOT match.
	ok, err := ch.Match(connector.Event{Connector: "fs-in", Kind: connector.EventDelete})
	if err != nil {
		t.Fatalf("Match(EventDelete): %v", err)
	}
	if ok {
		t.Error("channel matched EventDelete; should only match listed kinds")
	}
}

// TestTrigger_LegacySingleEventStillWorks confirms the original
// single-event YAML form is unchanged. Backwards-compat is the whole
// reason Event lives alongside Events.
func TestTrigger_LegacySingleEventStillWorks(t *testing.T) {
	cfg, err := Load(writeTempYAML(t, sampleYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg, _ := NewRegistry(cfg.Channels, nil)
	ch := reg.Lookup("smoke")

	ok, _ := ch.Match(connector.Event{Connector: "fs-in", Kind: connector.EventCreate, Entry: connector.Entry{Size: 1}})
	if !ok {
		t.Error("legacy event: OnCreate did not match an OnCreate event")
	}
	ok, _ = ch.Match(connector.Event{Connector: "fs-in", Kind: connector.EventUpdate, Entry: connector.Entry{Size: 1}})
	if ok {
		t.Error("legacy event: OnCreate matched an OnUpdate event")
	}
}

// TestTrigger_RejectsBothEventAndEvents covers the validation error
// for an ambiguous trigger spec.
func TestTrigger_RejectsBothEventAndEvents(t *testing.T) {
	yml := strings.Replace(sampleYAML,
		"event: OnCreate",
		"event: OnCreate\n      events: [OnUpdate]",
		1)
	_, err := Load(writeTempYAML(t, yml))
	if err == nil {
		t.Fatal("expected error when both event and events are set")
	}
	if !strings.Contains(err.Error(), "not both") {
		t.Errorf("error %q does not explain the rule", err)
	}
}

// TestTrigger_RejectsMissingEvent covers the validation error for a
// trigger with neither field set.
func TestTrigger_RejectsMissingEvent(t *testing.T) {
	// Replace the event line with the filter alone — no event, no events.
	yml := strings.Replace(sampleYAML,
		"event: OnCreate\n      filter: 'size > 0'",
		"filter: 'size > 0'",
		1)
	_, err := Load(writeTempYAML(t, yml))
	if err == nil {
		t.Fatal("expected error when trigger has neither event nor events")
	}
	if !strings.Contains(err.Error(), "required") {
		t.Errorf("error %q does not mention required field", err)
	}
}

// TestTrigger_RejectsUnknownEventKind catches typos.
func TestTrigger_RejectsUnknownEventKind(t *testing.T) {
	yml := strings.Replace(sampleYAML, "event: OnCreate", "events: [OnCreate, OnTypo]", 1)
	_, err := Load(writeTempYAML(t, yml))
	if err == nil {
		t.Fatal("expected error for unknown event kind")
	}
	if !strings.Contains(err.Error(), "unknown event kind") {
		t.Errorf("error %q does not mention unknown kind", err)
	}
}
