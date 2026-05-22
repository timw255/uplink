// Package channel parses the Uplink YAML config and compiles each
// channel's trigger filter (CEL) into a runtime-ready form.
//
// A channel binds a source connector, a destination connector, a trigger,
// and an optional transformation. Channels are the unit a customer edits.
package channel

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level shape of configs/<name>.yaml.
type Config struct {
	Logging    LoggingConfig   `yaml:"logging"`
	Storage    StorageConfig   `yaml:"storage"`
	Engine     EngineConfig    `yaml:"engine"`
	Connectors []ConnectorSpec `yaml:"connectors"`
	Channels   []ChannelSpec   `yaml:"channels"`
}

// EngineConfig holds runtime tuning for the worker pool + dispatcher.
// All values have sensible defaults; the YAML block is optional.
type EngineConfig struct {
	// Workers caps how many jobs run concurrently. Default 4. For
	// large-tenant deployments raise to 16–32. Higher values can
	// thrash against Aprimo's rate limit — pair with the aprimo
	// connector's max_concurrent knob.
	Workers int `yaml:"workers"`

	// PollIdle is how long workers sleep when ClaimNextJob returns no
	// claimable job. Default 500ms. Sub-second precision allowed here
	// (unlike connector-level poll_interval) because this is a tight
	// internal loop, not a backend hit.
	PollIdle string `yaml:"poll_idle"`

	// MaxAttempts caps retries per job before it lands in failed.
	// Default 5.
	MaxAttempts int `yaml:"max_attempts"`

	// BaseBackoff is the initial retry delay; subsequent attempts
	// double it up to channel.MaxRetryBackoff. Default 2s.
	BaseBackoff string `yaml:"base_backoff"`
}

// LoggingConfig controls daemon log verbosity and serialization.
// Both fields have safe defaults applied at load time; the CLI on
// `uplink run` can override either with a flag.
type LoggingConfig struct {
	// Level is one of "debug", "info", "warn", "error". Defaults to
	// "info" when omitted.
	Level string `yaml:"level"`

	// Format is one of "text", "json". Defaults to "text" when
	// omitted. Use "json" to pipe into log aggregators.
	Format string `yaml:"format"`
}

// StorageConfig holds on-disk storage settings.
type StorageConfig struct {
	DataDir string `yaml:"data_dir"`
}

// ConnectorSpec is one entry in the connectors: list. The Config field
// is a raw YAML block; each connector type defines its own schema and
// decodes the block on its own.
type ConnectorSpec struct {
	Name   string         `yaml:"name"`
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

// ChannelSpec is one entry in the channels: list.
type ChannelSpec struct {
	Name        string          `yaml:"name"`
	Source      string          `yaml:"source"`
	Destination string          `yaml:"destination"`
	Trigger     TriggerSpec     `yaml:"trigger"`
	Transform   *TransformSpec  `yaml:"transform,omitempty"`
	Companions  []CompanionSpec `yaml:"companions,omitempty"`
}

// TriggerSpec defines what fires the channel. Filter is an optional CEL
// expression that must evaluate to true for the event to be enqueued.
//
// Exactly one of Event or Events must be set:
//
//	trigger:
//	  event: OnCreate            # legacy single-event form
//
//	trigger:
//	  events: [OnCreate, OnUpdate]   # one channel fires on multiple kinds
//
// The two forms are mutually exclusive; mixing them in one trigger is a
// config error.
type TriggerSpec struct {
	Event  string   `yaml:"event,omitempty"`
	Events []string `yaml:"events,omitempty"`
	Filter string   `yaml:"filter,omitempty"`
}

// validEventKinds enumerates the event kinds a channel trigger can name.
// Matches the EventKind constants in internal/connector/connector.go.
var validEventKinds = map[string]struct{}{
	"OnCreate":         {},
	"OnUpdate":         {},
	"OnDelete":         {},
	"OnMove":           {},
	"OnMetadataChange": {},
}

// kinds normalizes a TriggerSpec into the set of event kinds it fires on.
// Returns an error when the spec is malformed: both Event and Events set,
// neither set, an empty entry in the list, or an unknown kind name.
//
// Called by validate (to surface errors at Load time) and by the runtime
// Registry (to build the per-channel match set).
func (t TriggerSpec) kinds() (map[string]struct{}, error) {
	hasSingle := t.Event != ""
	hasList := len(t.Events) > 0
	switch {
	case hasSingle && hasList:
		return nil, fmt.Errorf("trigger: set either event or events, not both")
	case !hasSingle && !hasList:
		return nil, fmt.Errorf("trigger: event or events is required")
	}

	source := t.Events
	if hasSingle {
		source = []string{t.Event}
	}

	set := make(map[string]struct{}, len(source))
	for _, k := range source {
		if k == "" {
			return nil, fmt.Errorf("trigger: empty event kind")
		}
		if _, ok := validEventKinds[k]; !ok {
			return nil, fmt.Errorf("trigger: unknown event kind %q", k)
		}
		if _, dup := set[k]; dup {
			return nil, fmt.Errorf("trigger: duplicate event kind %q", k)
		}
		set[k] = struct{}{}
	}
	return set, nil
}

// TransformSpec describes optional content/metadata transformation.
// Declarative Map handles ~80% of cases; Script is the escape hatch.
type TransformSpec struct {
	Script string         `yaml:"script,omitempty"`
	Map    map[string]any `yaml:"map,omitempty"`
}

// Load reads, parses, and validates a YAML config file. Relative
// companion.script paths are resolved against the directory holding
// the `uplink` binary (the same directory the default uplink.yaml
// lives in) and rewritten in place to absolute paths so downstream
// consumers don't have to track CWD or the config path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("channel: read config %s: %w", path, err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("channel: parse config %s: %w", path, err)
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("channel: resolve executable path: %w", err)
	}
	exeDir := filepath.Dir(exe)
	for i := range cfg.Channels {
		for j := range cfg.Channels[i].Companions {
			co := &cfg.Channels[i].Companions[j]
			if co.Script != "" && !filepath.IsAbs(co.Script) {
				co.Script = filepath.Join(exeDir, co.Script)
			}
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("channel: invalid config %s: %w", path, err)
	}
	return &cfg, nil
}

// MaxRetryBackoff is the cap applied to exponential backoff between attempts.
const MaxRetryBackoff = 5 * time.Minute

// validLogLevels enumerates the strings accepted in logging.level
// (and from --log-level). Lower-cased; matched directly.
var validLogLevels = map[string]struct{}{
	"debug": {}, "info": {}, "warn": {}, "error": {},
}

// validLogFormats enumerates the strings accepted in logging.format
// (and from --log-format).
var validLogFormats = map[string]struct{}{
	"text": {}, "json": {},
}

func (c *Config) validate() error {
	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage.data_dir is required")
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "text"
	}
	if _, ok := validLogLevels[c.Logging.Level]; !ok {
		return fmt.Errorf("logging.level %q must be one of debug, info, warn, error", c.Logging.Level)
	}
	if _, ok := validLogFormats[c.Logging.Format]; !ok {
		return fmt.Errorf("logging.format %q must be one of text, json", c.Logging.Format)
	}

	// connectorTypes maps connector instance name → connector type.
	connectorTypes := make(map[string]string, len(c.Connectors))
	for i, cc := range c.Connectors {
		if cc.Name == "" {
			return fmt.Errorf("connectors[%d]: name is required", i)
		}
		if cc.Type == "" {
			return fmt.Errorf("connectors[%d] (%s): type is required", i, cc.Name)
		}
		if _, dup := connectorTypes[cc.Name]; dup {
			return fmt.Errorf("connectors[%d]: duplicate name %q", i, cc.Name)
		}
		connectorTypes[cc.Name] = cc.Type
	}

	channelNames := make(map[string]struct{}, len(c.Channels))
	for i, ch := range c.Channels {
		if ch.Name == "" {
			return fmt.Errorf("channels[%d]: name is required", i)
		}
		if _, dup := channelNames[ch.Name]; dup {
			return fmt.Errorf("channels[%d]: duplicate name %q", i, ch.Name)
		}
		channelNames[ch.Name] = struct{}{}

		srcType, ok := connectorTypes[ch.Source]
		if !ok {
			return fmt.Errorf("channel %q: source %q is not a defined connector", ch.Name, ch.Source)
		}
		dstType, ok := connectorTypes[ch.Destination]
		if !ok {
			return fmt.Errorf("channel %q: destination %q is not a defined connector", ch.Name, ch.Destination)
		}
		if _, err := ch.Trigger.kinds(); err != nil {
			return fmt.Errorf("channel %q: %w", ch.Name, err)
		}

		// Uplink is storage → Aprimo, one direction only. Destination
		// must be the aprimo connector type; source must NOT be.
		if dstType != "aprimo" {
			return fmt.Errorf("channel %q: destination connector %q has type %q; channels must end at aprimo",
				ch.Name, ch.Destination, dstType)
		}
		if srcType == "aprimo" {
			return fmt.Errorf("channel %q: source connector %q is type aprimo; Aprimo is destination-only",
				ch.Name, ch.Source)
		}

		// Companion patterns: compile each so syntactic errors surface
		// here, not at scan time. Duplicate raw patterns are rejected
		// (two compiled patterns on one channel with identical regex
		// would always co-fire and there's no way to choose one).
		companionPatterns := make(map[string]struct{}, len(ch.Companions))
		for j, co := range ch.Companions {
			if co.Pattern == "" {
				return fmt.Errorf("channel %q: companions[%d]: pattern is required", ch.Name, j)
			}
			if _, dup := companionPatterns[co.Pattern]; dup {
				return fmt.Errorf("channel %q: duplicate companion pattern %q", ch.Name, co.Pattern)
			}
			companionPatterns[co.Pattern] = struct{}{}
			if _, err := CompilePattern(co.Pattern); err != nil {
				return fmt.Errorf("channel %q: companions[%d]: %w", ch.Name, j, err)
			}
			if co.Script == "" {
				return fmt.Errorf("channel %q: companion %q: script is required", ch.Name, co.Pattern)
			}
			info, err := os.Stat(co.Script)
			if err != nil {
				return fmt.Errorf("channel %q: companion %q: script %s: %w", ch.Name, co.Pattern, co.Script, err)
			}
			if info.IsDir() {
				return fmt.Errorf("channel %q: companion %q: script %s is a directory", ch.Name, co.Pattern, co.Script)
			}
		}
	}
	return nil
}

// Warnings reports configurations that load cleanly but are unlikely to
// behave the way an operator expects: zero channels, connectors that no
// channel references, channels whose only trigger is OnDelete (which
// the engine drops before dispatch). Each warning is a complete,
// human-readable sentence; callers typically log them at WARN level.
//
// validate() must have returned nil before calling this; Warnings does
// not re-check structural validity.
func (c *Config) Warnings() []string {
	var warnings []string

	if len(c.Channels) == 0 {
		warnings = append(warnings, "no channels configured; the daemon will start but never enqueue work")
	}

	// Connectors referenced by at least one channel as source or
	// destination — anything missing is dead weight.
	referenced := make(map[string]struct{}, len(c.Connectors))
	for _, ch := range c.Channels {
		referenced[ch.Source] = struct{}{}
		referenced[ch.Destination] = struct{}{}
	}
	for _, cc := range c.Connectors {
		if _, ok := referenced[cc.Name]; !ok {
			warnings = append(warnings, fmt.Sprintf(
				"connector %q (%s) is defined but no channel references it",
				cc.Name, cc.Type))
		}
	}

	// OnDelete-only channels: the engine drops EventDelete events before
	// matching, so a channel firing only on OnDelete will never produce
	// a job. Other kinds in the same trigger save it.
	for _, ch := range c.Channels {
		kinds, err := ch.Trigger.kinds()
		if err != nil || len(kinds) == 0 {
			continue // validate() already covered the malformed case
		}
		_, hasDelete := kinds["OnDelete"]
		if hasDelete && len(kinds) == 1 {
			warnings = append(warnings, fmt.Sprintf(
				"channel %q triggers only on OnDelete; the engine drops delete events, so this channel will never propagate work",
				ch.Name))
		}
	}

	return warnings
}
