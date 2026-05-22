package b2

import (
	"fmt"
	"os"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// Config is the YAML schema for a b2 connector instance.
type Config struct {
	// Bucket is the B2 bucket name. Required.
	Bucket string

	// Prefix scopes the source to objects whose names start with
	// this string. Empty means the whole bucket.
	Prefix string

	// KeyID and ApplicationKey are the B2 credentials. Provide them
	// inline (when the config file is gitignored) or via the *Env
	// fields, which resolve from environment variables at startup.
	// Exactly one of (inline KeyID, KeyIDEnv) must be set; same for
	// ApplicationKey / ApplicationKeyEnv.
	KeyID             string
	ApplicationKey    string
	KeyIDEnv          string
	ApplicationKeyEnv string

	// PollInterval is how often the EventSource lists the bucket
	// and diffs against last-known state. Defaults to 60s.
	PollInterval time.Duration

	// Watchers, when set, declares additional per-prefix poll loops
	// running at their own cadences. See internal/connector/watcher.go.
	Watchers []connector.WatcherSpec
}

func loadConfig(name string, raw map[string]any) (*Config, error) {
	cfg := &Config{
		PollInterval: 60 * time.Second,
	}
	if v, ok := raw["bucket"].(string); ok {
		cfg.Bucket = v
	}
	if v, ok := raw["prefix"].(string); ok {
		cfg.Prefix = v
	}
	if v, ok := raw["key_id"].(string); ok {
		cfg.KeyID = v
	}
	if v, ok := raw["application_key"].(string); ok {
		cfg.ApplicationKey = v
	}
	if v, ok := raw["key_id_env"].(string); ok {
		cfg.KeyIDEnv = v
	}
	if v, ok := raw["application_key_env"].(string); ok {
		cfg.ApplicationKeyEnv = v
	}
	if v, ok := raw["poll_interval"].(string); ok {
		d, err := connector.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("b2[%s]: poll_interval: %w", name, err)
		}
		cfg.PollInterval = d
	}
	watchers, err := connector.ParseWatchersYAML("b2", name, raw["watchers"])
	if err != nil {
		return nil, err
	}
	cfg.Watchers = watchers

	if cfg.Bucket == "" {
		return nil, fmt.Errorf("b2[%s]: bucket is required", name)
	}
	// Env-var indirection wins over inline so a tracked example.yaml
	// referencing env vars can be overridden by a gitignored local.yaml
	// without changing the env-var name.
	if cfg.KeyIDEnv != "" {
		cfg.KeyID = os.Getenv(cfg.KeyIDEnv)
	}
	if cfg.ApplicationKeyEnv != "" {
		cfg.ApplicationKey = os.Getenv(cfg.ApplicationKeyEnv)
	}
	if cfg.KeyID == "" {
		return nil, fmt.Errorf("b2[%s]: key_id (or key_id_env) is required", name)
	}
	if cfg.ApplicationKey == "" {
		return nil, fmt.Errorf("b2[%s]: application_key (or application_key_env) is required", name)
	}
	return cfg, nil
}
