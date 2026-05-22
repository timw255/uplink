package azblob

import (
	"fmt"
	"os"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// Config is the YAML schema for an azblob connector instance.
type Config struct {
	// Account is the storage account name (the `<account>` in
	// https://<account>.blob.core.windows.net). Required unless a
	// connection string is supplied.
	Account string

	// Container is the blob container to watch. Required.
	Container string

	// Prefix scopes the source to blobs whose names start with this
	// string. Empty means the whole container.
	Prefix string

	// AccountKey / SASToken / ConnectionString are the three auth
	// modes. Provide one (and only one). Each has an *Env variant
	// that resolves the value from an environment variable at startup;
	// env-var indirection wins over the inline value of the same kind.
	// Leave all six empty to use the ambient DefaultAzureCredential.
	AccountKey          string
	SASToken            string
	ConnectionString    string
	AccountKeyEnv       string
	SASTokenEnv         string
	ConnectionStringEnv string

	// ServiceURL overrides the default https://<account>.blob.core.windows.net.
	// Use for Azurite (local emulator) or sovereign clouds. ServiceURLEnv
	// is the env-var indirection variant.
	ServiceURL    string
	ServiceURLEnv string

	// PollInterval is how often the EventSource lists the container
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
	if v, ok := raw["account"].(string); ok {
		cfg.Account = v
	}
	if v, ok := raw["container"].(string); ok {
		cfg.Container = v
	}
	if v, ok := raw["prefix"].(string); ok {
		cfg.Prefix = v
	}
	if v, ok := raw["account_key"].(string); ok {
		cfg.AccountKey = v
	}
	if v, ok := raw["sas_token"].(string); ok {
		cfg.SASToken = v
	}
	if v, ok := raw["connection_string"].(string); ok {
		cfg.ConnectionString = v
	}
	if v, ok := raw["account_key_env"].(string); ok {
		cfg.AccountKeyEnv = v
	}
	if v, ok := raw["sas_token_env"].(string); ok {
		cfg.SASTokenEnv = v
	}
	if v, ok := raw["connection_string_env"].(string); ok {
		cfg.ConnectionStringEnv = v
	}
	if v, ok := raw["service_url"].(string); ok {
		cfg.ServiceURL = v
	}
	if v, ok := raw["service_url_env"].(string); ok {
		cfg.ServiceURLEnv = v
	}
	if v, ok := raw["poll_interval"].(string); ok {
		d, err := connector.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("azblob[%s]: poll_interval: %w", name, err)
		}
		cfg.PollInterval = d
	}
	watchers, err := connector.ParseWatchersYAML("azblob", name, raw["watchers"])
	if err != nil {
		return nil, err
	}
	cfg.Watchers = watchers

	if cfg.Container == "" {
		return nil, fmt.Errorf("azblob[%s]: container is required", name)
	}

	// Env-var indirection wins over inline so a tracked example can
	// be overridden without rewriting the inline value.
	if cfg.AccountKeyEnv != "" {
		cfg.AccountKey = os.Getenv(cfg.AccountKeyEnv)
	}
	if cfg.SASTokenEnv != "" {
		cfg.SASToken = os.Getenv(cfg.SASTokenEnv)
	}
	if cfg.ConnectionStringEnv != "" {
		cfg.ConnectionString = os.Getenv(cfg.ConnectionStringEnv)
	}
	if cfg.ServiceURLEnv != "" {
		cfg.ServiceURL = os.Getenv(cfg.ServiceURLEnv)
	}

	authCount := 0
	if cfg.AccountKey != "" {
		authCount++
	}
	if cfg.SASToken != "" {
		authCount++
	}
	if cfg.ConnectionString != "" {
		authCount++
	}
	if authCount > 1 {
		return nil, fmt.Errorf("azblob[%s]: only one of account_key / sas_token / connection_string may be set", name)
	}
	if cfg.ConnectionString == "" && cfg.Account == "" {
		return nil, fmt.Errorf("azblob[%s]: account is required (unless connection_string is set)", name)
	}
	return cfg, nil
}
