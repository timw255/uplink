package s3

import (
	"fmt"
	"os"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// Config is the YAML schema for an s3 connector instance.
type Config struct {
	// Region is the AWS region, e.g. "us-east-1". Required unless the
	// ambient SDK config already provides one.
	Region string

	// Bucket is the bucket name. Required.
	Bucket string

	// Prefix scopes the source to objects whose keys start with this
	// string. Empty means the whole bucket.
	Prefix string

	// AccessKey / SecretKey are static credentials. Provide them
	// inline (when the config file is gitignored) or via the *Env
	// fields, which resolve from environment variables at startup.
	// Leave all four empty to use the SDK's ambient credential chain
	// (instance profile on EC2/EKS, shared config, etc.).
	AccessKey    string
	SecretKey    string
	AccessKeyEnv string
	SecretKeyEnv string

	// Endpoint overrides the S3 endpoint URL. Use for S3-compatible
	// services: MinIO, R2, Backblaze B2's S3 API, etc. EndpointEnv is
	// the env-var indirection variant.
	Endpoint    string
	EndpointEnv string

	// PollInterval is how often the EventSource lists the bucket and
	// diffs against last-known state. Defaults to 60s.
	PollInterval time.Duration

	// UsePathStyle forces path-style addressing (bucket in path) vs
	// virtual-hosted-style (bucket in subdomain). Required for some
	// S3-compatible endpoints like MinIO.
	UsePathStyle bool

	// Watchers, when set, declares additional per-prefix poll loops
	// running at their own cadences. See internal/connector/watcher.go.
	Watchers []connector.WatcherSpec
}

// loadConfig parses the raw map[string]any from YAML.
func loadConfig(name string, raw map[string]any) (*Config, error) {
	cfg := &Config{
		PollInterval: 60 * time.Second,
	}
	if v, ok := raw["region"].(string); ok {
		cfg.Region = v
	}
	if v, ok := raw["bucket"].(string); ok {
		cfg.Bucket = v
	}
	if v, ok := raw["prefix"].(string); ok {
		cfg.Prefix = v
	}
	if v, ok := raw["access_key"].(string); ok {
		cfg.AccessKey = v
	}
	if v, ok := raw["secret_key"].(string); ok {
		cfg.SecretKey = v
	}
	if v, ok := raw["access_key_env"].(string); ok {
		cfg.AccessKeyEnv = v
	}
	if v, ok := raw["secret_key_env"].(string); ok {
		cfg.SecretKeyEnv = v
	}
	if v, ok := raw["endpoint"].(string); ok {
		cfg.Endpoint = v
	}
	if v, ok := raw["endpoint_env"].(string); ok {
		cfg.EndpointEnv = v
	}
	if v, ok := raw["use_path_style"].(bool); ok {
		cfg.UsePathStyle = v
	}
	if v, ok := raw["poll_interval"].(string); ok {
		d, err := connector.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("s3[%s]: poll_interval: %w", name, err)
		}
		cfg.PollInterval = d
	}
	watchers, err := connector.ParseWatchersYAML("s3", name, raw["watchers"])
	if err != nil {
		return nil, err
	}
	cfg.Watchers = watchers

	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3[%s]: bucket is required", name)
	}

	// Env-var indirection wins over inline so a tracked example.yaml
	// can be overridden by env without rewriting the inline value.
	if cfg.AccessKeyEnv != "" {
		cfg.AccessKey = os.Getenv(cfg.AccessKeyEnv)
	}
	if cfg.SecretKeyEnv != "" {
		cfg.SecretKey = os.Getenv(cfg.SecretKeyEnv)
	}
	if cfg.EndpointEnv != "" {
		cfg.Endpoint = os.Getenv(cfg.EndpointEnv)
	}

	// Static credentials are either both present or both absent; mixing
	// produces a confusing partial-auth state.
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, fmt.Errorf("s3[%s]: access_key and secret_key must be set together (or neither, to use the ambient credential chain)", name)
	}
	return cfg, nil
}
