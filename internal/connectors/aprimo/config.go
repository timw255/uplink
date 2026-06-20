package aprimo

import (
	"fmt"
	"os"
	"time"

	"github.com/timw255/uplink/internal/connector"
)

// Config is the YAML schema for an aprimo connector instance.
type Config struct {
	// Environment is the Aprimo subdomain (the `<env>` in
	// https://<env>.aprimo.com). Required.
	Environment string

	// ClientID / ClientSecret authenticate via client_credentials.
	// They may be supplied directly OR via the *Env fields, which
	// take precedence and resolve from process environment variables.
	ClientID        string
	ClientSecret    string
	ClientIDEnv     string
	ClientSecretEnv string

	// DefaultStatus is the lifecycle status applied to new records.
	// Defaults to "draft".
	DefaultStatus string

	// DefaultCollection, if set, files every new record into this
	// collection id.
	DefaultCollection string

	// HTTPTimeout for outbound API calls. Defaults to 60s.
	HTTPTimeout time.Duration

	// DirectUpload sends file bytes straight to Aprimo's Azure Blob
	// storage via a SAS URL, bypassing the rate-limited Aprimo upload
	// service — far faster for large files and it frees the RPS budget
	// for metadata. Defaults to true. Set false to force the segmented
	// upload service (a kill-switch if a tenant's direct path misbehaves).
	DirectUpload bool

	// DirectUploadConcurrency caps the direct path's block-staging budget.
	// By default uplink ramps the live budget toward this on its own to
	// fill the pipe; this value is just the ceiling (peak upload memory ≈
	// this × the 16 MiB block size). Lower it only when the full ramp is
	// too aggressive for the machine uplink runs on. 0 → a default ceiling.
	DirectUploadConcurrency int

	// DefaultLanguage is the IETF culture tag (e.g., "en-US") used for
	// companion-script-produced field values that don't specify a
	// language. Validated at connector Init against the tenant's
	// configured languages — must match one of the cultures returned
	// by /api/core/languages. Required only when at least one channel
	// using this connector declares companions that emit field
	// values; an empty value with no companion-script traffic is
	// fine.
	DefaultLanguage string

	// RefreshInterval is how often the prefetched catalogs (field
	// definitions, languages, classifications, option items, users,
	// user groups) are reloaded from Aprimo in the background.
	// Default 1h. Set to 0 to disable periodic refresh — operators
	// then restart the daemon to pick up new fields. Sub-second values
	// are rejected by connector.ParseDuration.
	RefreshInterval time.Duration

	// MaxConcurrent caps the in-flight Aprimo API request count.
	// 0 = uncapped. Memory + socket-pool safety net independent of
	// rate limiting (which is handled by RPS below).
	MaxConcurrent int

	// catalogUsage, when set, restricts catalog prefetch to the field types
	// a run references. Injected programmatically by the import command
	// (see cmd/uplink/import.go), never set from YAML. nil → fetch all.
	catalogUsage *catalogUsage

	// RPS is the sustained per-second request budget the SDK paces
	// itself against. Set to your tenant's licensed Aprimo RPS (the
	// default Aprimo allowance is on the order of 15; higher values
	// are licensable per environment). Burst capacity is fixed at
	// 100 (Aprimo's documented burst buffer). 0 disables rate
	// limiting — the SDK falls back to 429 retry.
	//
	// Match this knob to your environment's licensed RPS exactly:
	// setting it lower wastes capacity, setting it higher trips
	// 429s. The SDK pairs RPS with the fixed 100-token burst to
	// mirror Aprimo's server-side token bucket.
	RPS float64
}

// loadConfig parses the raw map[string]any from YAML into a typed
// Config and resolves any *Env references against the process
// environment.
func loadConfig(name string, raw map[string]any) (*Config, error) {
	cfg := &Config{
		DefaultStatus:           "draft",
		HTTPTimeout:             60 * time.Second,
		RefreshInterval:         1 * time.Hour,
		DirectUpload:            true,
		DirectUploadConcurrency: defaultBlockCeiling,
	}

	if v, ok := raw["environment"].(string); ok {
		cfg.Environment = v
	}
	if v, ok := raw["client_id"].(string); ok {
		cfg.ClientID = v
	}
	if v, ok := raw["client_secret"].(string); ok {
		cfg.ClientSecret = v
	}
	if v, ok := raw["client_id_env"].(string); ok {
		cfg.ClientIDEnv = v
	}
	if v, ok := raw["client_secret_env"].(string); ok {
		cfg.ClientSecretEnv = v
	}
	if v, ok := raw["default_status"].(string); ok {
		cfg.DefaultStatus = v
	}
	if v, ok := raw["default_collection"].(string); ok {
		cfg.DefaultCollection = v
	}
	if v, ok := raw["direct_upload"].(bool); ok {
		cfg.DirectUpload = v
	}
	if v, ok := raw["direct_upload_concurrency"].(int); ok && v > 0 {
		cfg.DirectUploadConcurrency = v
	}
	if v, ok := raw["http_timeout"].(string); ok {
		d, err := connector.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("aprimo[%s]: http_timeout: %w", name, err)
		}
		cfg.HTTPTimeout = d
	}
	if v, ok := raw["default_language"].(string); ok {
		cfg.DefaultLanguage = v
	}
	if v, ok := raw["refresh_interval"].(string); ok {
		d, err := connector.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("aprimo[%s]: refresh_interval: %w", name, err)
		}
		cfg.RefreshInterval = d
	}
	if v, ok := raw["max_concurrent"].(int); ok {
		cfg.MaxConcurrent = v
	}
	if v, ok := raw["rps"].(int); ok {
		cfg.RPS = float64(v)
	} else if v, ok := raw["rps"].(float64); ok {
		cfg.RPS = v
	}

	// Programmatic catalog-usage hint (import command only). Restricts which
	// catalogs Init prefetches to the field types the manifest references.
	if names, ok := raw["_catalog_field_names"].([]string); ok {
		usage := &catalogUsage{fieldNames: make(map[string]bool, len(names))}
		for _, n := range names {
			usage.fieldNames[canonicalFieldName(n)] = true
		}
		usage.usesLanguage, _ = raw["_catalog_uses_language"].(bool)
		cfg.catalogUsage = usage
	}

	// Resolve env-var indirections, *Env wins over inline.
	if cfg.ClientIDEnv != "" {
		cfg.ClientID = os.Getenv(cfg.ClientIDEnv)
	}
	if cfg.ClientSecretEnv != "" {
		cfg.ClientSecret = os.Getenv(cfg.ClientSecretEnv)
	}

	if cfg.Environment == "" {
		return nil, fmt.Errorf("aprimo[%s]: environment is required", name)
	}
	if cfg.ClientID == "" {
		return nil, fmt.Errorf("aprimo[%s]: client_id (or client_id_env) is required", name)
	}
	if cfg.ClientSecret == "" {
		return nil, fmt.Errorf("aprimo[%s]: client_secret (or client_secret_env) is required", name)
	}
	return cfg, nil
}
