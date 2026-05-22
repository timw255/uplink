package s3

import (
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig("s3-test", map[string]any{
		"bucket": "my-bucket",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Bucket != "my-bucket" {
		t.Fatalf("bucket = %q", cfg.Bucket)
	}
	if cfg.PollInterval == 0 {
		t.Fatal("PollInterval default not applied")
	}
}

func TestLoadConfigRequiresBucket(t *testing.T) {
	_, err := loadConfig("s3-test", map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "bucket is required") {
		t.Fatalf("expected bucket-required error, got %v", err)
	}
}

func TestLoadConfigCredentialPairing(t *testing.T) {
	t.Setenv("ENV_AK", "ak-value")
	_, err := loadConfig("s3-test", map[string]any{
		"bucket":         "b",
		"access_key_env": "ENV_AK",
	})
	if err == nil || !strings.Contains(err.Error(), "must be set together") {
		t.Fatalf("expected paired-credential error, got %v", err)
	}
}

func TestLoadConfigInlineCredentials(t *testing.T) {
	cfg, err := loadConfig("s3-test", map[string]any{
		"bucket":     "b",
		"access_key": "AKIA-inline",
		"secret_key": "secret-inline",
		"endpoint":   "https://minio.local:9000",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.AccessKey != "AKIA-inline" || cfg.SecretKey != "secret-inline" {
		t.Errorf("inline credentials not retained: %+v", cfg)
	}
	if cfg.Endpoint != "https://minio.local:9000" {
		t.Errorf("inline endpoint not retained: %q", cfg.Endpoint)
	}
}

func TestLoadConfigEnvVarWinsOverInline(t *testing.T) {
	t.Setenv("S3_AK", "from-env")
	t.Setenv("S3_SK", "secret-from-env")
	cfg, err := loadConfig("s3-test", map[string]any{
		"bucket":         "b",
		"access_key":     "inline-loses",
		"secret_key":     "inline-loses",
		"access_key_env": "S3_AK",
		"secret_key_env": "S3_SK",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.AccessKey != "from-env" {
		t.Errorf("AccessKey = %q, want env value", cfg.AccessKey)
	}
	if cfg.SecretKey != "secret-from-env" {
		t.Errorf("SecretKey = %q, want env value", cfg.SecretKey)
	}
}

func TestLoadConfigAmbientCredentialsAllowed(t *testing.T) {
	// Neither inline nor *Env credentials set — the SDK falls back to
	// the ambient credential chain. Must not error at load time.
	cfg, err := loadConfig("s3-test", map[string]any{
		"bucket": "b",
	})
	if err != nil {
		t.Fatalf("loadConfig with no creds: %v", err)
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		t.Errorf("expected empty creds, got %+v", cfg)
	}
}

func TestLoadConfigPollIntervalParse(t *testing.T) {
	cfg, err := loadConfig("s3-test", map[string]any{
		"bucket":        "b",
		"poll_interval": "30s",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PollInterval.Seconds() != 30 {
		t.Fatalf("poll interval = %v", cfg.PollInterval)
	}
}

func TestFullKey(t *testing.T) {
	c := &Connector{cfg: &Config{Prefix: "media"}}
	if got := c.fullKey("video.mp4"); got != "media/video.mp4" {
		t.Fatalf("fullKey = %q", got)
	}
}

func TestRelKey(t *testing.T) {
	c := &Connector{cfg: &Config{Prefix: "media"}}
	if got := c.relKey("media/video.mp4"); got != "video.mp4" {
		t.Fatalf("relKey = %q", got)
	}
}

func TestTrimETag(t *testing.T) {
	if trimETag(`"abc123"`) != "abc123" {
		t.Fatal("trimETag failed")
	}
	if trimETag("abc123") != "abc123" {
		t.Fatal("trimETag on unquoted")
	}
}
