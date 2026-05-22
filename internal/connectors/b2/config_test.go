package b2

import (
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("B2_KEY", "id")
	t.Setenv("B2_SECRET", "key")
	cfg, err := loadConfig("b2-test", map[string]any{
		"bucket":              "my-bucket",
		"key_id_env":          "B2_KEY",
		"application_key_env": "B2_SECRET",
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
	_, err := loadConfig("b2-test", map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "bucket is required") {
		t.Fatalf("expected bucket-required error, got %v", err)
	}
}

func TestLoadConfigRequiresApplicationKey(t *testing.T) {
	t.Setenv("X", "set")
	_, err := loadConfig("b2-test", map[string]any{
		"bucket":     "b",
		"key_id_env": "X",
	})
	if err == nil || !strings.Contains(err.Error(), "application_key") {
		t.Fatalf("expected application_key-required error, got %v", err)
	}
}

func TestLoadConfigRejectsEmptyEnvVarWithoutInline(t *testing.T) {
	t.Setenv("UNSET_KID", "")
	t.Setenv("UNSET_AK", "")
	_, err := loadConfig("b2-test", map[string]any{
		"bucket":              "b",
		"key_id_env":          "UNSET_KID",
		"application_key_env": "UNSET_AK",
	})
	if err == nil || !strings.Contains(err.Error(), "key_id") {
		t.Fatalf("expected key_id-required error when env var resolves empty, got %v", err)
	}
}

func TestLoadConfigInlineCredentials(t *testing.T) {
	cfg, err := loadConfig("b2-test", map[string]any{
		"bucket":          "b",
		"key_id":          "kid-inline",
		"application_key": "ak-inline",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.KeyID != "kid-inline" || cfg.ApplicationKey != "ak-inline" {
		t.Fatalf("inline credentials not retained: %+v", cfg)
	}
}

func TestLoadConfigEnvVarWinsOverInline(t *testing.T) {
	t.Setenv("OVERRIDE_KID", "from-env")
	t.Setenv("OVERRIDE_AK", "from-env-too")
	cfg, err := loadConfig("b2-test", map[string]any{
		"bucket":              "b",
		"key_id":              "inline-loses",
		"application_key":     "inline-loses",
		"key_id_env":          "OVERRIDE_KID",
		"application_key_env": "OVERRIDE_AK",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.KeyID != "from-env" {
		t.Errorf("KeyID = %q, want env value", cfg.KeyID)
	}
	if cfg.ApplicationKey != "from-env-too" {
		t.Errorf("ApplicationKey = %q, want env value", cfg.ApplicationKey)
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
