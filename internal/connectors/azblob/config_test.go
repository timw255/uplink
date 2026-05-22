package azblob

import (
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig("azblob-test", map[string]any{
		"account":   "myaccount",
		"container": "my-container",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Container != "my-container" {
		t.Fatalf("container = %q", cfg.Container)
	}
	if cfg.Account != "myaccount" {
		t.Fatalf("account = %q", cfg.Account)
	}
	if cfg.PollInterval == 0 {
		t.Fatal("PollInterval default not applied")
	}
}

func TestLoadConfigRequiresContainer(t *testing.T) {
	_, err := loadConfig("azblob-test", map[string]any{
		"account": "myaccount",
	})
	if err == nil || !strings.Contains(err.Error(), "container is required") {
		t.Fatalf("expected container-required error, got %v", err)
	}
}

func TestLoadConfigRequiresAccountUnlessConnectionString(t *testing.T) {
	_, err := loadConfig("azblob-test", map[string]any{
		"container": "c",
	})
	if err == nil || !strings.Contains(err.Error(), "account is required") {
		t.Fatalf("expected account-required error, got %v", err)
	}
}

func TestLoadConfigConnectionStringSkipsAccount(t *testing.T) {
	t.Setenv("AZ_CONN", "DefaultEndpointsProtocol=https;AccountName=x;AccountKey=y;")
	cfg, err := loadConfig("azblob-test", map[string]any{
		"container":             "c",
		"connection_string_env": "AZ_CONN",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if !strings.Contains(cfg.ConnectionString, "AccountName=x") {
		t.Fatalf("ConnectionString = %q (expected env-resolved value)", cfg.ConnectionString)
	}
}

func TestLoadConfigOnlyOneAuthMode(t *testing.T) {
	t.Setenv("AZ_KEY", "k")
	t.Setenv("AZ_SAS", "s")
	_, err := loadConfig("azblob-test", map[string]any{
		"account":         "a",
		"container":       "c",
		"account_key_env": "AZ_KEY",
		"sas_token_env":   "AZ_SAS",
	})
	if err == nil || !strings.Contains(err.Error(), "only one of") {
		t.Fatalf("expected single-auth-mode error, got %v", err)
	}
}

// TestLoadConfigEmptyEnvIsAmbient confirms that an env-var-indirected
// credential which resolves to the empty string is treated as "no
// credential configured here," which lets the ambient
// DefaultAzureCredential fall through. The connector's Init returns
// "no credential configured" only at runtime, not at load time.
func TestLoadConfigEmptyEnvIsAmbient(t *testing.T) {
	t.Setenv("UNSET_KEY", "")
	cfg, err := loadConfig("azblob-test", map[string]any{
		"account":         "a",
		"container":       "c",
		"account_key_env": "UNSET_KEY",
	})
	if err != nil {
		t.Fatalf("loadConfig should accept empty-env (ambient-creds fallback): %v", err)
	}
	if cfg.AccountKey != "" {
		t.Errorf("expected empty AccountKey, got %q", cfg.AccountKey)
	}
}

func TestLoadConfigInlineCredentials(t *testing.T) {
	cfg, err := loadConfig("azblob-test", map[string]any{
		"account":     "myacct",
		"container":   "c",
		"account_key": "key-inline",
		"service_url": "http://127.0.0.1:10000/devstoreaccount1",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.AccountKey != "key-inline" {
		t.Errorf("AccountKey = %q, want inline value", cfg.AccountKey)
	}
	if cfg.ServiceURL != "http://127.0.0.1:10000/devstoreaccount1" {
		t.Errorf("ServiceURL = %q, want inline value", cfg.ServiceURL)
	}
}

func TestLoadConfigEnvWinsOverInline(t *testing.T) {
	t.Setenv("AZ_KEY", "from-env")
	cfg, err := loadConfig("azblob-test", map[string]any{
		"account":         "a",
		"container":       "c",
		"account_key":     "inline-loses",
		"account_key_env": "AZ_KEY",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.AccountKey != "from-env" {
		t.Errorf("AccountKey = %q, want env value", cfg.AccountKey)
	}
}

func TestLoadConfigPollIntervalParse(t *testing.T) {
	cfg, err := loadConfig("azblob-test", map[string]any{
		"account":       "a",
		"container":     "c",
		"poll_interval": "30s",
	})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PollInterval.Seconds() != 30 {
		t.Fatalf("poll interval = %v", cfg.PollInterval)
	}
}

func TestLoadConfigPollIntervalInvalid(t *testing.T) {
	_, err := loadConfig("azblob-test", map[string]any{
		"account":       "a",
		"container":     "c",
		"poll_interval": "not-a-duration",
	})
	if err == nil || !strings.Contains(err.Error(), "poll_interval") {
		t.Fatalf("expected poll_interval parse error, got %v", err)
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

func TestServiceURLDefault(t *testing.T) {
	c := &Connector{cfg: &Config{Account: "myacct"}}
	want := "https://myacct.blob.core.windows.net/"
	if got := c.serviceURL(); got != want {
		t.Fatalf("serviceURL = %q want %q", got, want)
	}
}

func TestServiceURLOverride(t *testing.T) {
	c := &Connector{cfg: &Config{Account: "myacct", ServiceURL: "http://127.0.0.1:10000/devstoreaccount1"}}
	want := "http://127.0.0.1:10000/devstoreaccount1/"
	if got := c.serviceURL(); got != want {
		t.Fatalf("serviceURL = %q want %q", got, want)
	}
}
