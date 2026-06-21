package aprimo

import "testing"

func baseRaw() map[string]any {
	return map[string]any{"environment": "x", "client_id": "a", "client_secret": "b"}
}

// rps is optional and defaults to 15, but rate limiting can't be disabled:
// an explicit 0 or negative value is a config error.
func TestLoadConfig_RPS(t *testing.T) {
	t.Run("unset defaults to 15", func(t *testing.T) {
		cfg, err := loadConfig("t", baseRaw())
		if err != nil {
			t.Fatal(err)
		}
		if cfg.RPS != defaultRPS {
			t.Fatalf("RPS = %v, want %d", cfg.RPS, defaultRPS)
		}
	})
	t.Run("positive is honored", func(t *testing.T) {
		raw := baseRaw()
		raw["rps"] = 30
		cfg, err := loadConfig("t", raw)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.RPS != 30 {
			t.Fatalf("RPS = %v, want 30", cfg.RPS)
		}
	})
	for _, bad := range []any{0, -5, 0.0} {
		raw := baseRaw()
		raw["rps"] = bad
		if _, err := loadConfig("t", raw); err == nil {
			t.Fatalf("rps=%v must be a config error (rate limiting can't be disabled)", bad)
		}
	}
}
