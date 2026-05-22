package connector

import (
	"strings"
	"testing"
	"time"
)

func TestParseDuration_AcceptsSecondAndCoarser(t *testing.T) {
	cases := map[string]time.Duration{
		"30s":     30 * time.Second,
		"2m":      2 * time.Minute,
		"1h":      time.Hour,
		"2h30m":   2*time.Hour + 30*time.Minute,
		"1.5h":    90 * time.Minute,
		"0s":      0,
		"3600s":   time.Hour,
		"24h":     24 * time.Hour,
	}
	for in, want := range cases {
		got, err := ParseDuration(in)
		if err != nil {
			t.Errorf("ParseDuration(%q): unexpected error %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseDuration(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestParseDuration_RejectsSubSecond(t *testing.T) {
	subSec := []string{
		"100ms",
		"1500ms",
		"500us",
		"1500ns",
		"1.5s", // valid Go duration, but the .5 lands in milliseconds
		"0.5s",
		"30m1ms",
	}
	for _, in := range subSec {
		_, err := ParseDuration(in)
		if err == nil {
			t.Errorf("ParseDuration(%q): expected sub-second rejection, got nil error", in)
			continue
		}
		if !strings.Contains(err.Error(), "sub-second") {
			t.Errorf("ParseDuration(%q): error %q does not mention sub-second precision", in, err.Error())
		}
	}
}

func TestParseDuration_RejectsNegative(t *testing.T) {
	for _, in := range []string{"-1s", "-30m", "-2h", "-1.5h"} {
		_, err := ParseDuration(in)
		if err == nil {
			t.Errorf("ParseDuration(%q): expected negative-rejection, got nil error", in)
			continue
		}
		if !strings.Contains(err.Error(), "non-negative") {
			t.Errorf("ParseDuration(%q): error %q does not mention non-negative", in, err.Error())
		}
	}
}

func TestParseDuration_PassesUpUnitErrors(t *testing.T) {
	// Errors from time.ParseDuration (bad units, whitespace) pass
	// through verbatim — callers shouldn't need to know whether the
	// failure is a sub-second rejection or a parse error.
	for _, in := range []string{"1d", "1h 30m", "garbage"} {
		_, err := ParseDuration(in)
		if err == nil {
			t.Errorf("ParseDuration(%q): expected an error", in)
		}
	}
}
