package connector

import (
	"fmt"
	"time"
)

// ParseDuration parses a config-style duration string with Go's
// time.ParseDuration syntax — sequences like "30s", "2m", "1h",
// "2h30m", "1.5h" — with two extra constraints: durations must be
// non-negative, and the smallest accepted unit is the second.
// Sub-second units (ns, us, ms) and negative values are both rejected.
//
// The intent is to keep poll intervals and timeouts honest. A
// millisecond poll loop in a config file is almost always a typo, and
// a negative poll interval has no operational meaning. Catching either
// at load time is cheaper than catching it from a CPU graph in
// production.
//
// Fractional values are accepted when they round to whole seconds
// (1.5h, 1.5m), and rejected when they don't (0.5s, 1.001s).
func ParseDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("duration %q must be non-negative", s)
	}
	if d != 0 && d%time.Second != 0 {
		return 0, fmt.Errorf("duration %q has sub-second precision; smallest unit is s", s)
	}
	return d, nil
}
