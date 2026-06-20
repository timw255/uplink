package importer

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/timw255/uplink/internal/tui"
)

// plainStyler is a color-disabled styler (io.Discard isn't a TTY), so the
// format tests assert on plain text without ANSI codes.
func plainStyler() *tui.Styler { return tui.New(io.Discard) }

func TestProgressLineFormat(t *testing.T) {
	s := Summary{Total: 36, Created: 30, Metadata: 2, Failed: 4}
	line := progressLine(plainStyler(), false, s, 100, 14, 312, 8, 45*time.Second)
	for _, want := range []string{"importing", "36/100", "36%", "14/s", "↑8", "312 MB/s", "4 failed", "45s"} {
		if !strings.Contains(line, want) {
			t.Errorf("line %q missing %q", line, want)
		}
	}
	if strings.Contains(strings.ToLower(line), "eta") {
		t.Errorf("line should carry no ETA: %q", line)
	}
}

func TestProgressLineDryRunVerbAndCounts(t *testing.T) {
	s := Summary{Total: 10, Valid: 7, Invalid: 3}
	line := progressLine(plainStyler(), true, s, 10, 0, 0, 0, time.Second)
	for _, want := range []string{"validating", "10/10", "100%", "valid 7", "invalid 3"} {
		if !strings.Contains(line, want) {
			t.Errorf("dry-run line %q missing %q", line, want)
		}
	}
}

func TestProgressLineUnknownTotal(t *testing.T) {
	// total == 0 (count unknown): no percent, just a running count.
	s := Summary{Total: 5}
	line := progressLine(plainStyler(), false, s, 0, 2, 0, 0, time.Second)
	if strings.Contains(line, "%") {
		t.Errorf("unknown-total line should omit percent: %q", line)
	}
	if !strings.Contains(line, "importing  5") {
		t.Errorf("unknown-total line wrong: %q", line)
	}
}

func TestReporterRendersInPlaceStatusLine(t *testing.T) {
	manifest := writeManifest(t,
		`{"id":"r1","fields":[{"name":"T","value":"a"}]}`,
		`{"id":"r2","fields":[{"name":"T","value":"b"}]}`,
	)
	var buf bytes.Buffer
	im, err := New(Options{
		ManifestPath:   manifest,
		Dest:           &fakeDest{},
		MaxWorkers:     2,
		StatusWriter:   &buf, // non-nil => TTY render path
		StatusInterval: time.Millisecond,
		Logger:         quietLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "\r") {
		t.Errorf("expected carriage-return in-place updates, got %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("status line should finish with a newline, got %q", out)
	}
	if !strings.Contains(out, "importing") {
		t.Errorf("status missing verb, got %q", out)
	}
}
