// Package tui holds small terminal-presentation helpers: ANSI color that
// degrades to plain text when the output isn't an interactive terminal, plus
// a couple of formatting utilities. It exists so the import command and the
// importer's live status line share one Windows-safe color implementation.
package tui

import (
	"fmt"
	"io"
	"os"

	"github.com/mattn/go-isatty"
)

// Styler wraps text in ANSI color codes, or returns it unchanged when the
// target isn't a color-capable terminal.
type Styler struct{ enabled bool }

// New returns a Styler that colors only when w is an interactive terminal.
// On Windows it first enables virtual-terminal processing so the codes
// render instead of printing as literal escapes.
func New(w io.Writer) *Styler {
	f, ok := w.(*os.File)
	if !ok {
		return &Styler{}
	}
	if !isatty.IsTerminal(f.Fd()) && !isatty.IsCygwinTerminal(f.Fd()) {
		return &Styler{}
	}
	enableVirtualTerminal(f) // no-op off Windows
	return &Styler{enabled: true}
}

// Enabled reports whether color output is on (i.e. the target is a terminal).
func (s *Styler) Enabled() bool { return s.enabled }

func (s *Styler) wrap(code, text string) string {
	if !s.enabled {
		return text
	}
	return "\x1b[" + code + "m" + text + "\x1b[0m"
}

// The uplink palette is deliberately minimal: one brand accent (the logo
// blue #2A53E6, 24-bit) plus bold and dim. No status colors — the words say
// what happened.
func (s *Styler) Brand(t string) string { return s.wrap("38;2;42;83;230", t) }
func (s *Styler) Dim(t string) string   { return s.wrap("2", t) }
func (s *Styler) Bold(t string) string  { return s.wrap("1", t) }

// Mark is the uplink brand mark, "[u]", in brand blue (bold).
func (s *Styler) Mark() string { return s.wrap("1;38;2;42;83;230", "[u]") }

// Commas formats n with thousands separators (e.g. 12480 -> "12,480"), so
// large record counts read at a glance.
func Commas(n int) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return "-" + Commas(-n)
	}
	out := make([]byte, 0, len(s)+len(s)/3)
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}
