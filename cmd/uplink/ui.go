package main

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"time"

	"github.com/timw255/uplink/internal/importer"
	"github.com/timw255/uplink/internal/tui"
)

// importUI renders the human-facing import experience to stdout: a brand
// banner, the pre-flight steps, and the final tally. On a non-terminal
// (piped / CI) the decorative output is suppressed and only the plain
// summary prints — the structured log carries the rest.
type importUI struct {
	w     io.Writer
	s     *tui.Styler
	tty   bool
	open  bool // a step line is awaiting its ok/failed
	start time.Time
}

func newImportUI(w io.Writer) *importUI {
	s := tui.New(w)
	return &importUI{w: w, s: s, tty: s.Enabled(), start: time.Now()}
}

// banner prints the brand header. mode is "import" or "dry run".
func (u *importUI) banner(mode string) {
	if !u.tty {
		return
	}
	fmt.Fprintf(u.w, "\n  %s %s %s\n\n", u.s.Mark(), u.s.Bold("uplink"), u.s.Dim(mode))
}

// step prints a pending pre-flight line ("  › <msg>… ") with no newline; the
// next ok/fail completes it.
func (u *importUI) step(format string, args ...any) {
	if !u.tty {
		return
	}
	fmt.Fprintf(u.w, "  %s %s… ", u.s.Dim("›"), fmt.Sprintf(format, args...))
	u.open = true
}

func (u *importUI) ok() {
	if !u.tty {
		return
	}
	fmt.Fprintln(u.w, u.s.Dim("ok"))
	u.open = false
}

func (u *importUI) failed() {
	if !u.tty {
		return
	}
	if u.open {
		fmt.Fprintln(u.w, u.s.Bold("failed"))
		u.open = false
	}
}

// note prints a settled fact line ("  › Manifest records.jsonl · 12,480 records").
func (u *importUI) note(label, value, detail string) {
	if !u.tty {
		return
	}
	line := fmt.Sprintf("  %s %s %s", u.s.Dim("›"), label, u.s.Bold(value))
	if detail != "" {
		line += " " + u.s.Dim("· "+detail)
	}
	fmt.Fprintln(u.w, line)
}

// summary prints the final tally. Always prints (colored on a terminal, plain
// otherwise) — it's the result the operator and any CI step care about.
func (u *importUI) summary(s importer.Summary, dryRun bool) {
	elapsed := s.Elapsed.Round(time.Millisecond)
	fmt.Fprintln(u.w)

	if dryRun {
		head := fmt.Sprintf("Dry run complete · %s", elapsed)
		fmt.Fprintf(u.w, "  %s %s\n\n", u.s.Mark(), u.s.Bold(head))
		u.row("checked", s.Total)
		u.row("valid", s.Valid)
		u.row("invalid", s.Invalid)
		if s.Rewritten > 0 {
			u.row("to rename", s.Rewritten)
		}
		if s.Skipped > 0 {
			u.row("skipped", s.Skipped)
		}
		u.problems(s.Problems, s.Invalid, "Fix these before importing")
		return
	}

	headline := "Import complete"
	if s.Aborted > 0 {
		headline = "Import stopped early"
	}
	fmt.Fprintf(u.w, "  %s %s %s\n\n", u.s.Mark(), u.s.Bold(headline), u.s.Dim("· "+elapsed.String()))

	u.row("created", s.Created)
	u.row("updated", s.Updated)
	u.row("metadata", s.Metadata)
	if s.Skipped > 0 {
		u.row("skipped", s.Skipped)
	}
	if s.Filed > 0 {
		u.row("filed", s.Filed)
	}
	if s.Unfiled > 0 {
		fmt.Fprintf(u.w, "      %-9s %s\n", "unfiled",
			u.s.Dim(tui.Commas(s.Unfiled)+" couldn't be filed into the collection — rerun to finish"))
	}
	u.row("failed", s.Failed)
	if s.Aborted > 0 {
		fmt.Fprintf(u.w, "      %-9s %s\n", "pending",
			u.s.Dim(tui.Commas(s.Aborted)+" not processed — rerun to resume"))
	}
	u.problems(s.Problems, s.Failed, "What failed")
}

// problems lists the first few failed/invalid records (line + reason), with a
// trailing "… and N more" when the true total exceeds what was captured.
func (u *importUI) problems(list []importer.Problem, total int, heading string) {
	if len(list) == 0 {
		return
	}
	const showMax = 15
	shown := append([]importer.Problem(nil), list...)
	sort.Slice(shown, func(i, j int) bool { return shown[i].Line < shown[j].Line })
	if len(shown) > showMax {
		shown = shown[:showMax]
	}
	fmt.Fprintf(u.w, "\n  %s\n", u.s.Bold(heading))
	for _, p := range shown {
		fmt.Fprintf(u.w, "      %s  %s\n", u.s.Dim(fmt.Sprintf("line %d", p.Line)), cleanReason(p.Reason))
	}
	if more := total - len(shown); more > 0 {
		fmt.Fprintf(u.w, "      %s\n", u.s.Dim("… and "+tui.Commas(more)+" more"))
	}
}

// entryField matches the resolver's `entry[N] ("Field"): reason` fragment
// anywhere in a message — the real error arrives wrapped (e.g.
// `aprimo[name]: resolve companion-script fields: entry[0] ("Owner"): …`), so
// this drops the wrapper and leads with the field name.
var entryField = regexp.MustCompile(`entry\[\d+\] \("(.*?)"\): (.*)$`)

// cleanReason turns a raw resolver/validation error into a readable line:
// `…: entry[0] ("Owner"): unknown user "x"` becomes `Owner — unknown user "x"`.
// Messages without that fragment (file-not-found, missing id/file) pass through.
func cleanReason(reason string) string {
	if m := entryField.FindStringSubmatch(reason); m != nil {
		return m[1] + " — " + m[2]
	}
	return reason
}

// row prints one aligned "label   count" line. Zero counts are dimmed so the
// eye lands on what actually happened; non-zero counts are plain text.
func (u *importUI) row(label string, n int) {
	count := tui.Commas(n)
	if n == 0 {
		count = u.s.Dim(count)
	}
	fmt.Fprintf(u.w, "      %-9s %s\n", label, count)
}
