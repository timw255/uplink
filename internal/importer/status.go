package importer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/timw255/uplink/internal/adaptive"
	"github.com/timw255/uplink/internal/tui"
)

// runReporter renders progress until reportStop is closed. On a TTY it
// rewrites a single line in place via carriage return; otherwise it emits
// a periodic structured log line. Per-record detail goes to the ledger and
// debug log, never here.
//
// Two smoothed gauges: req/s (from the rate observer — mints + creates)
// and upload MB/s (from the pipe). Both are EMAs so the line tracks real
// changes without twitching, and no ETA is shown — import work is too
// heterogeneous to estimate honestly.
func (im *Importer) runReporter(
	reportStop, reportDone chan struct{},
	sum *Summary, mu *sync.Mutex,
	m *adaptive.Metrics, stats *liveStats,
	total int, start time.Time,
) {
	defer close(reportDone)

	tty := im.opts.StatusWriter != nil
	interval := im.opts.StatusInterval
	if interval <= 0 {
		if tty {
			interval = time.Second
		} else {
			interval = 5 * time.Second
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	style := tui.New(im.opts.StatusWriter) // enabled iff StatusWriter is a TTY

	const smoothing = 0.3
	var (
		lastReq, lastByt int64
		lastT            = start
		emaRPS, emaMBps  float64
		seeded           bool
	)
	render := func(final bool) {
		mu.Lock()
		snap := *sum
		mu.Unlock()

		now := time.Now()
		elapsed := now.Sub(start)

		// Fold a new window into the EMAs only on a full tick; the final
		// render is a short tail window that would read high.
		if !final {
			if dt := now.Sub(lastT).Seconds(); dt > 0 {
				reqs := m.Completed()
				instRPS := float64(reqs-lastReq) / dt
				instMBps := 0.0
				if stats != nil {
					b := stats.bytesUploaded.Load()
					instMBps = float64(b-lastByt) / dt / (1 << 20)
					lastByt = b
				}
				if seeded {
					emaRPS = smoothing*instRPS + (1-smoothing)*emaRPS
					emaMBps = smoothing*instMBps + (1-smoothing)*emaMBps
				} else {
					emaRPS, emaMBps, seeded = instRPS, instMBps, true
				}
				lastReq, lastT = reqs, now
			}
		}
		inFlight := 0
		if stats != nil {
			inFlight = int(stats.uploadsInFlight.Load())
		}

		if tty {
			line := progressLine(style, im.opts.DryRun, snap, total, emaRPS, emaMBps, inFlight, elapsed)
			// \r returns to column 0; \033[K erases to end of line — robust to
			// the ANSI color codes in the line (a byte-length pad is not).
			fmt.Fprintf(im.opts.StatusWriter, "\r\033[K%s", line)
			if final {
				fmt.Fprintln(im.opts.StatusWriter)
			}
			return
		}
		im.logger.Info("import progress", progressArgs(im.opts.DryRun, snap, total, emaRPS, emaMBps, inFlight, elapsed)...)
	}

	for {
		select {
		case <-reportStop:
			render(true)
			return
		case <-ticker.C:
			render(false)
		}
	}
}

// progressLine is the single live status line. Real runs show a progress bar
// plus the two gauges that matter — create throughput (req/s) and the upload
// pipe (in-flight count + MB/s); dry runs show validity counts. The styler is
// always color-enabled here (the line is only rendered to a TTY).
func progressLine(st *tui.Styler, dryRun bool, s Summary, total int, rps, mbps float64, inFlight int, elapsed time.Duration) string {
	sep := "  " + st.Dim("·") + "  "
	var b strings.Builder

	if dryRun {
		b.WriteString(st.Brand("validating") + "  ")
		b.WriteString(progressBar(st, s.Total, total))
		b.WriteString(sep + "valid " + tui.Commas(s.Valid))
		b.WriteString(sep + "invalid " + tui.Commas(s.Invalid))
		b.WriteString(sep + st.Dim(elapsed.Round(time.Second).String()))
		return b.String()
	}

	b.WriteString(st.Brand("importing") + "  ")
	b.WriteString(progressBar(st, s.Total, total))
	b.WriteString(sep + tui.Commas(int(rps+0.5)) + st.Dim("/s"))
	b.WriteString(sep + st.Dim("↑") + fmt.Sprintf("%d ", inFlight) + st.Dim(fmt.Sprintf("%.0f MB/s", mbps)))
	if s.Failed > 0 {
		b.WriteString(sep + tui.Commas(s.Failed) + " failed")
	}
	b.WriteString(sep + st.Dim(elapsed.Round(time.Second).String()))
	return b.String()
}

// progressBar renders "███████░░░░  68%  7,240/12,480", or just a running
// count when the total isn't known.
func progressBar(st *tui.Styler, done, total int) string {
	if total <= 0 {
		return st.Bold(tui.Commas(done))
	}
	const width = 16
	filled := min(done*width/total, width)
	bar := st.Brand(strings.Repeat("█", filled)) + st.Dim(strings.Repeat("░", width-filled))
	return fmt.Sprintf("%s  %s  %s", bar,
		st.Bold(fmt.Sprintf("%3d%%", done*100/total)),
		st.Dim(tui.Commas(done)+"/"+tui.Commas(total)))
}

// progressArgs is the structured-log equivalent of progressLine.
func progressArgs(dryRun bool, s Summary, total int, rps, mbps float64, inFlight int, elapsed time.Duration) []any {
	args := []any{"processed", s.Total}
	if total > 0 {
		args = append(args, "total", total)
	}
	if dryRun {
		return append(args, "valid", s.Valid, "invalid", s.Invalid, "elapsed", elapsed.Round(time.Second).String())
	}
	return append(args,
		"rps", int(rps+0.5),
		"uploading", inFlight,
		"mbps", int(mbps+0.5),
		"fail", s.Failed,
		"elapsed", elapsed.Round(time.Second).String(),
	)
}
