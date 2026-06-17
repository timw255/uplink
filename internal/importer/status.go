package importer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/timw255/uplink/internal/adaptive"
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

	const smoothing = 0.3
	var (
		prevLen          int
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
			line := progressLine(im.opts.DryRun, snap, total, emaRPS, emaMBps, inFlight, elapsed)
			pad := ""
			if prevLen > len(line) {
				pad = strings.Repeat(" ", prevLen-len(line))
			}
			fmt.Fprintf(im.opts.StatusWriter, "\r%s%s", line, pad)
			prevLen = len(line)
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

// progressLine is the single live status line. Real runs show the two
// gauges that matter — create throughput (rps) and the upload pipe (in-
// flight count + MB/s); dry runs show validity counts.
func progressLine(dryRun bool, s Summary, total int, rps, mbps float64, inFlight int, elapsed time.Duration) string {
	var b strings.Builder
	if dryRun {
		b.WriteString("validating  ")
		writeCount(&b, s.Total, total)
		fmt.Fprintf(&b, "   ok %d   invalid %d   %s", s.Valid, s.Invalid, elapsed.Round(time.Second))
		return b.String()
	}
	b.WriteString("importing  ")
	writeCount(&b, s.Total, total)
	fmt.Fprintf(&b, "   rps %.0f   up %d   %.0f MB/s   fail %d   %s",
		rps, inFlight, mbps, s.Failed, elapsed.Round(time.Second))
	return b.String()
}

func writeCount(b *strings.Builder, processed, total int) {
	if total > 0 {
		fmt.Fprintf(b, "%d/%d (%d%%)", processed, total, processed*100/total)
	} else {
		fmt.Fprintf(b, "%d", processed)
	}
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
