package importer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sync"

	"github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/connector"
)

// validator runs the offline pre-flight checks a write implies, in
// parallel, before any bytes move. It is the input stage shared by a real
// run's pre-scan and a dry run.
type validator struct {
	dest   Destination
	source connector.Connector
	logger *slog.Logger
}

// record runs every offline check: structural rules, metadata resolution
// against the live catalog, and (for file records) our own stat for
// existence + size. Returns the stat'd size.
//
// requireFile makes a missing/unstatable file fatal. It is false only for
// a record on resume whose bytes are ALREADY uploaded (a saved token): its
// record can be created without the source file at all, so a since-deleted
// source must not fail it — the stat is then best-effort, just to recover
// a size for the rare swept-token re-upload path.
func (v *validator) record(ctx context.Context, rec Record, requireFile bool) (size int64, err error) {
	if err := rec.validate(); err != nil {
		return 0, err
	}
	if err := v.dest.ValidateFields(rec.meta()); err != nil {
		return 0, err
	}
	if rec.File == "" {
		return 0, nil
	}
	if v.source == nil {
		if requireFile {
			return 0, errors.New(`record has a "file" but no --source connector was given`)
		}
		return 0, nil
	}
	entry, serr := v.source.Stat(ctx, rec.File)
	if serr != nil {
		if requireFile {
			return 0, fmt.Errorf("file %q: %w", rec.File, serr)
		}
		return 0, nil
	}
	return entry.Size, nil
}

// safe runs record with panic recovery, so a record-triggered panic deep
// in the resolver/connector becomes a failed record, not a dead run.
func (v *validator) safe(ctx context.Context, rec Record, line int, requireFile bool) (size int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = recoverPanic(v.logger, line, r)
		}
	}()
	return v.record(ctx, rec, requireFile)
}

// prescan validates and stats every record in parallel (our ground truth
// on size + existence), emitting an error Result for any that fail and
// returning those that pass, each annotated with its size — so a record
// with broken metadata or a missing file never reaches the upload stage.
func (v *validator) prescan(ctx context.Context, recs []workRecord, results chan<- Result, concurrency int, uploaded map[string]string) []workRecord {
	var (
		mu     sync.Mutex
		passed = make([]workRecord, 0, len(recs))
	)
	parallelScan(ctx, recs, concurrency, func(wr workRecord) {
		// A record with a saved token can be created without its source
		// file, so a since-deleted file must not fail it.
		_, preUploaded := uploaded[wr.hash]
		size, err := v.safe(ctx, wr.rec, wr.line, !preUploaded)
		if err != nil {
			emit(ctx, results, fail(wr.result(), err))
			return
		}
		wr.size = size
		mu.Lock()
		passed = append(passed, wr)
		mu.Unlock()
	})
	return passed
}

// dryRun validates every record in parallel and reports valid/invalid —
// the whole job of a dry run. It writes nothing.
func (v *validator) dryRun(ctx context.Context, recs []workRecord, results chan<- Result, concurrency int) {
	parallelScan(ctx, recs, concurrency, func(wr workRecord) {
		res := wr.result()
		if _, err := v.safe(ctx, wr.rec, wr.line, true); err != nil {
			emit(ctx, results, invalid(res, err))
			return
		}
		// Valid — but flag a filename Aprimo will rewrite so collisions are
		// visible before the real run.
		if wr.rec.File != "" {
			if base := path.Base(wr.rec.File); base != "" {
				if clean := aprimo.SanitizeFilename(base); clean != base {
					res.Warn = fmt.Sprintf("filename rewritten for Aprimo: %q -> %q", base, clean)
				}
			}
		}
		res.Action, res.DestID = "valid", wr.rec.ID
		emit(ctx, results, res)
	})
}

// parallelScan runs fn over every record with bounded concurrency,
// stopping early if ctx is cancelled.
func parallelScan(ctx context.Context, recs []workRecord, concurrency int, fn func(workRecord)) {
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, wr := range recs {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(wr workRecord) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(wr)
		}(wr)
	}
	wg.Wait()
}
