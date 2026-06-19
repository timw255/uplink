package importer

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

// ledger appends per-line outcomes as JSONL. It is written only by the
// single drainer goroutine, but the mutex keeps close/write honest and
// the type self-contained. A nil ResultsPath yields a no-op ledger.
type ledger struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// newLedger opens (or, when append is true, opens-for-append) the ledger
// file. An empty path returns a no-op ledger so callers don't special-case
// the no-ledger case (e.g. a dry run).
func newLedger(path string, appendMode bool) (*ledger, error) {
	if path == "" {
		return &ledger{}, nil
	}
	flags := os.O_CREATE | os.O_WRONLY
	if appendMode {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	// 0600: the ledger holds upload tokens and record metadata — owner-only.
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	return &ledger{f: f, w: bufio.NewWriter(f)}, nil
}

func (l *ledger) write(r Result) error {
	if l == nil || l.f == nil {
		return nil
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.w.Write(b); err != nil {
		return err
	}
	// Flush each line so an interrupted run leaves a complete,
	// resumable ledger rather than a buffer-truncated one.
	if err := l.w.WriteByte('\n'); err != nil {
		return err
	}
	return l.w.Flush()
}

func (l *ledger) close() error {
	if l == nil || l.f == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ferr := l.w.Flush()
	cerr := l.f.Close()
	if ferr != nil {
		return ferr
	}
	return cerr
}

// resumeState is what a prior ledger tells a resuming run.
type resumeState struct {
	// done: hashes that completed (created/updated/metadata) — skipped
	// entirely on resume.
	done map[string]bool
	// uploaded: hash → token for records whose bytes were uploaded but
	// whose record was never created (a crash between the two stages).
	// Resume skips the upload and creates straight from the saved token;
	// if Aprimo has since swept the blob, the create fails with
	// ErrUploadTokenMissing and the pipeline just re-uploads.
	uploaded map[string]string
	// unfiled: record ids that were created but have no "filed" marker —
	// a crash (or interrupt) before the collection-filing phase finished.
	// Resume re-files them (idempotent). Collection membership can't be
	// read back from a record, and the collection is too large to
	// enumerate, so this ledger is the only record of what's been filed.
	unfiled []string
}

// loadLedgerState reads an existing ledger into a resumeState. A "done"
// hash is removed from uploaded (the record finished). Failed/invalid/
// skipped lines are excluded so resume re-attempts them. A missing file is
// not an error — nothing is done yet.
func loadLedgerState(path string) (resumeState, error) {
	st := resumeState{done: map[string]bool{}, uploaded: map[string]string{}}
	f, oerr := os.Open(path)
	if oerr != nil {
		if errors.Is(oerr, os.ErrNotExist) {
			return st, nil
		}
		return resumeState{}, oerr
	}
	defer func() { _ = f.Close() }()

	var created []string       // record ids of created rows
	filed := map[string]bool{} // record ids confirmed filed
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(trimSpace(line)) == 0 {
			continue
		}
		var r Result
		if err := json.Unmarshal(line, &r); err != nil {
			continue // tolerate a partially-written trailing line
		}
		switch Action(r.Action) {
		case ActionFiled:
			for _, id := range r.Filed {
				filed[id] = true
			}
			continue // marker row: no hash, no record outcome
		case ActionCreated:
			if r.DestID != "" {
				created = append(created, r.DestID)
			}
		}
		if r.Hash == "" {
			continue
		}
		switch Action(r.Action) {
		case ActionCreated, ActionUpdated, ActionMetadata:
			st.done[r.Hash] = true
		case ActionUploaded:
			if r.Token != "" {
				st.uploaded[r.Hash] = r.Token
			}
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return resumeState{}, err
	}
	for h := range st.done {
		delete(st.uploaded, h)
	}
	for _, id := range created {
		if !filed[id] {
			st.unfiled = append(st.unfiled, id)
		}
	}
	return st, nil
}
