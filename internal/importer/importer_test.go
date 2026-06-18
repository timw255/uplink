package importer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/connector"
)

// --- fakes ---------------------------------------------------------------

type uploadCall struct {
	path string
	meta map[string]any
}

type createCall struct {
	path  string
	token string
	meta  map[string]any
}

type metaCall struct {
	id   string
	meta map[string]any
}

// fakeDest implements the two-stage Destination. It does NOT implement
// rateControlled, so runs use fixed concurrency.
type fakeDest struct {
	mu          sync.Mutex
	uploads     []uploadCall
	creates     []createCall
	metas       []metaCall
	validateErr func(meta map[string]any) error
	uploadErr   error
	createErr   error
	// sweptToken: CreateFromToken returns ErrUploadTokenMissing for this
	// token, simulating Aprimo's cleanup of a saved upload.
	sweptToken string
	// panicPath: UploadOnly panics for this source path, simulating a
	// record-triggered bug deep in the SDK/connector.
	panicPath string
	// failCreatePath: CreateFromToken returns a hard error for this path
	// (used to trip --stop-on-error in tests).
	failCreatePath string
	// blockCreateUntilCancel: CreateFromToken for any other path blocks
	// until ctx is canceled, then returns ctx.Err() — simulating the
	// in-flight records that get aborted when the run stops.
	blockCreateUntilCancel bool
}

func (d *fakeDest) UploadOnly(_ context.Context, srcPath string, _ connector.SegmentSource, meta map[string]any) (string, error) {
	if d.panicPath != "" && srcPath == d.panicPath {
		panic("simulated SDK panic on " + srcPath)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.uploadErr != nil {
		return "", d.uploadErr
	}
	d.uploads = append(d.uploads, uploadCall{path: srcPath, meta: meta})
	return "tok-" + srcPath, nil
}

func (d *fakeDest) CreateFromToken(ctx context.Context, srcPath, token string, meta map[string]any) (connector.Entry, error) {
	// Checked without the lock so a blocking create can't stall the others.
	if d.failCreatePath != "" && srcPath == d.failCreatePath {
		return connector.Entry{}, errors.New("synthetic create failure")
	}
	if d.blockCreateUntilCancel {
		<-ctx.Done()
		return connector.Entry{}, ctx.Err()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.createErr != nil {
		return connector.Entry{}, d.createErr
	}
	if d.sweptToken != "" && token == d.sweptToken {
		return connector.Entry{}, aprimo.ErrUploadTokenMissing
	}
	id, _ := meta["dest_id"].(string)
	if id == "" {
		id = "new-" + srcPath
	}
	d.creates = append(d.creates, createCall{path: srcPath, token: token, meta: meta})
	return connector.Entry{Path: id}, nil
}

func (d *fakeDest) WriteMetadata(_ context.Context, recordID string, meta map[string]any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metas = append(d.metas, metaCall{id: recordID, meta: meta})
	return nil
}

func (d *fakeDest) ValidateFields(meta map[string]any) error {
	if d.validateErr != nil {
		return d.validateErr(meta)
	}
	return nil
}

func (d *fakeDest) counts() (uploads, creates, metas int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.uploads), len(d.creates), len(d.metas)
}

// fakeSource implements connector.Connector via interface embedding;
// only Stat is exercised. Non-overridden methods would panic if called,
// which no test path does.
type fakeSource struct {
	connector.Connector
	files map[string]int64
}

func (s *fakeSource) Stat(_ context.Context, path string) (connector.Entry, error) {
	if sz, ok := s.files[path]; ok {
		return connector.Entry{Path: path, Size: sz}, nil
	}
	return connector.Entry{}, fmt.Errorf("not found")
}

// --- helpers -------------------------------------------------------------

func writeManifest(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "records.jsonl")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func runImporter(t *testing.T, opts Options) Summary {
	t.Helper()
	opts.Logger = quietLogger()
	// Force the deterministic fixed pool unless a test opts in otherwise.
	if opts.MaxWorkers == 0 {
		opts.MaxWorkers = 4
	}
	im, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sum, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return sum
}

// --- record-level tests --------------------------------------------------

func TestParseLineRejectsUnknownKeys(t *testing.T) {
	_, err := parseLine([]byte(`{"id":"x","title":"oops"}`), false)
	if err == nil {
		t.Fatal("expected unknown-key error in strict mode")
	}
	if !strings.Contains(err.Error(), "fields") {
		t.Fatalf("error should hint at fields[]: %v", err)
	}
	if _, err := parseLine([]byte(`{"id":"x","title":"oops"}`), true); err != nil {
		t.Fatalf("lenient mode should ignore unknown keys: %v", err)
	}
}

func TestRecordActionAndValidate(t *testing.T) {
	cases := []struct {
		rec    Record
		action Action
		valid  bool
	}{
		{Record{ID: "a"}, ActionMetadata, true},
		{Record{File: "f.jpg"}, ActionCreated, true},
		{Record{ID: "a", File: "f.jpg"}, ActionUpdated, true},
		{Record{}, "", false},
		{Record{ID: "a", Status: "bogus"}, ActionMetadata, false},
		{Record{ID: "a", Status: "Released"}, ActionMetadata, true},
	}
	for i, c := range cases {
		err := c.rec.validate()
		if c.valid != (err == nil) {
			t.Errorf("case %d: validate valid=%v err=%v", i, c.valid, err)
		}
		if c.valid && c.rec.action() != c.action {
			t.Errorf("case %d: action=%v want %v", i, c.rec.action(), c.action)
		}
	}
}

func TestRecordMetaShape(t *testing.T) {
	rec := Record{
		ID:     "rec1",
		Status: "Draft",
		Fields: []FieldEntry{
			{Name: "Title", Value: "Sunset"},
			{Name: "Caption", Value: "Hi", Language: "en-US"},
		},
	}
	m := rec.meta()
	if m["dest_id"] != "rec1" {
		t.Errorf("dest_id = %v", m["dest_id"])
	}
	if m["dest_status"] != "draft" {
		t.Errorf("dest_status = %v (want lowercased)", m["dest_status"])
	}
	entries, ok := m["dest_fields"].([]any)
	if !ok || len(entries) != 2 {
		t.Fatalf("dest_fields = %v", m["dest_fields"])
	}
	first := entries[0].(map[string]any)
	if first["name"] != "Title" || first["value"] != "Sunset" {
		t.Errorf("first field = %v", first)
	}
	second := entries[1].(map[string]any)
	if second["language"] != "en-US" {
		t.Errorf("second field language = %v", second["language"])
	}
}

// --- run-level tests -----------------------------------------------------

func TestRunRealMixedActions(t *testing.T) {
	manifest := writeManifest(t,
		`{"file":"photos/a.jpg","fields":[{"name":"Title","value":"A"}]}`, // create
		`{"id":"rec2","file":"photos/b.jpg"}`,                             // update + file
		`{"id":"rec3","fields":[{"name":"Caption","value":"hi"}]}`,        // metadata only
	)
	dest := &fakeDest{}
	src := &fakeSource{files: map[string]int64{"photos/a.jpg": 10, "photos/b.jpg": 20}}

	sum := runImporter(t, Options{ManifestPath: manifest, Dest: dest, Source: src})

	if sum.Total != 3 || sum.Created != 1 || sum.Updated != 1 || sum.Metadata != 1 || sum.Failed != 0 {
		t.Fatalf("summary = %+v", sum)
	}
	uploads, creates, metas := dest.counts()
	if uploads != 2 || creates != 2 {
		t.Fatalf("expected 2 uploads + 2 creates, got uploads=%d creates=%d", uploads, creates)
	}
	if metas != 1 || dest.metas[0].id != "rec3" {
		t.Fatalf("expected 1 metadata patch on rec3, got %+v", dest.metas)
	}
}

func TestRunFileWithoutSourceFails(t *testing.T) {
	manifest := writeManifest(t, `{"file":"photos/a.jpg"}`)
	dest := &fakeDest{}
	sum := runImporter(t, Options{ManifestPath: manifest, Dest: dest, Source: nil})
	if sum.Failed != 1 {
		t.Fatalf("expected 1 failure when source missing, got %+v", sum)
	}
}

func TestDryRunValidatesWithoutWriting(t *testing.T) {
	manifest := writeManifest(t,
		`{"file":"photos/a.jpg","fields":[{"name":"Title","value":"A"}]}`, // valid
		`{"file":"photos/missing.jpg"}`,                                   // invalid: file not found
		`{"fields":[{"name":"X","value":"y"}]}`,                           // invalid: no id/file
		`{"id":"rec","fields":[{"name":"BadField","value":"z"}]}`,         // invalid: bad field
	)
	dest := &fakeDest{
		validateErr: func(meta map[string]any) error {
			entries, _ := meta["dest_fields"].([]any)
			for _, e := range entries {
				if e.(map[string]any)["name"] == "BadField" {
					return fmt.Errorf("no field named BadField")
				}
			}
			return nil
		},
	}
	src := &fakeSource{files: map[string]int64{"photos/a.jpg": 10}}

	sum := runImporter(t, Options{ManifestPath: manifest, Dest: dest, Source: src, DryRun: true})

	if sum.Valid != 1 || sum.Invalid != 3 {
		t.Fatalf("dry-run summary = %+v (want 1 valid, 3 invalid)", sum)
	}
	if uploads, creates, metas := dest.counts(); uploads != 0 || creates != 0 || metas != 0 {
		t.Fatalf("dry-run must not write: uploads=%d creates=%d metas=%d", uploads, creates, metas)
	}
}

func TestDryRunFlagsRewrittenFilenames(t *testing.T) {
	manifest := writeManifest(t,
		`{"file":"photos/good.jpg"}`,      // clean name, no warning
		`{"file":"photos/bad:name?.jpg"}`, // forbidden chars -> rewrite warning
	)
	dest := &fakeDest{}
	src := &fakeSource{files: map[string]int64{
		"photos/good.jpg":      10,
		"photos/bad:name?.jpg": 20,
	}}

	sum := runImporter(t, Options{ManifestPath: manifest, Dest: dest, Source: src, DryRun: true})

	if sum.Valid != 2 {
		t.Fatalf("both lines should be valid, got %+v", sum)
	}
	if sum.Rewritten != 1 {
		t.Fatalf("expected exactly 1 rewritten filename, got %d", sum.Rewritten)
	}
}

func TestRunWritesLedgerAndResumes(t *testing.T) {
	manifest := writeManifest(t,
		`{"id":"rec1","fields":[{"name":"Title","value":"A"}]}`,
		`{"id":"rec2","fields":[{"name":"Title","value":"B"}]}`,
	)
	ledgerPath := filepath.Join(t.TempDir(), "results.jsonl")
	dest := &fakeDest{}

	sum := runImporter(t, Options{ManifestPath: manifest, Dest: dest, ResultsPath: ledgerPath})
	if sum.Metadata != 2 {
		t.Fatalf("first run metadata = %d", sum.Metadata)
	}

	// Ledger should hold two success rows.
	done, _, err := loadLedgerState(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 2 {
		t.Fatalf("ledger done set = %d, want 2", len(done))
	}

	// Second run resuming: both lines already done => skipped, no
	// new destination calls.
	dest2 := &fakeDest{}
	sum2 := runImporter(t, Options{ManifestPath: manifest, Dest: dest2, ResultsPath: ledgerPath, Resume: true})
	if sum2.Skipped != 2 || sum2.Metadata != 0 {
		t.Fatalf("resume summary = %+v (want 2 skipped, 0 metadata)", sum2)
	}
	if len(dest2.metas) != 0 {
		t.Fatal("resume must not re-issue completed work")
	}
}

func TestLedgerContent(t *testing.T) {
	manifest := writeManifest(t, `{"id":"rec1","fields":[{"name":"Title","value":"A"}]}`)
	ledgerPath := filepath.Join(t.TempDir(), "results.jsonl")
	runImporter(t, Options{ManifestPath: manifest, Dest: &fakeDest{}, ResultsPath: ledgerPath})

	data, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	var r Result
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &r); err != nil {
		t.Fatalf("ledger line not valid JSON: %v", err)
	}
	if r.Action != string(ActionMetadata) || r.DestID != "rec1" || r.Line != 1 || r.Hash == "" {
		t.Fatalf("ledger row = %+v", r)
	}
}

func TestStopOnError(t *testing.T) {
	// First line fails validation (no id/file); with StopOnError the run
	// cancels. Use a single worker so ordering is deterministic.
	manifest := writeManifest(t,
		`{"fields":[{"name":"X","value":"y"}]}`,
		`{"id":"rec2"}`,
		`{"id":"rec3"}`,
	)
	dest := &fakeDest{}
	im, err := New(Options{
		ManifestPath: manifest, Dest: dest, MaxWorkers: 1,
		StopOnError: true, Logger: quietLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	sum, err := im.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sum.Failed != 1 {
		t.Fatalf("expected exactly 1 failure recorded before stop, got %+v", sum)
	}
}

func TestBlankLinesSkipped(t *testing.T) {
	manifest := writeManifest(t,
		`{"id":"rec1"}`,
		``,
		`   `,
		`{"id":"rec2"}`,
	)
	sum := runImporter(t, Options{ManifestPath: manifest, Dest: &fakeDest{}})
	if sum.Total != 2 || sum.Metadata != 2 {
		t.Fatalf("summary = %+v (blank lines should be ignored)", sum)
	}
}
