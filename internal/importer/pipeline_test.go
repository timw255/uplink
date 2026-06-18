package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestPipelineClampsDegenerateConcurrency exercises the pipeline engine in
// isolation and proves zero-sized pools can't hang or silently drop work:
// uploadCap and createConc both 0 must still process the record.
func TestPipelineClampsDegenerateConcurrency(t *testing.T) {
	dest := &fakeDest{}
	src := &fakeSource{files: map[string]int64{"a.jpg": 10}}
	results := make(chan Result, 16)

	var got []Result
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := range results {
			got = append(got, r)
		}
	}()

	p := &pipeline{
		dest: dest, source: src, results: results,
		stats: &liveStats{}, logger: quietLogger(),
		uploadCap: 0, createConc: 0, // degenerate on purpose
	}
	recs := []workRecord{{line: 1, hash: "h", rec: Record{File: "a.jpg"}, size: 10}}
	p.run(context.Background(), recs, nil)
	close(results)
	<-done

	var uploaded, created int
	for _, r := range got {
		switch Action(r.Action) {
		case ActionUploaded:
			uploaded++
		case ActionCreated:
			created++
		}
	}
	if uploaded != 1 || created != 1 {
		t.Fatalf("want 1 uploaded + 1 created, got uploaded=%d created=%d (%+v)", uploaded, created, got)
	}
}

// TestAbortReportsInflightAsNotProcessed: when --stop-on-error trips on a
// real failure, the in-flight records that then die with context.Canceled
// must be reported as "aborted" (not processed), NOT counted or logged as
// failures. One bad record can't masquerade as thousands.
func TestAbortReportsInflightAsNotProcessed(t *testing.T) {
	const n = 6
	lines := make([]string, n)
	files := map[string]int64{}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("f-%02d.bin", i)
		lines[i] = fmt.Sprintf(`{"file":%q,"fields":[{"name":"T","value":"x"}]}`, name)
		files[name] = 10
	}
	manifest := writeManifest(t, lines...)
	dest := &fakeDest{failCreatePath: "f-03.bin", blockCreateUntilCancel: true}
	src := &fakeSource{files: files}

	sum := runImporter(t, Options{
		ManifestPath: manifest, Dest: dest, Source: src, StopOnError: true,
	})

	if sum.Failed != 1 {
		t.Fatalf("Failed = %d, want exactly 1 (the real error, not the cascade)", sum.Failed)
	}
	if sum.Aborted == 0 {
		t.Fatalf("Aborted = 0, want the in-flight records reported as not-processed")
	}
	if got := sum.Failed + sum.Aborted + sum.Created; got != n {
		t.Fatalf("accounting off: failed=%d aborted=%d created=%d sum=%d, want %d",
			sum.Failed, sum.Aborted, sum.Created, got, n)
	}
}

// seedLedger writes the given rows to a fresh ledger file for resume tests.
func seedLedger(t *testing.T, rows ...Result) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "results.jsonl")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestSchedulerBlend(t *testing.T) {
	// recs sorted ascending by size, as runPipeline arranges them.
	recs := []workRecord{{size: 1}, {size: 2}, {size: 3}, {size: 4}}
	s := newScheduler(recs)

	// Starving create queue (< half full) → hand out the smallest (fast
	// upload, refills the queue quickly).
	if r, ok := s.next(0, 10); !ok || r.size != 1 {
		t.Fatalf("starving queue: got size=%d ok=%v, want smallest (1)", r.size, ok)
	}
	// Full queue → spend spare upload capacity on the biggest.
	if r, ok := s.next(10, 10); !ok || r.size != 4 {
		t.Fatalf("full queue: got size=%d ok=%v, want biggest (4)", r.size, ok)
	}
	if r, _ := s.next(0, 10); r.size != 2 {
		t.Fatalf("got size=%d, want 2", r.size)
	}
	if r, _ := s.next(10, 10); r.size != 3 {
		t.Fatalf("got size=%d, want 3", r.size)
	}
	if _, ok := s.next(0, 10); ok {
		t.Fatal("scheduler should be exhausted")
	}
}

// TestPipelineSkipsUploadOnBadMetadata proves the pre-scan's payoff: a
// record whose metadata won't resolve is caught BEFORE the upload stage,
// so its bytes are never streamed.
func TestPipelineSkipsUploadOnBadMetadata(t *testing.T) {
	manifest := writeManifest(t,
		`{"file":"photos/a.jpg","fields":[{"name":"Good","value":"x"}]}`,
		`{"file":"photos/b.jpg","fields":[{"name":"Bad","value":"y"}]}`,
	)
	dest := &fakeDest{
		validateErr: func(meta map[string]any) error {
			entries, _ := meta["dest_fields"].([]any)
			for _, e := range entries {
				if e.(map[string]any)["name"] == "Bad" {
					return fmt.Errorf("no field named Bad")
				}
			}
			return nil
		},
	}
	src := &fakeSource{files: map[string]int64{"photos/a.jpg": 10, "photos/b.jpg": 20}}

	sum := runImporter(t, Options{ManifestPath: manifest, Dest: dest, Source: src})

	if sum.Created != 1 || sum.Failed != 1 {
		t.Fatalf("summary = %+v (want 1 created, 1 failed)", sum)
	}
	uploads, creates, _ := dest.counts()
	if uploads != 1 || creates != 1 {
		t.Fatalf("bad-metadata record must not upload: uploads=%d creates=%d", uploads, creates)
	}
}

// TestPipelineRecoversWorkerPanic: a record that triggers a panic deep in
// the upload path is marked failed, and the rest of the run completes —
// one bad record can't take down an unattended overnight import.
func TestPipelineRecoversWorkerPanic(t *testing.T) {
	manifest := writeManifest(t,
		`{"file":"photos/good.jpg","fields":[{"name":"T","value":"x"}]}`,
		`{"file":"photos/boom.jpg","fields":[{"name":"T","value":"y"}]}`,
	)
	dest := &fakeDest{panicPath: "photos/boom.jpg"}
	src := &fakeSource{files: map[string]int64{"photos/good.jpg": 10, "photos/boom.jpg": 20}}

	sum := runImporter(t, Options{ManifestPath: manifest, Dest: dest, Source: src})

	if sum.Created != 1 || sum.Failed != 1 {
		t.Fatalf("summary = %+v (want 1 created, 1 failed — a panic must not crash the run)", sum)
	}
}

// TestResumeUsesSavedTokenSkippingUpload: a record uploaded but not
// created in a prior run (an "uploaded" ledger row) is created straight
// from the saved token on resume — its bytes are not re-uploaded.
func TestResumeUsesSavedTokenSkippingUpload(t *testing.T) {
	line := `{"file":"photos/a.jpg","fields":[{"name":"T","value":"x"}]}`
	manifest := writeManifest(t, line)
	hash := hashLine([]byte(line))
	ledger := seedLedger(t, Result{
		Line: 1, Hash: hash, File: "photos/a.jpg",
		Action: string(ActionUploaded), Token: "saved-tok",
	})

	dest := &fakeDest{}
	src := &fakeSource{files: map[string]int64{"photos/a.jpg": 10}}
	sum := runImporter(t, Options{
		ManifestPath: manifest, Dest: dest, Source: src,
		ResultsPath: ledger, Resume: true,
	})

	if sum.Created != 1 {
		t.Fatalf("summary = %+v (want 1 created)", sum)
	}
	uploads, creates, _ := dest.counts()
	if uploads != 0 {
		t.Fatalf("saved token must skip the upload, got %d uploads", uploads)
	}
	if creates != 1 || dest.creates[0].token != "saved-tok" {
		t.Fatalf("create should use the saved token, got %+v", dest.creates)
	}
}

// TestResumeSavedTokenWorksWhenSourceFileGone: a record uploaded in a
// prior run can be created from its saved token even if the source file
// has since been deleted — the bytes are already in Aprimo, so the file
// isn't needed and a now-missing source must not fail the record.
func TestResumeSavedTokenWorksWhenSourceFileGone(t *testing.T) {
	line := `{"file":"photos/gone.jpg","fields":[{"name":"T","value":"x"}]}`
	manifest := writeManifest(t, line)
	hash := hashLine([]byte(line))
	ledger := seedLedger(t, Result{
		Line: 1, Hash: hash, File: "photos/gone.jpg",
		Action: string(ActionUploaded), Token: "saved-tok",
	})

	dest := &fakeDest{}
	src := &fakeSource{files: map[string]int64{}} // source file no longer present
	sum := runImporter(t, Options{
		ManifestPath: manifest, Dest: dest, Source: src,
		ResultsPath: ledger, Resume: true,
	})

	if sum.Created != 1 || sum.Failed != 0 {
		t.Fatalf("summary = %+v (saved token must create even with the source file gone)", sum)
	}
	if uploads, creates, _ := dest.counts(); uploads != 0 || creates != 1 {
		t.Fatalf("uploads=%d creates=%d (want 0 uploads, 1 create from token)", uploads, creates)
	}
}

// TestResumeReuploadsOnSweptToken: if the saved token's blob was swept by
// Aprimo's cleanup (CreateFromToken → ErrUploadTokenMissing), the pipeline
// transparently re-uploads and retries.
func TestResumeReuploadsOnSweptToken(t *testing.T) {
	line := `{"file":"photos/a.jpg","fields":[{"name":"T","value":"x"}]}`
	manifest := writeManifest(t, line)
	hash := hashLine([]byte(line))
	ledger := seedLedger(t, Result{
		Line: 1, Hash: hash, File: "photos/a.jpg",
		Action: string(ActionUploaded), Token: "stale-tok",
	})

	dest := &fakeDest{sweptToken: "stale-tok"}
	src := &fakeSource{files: map[string]int64{"photos/a.jpg": 10}}
	sum := runImporter(t, Options{
		ManifestPath: manifest, Dest: dest, Source: src,
		ResultsPath: ledger, Resume: true,
	})

	if sum.Created != 1 {
		t.Fatalf("summary = %+v (want 1 created after re-upload)", sum)
	}
	uploads, creates, _ := dest.counts()
	if uploads != 1 {
		t.Fatalf("swept token should trigger exactly one re-upload, got %d", uploads)
	}
	if creates != 1 {
		t.Fatalf("want 1 successful create, got %d", creates)
	}
}
