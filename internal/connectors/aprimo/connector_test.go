package aprimo

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	aprimosdk "github.com/timw255/uplink/internal/aprimo"
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// fakeAprimo stands up an httptest.Server that mimics just enough of
// Aprimo's API surface for the connector tests:
//
//   POST /uploads/segments    setup
//   POST /upload/<id>?index=N segment
//   POST /upload/<id>/commit  final
//   DELETE /upload/<id>        cancel
//   POST /api/core/records    create
//   PUT  /api/core/record/<id> update
//   DELETE /api/core/record/<id> delete
//
// Configurable fault injection: a CreateStatus field controls the
// response code returned by /api/core/records (200 vs 500). A
// SegmentFailMod field, if > 0, returns 500 on every Nth segment.
type fakeAprimo struct {
	srv *httptest.Server

	mu              sync.Mutex
	setupHits       int
	commitHits      int
	deleteUploadHit int
	segments        map[int][]byte
	creates         int
	updates         []string // record ids that received PUT
	deletes         []string

	// Fault injection.
	createStatus       int
	updateStatus       int
	segmentFailIndices map[int]bool

	// When non-empty, replaces the default text body returned for a
	// non-2xx response. Use to inject Aprimo-shaped error envelopes
	// (e.g. NoDataFoundException) so the connector's error-detection
	// can be exercised end-to-end.
	createBody      []byte
	updateBody      []byte
	segmentFailBody []byte
	// segmentFailStatus is the status returned when a segment index is
	// flagged. Defaults to 500 if zero.
	segmentFailStatus int

	createdRecordID string
	uploadPath      string

	// Masterfile-lookup state. masterfileHits records record ids that
	// triggered a GET /masterfile call (the connector hits this during
	// Update to discover the existing master file id). masterfileID is
	// the id the fake returns; empty means "file-default".
	masterfileHits   []string
	masterfileID     string
	masterfileStatus int

	// Captured PUT bodies for Update assertions.
	updateBodies [][]byte

	// Collection-filing state. collectionFilings captures the (collection
	// id, record ids) seen by PUT /api/core/collection/<id>. If
	// collectionStatus is set to a non-2xx value, the fake responds with
	// that status — used to verify the connector tolerates collection
	// failures without losing the new record.
	collectionFilings []collectionCall
	collectionStatus  int
}

type collectionCall struct {
	collectionID string
	addOrUpdate  []string
}

func newFakeAprimo(t *testing.T) *fakeAprimo {
	t.Helper()
	f := &fakeAprimo{
		segments:           make(map[int][]byte),
		createStatus:       http.StatusOK,
		updateStatus:       http.StatusOK,
		collectionStatus:   http.StatusOK,
		createdRecordID:    "rec-new",
		segmentFailIndices: make(map[int]bool),
	}
	f.uploadPath = "/upload/abc"
	mux := http.NewServeMux()
	mux.HandleFunc("/login/connect/token", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	})
	mux.HandleFunc("/uploads/segments", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		f.setupHits++
		f.mu.Unlock()
		// Build absolute URI so the SDK's pathFromURI strips the host
		// and uses just /upload/abc against our baseURL.
		_, _ = w.Write([]byte(`{"uri":"` + f.srv.URL + f.uploadPath + `"}`))
	})
	mux.HandleFunc("/upload/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			f.mu.Lock()
			f.deleteUploadHit++
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		case strings.HasSuffix(r.URL.Path, "/commit"):
			f.mu.Lock()
			f.commitHits++
			f.mu.Unlock()
			_, _ = w.Write([]byte(`{"token":"tok-final"}`))
			return
		}
		index := r.URL.Query().Get("index")
		if index == "" {
			http.Error(w, "missing index", http.StatusBadRequest)
			return
		}
		idx := atoi(index)
		f.mu.Lock()
		if f.segmentFailIndices[idx] {
			status := f.segmentFailStatus
			if status == 0 {
				status = http.StatusInternalServerError
			}
			body := f.segmentFailBody
			f.mu.Unlock()
			if len(body) > 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write(body)
				return
			}
			http.Error(w, "synthetic segment failure", status)
			return
		}
		f.mu.Unlock()
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, "parse", http.StatusBadRequest)
			return
		}
		fhs := r.MultipartForm.File["segment"+index]
		if len(fhs) == 0 {
			http.Error(w, "no part", http.StatusBadRequest)
			return
		}
		fh, _ := fhs[0].Open()
		body, _ := io.ReadAll(fh)
		_ = fh.Close()
		f.mu.Lock()
		f.segments[idx] = body
		f.mu.Unlock()
	})
	mux.HandleFunc("/api/core/records", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.creates++
		status := f.createStatus
		body := f.createBody
		id := f.createdRecordID
		f.mu.Unlock()
		if status != http.StatusOK {
			if len(body) > 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write(body)
				return
			}
			http.Error(w, "synthetic create failure", status)
			return
		}
		_, _ = w.Write([]byte(`{"id":"` + id + `"}`))
	})
	mux.HandleFunc("/api/core/record/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/core/record/")

		// Sub-resource: /api/core/record/{id}/masterfile — used by the
		// connector's Update flow to discover the existing master file
		// id before sending a versioned addOrUpdate.
		if strings.HasSuffix(path, "/masterfile") {
			id := strings.TrimSuffix(path, "/masterfile")
			f.mu.Lock()
			f.masterfileHits = append(f.masterfileHits, id)
			masterID := f.masterfileID
			status := f.masterfileStatus
			f.mu.Unlock()
			if status != 0 && status != http.StatusOK {
				http.Error(w, "synthetic masterfile failure", status)
				return
			}
			if masterID == "" {
				masterID = "file-default" // arbitrary non-empty default
			}
			w.Header().Set("Content-Type", "application/hal+json")
			_, _ = w.Write([]byte(`{"id":"` + masterID + `","checkedOut":false}`))
			return
		}

		id := path
		f.mu.Lock()
		switch r.Method {
		case http.MethodPut:
			if f.updateStatus != http.StatusOK {
				st := f.updateStatus
				body := f.updateBody
				f.mu.Unlock()
				if len(body) > 0 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(st)
					_, _ = w.Write(body)
					return
				}
				http.Error(w, "synthetic update failure", st)
				return
			}
			// Capture the request body so tests can assert on the
			// versioned-update payload shape.
			f.mu.Unlock()
			bodyBytes, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			f.updateBodies = append(f.updateBodies, bodyBytes)
			f.updates = append(f.updates, id)
		case http.MethodDelete:
			f.deletes = append(f.deletes, id)
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/core/collection/", func(w http.ResponseWriter, r *http.Request) {
		collID := strings.TrimPrefix(r.URL.Path, "/api/core/collection/")
		if r.Method != http.MethodPut {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Records struct {
				AddOrUpdate []struct {
					ID string `json:"id"`
				} `json:"addOrUpdate"`
			} `json:"records"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		ids := make([]string, 0, len(body.Records.AddOrUpdate))
		for _, e := range body.Records.AddOrUpdate {
			ids = append(ids, e.ID)
		}
		f.mu.Lock()
		f.collectionFilings = append(f.collectionFilings, collectionCall{
			collectionID: collID,
			addOrUpdate:  ids,
		})
		status := f.collectionStatus
		f.mu.Unlock()
		if status != http.StatusOK {
			http.Error(w, "synthetic collection failure", status)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// newTestConnector wires a Connector with a real store + an aprimo
// client pointed at the fake server.
func newTestConnector(t *testing.T, fake *fakeAprimo) (*Connector, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	client, err := aprimosdk.New(aprimosdk.Config{
		Environment: "test",
		TokenProvider: func(context.Context) (string, error) {
			return "tok-test", nil
		},
		HTTPClient: fake.srv.Client(),
	})
	if err != nil {
		t.Fatalf("aprimo client: %v", err)
	}
	client.SetTestEndpoints(fake.srv.URL, fake.srv.URL)

	c := &Connector{
		name:  "aprimo-test",
		cfg:   &Config{DefaultStatus: "draft"},
		api:   client,
		store: st,
	}
	return c, st
}

// writeBody returns a small SegmentSource the connector will upload.
// `forceSegmented=true` makes the body exercise the segmented path
// regardless of size.
func writeBody(data []byte) connector.SegmentSource {
	return &connector.ReaderSource{Data: data}
}

func TestConnector_WriteCreatesNewRecord(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)

	meta := map[string]any{
		"_job_id":                  "job-1",
		"_channel":                 "ch1",
		"_source_connector":        "fs-in",
		"_source_version":          "v1",
		"aprimo_parallel_segments": 2,
		"aprimo_segment_size":      512,
	}
	body := writeBody(makeBytes(2048))
	entry, err := c.Write(context.Background(), "media/video.mp4", body, meta)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if entry.Path != "rec-new" {
		t.Fatalf("returned entry.Path = %q, want rec-new", entry.Path)
	}

	if fake.setupHits != 1 || fake.commitHits != 1 || fake.creates != 1 {
		t.Fatalf("aprimo calls: setup=%d commit=%d creates=%d", fake.setupHits, fake.commitHits, fake.creates)
	}
	if len(fake.updates) != 0 {
		t.Fatalf("expected no PUT calls, got %v", fake.updates)
	}

	// Marker should be in state=created with the new record id.
	m, _ := st.LoadMarker("job-1")
	if m == nil {
		t.Fatal("expected marker file to exist post-Write")
	}
	if m.State != store.MarkerCreated || m.DestID != "rec-new" {
		t.Fatalf("marker after Write = %+v", m)
	}
}

func TestConnector_WriteUpdatesExistingRecord(t *testing.T) {
	fake := newFakeAprimo(t)
	c, _ := newTestConnector(t, fake)

	meta := map[string]any{
		"_job_id":                  "job-2",
		"_source_version":          "v2",
		"dest_id":         "rec-existing",
		"aprimo_parallel_segments": 2,
		"aprimo_segment_size":      512,
	}
	body := writeBody(makeBytes(1024))
	entry, err := c.Write(context.Background(), "video.mp4", body, meta)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if entry.Path != "rec-existing" {
		t.Fatalf("returned entry.Path = %q, want rec-existing (update flow)", entry.Path)
	}

	if fake.creates != 0 {
		t.Fatalf("expected no Create calls on update flow, got %d", fake.creates)
	}
	if len(fake.updates) != 1 || fake.updates[0] != "rec-existing" {
		t.Fatalf("expected PUT for rec-existing, got %v", fake.updates)
	}
}

func TestConnector_ResumeFromUploadingState(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)

	// Seed a marker that says: we already set up an upload session
	// and committed segments 0 and 1. The resume should NOT hit
	// /uploads/segments again and should NOT re-post segments 0 or 1.
	if err := st.SaveMarker(&store.UploadMarker{
		JobID:           "job-3",
		State:           store.MarkerUploading,
		UploadPath:      "/upload/abc",
		SegmentsTotal:   4,
		SegmentsDone:    []int{0, 1},
		Channel:         "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "big.bin",
		Filename:        "big.bin",
	}); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	body := writeBody(makeBytes(2048))
	_, err := c.Write(context.Background(), "big.bin", body, map[string]any{
		"_job_id":                  "job-3",
		"aprimo_parallel_segments": 2,
		"aprimo_segment_size":      512,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if fake.setupHits != 0 {
		t.Fatalf("expected setup endpoint NOT called on resume, got %d", fake.setupHits)
	}
	// Only segments 2 and 3 should have been POSTed.
	if _, ok := fake.segments[0]; ok {
		t.Fatal("segment 0 was uploaded but should have been skipped")
	}
	if _, ok := fake.segments[1]; ok {
		t.Fatal("segment 1 was uploaded but should have been skipped")
	}
	if _, ok := fake.segments[2]; !ok {
		t.Fatal("segment 2 was not uploaded")
	}
	if _, ok := fake.segments[3]; !ok {
		t.Fatal("segment 3 was not uploaded")
	}
}

func TestConnector_ResumeFromCommittedSkipsUpload(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)

	if err := st.SaveMarker(&store.UploadMarker{
		JobID:           "job-4",
		State:           store.MarkerCommitted,
		UploadToken:     "tok-retained",
		SegmentsTotal:   4,
		SegmentsDone:    []int{0, 1, 2, 3},
		Channel:         "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "x.bin",
		Filename:        "x.bin",
	}); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	body := writeBody(makeBytes(2048))
	entry, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id": "job-4",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if entry.Path != "rec-new" {
		t.Fatalf("entry.Path = %q", entry.Path)
	}

	if fake.setupHits != 0 || fake.commitHits != 0 {
		t.Fatalf("expected uploader NOT called in committed-resume path, got setup=%d commit=%d", fake.setupHits, fake.commitHits)
	}
	if len(fake.segments) != 0 {
		t.Fatalf("expected no segment POSTs, got %d", len(fake.segments))
	}
	if fake.creates != 1 {
		t.Fatalf("expected exactly one Create call, got %d", fake.creates)
	}

	m, _ := st.LoadMarker("job-4")
	if m == nil || m.State != store.MarkerCreated {
		t.Fatalf("marker should be in state=created, got %+v", m)
	}
}

func TestConnector_ResumeFromCreatedShortCircuits(t *testing.T) {
	fake := newFakeAprimo(t)
	c, _ := newTestConnector(t, fake)

	// All work was already done in a previous attempt; the engine just
	// hasn't deleted the marker yet. Write should return the existing
	// record id without touching Aprimo at all.
	if err := connStore(t, c).SaveMarker(&store.UploadMarker{
		JobID:          "job-5",
		State:          store.MarkerCreated,
		DestID: "rec-existing-X",
		Filename:       "x.bin",
	}); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	body := writeBody(makeBytes(1024))
	entry, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id": "job-5",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if entry.Path != "rec-existing-X" {
		t.Fatalf("entry.Path = %q, want rec-existing-X", entry.Path)
	}
	if fake.setupHits != 0 || fake.commitHits != 0 || fake.creates != 0 || len(fake.updates) != 0 {
		t.Fatalf("expected zero Aprimo calls, got setup=%d commit=%d creates=%d updates=%v",
			fake.setupHits, fake.commitHits, fake.creates, fake.updates)
	}
}

func TestConnector_MarkerSurvivesCreateFailure(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)
	fake.createStatus = http.StatusInternalServerError

	body := writeBody(makeBytes(1024))
	_, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id":                  "job-6",
		"aprimo_parallel_segments": 2,
		"aprimo_segment_size":      512,
	})
	if err == nil {
		t.Fatal("expected Create failure")
	}

	m, _ := st.LoadMarker("job-6")
	if m == nil {
		t.Fatal("marker should still exist after Create failure")
	}
	if m.State != store.MarkerCommitted {
		t.Fatalf("marker should be stuck at state=committed after Create failure, got %s", m.State)
	}
	if m.UploadToken != "tok-final" {
		t.Fatalf("marker.UploadToken = %q, want tok-final (so next attempt can retry Create)", m.UploadToken)
	}
}

func TestConnector_MarkerReflectsPartialUploadOnFailure(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)
	// Force segment 2 to fail. The for-spawn goroutines coordinate via
	// a semaphore but Go's scheduler does NOT FIFO their channel sends,
	// so we can't assert WHICH non-2 segments completed before the
	// failure — only that segment 2 didn't, at least one other did,
	// and the marker is in resume-ready shape.
	fake.segmentFailIndices[2] = true

	body := writeBody(makeBytes(2048))
	_, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id":                  "job-7",
		"aprimo_parallel_segments": 1,
		"aprimo_segment_size":      512,
	})
	if err == nil {
		t.Fatal("expected upload failure on segment 2")
	}

	m, _ := st.LoadMarker("job-7")
	if m == nil {
		t.Fatal("marker should exist after segment failure")
	}
	if m.State != store.MarkerUploading {
		t.Fatalf("marker should remain state=uploading, got %s", m.State)
	}
	if containsInt(m.SegmentsDone, 2) {
		t.Fatalf("segment 2 failed and must not be in SegmentsDone, got %v", m.SegmentsDone)
	}
	if len(m.SegmentsDone) == 0 {
		t.Fatalf("expected at least one segment committed before the failure, got %v", m.SegmentsDone)
	}
	if m.UploadPath == "" {
		t.Fatal("marker should carry UploadPath so the next attempt can resume")
	}
}

// noDataFoundBody returns the JSON envelope Aprimo emits when an upload
// token references nothing on disk. Captured from a live trial environment
// via the diagnostic probe.
func noDataFoundBody(token string) []byte {
	return []byte(`{"exceptionType":"Adam.Rest.Common.NoDataFoundException","exceptionMessage":"Cannot find the uploaded file specified with the token '` + token + `'.","stackTrace":null,"innerException":null}`)
}

// TestConnector_TokenPurgedOnCommittedResume covers the most likely
// real-world failure mode: a job sat in state=committed for so long that
// Aprimo cleaned up the orphan upload. On resume, applyRecord fails
// with NoDataFoundException; the connector must drop the marker so the
// next attempt does a fresh upload, and surface a retryable error.
func TestConnector_TokenPurgedOnCommittedResume(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)

	const staleToken = "tok-stale"
	fake.createStatus = http.StatusNotFound
	fake.createBody = noDataFoundBody(staleToken)

	if err := st.SaveMarker(&store.UploadMarker{
		JobID:           "job-purged",
		State:           store.MarkerCommitted,
		UploadToken:     staleToken,
		SegmentsTotal:   4,
		SegmentsDone:    []int{0, 1, 2, 3},
		Channel:         "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "x.bin",
		Filename:        "x.bin",
	}); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	body := writeBody(makeBytes(2048))
	_, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id": "job-purged",
	})
	if err == nil {
		t.Fatal("expected error when Aprimo reports the upload is gone")
	}
	if !errors.Is(err, aprimosdk.ErrUploadTokenMissing) {
		t.Fatalf("expected ErrUploadTokenMissing in error chain, got: %v", err)
	}

	// Marker must be gone — next attempt does a fresh upload + create.
	m, _ := st.LoadMarker("job-purged")
	if m != nil {
		t.Fatalf("marker should be deleted after token-purge detection, got: %+v", m)
	}

	// The uploader was never invoked (we were resuming from committed).
	if fake.setupHits != 0 || fake.commitHits != 0 || len(fake.segments) != 0 {
		t.Fatalf("uploader should not be called on committed-resume token-purge, got setup=%d commit=%d segments=%d",
			fake.setupHits, fake.commitHits, len(fake.segments))
	}
	if fake.creates != 1 {
		t.Fatalf("expected exactly one Create attempt, got %d", fake.creates)
	}
}

// TestConnector_TokenPurgedOnPostUploadCreate covers the unlikely but
// possible case where a brand-new upload commits successfully, then
// applyRecord fails with NoDataFoundException. Same recovery applies.
func TestConnector_TokenPurgedOnPostUploadCreate(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)

	fake.createStatus = http.StatusNotFound
	fake.createBody = noDataFoundBody("tok-final") // the fake's committed token

	body := writeBody(makeBytes(2048))
	_, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id":                  "job-fresh-purge",
		"aprimo_parallel_segments": 2,
		"aprimo_segment_size":      512,
	})
	if err == nil {
		t.Fatal("expected error from token-missing")
	}
	if !errors.Is(err, aprimosdk.ErrUploadTokenMissing) {
		t.Fatalf("expected ErrUploadTokenMissing, got: %v", err)
	}

	m, _ := st.LoadMarker("job-fresh-purge")
	if m != nil {
		t.Fatalf("marker should be deleted, got: %+v", m)
	}
	if fake.setupHits == 0 || fake.commitHits == 0 {
		t.Fatalf("upload should have completed before the Create failure, got setup=%d commit=%d",
			fake.setupHits, fake.commitHits)
	}
}

// TestConnector_TokenPurgedOnSegmentResume covers a partially uploaded
// segmented session whose underlying storage Aprimo has already cleaned
// up. The exact error envelope for this case has not been verified
// against a live tenant — the test asserts the canonical
// NoDataFoundException shape that Records.Create returns and expects
// the connector to treat it the same way. If production logs show a
// different shape, extend the ErrUploadTokenMissing matcher.
func TestConnector_TokenPurgedOnSegmentResume(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)

	// Mark segments 2 and 3 as failing with the canonical
	// NoDataFoundException body. (Resume targets these indices because
	// SegmentsDone covers 0 and 1.)
	fake.segmentFailIndices[2] = true
	fake.segmentFailIndices[3] = true
	fake.segmentFailStatus = http.StatusNotFound
	fake.segmentFailBody = noDataFoundBody("tok-session")

	if err := st.SaveMarker(&store.UploadMarker{
		JobID:           "job-session-purged",
		State:           store.MarkerUploading,
		UploadPath:      "/upload/abc",
		SegmentsTotal:   4,
		SegmentsDone:    []int{0, 1},
		Channel:         "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "big.bin",
		Filename:        "big.bin",
	}); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	body := writeBody(makeBytes(2048))
	_, err := c.Write(context.Background(), "big.bin", body, map[string]any{
		"_job_id":                  "job-session-purged",
		"aprimo_parallel_segments": 1,
		"aprimo_segment_size":      512,
	})
	if err == nil {
		t.Fatal("expected error when segment POST hits an expired session")
	}
	if !errors.Is(err, aprimosdk.ErrUploadTokenMissing) {
		t.Fatalf("expected ErrUploadTokenMissing for segment-resume failure, got: %v", err)
	}

	m, _ := st.LoadMarker("job-session-purged")
	if m != nil {
		t.Fatalf("marker should be deleted after segment-resume token-purge, got: %+v", m)
	}
}

// failingDeleteMarkerStore wraps a real *store.Store but returns a
// fixed error from DeleteMarker. Embedding gives LoadMarker / SaveMarker
// for free. Used to verify the connector still surfaces the upstream
// error when marker cleanup fails — forward progress depends on the
// engine seeing the original retryable cause, not a marker-delete error
// that masks it.
type failingDeleteMarkerStore struct {
	*store.Store
	err error
}

func (f *failingDeleteMarkerStore) DeleteMarker(_ string) error {
	return f.err
}

// TestConnector_TokenPurgedWithFailingDeleteMarker exercises the loud
// branch in resetUploadForRetry: DeleteMarker errors out. The original
// ErrUploadTokenMissing must still bubble up so the engine retries; if
// it didn't, a stuck marker would be re-tried with a stuck token forever.
func TestConnector_TokenPurgedWithFailingDeleteMarker(t *testing.T) {
	fake := newFakeAprimo(t)
	c, st := newTestConnector(t, fake)

	const staleToken = "tok-stale-faildelete"
	fake.createStatus = http.StatusNotFound
	fake.createBody = noDataFoundBody(staleToken)

	if err := st.SaveMarker(&store.UploadMarker{
		JobID:           "job-faildelete",
		State:           store.MarkerCommitted,
		UploadToken:     staleToken,
		Channel:         "ch1",
		SourceConnector: "fs-in",
		SourcePath:      "x.bin",
		Filename:        "x.bin",
	}); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
	c.store = &failingDeleteMarkerStore{
		Store: st,
		err:   errors.New("synthetic delete failure"),
	}

	body := writeBody(makeBytes(2048))
	_, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id": "job-faildelete",
	})
	if err == nil {
		t.Fatal("expected error from token-missing path")
	}
	if !errors.Is(err, aprimosdk.ErrUploadTokenMissing) {
		t.Fatalf("DeleteMarker failure must not mask the upstream cause; got: %v", err)
	}

	// The marker is still on disk (DeleteMarker failed). That's fine —
	// the next worker claim will hit the same code path and retry the
	// cleanup. We're asserting forward progress: the engine sees the
	// original retryable error, not the marker-delete error.
	m, _ := st.LoadMarker("job-faildelete")
	if m == nil {
		t.Fatal("marker should still exist after a failed DeleteMarker; the test setup is wrong if it's gone")
	}
}

// TestConnector_DefaultCollectionUnsetSkipsFiling confirms that when
// no DefaultCollection is configured, we never call the collection
// endpoint — costs nothing and avoids accidental writes.
func TestConnector_DefaultCollectionUnsetSkipsFiling(t *testing.T) {
	fake := newFakeAprimo(t)
	c, _ := newTestConnector(t, fake)

	body := writeBody(makeBytes(1024))
	_, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id":                  "job-no-coll",
		"aprimo_parallel_segments": 1,
		"aprimo_segment_size":      512,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(fake.collectionFilings) != 0 {
		t.Fatalf("no DefaultCollection configured — expected 0 collection calls, got %d: %+v",
			len(fake.collectionFilings), fake.collectionFilings)
	}
}

// TestConnector_DefaultCollectionFilesNewRecord verifies that when
// DefaultCollection is set, the new record is filed into that
// collection in a single PUT after the record is created.
func TestConnector_DefaultCollectionFilesNewRecord(t *testing.T) {
	fake := newFakeAprimo(t)
	c, _ := newTestConnector(t, fake)
	c.cfg.DefaultCollection = "coll-target"

	body := writeBody(makeBytes(1024))
	entry, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id":                  "job-coll-success",
		"aprimo_parallel_segments": 1,
		"aprimo_segment_size":      512,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if entry.Path != "rec-new" {
		t.Fatalf("entry.Path = %q, want rec-new", entry.Path)
	}
	if len(fake.collectionFilings) != 1 {
		t.Fatalf("expected exactly 1 collection call, got %d: %+v",
			len(fake.collectionFilings), fake.collectionFilings)
	}
	call := fake.collectionFilings[0]
	if call.collectionID != "coll-target" {
		t.Errorf("collectionID = %q, want coll-target", call.collectionID)
	}
	if len(call.addOrUpdate) != 1 || call.addOrUpdate[0] != "rec-new" {
		t.Errorf("addOrUpdate = %v, want [rec-new]", call.addOrUpdate)
	}
}

// TestConnector_DefaultCollectionFilingFailureIsSwallowed verifies the
// pragmatic behavior in fileIntoDefaultCollection: if the collection
// call fails, the record id is still returned (the record exists in
// Aprimo at this point — propagating the error would create a duplicate
// record on the next retry attempt since Records.Create is not
// idempotent). The operator can re-file manually from the warning log.
func TestConnector_DefaultCollectionFilingFailureIsSwallowed(t *testing.T) {
	fake := newFakeAprimo(t)
	c, _ := newTestConnector(t, fake)
	c.cfg.DefaultCollection = "coll-target"
	// 404 is non-retryable and matches the realistic "collection id was
	// renamed / deleted" failure shape. Using 5xx would trigger the SDK's
	// PUT retry policy and the assertion would flap on retry count.
	fake.collectionStatus = http.StatusNotFound

	body := writeBody(makeBytes(1024))
	entry, err := c.Write(context.Background(), "x.bin", body, map[string]any{
		"_job_id":                  "job-coll-failure",
		"aprimo_parallel_segments": 1,
		"aprimo_segment_size":      512,
	})
	if err != nil {
		t.Fatalf("Write should NOT propagate a collection-filing failure; got: %v", err)
	}
	if entry.Path != "rec-new" {
		t.Fatalf("entry.Path = %q, want rec-new (record id must still come back)", entry.Path)
	}
	if len(fake.collectionFilings) != 1 {
		t.Fatalf("collection endpoint should have been called once, got %d", len(fake.collectionFilings))
	}
}

// TestConnector_UpdateLooksUpMasterFileAndAddsVersion covers the
// version-not-sibling fix: on Update, the connector should fetch the
// current master file id and send addOrUpdate WITH that id so Aprimo
// appends a version to the existing file rather than creating a
// second sibling file. The earlier (buggy) flow omitted the id and
// produced a record with two files instead of one file with two
// versions.
func TestConnector_UpdateLooksUpMasterFileAndAddsVersion(t *testing.T) {
	fake := newFakeAprimo(t)
	c, _ := newTestConnector(t, fake)
	fake.masterfileID = "file-existing-master"

	meta := map[string]any{
		"_job_id":                  "job-update",
		"dest_id":         "rec-existing",
		"aprimo_parallel_segments": 1,
		"aprimo_segment_size":      512,
	}
	body := writeBody(makeBytes(1024))
	_, err := c.Write(context.Background(), "report.pdf", body, meta)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(fake.masterfileHits) != 1 || fake.masterfileHits[0] != "rec-existing" {
		t.Fatalf("expected one masterfile lookup for rec-existing, got %v", fake.masterfileHits)
	}
	if len(fake.updateBodies) != 1 {
		t.Fatalf("expected one PUT, got %d", len(fake.updateBodies))
	}

	// Decode the PUT body and assert the addOrUpdate FileAction carries
	// the existing master's id. Without this, the bug recurs and Aprimo
	// silently adds a sibling file.
	var put struct {
		Files struct {
			Master      string `json:"master"`
			AddOrUpdate []struct {
				ID       string `json:"id"`
				Versions struct {
					AddOrUpdate []struct {
						ID       string `json:"id"`
						FileName string `json:"fileName"`
					} `json:"addOrUpdate"`
				} `json:"versions"`
			} `json:"addOrUpdate"`
		} `json:"files"`
	}
	if err := json.Unmarshal(fake.updateBodies[0], &put); err != nil {
		t.Fatalf("unmarshal PUT body: %v\n%s", err, fake.updateBodies[0])
	}
	if len(put.Files.AddOrUpdate) != 1 {
		t.Fatalf("expected one FileAction in addOrUpdate, got %d", len(put.Files.AddOrUpdate))
	}
	if put.Files.AddOrUpdate[0].ID != "file-existing-master" {
		t.Errorf("FileAction.id = %q, want %q (the version must attach to the existing file)",
			put.Files.AddOrUpdate[0].ID, "file-existing-master")
	}
	if put.Files.Master == "" {
		t.Error("master pointer is empty; the new upload token must become the new master")
	}
	if len(put.Files.AddOrUpdate[0].Versions.AddOrUpdate) != 1 {
		t.Errorf("expected one version in addOrUpdate")
	}
}

// TestConnector_UpdateFallsBackWhenNoMasterFile covers the recovery
// path: if a record exists but has no master file (Aprimo returns 404
// on /masterfile), the Update sends a Create-shape payload so the new
// upload becomes the record's first file.
func TestConnector_UpdateFallsBackWhenNoMasterFile(t *testing.T) {
	fake := newFakeAprimo(t)
	c, _ := newTestConnector(t, fake)
	fake.masterfileStatus = http.StatusNotFound

	meta := map[string]any{
		"_job_id":                  "job-update-no-master",
		"dest_id":         "rec-empty",
		"aprimo_parallel_segments": 1,
		"aprimo_segment_size":      512,
	}
	body := writeBody(makeBytes(1024))
	_, err := c.Write(context.Background(), "first.pdf", body, meta)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(fake.updateBodies) != 1 {
		t.Fatalf("expected one PUT, got %d", len(fake.updateBodies))
	}
	var put struct {
		Files struct {
			AddOrUpdate []struct {
				ID string `json:"id"`
			} `json:"addOrUpdate"`
		} `json:"files"`
	}
	if err := json.Unmarshal(fake.updateBodies[0], &put); err != nil {
		t.Fatalf("unmarshal PUT body: %v", err)
	}
	if len(put.Files.AddOrUpdate) != 1 {
		t.Fatalf("expected one FileAction in addOrUpdate, got %d", len(put.Files.AddOrUpdate))
	}
	if put.Files.AddOrUpdate[0].ID != "" {
		t.Errorf("FileAction.id = %q, want empty (no existing master to target)",
			put.Files.AddOrUpdate[0].ID)
	}
}

// --- helpers ---

// connStore returns the marker store the connector is using. Returns
// the narrow markerStore interface, not *store.Store, because tests may
// have wrapped the underlying store in a decorator. Callers that need
// SaveMarker / LoadMarker / DeleteMarker get them from the interface.
func connStore(t *testing.T, c *Connector) markerStore {
	t.Helper()
	if c.store == nil {
		t.Fatal("connector has no store wired")
	}
	return c.store
}

func makeBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + (i % 26))
	}
	return b
}

func containsInt(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func atoi(s string) int {
	if s == "" {
		return 0
	}
	n := 0
	neg := false
	for i, c := range s {
		if i == 0 && c == '-' {
			neg = true
			continue
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		return -n
	}
	return n
}
