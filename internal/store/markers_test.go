package store

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMarkerRoundTrip(t *testing.T) {
	s := openTestStore(t)

	in := &UploadMarker{
		JobID:           "job-abc",
		State:           MarkerUploading,
		UploadPath:      "/upload/abc",
		SegmentsTotal:   4,
		SegmentsDone:    []int{0, 1},
		ExpiresAt:       time.Now().Add(1 * time.Hour).UTC(),
		Channel:         "fs-to-aprimo",
		SourceConnector: "fs-in",
		SourcePath:      "movie.mp4",
		SourceVersion:   "v1",
		Filename:        "movie.mp4",
	}
	if err := s.SaveMarker(in); err != nil {
		t.Fatalf("SaveMarker: %v", err)
	}

	out, err := s.LoadMarker(in.JobID)
	if err != nil {
		t.Fatalf("LoadMarker: %v", err)
	}
	if out == nil {
		t.Fatal("LoadMarker returned nil")
	}
	if out.State != MarkerUploading {
		t.Fatalf("State = %q", out.State)
	}
	if out.UploadPath != in.UploadPath {
		t.Fatalf("UploadPath = %q", out.UploadPath)
	}
	if len(out.SegmentsDone) != 2 {
		t.Fatalf("SegmentsDone = %v", out.SegmentsDone)
	}
}

func TestMarkerStateTransition(t *testing.T) {
	s := openTestStore(t)

	m := &UploadMarker{
		JobID:      "job-xyz",
		State:      MarkerUploading,
		UploadPath: "/upload/xyz",
		Filename:   "x.bin",
	}
	if err := s.SaveMarker(m); err != nil {
		t.Fatalf("Save uploading: %v", err)
	}

	m.State = MarkerCommitted
	m.UploadToken = "tok-final"
	if err := s.SaveMarker(m); err != nil {
		t.Fatalf("Save committed: %v", err)
	}

	got, _ := s.LoadMarker(m.JobID)
	if got.State != MarkerCommitted || got.UploadToken != "tok-final" {
		t.Fatalf("committed state lost: %+v", got)
	}

	m.State = MarkerCreated
	m.DestID = "rec-1"
	if err := s.SaveMarker(m); err != nil {
		t.Fatalf("Save created: %v", err)
	}

	got, _ = s.LoadMarker(m.JobID)
	if got.State != MarkerCreated || got.DestID != "rec-1" {
		t.Fatalf("created state lost: %+v", got)
	}
}

func TestMarkerDeleteIdempotent(t *testing.T) {
	s := openTestStore(t)
	if err := s.DeleteMarker("never-existed"); err != nil {
		t.Fatalf("DeleteMarker on missing must not error, got %v", err)
	}

	m := &UploadMarker{JobID: "job-d", State: MarkerCommitted, Filename: "x"}
	_ = s.SaveMarker(m)
	if err := s.DeleteMarker("job-d"); err != nil {
		t.Fatalf("DeleteMarker: %v", err)
	}
	got, _ := s.LoadMarker("job-d")
	if got != nil {
		t.Fatalf("expected nil after delete, got %+v", got)
	}
}

func TestMarkerListEnumerates(t *testing.T) {
	s := openTestStore(t)

	for _, id := range []string{"a", "b", "c"} {
		if err := s.SaveMarker(&UploadMarker{
			JobID:    id,
			State:    MarkerUploading,
			Filename: id,
		}); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}
	// A stray file that shouldn't be picked up.
	if err := os.WriteFile(filepath.Join(s.markersDir(), "garbage.txt"), []byte("x"), 0o600); err != nil {
		t.Fatalf("stray file: %v", err)
	}

	ids, err := s.ListMarkers()
	if err != nil {
		t.Fatalf("ListMarkers: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 markers, got %v", ids)
	}
}

func TestLoadMarkerMissingReturnsNil(t *testing.T) {
	s := openTestStore(t)
	m, err := s.LoadMarker("nothing-here")
	if err != nil {
		t.Fatalf("LoadMarker err = %v", err)
	}
	if m != nil {
		t.Fatalf("expected nil marker, got %+v", m)
	}
	// Sanity: the directory exists.
	if _, err := os.Stat(s.markersDir()); errors.Is(err, fs.ErrNotExist) {
		t.Fatal("markers dir should exist after Open")
	}
}
