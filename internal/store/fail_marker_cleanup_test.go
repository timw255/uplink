package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestFailJob_DeletesUploadMarker proves a permanently-failed job
// drops its marker on the way to failed/. Without this, marker files
// for jobs that hit max-attempts would accumulate forever in
// data/uploads/ since nothing else cleans them up.
func TestFailJob_DeletesUploadMarker(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Enqueue + claim → job is in running/.
	jobID, err := s.EnqueueJob(ctx, Job{
		ChannelName: "ch",
		Kind:        "OnCreate",
		SourcePath:  "x.txt",
	})
	if err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	if _, err := s.ClaimNextJob(ctx); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}

	// Seed a marker the way the Aprimo connector would.
	if err := s.SaveMarker(&UploadMarker{
		JobID:    jobID,
		State:    MarkerUploading,
		Filename: "x.txt",
	}); err != nil {
		t.Fatalf("SaveMarker: %v", err)
	}

	// Sanity: marker exists.
	markerPath := filepath.Join(s.dataDir, "uploads", jobID+".session.json")
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("marker should exist before FailJob: %v", err)
	}

	// Fail the job. Marker should be cleaned up.
	if err := s.FailJob(ctx, jobID, 5, "no more attempts"); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("marker should have been deleted on FailJob, got err=%v", err)
	}
}

// TestFailJob_NoMarkerIsNotAnError proves FailJob still succeeds when
// the job never had a marker (single-shot uploads complete without
// one). The marker-cleanup step must tolerate the common no-marker
// case rather than spuriously erroring.
func TestFailJob_NoMarkerIsNotAnError(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	jobID, err := s.EnqueueJob(ctx, Job{
		ChannelName: "ch",
		Kind:        "OnCreate",
		SourcePath:  "x.txt",
	})
	if err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	if _, err := s.ClaimNextJob(ctx); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}

	if err := s.FailJob(ctx, jobID, 5, "no more attempts"); err != nil {
		t.Fatalf("FailJob with no marker should succeed: %v", err)
	}
}
