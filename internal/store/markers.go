package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MarkerState is the position of an in-flight upload in its four-state
// lifecycle: uploading → committed → created → (deleted).
//
//	uploading — segments are being POSTed. `segments_done` advances per
//	            successful segment commit.
//	committed — Aprimo's /commit endpoint succeeded; `upload_token`
//	            holds the final token. Records.Create has NOT yet been
//	            called.
//	created   — Records.Create (or Update) succeeded; `dest_id`
//	            is set. sync_log has NOT yet been written.
//
// On crash the engine reads the marker state and drives the next step:
//   - uploading: resume segments from `segments_done`
//   - committed: call Records.Create/Update with the retained token,
//     NO re-upload
//   - created: insert sync_log if missing, delete the marker
//
// Each transition is an atomic-rename rewrite of the marker file, and
// each next-step operation is idempotent against any partial completion
// of itself. The result is exactly-once Aprimo record creation per
// (channel, source_path, source_version) regardless of where a crash
// falls.
type MarkerState string

const (
	MarkerUploading MarkerState = "uploading"
	MarkerCommitted MarkerState = "committed"
	MarkerCreated   MarkerState = "created"
)

// UploadMarker is the on-disk record of an in-flight upload. One file
// per job at data/uploads/<job_id>.session.json.
type UploadMarker struct {
	JobID         string      `json:"job_id"`
	State         MarkerState `json:"state"`
	UploadToken   string      `json:"upload_token,omitempty"`
	SegmentsTotal int         `json:"segments_total"`
	// SegmentsDone is the list of segment indices that have committed
	// successfully. On resume, the uploader skips these. We store the
	// list (not a count) so out-of-order commits don't lose progress
	// when a worker resumes mid-batch.
	SegmentsDone    []int     `json:"segments_done,omitempty"`
	ExpiresAt       time.Time `json:"expires_at"`
	Channel         string    `json:"channel"`
	SourceConnector string    `json:"source_connector"`
	SourcePath      string    `json:"source_path"`
	SourceVersion   string    `json:"source_version,omitempty"`
	DestID          string    `json:"dest_id,omitempty"`
	UploadPath      string    `json:"upload_path,omitempty"` // path returned by /uploads/segments
	Filename        string    `json:"filename"`
	Updated         time.Time `json:"updated"`
}

// LoadMarker returns the marker for jobID, or (nil, nil) if there
// isn't one. Missing files are NOT an error — most jobs don't have a
// marker (single-shot uploads complete without one).
func (s *Store) LoadMarker(jobID string) (*UploadMarker, error) {
	path := s.markerPath(jobID)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read marker %s: %w", jobID, err)
	}
	data = stripBOM(data)
	var m UploadMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("store: parse marker %s: %w", jobID, err)
	}
	return &m, nil
}

// SaveMarker writes the marker atomically. Every state transition
// goes through here.
func (s *Store) SaveMarker(m *UploadMarker) error {
	if m == nil || m.JobID == "" {
		return errors.New("store: marker job_id is required")
	}
	m.Updated = time.Now().UTC()
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("store: marshal marker %s: %w", m.JobID, err)
	}
	return atomicWrite(s.markerPath(m.JobID), body)
}

// DeleteMarker removes the marker file. Idempotent — missing is fine.
func (s *Store) DeleteMarker(jobID string) error {
	if err := os.Remove(s.markerPath(jobID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("store: delete marker %s: %w", jobID, err)
	}
	return nil
}

// ListMarkers returns the job IDs for every marker file currently
// present. Used by startup recovery and `uplink status`.
func (s *Store) ListMarkers() ([]string, error) {
	entries, err := os.ReadDir(s.markersDir())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		const suffix = ".session.json"
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		out = append(out, strings.TrimSuffix(name, suffix))
	}
	return out, nil
}

func (s *Store) markersDir() string {
	return filepath.Join(s.dataDir, "uploads")
}

func (s *Store) markerPath(jobID string) string {
	return filepath.Join(s.markersDir(), jobID+".session.json")
}
