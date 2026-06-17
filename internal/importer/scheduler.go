package importer

import "sync"

// scheduler hands out file records for upload in a size-balanced blend.
// recs is sorted ascending by size; it dispenses from the small end when
// the create queue is starving (fast uploads keep the API fed) and from
// the big end otherwise (drain the long poles early so they never tail at
// the end). Safe for concurrent use by the upload pool.
type scheduler struct {
	mu    sync.Mutex
	recs  []workRecord
	small int
	big   int
}

func newScheduler(recs []workRecord) *scheduler {
	return &scheduler{recs: recs, small: 0, big: len(recs) - 1}
}

// next returns the next file to upload, choosing small-vs-big from the
// create queue's fill level (depth vs cap). Returns false when exhausted.
func (s *scheduler) next(queueDepth, queueCap int) (workRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.small > s.big {
		return workRecord{}, false
	}
	// Starving create queue (< half full) → feed it a fast small upload;
	// otherwise spend the spare upload capacity draining a big one.
	if queueCap > 0 && queueDepth*2 < queueCap {
		r := s.recs[s.small]
		s.small++
		return r, true
	}
	r := s.recs[s.big]
	s.big--
	return r, true
}
