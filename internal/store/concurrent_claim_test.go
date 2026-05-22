package store

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
)

// TestClaimNextJob_ConcurrentClaimsAreSerialized hammers ClaimNextJob
// with concurrent workers to prove the in-process claim mutex prevents
// two goroutines from observing the same pending file twice. Without
// the mutex, on Windows the ReadDir + Rename race causes occasional
// double-claims (one rename wins, the loser sees ErrNotExist and moves
// on, but it ALSO got a valid *Job back from readJobFile and would have
// processed it). With the mutex, every job is claimed by exactly one
// goroutine.
func TestClaimNextJob_ConcurrentClaimsAreSerialized(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	const jobCount = 50
	const workerCount = 8

	// Seed jobCount jobs into pending/.
	jobs := make([]Job, jobCount)
	for i := range jobs {
		jobs[i] = Job{
			ChannelName: "ch",
			Kind:        "OnCreate",
			SourcePath:  "p" + strconv.Itoa(i) + ".txt",
		}
	}
	if _, err := s.EnqueueJobs(ctx, jobs); err != nil {
		t.Fatalf("EnqueueJobs: %v", err)
	}

	var (
		mu      sync.Mutex
		claimed = make(map[string]int) // jobID -> claim count
		wg      sync.WaitGroup
	)
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for {
				j, err := s.ClaimNextJob(ctx)
				if errors.Is(err, ErrNoJob) {
					return
				}
				if err != nil {
					t.Errorf("ClaimNextJob: %v", err)
					return
				}
				mu.Lock()
				claimed[j.ID]++
				mu.Unlock()
				// Don't transition back to pending — we want each job
				// claimed exactly once for this assertion. Leave them in
				// running/; the test doesn't care.
			}
		}()
	}
	wg.Wait()

	if len(claimed) != jobCount {
		t.Fatalf("expected %d distinct jobs claimed, got %d", jobCount, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Errorf("job %s was claimed %d times (want 1)", id, count)
		}
	}
}
