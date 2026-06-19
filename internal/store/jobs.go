package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// JobStatus enumerates the lifecycle states a job can be in. The
// claim path moves rows pending→running atomically via SQL; failures
// move running→failed, retries move running→pending with a future
// next_run_at.
type JobStatus string

const (
	StatusPending JobStatus = "pending"
	StatusRunning JobStatus = "running"
	StatusFailed  JobStatus = "failed"
)

// Job is one unit of work the engine processes. Stored as one row in
// the `jobs` SQLite table; the on-disk JSON-file representation that
// preceded this is gone.
type Job struct {
	ID              string    `json:"id"`
	ChannelName     string    `json:"channel_name"`
	Kind            string    `json:"kind"`
	SourceConnector string    `json:"source_connector"`
	SourcePath      string    `json:"source_path"`
	SourceVersion   string    `json:"source_version,omitempty"`
	DestID          string    `json:"dest_id,omitempty"`
	Payload         []byte    `json:"payload,omitempty"`
	Attempts        int       `json:"attempts"`
	NextRunAt       time.Time `json:"next_run_at"`
	LastError       string    `json:"last_error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

// ErrNoJob signals that nothing was claimable on a poll. Callers
// should sleep and retry.
var ErrNoJob = errors.New("store: no claimable job")

// NewJobID returns a fresh ULID. Sortable by creation time; usable
// as a primary key value.
func NewJobID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}

// maxLastErrorLen caps the size of last_error fields we persist.
// Some upstream errors (large response bodies, stack traces) are
// huge; persisting them unmodified makes the table grow without
// bound under high-retry churn.
const maxLastErrorLen = 4096

func truncateLastError(reason string) string {
	if len(reason) <= maxLastErrorLen {
		return reason
	}
	const suffix = "...[truncated]"
	return reason[:maxLastErrorLen-len(suffix)] + suffix
}

// EnqueueJobs inserts a batch of jobs in a single transaction. IDs
// auto-fill if empty; CreatedAt / NextRunAt default to now.
func (s *Store) EnqueueJobs(ctx context.Context, jobs []Job) ([]string, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: enqueue begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO jobs (id, channel_name, kind, source_connector, source_path,
                          source_version, dest_id, payload, status,
                          attempts, next_run_at, created_at, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("store: enqueue prepare: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UTC()
	ids := make([]string, len(jobs))
	for i := range jobs {
		j := &jobs[i]
		if j.ID == "" {
			j.ID = NewJobID()
		}
		if j.CreatedAt.IsZero() {
			j.CreatedAt = now
		}
		if j.NextRunAt.IsZero() {
			j.NextRunAt = now
		}
		if _, err := stmt.ExecContext(ctx,
			j.ID, j.ChannelName, j.Kind, j.SourceConnector, j.SourcePath,
			nullString(j.SourceVersion), nullString(j.DestID),
			nullBlob(j.Payload), string(StatusPending),
			j.Attempts, j.NextRunAt.UTC().Format(time.RFC3339Nano),
			j.CreatedAt.UTC().Format(time.RFC3339Nano), nullString(j.LastError),
		); err != nil {
			return nil, fmt.Errorf("store: enqueue job %s: %w", j.ID, err)
		}
		ids[i] = j.ID
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: enqueue commit: %w", err)
	}
	return ids, nil
}

// EnqueueJob is the single-job convenience wrapper.
func (s *Store) EnqueueJob(ctx context.Context, j Job) (string, error) {
	ids, err := s.EnqueueJobs(ctx, []Job{j})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

// ClaimNextJob atomically transitions the oldest claimable job from
// pending to running and returns it. SQLite's UPDATE…RETURNING gives
// us a one-statement claim with full transaction isolation, so no
// in-process mutex or filesystem rename race window is needed.
//
// Returns ErrNoJob when nothing is claimable.
func (s *Store) ClaimNextJob(ctx context.Context) (*Job, error) {
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)

	// UPDATE...RETURNING with a subquery: pick the earliest-eligible
	// pending row, flip its status to running, return the full row.
	row := s.db.QueryRowContext(ctx, `
        UPDATE jobs
           SET status = ?, attempts = attempts + 1
         WHERE id = (
            SELECT id FROM jobs
             WHERE status = ? AND next_run_at <= ?
             ORDER BY next_run_at, id
             LIMIT 1
         )
        RETURNING id, channel_name, kind, source_connector, source_path,
                  source_version, dest_id, payload, attempts,
                  next_run_at, created_at, last_error`,
		string(StatusRunning), string(StatusPending), nowStr)

	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoJob
		}
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	return j, nil
}

// CompleteJob removes the row. Call ONLY after the sync_log row has
// been inserted (the audit-of-record) and any upload marker has been
// deleted.
func (s *Store) CompleteJob(ctx context.Context, jobID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, jobID); err != nil {
		return fmt.Errorf("store: complete %s: %w", jobID, err)
	}
	return nil
}

// FailJob transitions the running job to failed status and records
// the reason in last_error. Any upload marker is best-effort deleted
// (permanently-failed jobs aren't retried, so a leftover marker is
// just unreachable garbage).
func (s *Store) FailJob(ctx context.Context, jobID string, attempts int, reason string) error {
	if _, err := s.db.ExecContext(ctx, `
        UPDATE jobs
           SET status = ?, attempts = ?, last_error = ?
         WHERE id = ?`,
		string(StatusFailed), attempts, truncateLastError(reason), jobID,
	); err != nil {
		return fmt.Errorf("store: fail %s: %w", jobID, err)
	}
	if err := s.DeleteMarker(jobID); err != nil {
		return fmt.Errorf("store: fail %s: cleanup marker: %w", jobID, err)
	}
	return nil
}

// RetryJob transitions a running job back to pending with a future
// next_run_at. Persisting attempts here is what makes the engine's
// max-attempts check work across retries.
func (s *Store) RetryJob(ctx context.Context, jobID string, attempts int, reason string, delay time.Duration) error {
	next := time.Now().UTC().Add(delay).Format(time.RFC3339Nano)
	if _, err := s.db.ExecContext(ctx, `
        UPDATE jobs
           SET status = ?, attempts = ?, last_error = ?, next_run_at = ?
         WHERE id = ?`,
		string(StatusPending), attempts, truncateLastError(reason), next, jobID,
	); err != nil {
		return fmt.Errorf("store: retry %s: %w", jobID, err)
	}
	return nil
}

// ListJobs returns every job currently in the given status. Used by
// the CLI's status / inspect commands. Order is by next_run_at then id
// for stability.
func (s *Store) ListJobs(ctx context.Context, status JobStatus) ([]*Job, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT id, channel_name, kind, source_connector, source_path,
               source_version, dest_id, payload, attempts,
               next_run_at, created_at, last_error
          FROM jobs
         WHERE status = ?
         ORDER BY next_run_at, id`, string(status))
	if err != nil {
		return nil, fmt.Errorf("store: list jobs %q: %w", status, err)
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate jobs %q: %w", status, err)
	}
	return out, nil
}

// recoverRunningJobs is called by Open: anything left in running
// status from a previous run is moved back to pending so a worker
// picks it up. The upload marker carries the resume position.
func (s *Store) recoverRunningJobs(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
        UPDATE jobs SET status = ?, next_run_at = ?
         WHERE status = ?`,
		string(StatusPending), time.Now().UTC().Format(time.RFC3339Nano), string(StatusRunning),
	); err != nil {
		return fmt.Errorf("store: recover running jobs: %w", err)
	}
	return nil
}

// --- row scanning helpers -----------------------------------------------

// rowScanner is what *sql.Row and *sql.Rows both satisfy.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(r rowScanner) (*Job, error) {
	var (
		j            Job
		srcVersion   sql.NullString
		aprimoRecID  sql.NullString
		payload      []byte
		nextRunAtStr string
		createdAtStr string
		lastErr      sql.NullString
	)
	if err := r.Scan(
		&j.ID, &j.ChannelName, &j.Kind, &j.SourceConnector, &j.SourcePath,
		&srcVersion, &aprimoRecID, &payload, &j.Attempts,
		&nextRunAtStr, &createdAtStr, &lastErr,
	); err != nil {
		return nil, err
	}
	j.SourceVersion = srcVersion.String
	j.DestID = aprimoRecID.String
	j.Payload = payload
	j.LastError = lastErr.String
	if t, err := time.Parse(time.RFC3339Nano, nextRunAtStr); err == nil {
		j.NextRunAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, createdAtStr); err == nil {
		j.CreatedAt = t
	}
	return &j, nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullBlob(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}
