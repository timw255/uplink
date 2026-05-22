package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// HeartbeatInterval is how often the running daemon touches its
// lockfile to prove liveness.
const HeartbeatInterval = 30 * time.Second

// LockStaleAfter is how old an unmodified lockfile must be before
// readLock treats it as stale, regardless of PID. 3× HeartbeatInterval
// tolerates one missed heartbeat (GC pause, slow disk) without false
// staleness. Windows needs this fallback because Signal(0) returns
// "not supported" for every pid and can't distinguish alive from dead.
const LockStaleAfter = 3 * HeartbeatInterval

// LockInfo describes the daemon-lockfile state for diagnostics + the
// `vacuum --full` safety guard.
type LockInfo struct {
	Path     string
	PID      int
	Modified time.Time
	Alive    bool
	Stale    bool // file exists but PID is dead OR heartbeat is too old
	Empty    bool // no lockfile at all
}

// lockfilePath is the conventional path of the daemon lockfile.
func lockfilePath(dataDir string) string {
	return filepath.Join(dataDir, "uplink.lock")
}

// readLock returns the current state of the daemon lockfile. A lock is
// considered alive only when the PID is alive AND the file was touched
// within LockStaleAfter; the heartbeat check is the load-bearing signal
// on Windows, where Signal(0) cannot detect dead processes.
func readLock(dataDir string) (LockInfo, error) {
	path := lockfilePath(dataDir)
	info := LockInfo{Path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		info.Empty = true
		return info, nil
	}
	if err != nil {
		return info, fmt.Errorf("read lockfile: %w", err)
	}
	stat, err := os.Stat(path)
	if err != nil {
		return info, fmt.Errorf("stat lockfile: %w", err)
	}
	info.Modified = stat.ModTime()
	trimmed := strings.TrimSpace(string(data))
	pid, err := strconv.Atoi(trimmed)
	if err != nil || pid <= 0 {
		// Malformed — treat as stale.
		info.Stale = true
		return info, nil
	}
	info.PID = pid
	heartbeatFresh := time.Since(info.Modified) <= LockStaleAfter
	info.Alive = isAlive(pid) && heartbeatFresh
	info.Stale = !info.Alive
	return info, nil
}

// acquireLock writes the current PID into the lockfile. Returns an
// error if the existing lockfile records a still-alive PID; stale locks
// are overwritten.
func acquireLock(dataDir string) error {
	info, err := readLock(dataDir)
	if err != nil {
		return err
	}
	if !info.Empty && info.Alive {
		return fmt.Errorf("daemon already running with pid %d (lockfile: %s)", info.PID, info.Path)
	}
	if err := os.MkdirAll(filepath.Dir(info.Path), 0o755); err != nil {
		return err
	}
	tmp := info.Path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return fmt.Errorf("write lockfile: %w", err)
	}
	if err := os.Rename(tmp, info.Path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename lockfile: %w", err)
	}
	return nil
}

// releaseLock removes the lockfile. Idempotent.
func releaseLock(dataDir string) error {
	if err := os.Remove(lockfilePath(dataDir)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// refreshLock updates the lockfile's mtime to prove the daemon is
// still ticking. Used as the heartbeat signal that lets readLock
// distinguish a healthy daemon from a stale PID — important on
// Windows where Signal(0) is meaningless.
func refreshLock(dataDir string) error {
	path := lockfilePath(dataDir)
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		return fmt.Errorf("refresh lockfile: %w", err)
	}
	return nil
}

// runLockHeartbeat ticks every HeartbeatInterval and refreshes the
// lockfile mtime. Returns when ctx is cancelled. Refresh errors are
// not fatal — they're typical when the daemon is shutting down and
// the lockfile has already been removed by releaseLock.
func runLockHeartbeat(ctx context.Context, dataDir string, onError func(error)) {
	t := time.NewTicker(HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := refreshLock(dataDir); err != nil && onError != nil {
				onError(err)
			}
		}
	}
}

// isAlive returns whether a process with the given pid is currently
// running. On Unix uses signal 0; on Windows uses os.FindProcess (which
// returns an error if the process is gone). Best-effort — operators
// should still confirm with their OS tools.
func isAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	// Common dead-process signals across platforms.
	if errors.Is(err, os.ErrProcessDone) {
		return false
	}
	if errors.Is(err, syscall.ESRCH) {
		return false
	}
	// On Windows, Signal(0) returns "not supported" — fall back to a
	// best-effort guess: if the process handle resolved, treat as alive.
	if strings.Contains(err.Error(), "not supported") {
		return true
	}
	return false
}
