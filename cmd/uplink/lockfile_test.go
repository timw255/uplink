package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestReadLock_StaleByMtimeOverridesAliveCheck proves a lockfile is
// treated as stale once its mtime exceeds LockStaleAfter — even if the
// recorded PID is still alive (which always trips on Windows where
// Signal(0) returns "not supported" for every pid).
//
// This is the regression for the Windows daemon-restart bug: a crashed
// daemon leaves its PID in the lockfile; the OS recycles the PID for
// some unrelated process; the next daemon start would otherwise see
// "isAlive=true" forever and refuse to start.
func TestReadLock_StaleByMtimeOverridesAliveCheck(t *testing.T) {
	dataDir := t.TempDir()
	lock := lockfilePath(dataDir)
	if err := os.WriteFile(lock, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatalf("seed lock: %v", err)
	}

	old := time.Now().Add(-2 * LockStaleAfter)
	if err := os.Chtimes(lock, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	info, err := readLock(dataDir)
	if err != nil {
		t.Fatalf("readLock: %v", err)
	}
	if info.Empty {
		t.Fatal("expected non-empty lockfile")
	}
	if info.PID != os.Getpid() {
		t.Fatalf("PID = %d, want %d", info.PID, os.Getpid())
	}
	if info.Alive {
		t.Fatal("lockfile with stale mtime should NOT be Alive, even though our PID is alive")
	}
	if !info.Stale {
		t.Fatal("lockfile with stale mtime should be Stale")
	}
}

// TestReadLock_FreshMtimeIsAlive is the positive case: a lockfile
// written moments ago with our PID is treated as alive.
func TestReadLock_FreshMtimeIsAlive(t *testing.T) {
	dataDir := t.TempDir()
	lock := lockfilePath(dataDir)
	if err := os.WriteFile(lock, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatalf("seed lock: %v", err)
	}

	info, err := readLock(dataDir)
	if err != nil {
		t.Fatalf("readLock: %v", err)
	}
	if !info.Alive {
		t.Fatalf("fresh lockfile with live PID should be Alive, got %+v", info)
	}
	if info.Stale {
		t.Fatalf("fresh lockfile should NOT be Stale, got %+v", info)
	}
}

// TestRefreshLock_UpdatesMtime proves refreshLock advances the
// lockfile's mtime so a long-running daemon doesn't see itself
// classified as stale.
func TestRefreshLock_UpdatesMtime(t *testing.T) {
	dataDir := t.TempDir()
	lock := lockfilePath(dataDir)
	if err := os.WriteFile(lock, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatalf("seed lock: %v", err)
	}

	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(lock, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if err := refreshLock(dataDir); err != nil {
		t.Fatalf("refreshLock: %v", err)
	}

	stat, err := os.Stat(filepath.Join(dataDir, "uplink.lock"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if time.Since(stat.ModTime()) > 5*time.Second {
		t.Fatalf("refresh did not update mtime (still %v old)", time.Since(stat.ModTime()))
	}
}

// TestAcquireLock_OverwritesStaleByMtime proves a stale-by-mtime
// lockfile doesn't block a new daemon from starting up.
func TestAcquireLock_OverwritesStaleByMtime(t *testing.T) {
	dataDir := t.TempDir()
	lock := lockfilePath(dataDir)
	// Use a fake PID (0 is rejected as malformed, so use 1).
	if err := os.WriteFile(lock, []byte("1"), 0o644); err != nil {
		t.Fatalf("seed lock: %v", err)
	}
	old := time.Now().Add(-2 * LockStaleAfter)
	if err := os.Chtimes(lock, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if err := acquireLock(dataDir); err != nil {
		t.Fatalf("acquireLock should overwrite stale lock: %v", err)
	}

	// Lockfile now carries our PID.
	data, err := os.ReadFile(lock)
	if err != nil {
		t.Fatalf("read lock: %v", err)
	}
	gotPID, _ := strconv.Atoi(string(data))
	if gotPID != os.Getpid() {
		t.Fatalf("lockfile PID = %d, want %d", gotPID, os.Getpid())
	}
}
