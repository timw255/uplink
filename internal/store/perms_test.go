package store

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Upload markers carry the Aprimo upload token + dest record id, and the
// uploads dir holds them — both must be owner-only so a shared host can't
// read them. (POSIX modes aren't enforced on Windows, so skip there.)

func TestAtomicWrite_OwnerOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file modes not enforced on Windows")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "uploads", "job.session.json")
	if err := atomicWrite(p, []byte(`{"upload_token":"secret"}`)); err != nil {
		t.Fatal(err)
	}
	if m := perm(t, p); m != 0o600 {
		t.Fatalf("marker file mode = %o, want 0600", m)
	}
	if m := perm(t, filepath.Dir(p)); m != 0o700 {
		t.Fatalf("marker dir mode = %o, want 0700", m)
	}
}

func TestOpen_UploadsDirOwnerOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file modes not enforced on Windows")
	}
	dir := t.TempDir()
	s, err := Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if m := perm(t, filepath.Join(dir, "uploads")); m != 0o700 {
		t.Fatalf("uploads dir mode = %o, want 0700", m)
	}
}

func perm(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}
