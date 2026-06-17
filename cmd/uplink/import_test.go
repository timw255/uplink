package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveLedgerRealRunDefaultsUnderDataDir(t *testing.T) {
	dataDir := t.TempDir()
	manifest := filepath.Join(t.TempDir(), "records.jsonl")

	path, resume, err := resolveLedger(dataDir, manifest, "aprimo-prod", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !resume {
		t.Error("real run should resume by default")
	}
	wantDir := filepath.Join(dataDir, "imports")
	if filepath.Dir(path) != wantDir {
		t.Errorf("ledger dir = %s, want %s", filepath.Dir(path), wantDir)
	}
	if !strings.HasPrefix(filepath.Base(path), "records-") || !strings.HasSuffix(path, ".jsonl") {
		t.Errorf("ledger name = %s, want records-<hash>.jsonl", filepath.Base(path))
	}
	if _, err := os.Stat(wantDir); err != nil {
		t.Errorf("imports dir not created: %v", err)
	}
}

func TestResolveLedgerStableAndKeyedByDestination(t *testing.T) {
	dataDir := t.TempDir()
	manifest := filepath.Join(t.TempDir(), "records.jsonl")

	a, _, _ := resolveLedger(dataDir, manifest, "destA", false, false)
	aAgain, _, _ := resolveLedger(dataDir, manifest, "destA", false, false)
	b, _, _ := resolveLedger(dataDir, manifest, "destB", false, false)

	if a != aAgain {
		t.Errorf("same manifest+dest should map to same ledger: %s vs %s", a, aAgain)
	}
	if a == b {
		t.Error("different destinations should map to different ledgers")
	}
}

func TestResolveLedgerRestartDisablesResume(t *testing.T) {
	dataDir := t.TempDir()
	manifest := filepath.Join(t.TempDir(), "records.jsonl")

	_, resume, err := resolveLedger(dataDir, manifest, "aprimo", false, true)
	if err != nil {
		t.Fatal(err)
	}
	if resume {
		t.Error("--restart should disable resume")
	}
}

func TestResolveLedgerDryRunKeepsNoLedger(t *testing.T) {
	dataDir := t.TempDir()
	manifest := filepath.Join(t.TempDir(), "records.jsonl")

	path, resume, err := resolveLedger(dataDir, manifest, "aprimo", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if path != "" || resume {
		t.Errorf("dry run should keep no ledger: path=%q resume=%v", path, resume)
	}
	// The imports dir must NOT be created for a dry run.
	if _, err := os.Stat(filepath.Join(dataDir, "imports")); !os.IsNotExist(err) {
		t.Error("dry run should not create the imports dir")
	}
}
