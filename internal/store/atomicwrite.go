package store

import (
	"fmt"
	"os"
	"path/filepath"
)

// stripBOM removes a leading UTF-8 BOM if present. Operator-edited
// upload-marker files via Notepad on Windows can get one; encoding/json
// won't accept it.
func stripBOM(b []byte) []byte {
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		return b[3:]
	}
	return b
}

// atomicWrite writes data to path via a sibling .tmp file plus rename.
// os.Rename is atomic on POSIX and on Windows when the destination is
// on the same volume. Used by the on-disk upload-marker files.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// 0600: markers carry the Aprimo upload token + dest record id.
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("store: write tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("store: rename %s: %w", path, err)
	}
	return nil
}
