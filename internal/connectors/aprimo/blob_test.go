package aprimo

import (
	"encoding/base64"
	"testing"
)

func TestPlanBlocks(t *testing.T) {
	const mib = 1 << 20
	cases := []struct {
		name      string
		size      int64
		wantCount int
		wantBS    int64
	}{
		{"empty", 0, 0, directBlockSize},
		{"one byte", 1, 1, directBlockSize},
		{"exactly one block", directBlockSize, 1, directBlockSize},
		{"one block plus a byte", directBlockSize + 1, 2, directBlockSize},
		{"three blocks", directBlockSize * 3, 3, directBlockSize},
		// Just over the 50k-block cap at 16 MiB → block must grow.
		{"over cap grows block", directBlockSize * (maxBlockCount + 1), 0, 0},
		// 1 TiB also stays within the cap by growing the block.
		{"one tebibyte", 1 << 40, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bs, count := planBlocks(c.size, directBlockSize)
			if count > maxBlockCount {
				t.Fatalf("count %d exceeds Azure's %d-block cap", count, maxBlockCount)
			}
			if c.size > 0 && int64(count)*bs < c.size {
				t.Fatalf("blocks don't cover the file: %d*%d < %d", count, bs, c.size)
			}
			if c.wantCount != 0 && count != c.wantCount {
				t.Fatalf("count = %d, want %d", count, c.wantCount)
			}
			if c.wantBS != 0 && bs != c.wantBS {
				t.Fatalf("blockSize = %d, want %d", bs, c.wantBS)
			}
			if count > maxBlockCount/2 && bs%mib != 0 {
				t.Fatalf("grown block %d is not MiB-aligned", bs)
			}
		})
	}
}

func TestBlockID_FixedWidthAndUnique(t *testing.T) {
	// Azure requires every block ID in a blob to decode to the same length.
	want := len(blockID(0))
	seen := map[string]bool{}
	for _, i := range []int{0, 1, 42, 49999, maxBlockCount} {
		id := blockID(i)
		if len(id) != want {
			t.Fatalf("blockID(%d) length %d != %d (not fixed-width)", i, len(id), want)
		}
		if _, err := base64.StdEncoding.DecodeString(id); err != nil {
			t.Fatalf("blockID(%d) is not valid base64: %v", i, err)
		}
		if seen[id] {
			t.Fatalf("blockID(%d) collides", i)
		}
		seen[id] = true
	}
}

func TestIsAzureBlobURL(t *testing.T) {
	cases := []struct {
		url  string
		want bool
	}{
		{"https://acct.blob.core.windows.net/c/x?sig=y", true},   // public cloud
		{"https://acct.blob.core.chinacloudapi.cn/c/x", true},    // Azure China
		{"https://acct.blob.core.usgovcloudapi.net/c/x", true},   // Azure US Gov
		{"http://acct.blob.core.windows.net/c/x", false},         // not https
		{"https://evil.example.com/c/x?sig=y", false},            // wrong host
		{"https://acct.blob.core.windows.net.evil.com/x", false}, // lookalike suffix
		{"https://evil-blob.core.windows.net/x", false},          // no dot boundary
		{"https://blob.core.windows.net.attacker.io/x", false},   // suffix in the middle
		{"ftp://acct.blob.core.windows.net/x", false},            // wrong scheme
		{"not a url", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isAzureBlobURL(c.url); got != c.want {
			t.Errorf("isAzureBlobURL(%q) = %v, want %v", c.url, got, c.want)
		}
	}
}
