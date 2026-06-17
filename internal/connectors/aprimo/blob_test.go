package aprimo

import "testing"

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
