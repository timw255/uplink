package aprimo

import "testing"

func TestSanitizeFilename(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"photo.jpg", "photo.jpg"},                               // ordinary: untouched
		{"a<b>c:d\"e/f\\g|h?i*j.png", "a_b_c_d_e_f_g_h_i_j.png"}, // every forbidden char
		{"weird\\name.jpg", "weird_name.jpg"},                    // backslash survives path.Base
		{"q?size=1.bin", "q_size=1.bin"},                         // S3-style key fragment
		{"ok_name-1.2.3.tiff", "ok_name-1.2.3.tiff"},             // dots, dashes, underscores kept
		{"line\r\nbreak.txt", "line__break.txt"},                 // control chars (CRLF injection guard)
		{"café_ünïcode.jpg", "café_ünïcode.jpg"},                 // non-ASCII passes through
	}
	for _, c := range cases {
		if got := SanitizeFilename(c.in); got != c.want {
			t.Errorf("SanitizeFilename(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
