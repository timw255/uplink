package connector

import (
	"reflect"
	"testing"
	"time"
)

func TestNormalizeWatchers_SortsLongestFirst(t *testing.T) {
	specs := []WatcherSpec{
		{Prefix: "", PollInterval: time.Hour},
		{Prefix: "images/", PollInterval: 5 * time.Minute},
		{Prefix: "images/hot", PollInterval: 10 * time.Second},
	}
	got, err := NormalizeWatchers(specs)
	if err != nil {
		t.Fatalf("NormalizeWatchers: %v", err)
	}
	wantPrefixes := []string{"images/hot", "images", ""}
	var prefixes []string
	for _, w := range got {
		prefixes = append(prefixes, w.Prefix)
	}
	if !reflect.DeepEqual(prefixes, wantPrefixes) {
		t.Errorf("prefixes = %v, want %v", prefixes, wantPrefixes)
	}
}

func TestNormalizeWatchers_RejectsDuplicates(t *testing.T) {
	specs := []WatcherSpec{
		{Prefix: "images/", PollInterval: 5 * time.Minute},
		{Prefix: "images", PollInterval: 10 * time.Minute},
	}
	if _, err := NormalizeWatchers(specs); err == nil {
		t.Fatal("expected duplicate-prefix error")
	}
}

func TestNormalizeWatchers_RejectsZeroInterval(t *testing.T) {
	if _, err := NormalizeWatchers([]WatcherSpec{{Prefix: "x", PollInterval: 0}}); err == nil {
		t.Fatal("expected non-positive interval error")
	}
}

func TestSubwatcherPrefixes(t *testing.T) {
	all := []WatcherSpec{
		{Prefix: "images/hot"},
		{Prefix: "images"},
		{Prefix: ""},
	}
	cases := []struct {
		of   string
		want []string
	}{
		{of: "", want: []string{"images/hot", "images"}},
		{of: "images", want: []string{"images/hot"}},
		{of: "images/hot", want: nil},
	}
	for _, c := range cases {
		var w WatcherSpec
		for _, s := range all {
			if s.Prefix == c.of {
				w = s
			}
		}
		got := SubwatcherPrefixes(all, w)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("SubwatcherPrefixes(of=%q) = %v, want %v", c.of, got, c.want)
		}
	}
}

func TestPathIsUnderAnyPrefix(t *testing.T) {
	prefixes := []string{"images/hot", "archive/2020"}
	cases := []struct {
		path string
		want bool
	}{
		{"images/hot/x.jpg", true},
		{"images/hot", true},
		{"images/cold/x.jpg", false},
		{"archive/2020/foo.bin", true},
		{"archive/2021/foo.bin", false},
		{"other/x.jpg", false},
	}
	for _, c := range cases {
		if got := PathIsUnderAnyPrefix(c.path, prefixes); got != c.want {
			t.Errorf("PathIsUnderAnyPrefix(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestScopeKey(t *testing.T) {
	if got := (WatcherSpec{Prefix: ""}).ScopeKey("fs-in"); got != "fs-in" {
		t.Errorf("empty prefix scope = %q, want fs-in", got)
	}
	if got := (WatcherSpec{Prefix: "images/hot"}).ScopeKey("fs-in"); got != "fs-in#images/hot" {
		t.Errorf("nested scope = %q, want fs-in#images/hot", got)
	}
}
