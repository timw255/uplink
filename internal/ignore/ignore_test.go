package ignore

import "testing"

func mustMatch(t *testing.T, m *Matcher, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if !m.ShouldIgnore(p) {
			t.Errorf("expected %q to be ignored", p)
		}
	}
}

func mustNotMatch(t *testing.T, m *Matcher, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if m.ShouldIgnore(p) {
			t.Errorf("expected %q NOT to be ignored", p)
		}
	}
}

func TestBlankAndComments(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("# comment\n\n   \n", "")
	if m.PatternCount() != 0 {
		t.Fatalf("expected 0 patterns, got %d", m.PatternCount())
	}
}

func TestBareFileMatchesAnyDepth(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("Thumbs.db\n", "")

	mustMatch(t, m, "Thumbs.db", "subdir/Thumbs.db", "a/b/c/Thumbs.db")
	mustNotMatch(t, m, "Thumbs.db.txt", "ThumbsXdb")
}

func TestAnchoredPattern(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("/build\n", "")

	mustMatch(t, m, "build")
	mustNotMatch(t, m, "src/build")
}

func TestDirectorySuffix(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("logs/\n", "")

	mustMatch(t, m, "logs", "logs/today.log", "sub/logs", "sub/logs/a/b.txt")
	mustNotMatch(t, m, "logsfile.txt")
}

func TestRecursiveSlashStarStar(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("tmp/**\n", "")

	mustMatch(t, m, "tmp", "tmp/a.txt", "tmp/nested/x")
	mustNotMatch(t, m, "src/tmp.txt")
}

func TestStarGlob(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("*.log\n", "")

	mustMatch(t, m, "a.log", "sub/a.log")
	mustNotMatch(t, m, "a.log.txt")
}

func TestRelativeDirectoryScopesPattern(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("notes.txt\n", "docs")

	mustMatch(t, m, "docs/notes.txt")
	mustNotMatch(t, m, "notes.txt", "src/notes.txt")
}

func TestAbsolutePathStrippedToRoot(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("Thumbs.db\n", "")

	mustMatch(t, m, "/root/Thumbs.db", "/root/sub/Thumbs.db")
}

func TestAnchoredPatternDoesNotMatchOutsideRoot(t *testing.T) {
	// A leading-slash pattern is anchored to the root, so it must not
	// match a path that lives elsewhere on disk.
	m := NewMatcher("/root")
	m.AddPatternsFromContent("/build\n", "")
	mustNotMatch(t, m, "/other/build")
}

func TestClearAndCount(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("a\nb\n# c\n", "")
	if m.PatternCount() != 2 {
		t.Fatalf("expected 2 patterns, got %d", m.PatternCount())
	}
	m.Clear()
	if m.PatternCount() != 0 {
		t.Fatalf("expected 0 after Clear, got %d", m.PatternCount())
	}
	mustNotMatch(t, m, "a", "b")
}

func TestCRLFLineEndings(t *testing.T) {
	m := NewMatcher("/root")
	m.AddPatternsFromContent("foo\r\nbar\r\n", "")
	mustMatch(t, m, "foo", "bar")
}
