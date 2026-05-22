// Package ignore implements a gitignore-style pattern matcher used to
// exclude files from synchronization.
//
// Matcher is provider-agnostic: the caller supplies .uplinkignore file
// content (read via whatever API the connector uses) along with the
// relative directory the file was found in. The Matcher compiles each
// non-comment, non-empty line into a regex and tests candidate paths
// against the full set.
package ignore

import (
	"path"
	"regexp"
	"strings"
)

// IgnoreFilename is the conventional name connectors look for.
const IgnoreFilename = ".uplinkignore"

const (
	pathSep          = "/"
	commentPrefix    = "#"
	directorySuffix  = "/"
	recursivePattern = "/**"
	wildcardPattern  = "**"
)

// Matcher tests paths against an accumulated set of ignore patterns. It
// is NOT safe for concurrent mutation — callers should treat construction
// + AddPatternsFromContent as a build phase and then only ShouldIgnore
// from multiple goroutines.
type Matcher struct {
	rootPath string
	patterns []compiledPattern
}

type compiledPattern struct {
	regex  *regexp.Regexp
	source string // relative directory where the pattern was defined
}

// NewMatcher returns an empty Matcher rooted at rootPath. Paths passed
// to ShouldIgnore that are absolute are made relative to rootPath
// before matching.
func NewMatcher(rootPath string) *Matcher {
	return &Matcher{rootPath: normalizePath(rootPath)}
}

// AddPatternsFromContent parses the given .uplinkignore file body and
// appends its rules. relativePath is the directory (relative to root)
// the file was found in; pass "" for a file at the root.
func (m *Matcher) AddPatternsFromContent(content, relativePath string) {
	relDir := normalizePath(relativePath)
	for raw := range strings.SplitSeq(content, "\n") {
		line := strings.TrimSpace(strings.TrimSuffix(raw, "\r"))
		if !isValidLine(line) {
			continue
		}
		m.appendLine(line, relDir)
	}
}

// ShouldIgnore reports whether filePath matches any compiled pattern.
// filePath may be absolute or relative; absolute paths are made
// relative to the matcher's root before testing.
func (m *Matcher) ShouldIgnore(filePath string) bool {
	rel := m.relPath(filePath)
	for _, p := range m.patterns {
		if p.regex.MatchString(rel) {
			return true
		}
	}
	return false
}

// PatternCount returns the number of compiled patterns.
func (m *Matcher) PatternCount() int { return len(m.patterns) }

// Clear discards all compiled patterns.
func (m *Matcher) Clear() { m.patterns = nil }

// --- internals ---

func (m *Matcher) appendLine(line, relativeDirectory string) {
	pat, isDir := parsePattern(line)
	full := buildFullPattern(pat, relativeDirectory)
	re := compilePattern(full, isDir)
	m.patterns = append(m.patterns, compiledPattern{regex: re, source: relativeDirectory})
}

func isValidLine(line string) bool {
	return line != "" && !strings.HasPrefix(line, commentPrefix)
}

func parsePattern(p string) (pat string, isDirectory bool) {
	switch {
	case strings.HasSuffix(p, recursivePattern):
		return p[:len(p)-len(recursivePattern)], true
	case strings.HasSuffix(p, directorySuffix):
		return p[:len(p)-len(directorySuffix)], true
	}
	return p, false
}

func buildFullPattern(pattern, relativeDirectory string) string {
	if rest, ok := strings.CutPrefix(pattern, "/"); ok {
		return rest
	}
	if relativeDirectory == "" && !strings.Contains(pattern, "/") {
		return "**/" + pattern
	}
	if relativeDirectory == "" {
		return pattern
	}
	return path.Join(relativeDirectory, pattern)
}

func compilePattern(pattern string, isDirectory bool) *regexp.Regexp {
	normalized := normalizePath(pattern)
	segments := strings.Split(normalized, pathSep)
	parts := make([]string, len(segments))
	for i, seg := range segments {
		parts[i] = segmentToRegex(seg)
	}

	matchAnywhere := len(parts) > 0 && parts[0] == "(?:.*/)?"

	var sb strings.Builder
	if matchAnywhere && len(parts) > 1 {
		sb.WriteString(`^(?:.*/)?(`)
		sb.WriteString(strings.Join(parts[1:], pathSep))
		sb.WriteString(`)`)
	} else {
		sb.WriteString(`^`)
		sb.WriteString(strings.Join(parts, pathSep))
	}
	if isDirectory {
		sb.WriteString(`(?:/.*)?$`)
	} else {
		sb.WriteString(`$`)
	}
	return regexp.MustCompile(sb.String())
}

func segmentToRegex(segment string) string {
	if segment == wildcardPattern {
		return "(?:.*/)?"
	}
	// Match the TS source: escape regex metachars (NOT * or ?), then
	// translate the glob metachars * and ? into their regex equivalents.
	escaped := regexpEscape(segment)
	escaped = strings.ReplaceAll(escaped, `*`, `[^/]*`)
	escaped = strings.ReplaceAll(escaped, `?`, `[^/]`)
	escaped = charClassRe.ReplaceAllString(escaped, "[$1]")
	return escaped
}

// regexpEscape escapes the same set the TS source escapes: .+^${}()|[]\
// (note: * and ? are intentionally NOT escaped here; they are handled as
// glob metachars in segmentToRegex above).
func regexpEscape(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for _, r := range s {
		switch r {
		case '.', '+', '^', '$', '{', '}', '(', ')', '|', '[', ']', '\\':
			sb.WriteByte('\\')
		}
		sb.WriteRune(r)
	}
	return sb.String()
}

// charClassRe rewrites escaped \[abc\] back into a real [abc] character class.
var charClassRe = regexp.MustCompile(`\\\[([^\]]+)\\\]`)

func normalizePath(s string) string {
	return strings.ReplaceAll(s, `\`, pathSep)
}

func (m *Matcher) relPath(p string) string {
	norm := normalizePath(p)
	if !isAbs(norm) {
		return norm
	}
	root := m.rootPath
	if !strings.HasSuffix(root, pathSep) {
		root += pathSep
	}
	if rest, ok := strings.CutPrefix(norm, root); ok {
		return rest
	}
	return norm
}

// isAbs returns true for both POSIX absolute paths and Windows drive-letter
// paths (after normalization to forward slashes).
func isAbs(p string) bool {
	if strings.HasPrefix(p, "/") {
		return true
	}
	if len(p) >= 3 && p[1] == ':' && (p[2] == '/' || p[2] == '\\') {
		return true
	}
	return false
}
