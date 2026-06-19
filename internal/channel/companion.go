package channel

import (
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// CompiledScript is the minimal interface a compiled companion script
// exposes to the channel package. Implementations live in the extract
// package; this interface keeps the channel package free of an extract
// import so config loading and registry construction don't pull in the
// Lua runtime.
type CompiledScript interface {
	Name() string
}

// ScriptCompiler is the minimal compiler interface required to turn a
// companion's source file into a CompiledScript. Provided by the
// extract package's Runtime; injected into NewRegistry by the engine.
type ScriptCompiler interface {
	Compile(name, path string) (CompiledScript, error)
}

// CompanionSpec describes a companion-file declaration attached to a
// channel. Companion files are files in the source that carry metadata
// for a primary asset (XMP sidecars, JSON descriptors, per-language
// caption files, etc). They do NOT become their own Aprimo records;
// instead a companion-file event re-runs the declared script against
// the parent asset's existing record.
//
//	companions:
//	  - pattern: "${basename}.xmp"
//	    script: scripts/xmp.lua
//	  - pattern: "${basename}.caption.${lang}.txt"
//	    script: scripts/captions.lua
//
// See Pattern for the grammar accepted in `pattern`. Script paths are
// resolved relative to the daemon binary at Load time.
type CompanionSpec struct {
	Pattern string `yaml:"pattern"`
	Script  string `yaml:"script"`
}

// Companion is the runtime form of a CompanionSpec — compiled pattern
// plus compiled script, ready to match and run.
type Companion struct {
	Pattern *Pattern
	Script  CompiledScript
}

// CompanionMatch is what Channel.MatchCompanion / Registry.MatchCompanion
// return when an event path looks like a declared companion. The
// dispatcher uses this to route the event to a companion job: the
// Match's Basename + Dir locate the parent asset in sync_log; the
// Companion's Script is the one that runs against that asset.
type CompanionMatch struct {
	Channel   *Channel
	Companion *Companion
	Match     *Match
}

// MatchCompanion runs each declared companion pattern on this channel
// against fullPath and returns the first match. Returns nil when none
// matches. Patterns within a channel cannot overlap (validate() rejects
// duplicate raw patterns at load time) so first-match is deterministic.
func (c *Channel) MatchCompanion(fullPath string) *CompanionMatch {
	for _, co := range c.Companions {
		if m := co.Pattern.Match(fullPath); m != nil {
			return &CompanionMatch{Channel: c, Companion: co, Match: m}
		}
	}
	return nil
}

// MatchCompanions checks fullPath against every companion pattern from
// every channel listening to sourceConnector. Returns all matches in
// ChannelsForSource order (which is config-file order). An empty slice
// means no channel claims this path as a companion.
//
// All-matches semantics mirror how asset events fan out across
// channels: if two channels both declare ${basename}.xmp as a
// companion for their respective JPG asset declarations, the same
// companion file feeds metadata to both channels' records.
func (r *Registry) MatchCompanions(sourceConnector, fullPath string) []*CompanionMatch {
	var out []*CompanionMatch
	for _, ch := range r.ChannelsForSource(sourceConnector) {
		if m := ch.MatchCompanion(fullPath); m != nil {
			out = append(out, m)
		}
	}
	return out
}

// Pattern is a compiled companion-path pattern. It is built once at
// config load and used for three things:
//
//   - Scan-time classification: does this entry look like a companion?
//     Match() answers; if it does, the asset's basename + extension are
//     recoverable from the result.
//   - Dispatch-time inverse lookup: given a companion path, recover the
//     parent asset's basename and directory so we can hit sync_log.
//   - Job-time match info: when a companion job runs, the inverse is
//     re-applied to populate the `input.match` table the script reads.
//
// The pattern grammar accepts these tokens:
//
//   - ${basename}    — the asset path with its final extension stripped.
//     Matches one path segment; may contain internal dots.
//     Required: every companion pattern must include
//     ${basename} so the inverse can recover the asset.
//   - ${extension}   — the asset's final extension, no dot. One segment,
//     no internal dots.
//   - ${name}        — a user-defined named capture (letters/digits/_).
//     One segment, no internal dots. Reserved names
//     `basename` and `extension` cannot be reused here.
//   - *              — anonymous wildcard. One segment, no internal dots.
//     Captured positionally; available to the script as
//     input.match.wildcards.
//
// Everything else is a literal, including `.`. Pattern matches operate on
// the path's basename (final segment); the directory is preserved separately.
type Pattern struct {
	// raw is the original pattern text, kept for diagnostics and for the
	// script's input.match.pattern field.
	raw string

	// inverse is the compiled regex applied to a companion path's
	// basename to recover named captures and wildcards.
	inverse *regexp.Regexp

	// wildcardIndices lists the regex capture-group indices (1-based)
	// that correspond to `*` tokens, in pattern order. SubexpNames() can
	// be used for ${name} groups but does not give an ordering for
	// anonymous groups, so we track them here.
	wildcardIndices []int

	// varNames lists the user-defined named capture names in pattern
	// order. Excludes the reserved `basename` and `extension`.
	varNames []string
}

// Match is the result of applying a Pattern to a companion path.
// All path-style fields use forward slashes regardless of host OS — the
// connector layer normalizes input paths before they reach us.
type Match struct {
	Path      string            // full companion path, as scanned
	Dir       string            // directory portion of Path
	Basename  string            // asset basename recovered from ${basename}
	Extension string            // asset extension recovered from ${extension}, "" if pattern omitted it
	Vars      map[string]string // user-defined named captures (${name})
	Wildcards []string          // positional `*` captures, in pattern order
	Pattern   string            // the original pattern text
}

// CompilePattern parses and validates a pattern string, returning a
// Pattern ready for matching. Errors surface at config-load time so a
// malformed YAML never reaches the runtime.
func CompilePattern(raw string) (*Pattern, error) {
	if raw == "" {
		return nil, fmt.Errorf("companion pattern is empty")
	}
	if strings.ContainsRune(raw, '/') {
		// Companion patterns are filename-only — same directory as the
		// asset. A `/` would imply matching across directories, which we
		// don't support and never want (it'd complicate the inverse
		// lookup, the completion sweep, and the script's mental model).
		return nil, fmt.Errorf("companion pattern %q must not contain '/'", raw)
	}

	segs, err := parsePatternSegments(raw)
	if err != nil {
		return nil, fmt.Errorf("companion pattern %q: %w", raw, err)
	}

	var (
		regexBuilder    strings.Builder
		groupIdx        int
		seenBasename    bool
		seenExtension   bool
		seenNames       = make(map[string]struct{})
		varNames        []string
		wildcardIndices []int
	)
	regexBuilder.WriteString("^")
	for _, s := range segs {
		switch s.kind {
		case segLiteral:
			regexBuilder.WriteString(regexp.QuoteMeta(s.value))
		case segBasename:
			if seenBasename {
				return nil, fmt.Errorf("companion pattern %q: ${basename} appears more than once", raw)
			}
			seenBasename = true
			groupIdx++
			regexBuilder.WriteString(`(?P<basename>[^/]+)`)
		case segExtension:
			if seenExtension {
				return nil, fmt.Errorf("companion pattern %q: ${extension} appears more than once", raw)
			}
			seenExtension = true
			groupIdx++
			regexBuilder.WriteString(`(?P<extension>[^/.]+)`)
		case segNamed:
			if s.value == "basename" || s.value == "extension" {
				return nil, fmt.Errorf("companion pattern %q: ${%s} is a reserved name; use a different identifier", raw, s.value)
			}
			if _, dup := seenNames[s.value]; dup {
				return nil, fmt.Errorf("companion pattern %q: named capture ${%s} appears more than once", raw, s.value)
			}
			seenNames[s.value] = struct{}{}
			varNames = append(varNames, s.value)
			groupIdx++
			regexBuilder.WriteString(`(?P<` + s.value + `>[^/.]+)`)
		case segWildcard:
			groupIdx++
			wildcardIndices = append(wildcardIndices, groupIdx)
			regexBuilder.WriteString(`([^/.]+)`)
		}
	}
	regexBuilder.WriteString("$")

	if !seenBasename {
		return nil, fmt.Errorf("companion pattern %q: ${basename} is required so the parent asset can be recovered", raw)
	}

	re, err := regexp.Compile(regexBuilder.String())
	if err != nil {
		// Shouldn't happen — we control every byte in the expression —
		// but surface the internal error cleanly if it ever does.
		return nil, fmt.Errorf("companion pattern %q: internal regex compile: %w", raw, err)
	}

	return &Pattern{
		raw:             raw,
		inverse:         re,
		wildcardIndices: wildcardIndices,
		varNames:        varNames,
	}, nil
}

// Raw returns the original pattern text passed to CompilePattern.
func (p *Pattern) Raw() string { return p.raw }

// HasExtensionVar reports whether the pattern uses ${extension}. Callers
// in the scan/dispatch path can use this to know whether a Match will
// have Extension populated.
func (p *Pattern) HasExtensionVar() bool {
	return slices.Contains(p.inverse.SubexpNames(), "extension")
}

// Match applies the pattern's inverse regex to the basename of fullPath.
// Returns nil when the basename doesn't match.
//
// On a match, the returned *Match has Dir set to the directory portion
// of fullPath (no trailing slash; empty string for top-level files) and
// Path set to the full input. Basename, Extension, Vars, and Wildcards
// are populated from the captured groups.
func (p *Pattern) Match(fullPath string) *Match {
	dir, file := splitPath(fullPath)
	subs := p.inverse.FindStringSubmatch(file)
	if subs == nil {
		return nil
	}

	m := &Match{
		Path:    fullPath,
		Dir:     dir,
		Vars:    make(map[string]string, len(p.varNames)),
		Pattern: p.raw,
	}

	for i, name := range p.inverse.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}
		switch name {
		case "basename":
			m.Basename = subs[i]
		case "extension":
			m.Extension = subs[i]
		default:
			m.Vars[name] = subs[i]
		}
	}

	if len(p.wildcardIndices) > 0 {
		m.Wildcards = make([]string, 0, len(p.wildcardIndices))
		for _, idx := range p.wildcardIndices {
			m.Wildcards = append(m.Wildcards, subs[idx])
		}
	}

	return m
}

// segmentKind identifies the role of one parsed token in a pattern.
type segmentKind int

const (
	segLiteral segmentKind = iota
	segBasename
	segExtension
	segNamed
	segWildcard
)

// segment is one parsed token from a pattern. For segLiteral the
// `value` field holds the literal characters; for segNamed it holds the
// capture name; for the other kinds it is unused.
type segment struct {
	kind  segmentKind
	value string
}

// parsePatternSegments lexes a pattern into segments. Errors here are
// purely syntactic (unterminated ${, empty ${}, invalid identifier).
func parsePatternSegments(raw string) ([]segment, error) {
	var (
		out     []segment
		literal strings.Builder
	)
	flush := func() {
		if literal.Len() > 0 {
			out = append(out, segment{kind: segLiteral, value: literal.String()})
			literal.Reset()
		}
	}
	i := 0
	for i < len(raw) {
		c := raw[i]
		switch {
		case c == '$' && i+1 < len(raw) && raw[i+1] == '{':
			end := strings.IndexByte(raw[i+2:], '}')
			if end < 0 {
				return nil, fmt.Errorf("unterminated ${...} starting at offset %d", i)
			}
			name := raw[i+2 : i+2+end]
			if name == "" {
				return nil, fmt.Errorf("empty ${} at offset %d", i)
			}
			flush()
			switch name {
			case "basename":
				out = append(out, segment{kind: segBasename})
			case "extension":
				out = append(out, segment{kind: segExtension})
			default:
				if !isValidIdent(name) {
					return nil, fmt.Errorf("invalid var name %q at offset %d (must be a letter/underscore followed by letters, digits, or underscores)", name, i)
				}
				out = append(out, segment{kind: segNamed, value: name})
			}
			i += 2 + end + 1
		case c == '$' && i+1 < len(raw) && raw[i+1] != '{':
			// Bare `$` is a literal; consume one byte.
			literal.WriteByte(c)
			i++
		case c == '*':
			flush()
			out = append(out, segment{kind: segWildcard})
			i++
		default:
			literal.WriteByte(c)
			i++
		}
	}
	flush()
	return out, nil
}

// isValidIdent reports whether s is a valid user-defined capture name.
// First rune must be a letter or underscore; subsequent runes letters,
// digits, or underscores. Matches the conservative subset of Go-style
// identifiers — keeps named captures embeddable in the regex grammar
// without escaping concerns.
func isValidIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !(unicode.IsLetter(r) || r == '_') {
				return false
			}
			continue
		}
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return true
}

// splitPath divides a forward-slash path into (dir, file). For a path
// with no slash the dir is empty and file is the whole input.
//
// We use `path` (not `path/filepath`) because source connectors
// uniformly emit forward-slash paths regardless of host OS — same
// reason the rest of internal/channel and internal/connector use it.
func splitPath(p string) (dir, file string) {
	dir, file = path.Split(p)
	return strings.TrimSuffix(dir, "/"), file
}
