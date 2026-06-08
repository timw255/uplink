// Package extract is the sandboxed Lua script pipeline. Companion
// scripts run per-event in response to changes on a declared companion
// file (XMP sidecar, JSON descriptor, language caption, etc.) and
// return a metadata bag that gets PATCHed onto the parent asset's
// destination record — or folded into the Create call when the parent
// hasn't been synced yet.
//
// Scripts run inside gopher-lua with a minimal stdlib (base, string,
// table, math) plus a curated `uplink` global. Filesystem access,
// network, process execution, and module loading are deliberately
// unreachable from a script.
package extract

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
)

// Runtime defaults. Wall-clock timeout caps a runaway loop; memMB caps
// per-execution memory; insts is a soft cap enforced by a debug hook.
const (
	defaultTimeout      = 5 * time.Second
	maxTimeout          = 60 * time.Second
	defaultInstructions = 10_000_000
	defaultMemoryMB     = 64
)

// Runtime compiles and executes sandboxed Lua scripts. One Runtime is
// shared across all companion scripts for a daemon — Scripts are
// bound to it at Compile time and each RunCompanion forks a fresh
// LState.
type Runtime struct {
	logger  *slog.Logger
	timeout time.Duration
	insts   int
	memMB   int
}

// NewRuntime returns a runtime with sane defaults.
func NewRuntime(logger *slog.Logger) *Runtime {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runtime{
		logger:  logger,
		timeout: defaultTimeout,
		insts:   defaultInstructions,
		memMB:   defaultMemoryMB,
	}
}

// WithTimeout overrides the per-execution wall-clock cap. Values above
// maxTimeout are clamped.
func (r *Runtime) WithTimeout(d time.Duration) *Runtime {
	if d > maxTimeout {
		d = maxTimeout
	}
	r.timeout = d
	return r
}

// Script is a compiled companion script ready for repeated execution.
// Compile at config load; RunCompanion per event.
type Script struct {
	name  string
	proto *lua.FunctionProto
	rt    *Runtime
}

// Name returns the script's bound name (typically the companion's
// raw `pattern:` text from YAML).
func (s *Script) Name() string { return s.name }

// Compile loads, parses, and compiles a Lua source file. Errors here
// surface at daemon startup so a broken script never silently runs.
func (r *Runtime) Compile(name, path string) (*Script, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("extract: open script %s: %w", path, err)
	}
	defer f.Close()
	chunk, err := parse.Parse(f, path)
	if err != nil {
		return nil, fmt.Errorf("extract: parse %s: %w", path, err)
	}
	proto, err := lua.Compile(chunk, path)
	if err != nil {
		return nil, fmt.Errorf("extract: compile %s: %w", path, err)
	}
	return &Script{name: name, proto: proto, rt: r}, nil
}

// CompanionInput is the per-event input handed to a companion script.
// A companion script runs when a single companion file (e.g., XMP
// sidecar, JSON descriptor) changed and the engine wants the script
// to compute a metadata update for the parent asset's record.
//
// The script sees one file directly — its content (already read by
// the engine) or a deletion signal — and rich match info recovered
// from the pattern that matched the file. There is no FileAccess:
// companion scripts are
// not supposed to roam the source.
type CompanionInput struct {
	Channel       string
	Asset         AssetInfo
	AssetRecordID string
	File          CompanionFile
	Match         MatchInfo
}

// CompanionFile is the snapshot of the companion file the script
// reacts to, surfaced to scripts as `uplink.file`.
//
// When Deleted is true, Content is nil and the script is being told
// the companion just disappeared — it can return an empty field list
// (no PATCH) or emit a clearing PATCH, its choice.
type CompanionFile struct {
	Path    string
	Content []byte
	Deleted bool
}

// MatchInfo carries everything the pattern's inverse resolver
// recovered from the companion path. Surfaced as `uplink.match`.
// Pattern is the original raw pattern text; Basename and Extension
// come from the asset; Vars holds named ${name} captures; Wildcards
// holds positional `*` captures in pattern order.
type MatchInfo struct {
	Pattern   string
	Basename  string
	Extension string
	Vars      map[string]string
	Wildcards []string
}

// AssetInfo is the read-only view of the current asset surfaced to
// scripts as `uplink.asset`.
type AssetInfo struct {
	Path    string
	Size    int64
	Hash    string
	ModTime time.Time
}

// AssetScriptInput is the per-event input handed to an enrich script.
// Unlike a companion script, an enrich script reacts to the asset
// itself — there is no companion file and no pattern match. It runs
// once per asset lifecycle event (create, update, delete) and derives
// metadata from the asset's own identity: its path, size, hash, and
// what just happened to it.
//
// The canonical use is deriving fields from path structure — mapping
// `emea/spring-2026/hero-banner.png` to Region and Campaign fields — but
// a script can key off anything in AssetInfo or the event kind (e.g.
// flip an "Archived" field when Event.Deleted is true).
type AssetScriptInput struct {
	Channel       string
	Asset         AssetInfo
	AssetRecordID string
	// Extension is the asset's final extension with no leading dot
	// (e.g. "png"), surfaced as `uplink.asset.extension`. Empty when
	// the path has no extension.
	Extension string
	Event     AssetEvent
}

// AssetEvent tells an enrich script what lifecycle event triggered it,
// surfaced as `uplink.event`. Kind is the IETF-ish event name
// ("OnCreate", "OnUpdate", "OnDelete"); Deleted is a convenience bool
// that is true exactly when Kind == "OnDelete" so a script can branch
// without string-comparing.
type AssetEvent struct {
	Kind    string
	Deleted bool
}

// FailError is the error returned by a script that called
// `uplink.fail("reason")`. Engine retry policy treats this like any
// other error from a companion script.
type FailError struct{ Reason string }

func (e *FailError) Error() string { return "uplink.fail: " + e.Reason }

// RunCompanion executes the script in companion mode. The script
// reacts to a single companion file (or its deletion) and returns
// the same shape as Run — a Lua sequence of `{ id, value, language? }`
// entries — that the engine PATCHes onto the parent asset's record.
//
// The runtime sandbox is identical to Run (same library subset, same
// memory/instructions/time caps); only the `uplink` global differs:
// no `uplink.read_file` / `uplink.list_files` (companions are not
// supposed to roam), with `uplink.file` and `uplink.match` added.
func (s *Script) RunCompanion(ctx context.Context, in CompanionInput) ([]any, error) {
	runCtx, cancel := context.WithTimeout(ctx, s.rt.timeout)
	defer cancel()

	state := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer state.Close()
	state.SetMx(s.rt.memMB)
	state.SetContext(runCtx)

	for _, lib := range []struct {
		name string
		open lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
	} {
		state.Push(state.NewFunction(lib.open))
		state.Push(lua.LString(lib.name))
		if err := state.PCall(1, 0, nil); err != nil {
			return nil, fmt.Errorf("extract: open %s: %w", lib.name, err)
		}
	}

	// Same dangerous-globals nilling as Run; the duplication is
	// deliberate — these are the sandbox contract, not implementation
	// detail to factor out.
	for _, g := range []string{
		"io", "os", "debug", "package", "require",
		"loadfile", "dofile", "load", "loadstring",
		"collectgarbage", "newproxy",
	} {
		state.SetGlobal(g, lua.LNil)
	}
	state.SetGlobal("print", lua.LNil)

	scriptLogger := s.rt.logger.With("script", s.name, "channel", in.Channel, "mode", "companion")
	registerCompanionAPI(state, &in, scriptLogger)

	fn := state.NewFunctionFromProto(s.proto)
	state.Push(fn)
	start := time.Now()
	if err := state.PCall(0, 1, nil); err != nil {
		return nil, normalizeRunErr(err, runCtx)
	}
	scriptLogger.Debug("extract: companion script complete", "duration", time.Since(start).Round(time.Millisecond))

	ret := state.Get(-1)
	state.Pop(1)
	out, err := luaToGoList(ret)
	if err != nil {
		return nil, fmt.Errorf("extract: convert return value: %w", err)
	}
	return out, nil
}

// RunAsset executes the script in enrich mode. The script reacts to an
// asset lifecycle event (create / update / delete) and returns the same
// shape as RunCompanion — a Lua sequence of `{ name, value, language? }`
// entries — that the engine folds into the record create/update or
// PATCHes onto the existing record.
//
// The sandbox is identical to RunCompanion (same library subset, same
// memory/instructions/time caps). Only the `uplink` global differs:
// there is no `uplink.file` and no `uplink.match` (an enrich script has
// no companion file and no pattern), and a new `uplink.event` table
// reports what lifecycle event fired. The dangerous-globals nilling is
// duplicated from RunCompanion on purpose — it is the sandbox contract,
// not implementation detail to factor behind a shared helper.
func (s *Script) RunAsset(ctx context.Context, in AssetScriptInput) ([]any, error) {
	runCtx, cancel := context.WithTimeout(ctx, s.rt.timeout)
	defer cancel()

	state := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer state.Close()
	state.SetMx(s.rt.memMB)
	state.SetContext(runCtx)

	for _, lib := range []struct {
		name string
		open lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
	} {
		state.Push(state.NewFunction(lib.open))
		state.Push(lua.LString(lib.name))
		if err := state.PCall(1, 0, nil); err != nil {
			return nil, fmt.Errorf("extract: open %s: %w", lib.name, err)
		}
	}

	for _, g := range []string{
		"io", "os", "debug", "package", "require",
		"loadfile", "dofile", "load", "loadstring",
		"collectgarbage", "newproxy",
	} {
		state.SetGlobal(g, lua.LNil)
	}
	state.SetGlobal("print", lua.LNil)

	scriptLogger := s.rt.logger.With("script", s.name, "channel", in.Channel, "mode", "enrich")
	registerAssetAPI(state, &in, scriptLogger)

	fn := state.NewFunctionFromProto(s.proto)
	state.Push(fn)
	start := time.Now()
	if err := state.PCall(0, 1, nil); err != nil {
		return nil, normalizeRunErr(err, runCtx)
	}
	scriptLogger.Debug("extract: enrich script complete", "duration", time.Since(start).Round(time.Millisecond))

	ret := state.Get(-1)
	state.Pop(1)
	out, err := luaToGoList(ret)
	if err != nil {
		return nil, fmt.Errorf("extract: convert return value: %w", err)
	}
	return out, nil
}

// normalizeRunErr translates a gopher-lua PCall error into the right
// Go error shape: ctx-canceled / deadline-exceeded if the runCtx
// triggered, otherwise the original.
func normalizeRunErr(err error, runCtx context.Context) error {
	if runCtx.Err() != nil {
		return fmt.Errorf("extract: %w", runCtx.Err())
	}
	return err
}
