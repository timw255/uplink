package extract

import (
	"fmt"
	"log/slog"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// registerCompanionAPI installs the `uplink` table for companion-mode
// runs. The companion script reacts to one specific file the engine
// has already read (or marked deleted), so the API surface is
// deliberately narrow:
//
//   - uplink.asset  — the parent asset (path, record_id, extension)
//   - uplink.file   — the companion file (path, content, deleted)
//   - uplink.match  — pattern + recovered captures (basename,
//                     extension, vars, wildcards)
//   - uplink.log    — structured logging
//   - uplink.fail   — explicit script-failure path
//   - uplink.parse_json / parse_xml / parse_csv — pure parsers
//
// Notably absent: read_file and list_files. A companion script that
// needs to consult other files is signalling that the declaration
// model is wrong — make it a separate companion entry instead. The
// script gets exactly the input it was triggered on.
func registerCompanionAPI(state *lua.LState, in *CompanionInput, logger *slog.Logger) {
	tbl := state.NewTable()

	asset := state.NewTable()
	asset.RawSetString("path", lua.LString(in.Asset.Path))
	asset.RawSetString("size", lua.LNumber(in.Asset.Size))
	asset.RawSetString("hash", lua.LString(in.Asset.Hash))
	asset.RawSetString("record_id", lua.LString(in.AssetRecordID))
	asset.RawSetString("extension", lua.LString(in.Match.Extension))
	tbl.RawSetString("asset", asset)

	file := state.NewTable()
	file.RawSetString("path", lua.LString(in.File.Path))
	if in.File.Deleted {
		file.RawSetString("deleted", lua.LTrue)
		file.RawSetString("content", lua.LNil)
	} else {
		file.RawSetString("deleted", lua.LFalse)
		file.RawSetString("content", lua.LString(in.File.Content))
	}
	tbl.RawSetString("file", file)

	match := state.NewTable()
	match.RawSetString("pattern", lua.LString(in.Match.Pattern))
	match.RawSetString("basename", lua.LString(in.Match.Basename))
	match.RawSetString("extension", lua.LString(in.Match.Extension))
	vars := state.NewTable()
	for k, v := range in.Match.Vars {
		vars.RawSetString(k, lua.LString(v))
	}
	match.RawSetString("vars", vars)
	wilds := state.NewTable()
	for i, w := range in.Match.Wildcards {
		wilds.RawSetInt(i+1, lua.LString(w))
	}
	match.RawSetString("wildcards", wilds)
	tbl.RawSetString("match", match)

	tbl.RawSetString("parse_json", state.NewFunction(func(L *lua.LState) int {
		s := L.CheckString(1)
		v, err := parseJSON(L, s)
		if err != nil {
			L.RaiseError("uplink.parse_json: %v", err)
			return 0
		}
		L.Push(v)
		return 1
	}))
	tbl.RawSetString("parse_xml", state.NewFunction(func(L *lua.LState) int {
		s := L.CheckString(1)
		v, err := parseXML(L, s)
		if err != nil {
			L.RaiseError("uplink.parse_xml: %v", err)
			return 0
		}
		L.Push(v)
		return 1
	}))
	tbl.RawSetString("parse_csv", state.NewFunction(func(L *lua.LState) int {
		s := L.CheckString(1)
		var opts *lua.LTable
		if L.GetTop() >= 2 {
			opts = L.CheckTable(2)
		}
		v, err := parseCSV(L, s, opts)
		if err != nil {
			L.RaiseError("uplink.parse_csv: %v", err)
			return 0
		}
		L.Push(v)
		return 1
	}))

	tbl.RawSetString("log", state.NewFunction(func(L *lua.LState) int {
		level := strings.ToLower(L.CheckString(1))
		msg := L.CheckString(2)
		var extras []any
		if L.GetTop() >= 3 {
			if t, ok := L.Get(3).(*lua.LTable); ok {
				t.ForEach(func(k, v lua.LValue) {
					extras = append(extras, k.String(), luaValueToGo(v))
				})
			}
		}
		switch level {
		case "debug":
			logger.Debug(msg, extras...)
		case "warn", "warning":
			logger.Warn(msg, extras...)
		case "error":
			logger.Error(msg, extras...)
		default:
			logger.Info(msg, extras...)
		}
		return 0
	}))

	tbl.RawSetString("fail", state.NewFunction(func(L *lua.LState) int {
		reason := L.CheckString(1)
		L.RaiseError("uplink.fail: %s", reason)
		return 0
	}))

	state.SetGlobal("uplink", tbl)
}

// luaToGoList converts a Lua return value into a Go []any. The
// expected shape is a sequence of `{ name = "...", value = ... }`
// entries; this function only validates that the top level is a
// sequence (or nil/missing). Non-table return values (or `nil`)
// yield an empty list — fail-soft is the recommended pattern for
// scripts that don't have anything to add.
func luaToGoList(v lua.LValue) ([]any, error) {
	if v == nil || v.Type() == lua.LTNil {
		return nil, nil
	}
	t, ok := v.(*lua.LTable)
	if !ok {
		return nil, fmt.Errorf("script returned %s, want a sequence of field entries", v.Type())
	}
	go_v := luaTableToGo(t)
	if list, ok := go_v.([]any); ok {
		return list, nil
	}
	// Empty Lua table is ambiguous (sequence vs map). Treat it as
	// an empty list — that's the no-op case.
	if m, ok := go_v.(map[string]any); ok && len(m) == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("script return is not a sequence; got a keyed table — companion scripts must return a list like { { name = \"...\", value = ... }, ... }")
}

// luaTableToGo recursively converts a Lua table into Go. A pure-integer
// table with contiguous 1..N keys becomes a []any; everything else
// becomes a map[string]any (string-keying everything for simplicity).
func luaTableToGo(t *lua.LTable) any {
	hasStringKey := false
	maxInt := 0
	count := 0
	t.ForEach(func(k, _ lua.LValue) {
		count++
		switch tk := k.(type) {
		case lua.LNumber:
			i := int(tk)
			if float64(i) == float64(tk) && i > maxInt {
				maxInt = i
			}
		case lua.LString:
			hasStringKey = true
		}
	})
	if !hasStringKey && count > 0 && maxInt == count {
		out := make([]any, count)
		t.ForEach(func(k, v lua.LValue) {
			if n, ok := k.(lua.LNumber); ok {
				idx := int(n) - 1
				if idx >= 0 && idx < count {
					out[idx] = luaValueToGo(v)
				}
			}
		})
		return out
	}
	out := make(map[string]any, count)
	t.ForEach(func(k, v lua.LValue) {
		out[k.String()] = luaValueToGo(v)
	})
	return out
}

// luaValueToGo converts a single Lua value to its Go equivalent.
func luaValueToGo(v lua.LValue) any {
	switch x := v.(type) {
	case lua.LBool:
		return bool(x)
	case lua.LNumber:
		i := int64(x)
		if lua.LNumber(i) == x {
			return i
		}
		return float64(x)
	case lua.LString:
		return string(x)
	case *lua.LTable:
		return luaTableToGo(x)
	case *lua.LNilType, nil:
		return nil
	default:
		return x.String()
	}
}
