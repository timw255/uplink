package channel

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"github.com/timw255/uplink/internal/connector"
)

// Filter is a compiled CEL expression that evaluates against an Event.
// A zero Filter is equivalent to "always match".
type Filter struct {
	src     string
	program cel.Program
}

// CompileFilter parses and type-checks a CEL expression for the channel's
// trigger.filter, returning a runnable Filter. An empty expression yields
// a zero Filter that always matches.
func CompileFilter(expr string) (Filter, error) {
	if expr == "" {
		return Filter{}, nil
	}

	env, err := cel.NewEnv(
		cel.Variable("kind", cel.StringType),
		cel.Variable("path", cel.StringType),
		cel.Variable("size", cel.IntType),
		cel.Variable("connector", cel.StringType),
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		return Filter{}, fmt.Errorf("channel: build CEL env: %w", err)
	}

	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return Filter{}, fmt.Errorf("channel: compile filter %q: %w", expr, iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		return Filter{}, fmt.Errorf("channel: build filter program: %w", err)
	}
	return Filter{src: expr, program: prg}, nil
}

// Matches returns true if the event satisfies the filter. A zero Filter
// always returns true.
func (f Filter) Matches(e connector.Event) (bool, error) {
	if f.program == nil {
		return true, nil
	}
	out, _, err := f.program.Eval(map[string]any{
		"kind":      string(e.Kind),
		"path":      e.Entry.Path,
		"size":      e.Entry.Size,
		"connector": e.Connector,
		"metadata":  nonNilMap(e.Entry.Metadata),
	})
	if err != nil {
		return false, fmt.Errorf("channel: eval filter %q: %w", f.src, err)
	}
	return boolValue(out), nil
}

// String returns the original CEL source for diagnostics.
func (f Filter) String() string { return f.src }

func nonNilMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func boolValue(v ref.Val) bool {
	b, ok := v.(types.Bool)
	if !ok {
		return false
	}
	return bool(b)
}
