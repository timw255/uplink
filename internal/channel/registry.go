package channel

import (
	"fmt"
	"sync"

	"github.com/timw255/uplink/internal/connector"
)

// Channel is the runtime form of a ChannelSpec: spec fields plus the
// compiled trigger filter, the resolved set of event kinds it fires
// on, and any compiled companion patterns + scripts. The kinds set is
// built once at registry construction so Match is a single map lookup
// per event.
type Channel struct {
	Spec       ChannelSpec
	Filter     Filter
	Companions []*Companion
	kinds      map[string]struct{}
}

// Match reports whether the given event should fire this channel.
// Empty kinds set (only reachable in tests that build Channel directly)
// matches any event kind so the existing test harness keeps working.
func (c *Channel) Match(e connector.Event) (bool, error) {
	if len(c.kinds) > 0 {
		if _, ok := c.kinds[string(e.Kind)]; !ok {
			return false, nil
		}
	}
	return c.Filter.Matches(e)
}

// Registry is the runtime catalog of channels keyed by source connector,
// so the event dispatcher can find which channels to fire for an event
// without scanning the whole list.
type Registry struct {
	mu       sync.RWMutex
	bySource map[string][]*Channel
	byName   map[string]*Channel
}

// NewRegistry compiles all channels in the config and indexes them by
// source connector for fast dispatch. compiler may be nil when no
// channel declares companions; passing nil with a channel that
// declares companions is a config error so a daemon can't silently
// no-op the pipeline.
func NewRegistry(channels []ChannelSpec, compiler ScriptCompiler) (*Registry, error) {
	r := &Registry{
		bySource: make(map[string][]*Channel),
		byName:   make(map[string]*Channel),
	}
	for _, spec := range channels {
		filter, err := CompileFilter(spec.Trigger.Filter)
		if err != nil {
			return nil, fmt.Errorf("channel %q: %w", spec.Name, err)
		}
		kinds, err := spec.Trigger.kinds()
		if err != nil {
			return nil, fmt.Errorf("channel %q: %w", spec.Name, err)
		}

		var companions []*Companion
		for _, co := range spec.Companions {
			if compiler == nil {
				return nil, fmt.Errorf("channel %q: companion %q declared but no script compiler was provided", spec.Name, co.Pattern)
			}
			pattern, err := CompilePattern(co.Pattern)
			if err != nil {
				// validate() already compiled this once; reaching here
				// means NewRegistry was called against a spec that
				// didn't pass Load. Surface the underlying error.
				return nil, fmt.Errorf("channel %q: companion %q: %w", spec.Name, co.Pattern, err)
			}
			script, err := compiler.Compile(co.Pattern, co.Script)
			if err != nil {
				return nil, fmt.Errorf("channel %q: companion %q: %w", spec.Name, co.Pattern, err)
			}
			companions = append(companions, &Companion{
				Pattern: pattern,
				Script:  script,
			})
		}

		ch := &Channel{Spec: spec, Filter: filter, Companions: companions, kinds: kinds}
		r.bySource[spec.Source] = append(r.bySource[spec.Source], ch)
		r.byName[spec.Name] = ch
	}
	return r, nil
}

// ChannelsForSource returns the channels that listen to a given source
// connector. Order matches config order.
func (r *Registry) ChannelsForSource(name string) []*Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Channel, len(r.bySource[name]))
	copy(out, r.bySource[name])
	return out
}

// Lookup returns the channel with the given name, or nil if none.
func (r *Registry) Lookup(name string) *Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byName[name]
}

// Names returns every registered channel name. Useful for diagnostics.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.byName))
	for k := range r.byName {
		out = append(out, k)
	}
	return out
}
