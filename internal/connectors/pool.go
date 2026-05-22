package connectors

import (
	"context"
	"fmt"
	"sync"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/store"
)

// Pool holds the running connector instances keyed by their config name.
// It implements engine.Connectors.
type Pool struct {
	mu       sync.RWMutex
	conns    map[string]connector.Connector
	sources  map[string]connector.EventSource
	registry *connector.Registry
}

// NewPool returns an empty pool wired to the given registry.
func NewPool(r *connector.Registry) *Pool {
	return &Pool{
		conns:    make(map[string]connector.Connector),
		sources:  make(map[string]connector.EventSource),
		registry: r,
	}
}

// Build constructs a connector by type+config, initializes it, and
// stores it in the pool. It also derives the appropriate EventSource
// for known-pollable types.
func (p *Pool) Build(ctx context.Context, name, typeName string, cfg map[string]any, st *store.Store) error {
	c, err := p.registry.Build(typeName, name, cfg)
	if err != nil {
		return err
	}
	if err := c.Init(ctx); err != nil {
		return fmt.Errorf("init connector %q: %w", name, err)
	}
	if sa, ok := c.(connector.StoreAware); ok && st != nil {
		sa.UseStore(st)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, dup := p.conns[name]; dup {
		return fmt.Errorf("connector %q already built", name)
	}
	p.conns[name] = c

	// Generic EventSource pickup. Connectors that emit events
	// implement either EventSourceFactory (the typical poll-based
	// case — takes a StateStore to diff against) or EventSource
	// directly. We try the factory first, then fall back to the
	// direct interface.
	if esf, ok := c.(connector.EventSourceFactory); ok {
		p.sources[name] = esf.NewEventSource(st)
	} else if src, ok := c.(connector.EventSource); ok {
		p.sources[name] = src
	}
	return nil
}

// Get returns a connector by name. Implements engine.Connectors.
func (p *Pool) Get(name string) (connector.Connector, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.conns[name]
	return c, ok
}

// Sources returns every registered EventSource, keyed by connector name.
func (p *Pool) Sources() map[string]connector.EventSource {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]connector.EventSource, len(p.sources))
	for k, v := range p.sources {
		out[k] = v
	}
	return out
}

// Close shuts down every connector in the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for name, c := range p.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", name, err)
		}
	}
	return firstErr
}
