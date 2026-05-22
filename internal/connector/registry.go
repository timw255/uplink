package connector

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Factory builds a Connector from its raw YAML config block. The config is
// passed as map[string]any so each connector defines its own schema.
type Factory func(name string, config map[string]any) (Connector, error)

// Registration bundles a Manifest with the Factory that builds instances
// of that type. Each connector package registers exactly one of these.
type Registration struct {
	Manifest Manifest
	Factory  Factory
}

// Registry maps connector type IDs (e.g. "localfs", "aprimo") to their
// Registration. Built-in connectors are added at startup.
type Registry struct {
	mu  sync.RWMutex
	reg map[string]Registration
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{reg: make(map[string]Registration)}
}

// Register adds a connector type. The Manifest's ID is authoritative;
// it must be set and unique. Panics on duplicate registration or empty
// ID — both are bugs that should fail at startup, not at runtime.
func (r *Registry) Register(reg Registration) {
	if reg.Manifest.ID == "" {
		panic("connector: manifest.id is required")
	}
	if reg.Factory == nil {
		panic(fmt.Sprintf("connector: factory required for %q", reg.Manifest.ID))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.reg[reg.Manifest.ID]; exists {
		panic(fmt.Sprintf("connector: type %q already registered", reg.Manifest.ID))
	}
	r.reg[reg.Manifest.ID] = reg
}

// RegisterEmbedded parses an embedded source.json blob to extract the
// Manifest and pairs it with f. This is the common path for built-in
// connectors that ship a source.json alongside their Go source.
func (r *Registry) RegisterEmbedded(sourceJSON []byte, f Factory) {
	var m Manifest
	if err := json.Unmarshal(sourceJSON, &m); err != nil {
		panic(fmt.Sprintf("connector: parse embedded source.json: %v", err))
	}
	r.Register(Registration{Manifest: m, Factory: f})
}

// Build looks up the registration and instantiates a connector.
func (r *Registry) Build(typeID, instanceName string, config map[string]any) (Connector, error) {
	r.mu.RLock()
	reg, ok := r.reg[typeID]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("connector: unknown type %q", typeID)
	}
	return reg.Factory(instanceName, config)
}

// Manifest returns the manifest for a registered type.
func (r *Registry) Manifest(typeID string) (Manifest, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.reg[typeID]
	if !ok {
		return Manifest{}, false
	}
	return reg.Manifest, true
}

// Manifests returns every registered manifest. Order is unspecified.
// Useful for status endpoints and tooling that wants to enumerate the
// installed connector types.
func (r *Registry) Manifests() []Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Manifest, 0, len(r.reg))
	for _, reg := range r.reg {
		out = append(out, reg.Manifest)
	}
	return out
}

// Types returns the registered type IDs, useful for diagnostics.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.reg))
	for k := range r.reg {
		out = append(out, k)
	}
	return out
}
