package connector

import "errors"

// ErrUnsupported is returned by connectors for operations they do not
// implement (e.g. Move on a connector with no native rename).
var ErrUnsupported = errors.New("connector: operation not supported")

// ErrNotFound is returned for missing entries. Connectors should wrap
// vendor-specific 404s in this sentinel so callers can branch portably.
var ErrNotFound = errors.New("connector: entry not found")
