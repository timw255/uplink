package localfs

import _ "embed"

//go:embed source.json
var sourceJSON []byte

// SourceJSON returns the raw bytes of the localfs source.json manifest.
// Used by the connector registry to derive the connector's identity.
func SourceJSON() []byte { return sourceJSON }
