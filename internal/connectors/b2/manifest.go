package b2

import _ "embed"

//go:embed source.json
var sourceJSON []byte

// SourceJSON returns the raw bytes of the b2 source.json manifest.
func SourceJSON() []byte { return sourceJSON }
