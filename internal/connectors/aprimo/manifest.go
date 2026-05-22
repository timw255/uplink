package aprimo

import _ "embed"

//go:embed source.json
var sourceJSON []byte

// SourceJSON returns the raw bytes of the aprimo source.json manifest.
func SourceJSON() []byte { return sourceJSON }
