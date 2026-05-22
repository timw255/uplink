package s3

import _ "embed"

//go:embed source.json
var sourceJSON []byte

// SourceJSON returns the raw bytes of the s3 source.json manifest.
func SourceJSON() []byte { return sourceJSON }
