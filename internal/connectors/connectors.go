// Package connectors holds the registry glue for every built-in
// connector. cmd/uplink calls Register once at startup, then
// instantiates by manifest ID via the registry.
package connectors

import (
	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/connectors/aprimo"
	"github.com/timw255/uplink/internal/connectors/azblob"
	"github.com/timw255/uplink/internal/connectors/b2"
	"github.com/timw255/uplink/internal/connectors/localfs"
	"github.com/timw255/uplink/internal/connectors/s3"
)

// Register adds every built-in connector type to r. cmd/uplink calls
// this once at startup; the connector packages do not import each
// other.
func Register(r *connector.Registry) {
	r.RegisterEmbedded(localfs.SourceJSON(), localfs.Factory)
	r.RegisterEmbedded(aprimo.SourceJSON(), aprimo.Factory)
	r.RegisterEmbedded(s3.SourceJSON(), s3.Factory)
	r.RegisterEmbedded(azblob.SourceJSON(), azblob.Factory)
	r.RegisterEmbedded(b2.SourceJSON(), b2.Factory)
}
