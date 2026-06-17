package aprimo

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/timw255/uplink/internal/connector"
)

// azureBlobHostSuffixes are the Azure Blob storage domains across the
// public and sovereign clouds. A SAS URL must target one of these before
// we stream file bytes to it.
var azureBlobHostSuffixes = []string{
	".blob.core.windows.net",       // public cloud
	".blob.core.chinacloudapi.cn",  // Azure China
	".blob.core.usgovcloudapi.net", // Azure US Government
}

// isAzureBlobURL reports whether raw is an HTTPS URL pointing at an Azure
// Blob storage host. Used to refuse streaming customer bytes anywhere a
// tampered or misconfigured upload response might point — bytes only ever
// go to Azure Blob storage, never an arbitrary host.
func isAzureBlobURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, suffix := range azureBlobHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

// blobUploader streams a source's bytes straight to a pre-authorized
// storage URL (the SAS Aprimo hands back from CreateDirectUpload),
// bypassing the rate-limited Aprimo upload API entirely. Behind an
// interface so the connector's direct path is testable without standing
// up real blob storage.
type blobUploader interface {
	Upload(ctx context.Context, sasURL string, body connector.SegmentSource, filename string) error
}

// Direct-upload block tuning. The Azure SDK buffers up to
// directBlockSize × directConcurrency bytes per in-flight file while
// staging blocks, and the worker pool multiplies that across files — so
// these are deliberately modest. Peak direct-upload memory is roughly
// workers × directBlockSize × directConcurrency; bound it with the
// connector's max_concurrent if needed.
const (
	directBlockSize   = 4 << 20 // 4 MiB
	directConcurrency = 4

	// blobTryTimeout bounds a single block PUT. A stalled connection (a TCP
	// black hole with no reset) would otherwise hang an upload worker
	// indefinitely — and enough hung workers silently collapse throughput
	// to zero. With a per-try timeout the SDK cancels and retries instead,
	// so a network blip costs seconds, not a dead overnight run. Generous
	// enough that a 4 MiB block on a slow-but-real link never trips it.
	blobTryTimeout = 5 * time.Minute
)

// azureBlobUploader uploads to an Azure Blob SAS URL using the same SDK
// (and the same block-parallel protocol) AzCopy uses — no external binary.
type azureBlobUploader struct {
	blockSize   int64
	concurrency int
}

func newAzureBlobUploader() azureBlobUploader {
	return azureBlobUploader{blockSize: directBlockSize, concurrency: directConcurrency}
}

// Upload streams the whole source to the SAS URL as a block blob. The SAS
// itself is the credential, so the client needs none. UploadStream stages
// and commits blocks with internal parallelism + retries.
func (a azureBlobUploader) Upload(ctx context.Context, sasURL string, body connector.SegmentSource, filename string) error {
	opts := &blockblob.ClientOptions{}
	opts.Retry.TryTimeout = blobTryTimeout
	client, err := blockblob.NewClientWithNoCredential(sasURL, opts)
	if err != nil {
		return fmt.Errorf("blob client: %w", err)
	}
	rc, err := body.Open(ctx, 0, body.Size())
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() { _ = rc.Close() }()

	if _, err := client.UploadStream(ctx, rc, &blockblob.UploadStreamOptions{
		BlockSize:   a.blockSize,
		Concurrency: a.concurrency,
	}); err != nil {
		return fmt.Errorf("upload %q to blob: %w", filename, err)
	}
	return nil
}
