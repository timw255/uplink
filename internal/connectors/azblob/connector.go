// Package azblob is the Azure Blob Storage source connector. It is
// read-only: List/Stat/Read/OpenRange against the container+prefix
// scope, and a poll-based EventSource that diffs the current listing
// against last-known state to emit OnCreate/OnUpdate/OnDelete events.
//
// Authentication picks one of three modes based on which env-var is
// set in the config (precedence: connection_string_env > sas_token_env
// > account_key_env). If none are set, the connector currently fails
// to Init — ambient Azure AD credential support is a v2 addition.
package azblob

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	azblobsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/ignore"
)

// Connector is one configured Azure Blob Storage instance.
type Connector struct {
	name          string
	cfg           *Config
	client        *azblobsdk.Client
	ignoreMatcher *ignore.Matcher
}

// Factory builds an azblob connector from its raw YAML config block.
func Factory(name string, raw map[string]any) (connector.Connector, error) {
	cfg, err := loadConfig(name, raw)
	if err != nil {
		return nil, err
	}
	return &Connector{name: name, cfg: cfg}, nil
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return c.name }

// Init builds the SDK client. Credentials resolution order:
//
//  1. Connection string (cfg.ConnectionString) — most self-contained.
//  2. SAS token (cfg.SASToken) — appended to the service URL.
//  3. Shared key (cfg.AccountKey) — paired with cfg.Account.
//
// Each field is populated either inline in YAML or via the matching
// *Env env-var indirection at load time. cfg.ServiceURL, when set,
// overrides the default https://<account>.blob.core.windows.net/
// service URL — use for Azurite or sovereign clouds.
func (c *Connector) Init(ctx context.Context) error {
	switch {
	case c.cfg.ConnectionString != "":
		client, err := azblobsdk.NewClientFromConnectionString(c.cfg.ConnectionString, nil)
		if err != nil {
			return fmt.Errorf("azblob[%s]: connection-string client: %w", c.name, err)
		}
		c.client = client

	case c.cfg.SASToken != "":
		fullURL := c.serviceURL() + "?" + strings.TrimPrefix(c.cfg.SASToken, "?")
		client, err := azblobsdk.NewClientWithNoCredential(fullURL, nil)
		if err != nil {
			return fmt.Errorf("azblob[%s]: sas client: %w", c.name, err)
		}
		c.client = client

	case c.cfg.AccountKey != "":
		cred, err := azblobsdk.NewSharedKeyCredential(c.cfg.Account, c.cfg.AccountKey)
		if err != nil {
			return fmt.Errorf("azblob[%s]: shared-key credential: %w", c.name, err)
		}
		client, err := azblobsdk.NewClientWithSharedKeyCredential(c.serviceURL(), cred, nil)
		if err != nil {
			return fmt.Errorf("azblob[%s]: shared-key client: %w", c.name, err)
		}
		c.client = client

	default:
		return fmt.Errorf("azblob[%s]: no credential configured (set one of connection_string / sas_token / account_key, or their *_env variants)", c.name)
	}

	m, err := connector.LoadIgnoreMatcher(ctx, c)
	if err != nil {
		return fmt.Errorf("azblob[%s]: load ignore: %w", c.name, err)
	}
	c.ignoreMatcher = m
	return nil
}

// Close is a no-op; the SDK manages connection pooling internally.
func (c *Connector) Close() error { return nil }

// List enumerates blobs under cfg.Prefix + prefix. Each Entry carries
// the blob's relative name (stripped of cfg.Prefix), size, mtime, and
// ETag as Hash. Pagination is followed transparently. Paths ignored by
// .uplinkignore (and the .uplinkignore file itself) are excluded —
// ignored paths are invisible to Uplink.
func (c *Connector) List(ctx context.Context, prefix string) ([]connector.Entry, error) {
	full := c.fullPrefix(prefix)
	opts := &azblobsdk.ListBlobsFlatOptions{}
	if full != "" {
		opts.Prefix = &full
	}
	pager := c.client.NewListBlobsFlatPager(c.cfg.Container, opts)
	var out []connector.Entry
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azblob[%s]: list: %w", c.name, err)
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			entry := c.entryFromItem(item)
			if !connector.IsEventEligible(entry.Path, c.ignoreMatcher) {
				continue
			}
			out = append(out, entry)
		}
	}
	return out, nil
}

// Walk streams entries under cfg.Prefix + prefix, yielding per
// pagination page. Memory cost is O(one page) — used by the
// streaming-scan path so corpus-size doesn't blow up RSS. Paths
// ignored by .uplinkignore (and the .uplinkignore file itself) are
// silently skipped.
func (c *Connector) Walk(ctx context.Context, prefix string, yield func(connector.Entry) error) error {
	full := c.fullPrefix(prefix)
	opts := &azblobsdk.ListBlobsFlatOptions{}
	if full != "" {
		opts.Prefix = &full
	}
	pager := c.client.NewListBlobsFlatPager(c.cfg.Container, opts)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("azblob[%s]: list: %w", c.name, err)
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			entry := c.entryFromItem(item)
			if !connector.IsEventEligible(entry.Path, c.ignoreMatcher) {
				continue
			}
			if err := yield(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// Stat returns the entry for a single blob. Returns ErrNotFound if the
// blob doesn't exist.
func (c *Connector) Stat(ctx context.Context, path string) (connector.Entry, error) {
	name := c.fullKey(path)
	blobClient := c.client.ServiceClient().NewContainerClient(c.cfg.Container).NewBlobClient(name)
	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		if isNotFound(err) {
			return connector.Entry{}, connector.ErrNotFound
		}
		return connector.Entry{}, fmt.Errorf("azblob[%s]: head %s: %w", c.name, name, err)
	}
	entry := connector.Entry{
		Path: path,
		Size: -1,
	}
	if props.ETag != nil {
		entry.Hash = trimETag(string(*props.ETag))
	}
	if props.ContentLength != nil {
		entry.Size = *props.ContentLength
	}
	if props.LastModified != nil {
		entry.ModTime = props.LastModified.UTC()
	}
	return entry, nil
}

// Read returns the full blob body. Paths matching .uplinkignore are
// treated as not present — the .uplinkignore file itself is still
// readable so LoadIgnoreMatcher can bootstrap during Init.
func (c *Connector) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	resp, err := c.client.DownloadStream(ctx, c.cfg.Container, c.fullKey(path), nil)
	if err != nil {
		if isNotFound(err) {
			return nil, connector.ErrNotFound
		}
		return nil, fmt.Errorf("azblob[%s]: get %s: %w", c.name, path, err)
	}
	return resp.Body, nil
}

// OpenRange returns bytes [start, start+length) of the blob via a
// ranged GET. The Aprimo uploader calls this once per parallel segment
// worker and streams the body straight into its segment POST; nothing
// is staged on local disk. Paths matching .uplinkignore are treated as
// not present.
func (c *Connector) OpenRange(ctx context.Context, path string, start, length int64) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	if length <= 0 {
		return nil, fmt.Errorf("azblob[%s]: OpenRange length must be > 0", c.name)
	}
	resp, err := c.client.DownloadStream(ctx, c.cfg.Container, c.fullKey(path), &azblobsdk.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: start, Count: length},
	})
	if err != nil {
		if isNotFound(err) {
			return nil, connector.ErrNotFound
		}
		return nil, fmt.Errorf("azblob[%s]: get range %s [%d-%d]: %w", c.name, path, start, start+length-1, err)
	}
	return resp.Body, nil
}

// PresignGetURL mints a short-lived read SAS URL for one blob, so a
// destination can have Azure copy the bytes server-side (StageBlockFromURL)
// instead of routing them through this machine — an intra-Azure copy that
// never touches our bandwidth. Works with shared-key and connection-string
// auth; a SAS-token-only source can't re-sign, so it returns an error and
// the destination falls back to streaming the bytes through.
func (c *Connector) PresignGetURL(_ context.Context, path string, ttl time.Duration) (string, error) {
	blobClient := c.client.ServiceClient().NewContainerClient(c.cfg.Container).NewBlobClient(c.fullKey(path))
	url, err := blobClient.GetSASURL(sas.BlobPermissions{Read: true}, time.Now().Add(ttl), nil)
	if err != nil {
		return "", fmt.Errorf("azblob[%s]: presign %q: %w", c.name, path, err)
	}
	return url, nil
}

// Write / Delete / Move return ErrUnsupported. Azure Blob is a
// source-only connector — channels flow storage → Aprimo, never the
// other way.
func (c *Connector) Write(_ context.Context, _ string, _ connector.SegmentSource, _ map[string]any) (connector.Entry, error) {
	return connector.Entry{}, connector.ErrUnsupported
}
func (c *Connector) Delete(_ context.Context, _ string) error  { return connector.ErrUnsupported }
func (c *Connector) Move(_ context.Context, _, _ string) error { return connector.ErrUnsupported }

// Reconcile walks the container+prefix, diffs against persisted state,
// and emits events for the differences. Reuses the same scan core the
// poll EventSource uses.
func (c *Connector) Reconcile(
	ctx context.Context,
	state connector.StateStore,
	onProgress connector.ProgressFunc,
) (connector.ReconcileResult, error) {
	return c.scanWatcher(ctx, connector.WatcherSpec{Prefix: ""}, nil, state, nil, onProgress)
}

// Prefix returns the configured prefix, exported for the EventSource.
func (c *Connector) Prefix() string { return c.cfg.Prefix }

// PollInterval reports the configured poll cadence.
func (c *Connector) PollInterval() time.Duration { return c.cfg.PollInterval }

// serviceURL returns the configured Azure Blob service URL. If
// cfg.ServiceURL is set (inline or via cfg.ServiceURLEnv, resolved at
// load time), that value is used; otherwise we build the canonical
// https://<account>.blob.core.windows.net/ form.
func (c *Connector) serviceURL() string {
	if c.cfg.ServiceURL != "" {
		if !strings.HasSuffix(c.cfg.ServiceURL, "/") {
			return c.cfg.ServiceURL + "/"
		}
		return c.cfg.ServiceURL
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/", c.cfg.Account)
}

// fullPrefix joins the connector's configured prefix with an extra
// listing prefix.
func (c *Connector) fullPrefix(extra string) string {
	if extra == "" {
		return c.cfg.Prefix
	}
	if c.cfg.Prefix == "" {
		return strings.TrimPrefix(extra, "/")
	}
	return strings.TrimRight(c.cfg.Prefix, "/") + "/" + strings.TrimLeft(extra, "/")
}

// fullKey converts a connector-relative path into an absolute blob name.
func (c *Connector) fullKey(relPath string) string {
	if c.cfg.Prefix == "" {
		return relPath
	}
	return strings.TrimRight(c.cfg.Prefix, "/") + "/" + strings.TrimLeft(relPath, "/")
}

// relKey converts an absolute blob name into a connector-relative path
// by stripping the configured prefix.
func (c *Connector) relKey(name string) string {
	if c.cfg.Prefix == "" {
		return name
	}
	stripped := strings.TrimPrefix(name, strings.TrimRight(c.cfg.Prefix, "/")+"/")
	return stripped
}

// entryFromItem turns a ListBlobsFlat result item into an Entry.
func (c *Connector) entryFromItem(item *container.BlobItem) connector.Entry {
	e := connector.Entry{
		Path: "",
		Size: -1,
	}
	if item == nil {
		return e
	}
	if item.Name != nil {
		e.Path = c.relKey(*item.Name)
	}
	if item.Properties != nil {
		if item.Properties.ETag != nil {
			e.Hash = trimETag(string(*item.Properties.ETag))
		}
		if item.Properties.ContentLength != nil {
			e.Size = *item.Properties.ContentLength
		}
		if item.Properties.LastModified != nil {
			e.ModTime = item.Properties.LastModified.UTC()
		}
	}
	return e
}

// trimETag strips the surrounding quotes Azure returns ETags wrapped in.
func trimETag(etag string) string {
	return strings.Trim(etag, `"`)
}

// isNotFound recognizes Azure's blob/container 404 codes.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return bloberror.HasCode(err, bloberror.BlobNotFound) ||
		bloberror.HasCode(err, bloberror.ContainerNotFound)
}
