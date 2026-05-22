// Package s3 is the Amazon S3 (and S3-compatible) source connector.
// It is read-only: List/Stat/Read/OpenRange against the bucket+prefix
// scope, and a poll-based EventSource that diffs the current listing
// against last-known state to emit OnCreate/OnUpdate/OnDelete events.
//
// Works against any S3-compatible endpoint (MinIO, R2, Backblaze B2's
// S3 API) via the optional endpoint_env config — the only knob a
// customer touches.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/timw255/uplink/internal/connector"
	"github.com/timw255/uplink/internal/ignore"
)

// Connector is one configured S3 instance.
type Connector struct {
	name          string
	cfg           *Config
	client        *awss3.Client
	ignoreMatcher *ignore.Matcher
}

// Factory builds an S3 connector from its raw YAML config block.
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
//  1. Static credentials from cfg.AccessKey / cfg.SecretKey (themselves
//     populated either inline or via the *Env env-var indirection).
//  2. Ambient AWS SDK credential chain (instance profile on EC2/EKS,
//     ~/.aws/credentials, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env).
func (c *Connector) Init(ctx context.Context) error {
	opts := []func(*awsconfig.LoadOptions) error{}
	if c.cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(c.cfg.Region))
	}
	if c.cfg.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				c.cfg.AccessKey,
				c.cfg.SecretKey,
				"", // session token; not used
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("s3[%s]: load aws config: %w", c.name, err)
	}

	s3Opts := []func(*awss3.Options){}
	if c.cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(c.cfg.Endpoint)
		})
	}
	if c.cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *awss3.Options) {
			o.UsePathStyle = true
		})
	}

	c.client = awss3.NewFromConfig(awsCfg, s3Opts...)

	m, err := connector.LoadIgnoreMatcher(ctx, c)
	if err != nil {
		return fmt.Errorf("s3[%s]: load ignore: %w", c.name, err)
	}
	c.ignoreMatcher = m
	return nil
}

// Close is a no-op; the SDK manages connection pooling internally.
func (c *Connector) Close() error { return nil }

// List enumerates objects under cfg.Prefix + prefix. Each Entry carries
// the object's relative key (stripped of cfg.Prefix), size, mtime, and
// ETag as Hash. Pagination is followed transparently. Paths ignored by
// .uplinkignore (and the .uplinkignore file itself) are excluded —
// ignored paths are invisible to Uplink.
func (c *Connector) List(ctx context.Context, prefix string) ([]connector.Entry, error) {
	full := c.fullPrefix(prefix)
	paginator := awss3.NewListObjectsV2Paginator(c.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(c.cfg.Bucket),
		Prefix: aws.String(full),
	})
	var out []connector.Entry
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3[%s]: list: %w", c.name, err)
		}
		for _, obj := range page.Contents {
			entry := c.entryFromObject(obj)
			if !connector.IsEventEligible(entry.Path, c.ignoreMatcher) {
				continue
			}
			out = append(out, entry)
		}
	}
	return out, nil
}

// Walk streams entries under cfg.Prefix + prefix, yielding per
// pagination page. Memory cost is O(one page) rather than O(corpus
// size), which is what makes million-asset bucket scans possible
// without unbounded RAM growth. Paths ignored by .uplinkignore (and
// the .uplinkignore file itself) are silently skipped.
func (c *Connector) Walk(ctx context.Context, prefix string, yield func(connector.Entry) error) error {
	full := c.fullPrefix(prefix)
	paginator := awss3.NewListObjectsV2Paginator(c.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(c.cfg.Bucket),
		Prefix: aws.String(full),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("s3[%s]: list: %w", c.name, err)
		}
		for _, obj := range page.Contents {
			entry := c.entryFromObject(obj)
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

// Stat returns the entry for a single key. Returns ErrNotFound if the
// object doesn't exist.
func (c *Connector) Stat(ctx context.Context, path string) (connector.Entry, error) {
	key := c.fullKey(path)
	out, err := c.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return connector.Entry{}, connector.ErrNotFound
		}
		return connector.Entry{}, fmt.Errorf("s3[%s]: head %s: %w", c.name, key, err)
	}
	entry := connector.Entry{
		Path: path,
		Size: -1,
		Hash: trimETag(aws.ToString(out.ETag)),
	}
	if out.ContentLength != nil {
		entry.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		entry.ModTime = out.LastModified.UTC()
	}
	return entry, nil
}

// Read returns the full object body. Paths matching .uplinkignore are
// treated as not present — the .uplinkignore file itself is still
// readable so LoadIgnoreMatcher can bootstrap during Init.
func (c *Connector) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	out, err := c.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(c.fullKey(path)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, connector.ErrNotFound
		}
		return nil, fmt.Errorf("s3[%s]: get %s: %w", c.name, path, err)
	}
	return out.Body, nil
}

// OpenRange returns bytes [start, start+length) of the object via a
// ranged GET. The Aprimo uploader calls this once per parallel segment
// worker and streams the body straight into its segment POST; nothing
// is staged on local disk. Paths matching .uplinkignore are treated as
// not present.
func (c *Connector) OpenRange(ctx context.Context, path string, start, length int64) (io.ReadCloser, error) {
	if c.ignoreMatcher != nil && c.ignoreMatcher.ShouldIgnore(path) {
		return nil, connector.ErrNotFound
	}
	if length <= 0 {
		return nil, fmt.Errorf("s3[%s]: OpenRange length must be > 0", c.name)
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, start+length-1)
	out, err := c.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(c.fullKey(path)),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, connector.ErrNotFound
		}
		return nil, fmt.Errorf("s3[%s]: get range %s [%s]: %w", c.name, path, rangeHeader, err)
	}
	return out.Body, nil
}

// Write / Delete / Move return ErrUnsupported. S3 is a source-only
// connector — channels flow storage → Aprimo, never the other way.
func (c *Connector) Write(_ context.Context, _ string, _ connector.SegmentSource, _ map[string]any) (connector.Entry, error) {
	return connector.Entry{}, connector.ErrUnsupported
}
func (c *Connector) Delete(_ context.Context, _ string) error          { return connector.ErrUnsupported }
func (c *Connector) Move(_ context.Context, _, _ string) error         { return connector.ErrUnsupported }

// Reconcile walks the bucket+prefix, diffs against persisted state,
// and emits events for the differences. Reuses the same scan core
// the poll EventSource uses.
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

// fullKey converts a connector-relative path into an absolute S3 key.
func (c *Connector) fullKey(relPath string) string {
	if c.cfg.Prefix == "" {
		return relPath
	}
	return strings.TrimRight(c.cfg.Prefix, "/") + "/" + strings.TrimLeft(relPath, "/")
}

// relKey converts an absolute S3 key into a connector-relative path
// by stripping the configured prefix.
func (c *Connector) relKey(key string) string {
	if c.cfg.Prefix == "" {
		return key
	}
	stripped := strings.TrimPrefix(key, strings.TrimRight(c.cfg.Prefix, "/")+"/")
	return stripped
}

// entryFromObject turns an S3 ListObjectsV2 result into an Entry.
func (c *Connector) entryFromObject(obj s3types.Object) connector.Entry {
	e := connector.Entry{
		Path: c.relKey(aws.ToString(obj.Key)),
		Size: -1,
		Hash: trimETag(aws.ToString(obj.ETag)),
	}
	if obj.Size != nil {
		e.Size = *obj.Size
	}
	if obj.LastModified != nil {
		e.ModTime = obj.LastModified.UTC()
	}
	return e
}

// trimETag strips the surrounding quotes S3 returns ETags wrapped in.
func trimETag(etag string) string {
	return strings.Trim(etag, `"`)
}

// isNotFound recognizes both the typed and string forms of S3's 404.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nfErr *s3types.NotFound
	if errors.As(err, &nfErr) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" || code == "404" {
			return true
		}
	}
	return false
}
