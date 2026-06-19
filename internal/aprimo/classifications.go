package aprimo

import (
	"context"
	"fmt"
	"strconv"
)

// Classifications is the classifications resource. Aprimo
// classifications are a hierarchical taxonomy — every classification
// has an `id`, an internal `name` (not localized), and a `namePath`
// (slash-separated root-to-node path of internal names).
//
// Uplink's companion pipeline calls List once at startup and again at
// the configured refresh_interval so scripts can specify
// classifications by human-readable path (e.g., "Topics/Sports" or
// "Topics > Sports") rather than the tenant-specific GUID.
type Classifications struct {
	r *requester
}

// Classification is the slice Uplink uses. The full upstream object
// has audit timestamps, permissions, labels, etc. that we don't need.
type Classification struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	NamePath string `json:"namePath"`
}

type classificationPage struct {
	Items []Classification `json:"items"`
	Links struct {
		Next *struct {
			Href string `json:"href"`
		} `json:"next"`
	} `json:"_links"`
}

// maxClassifications caps how many entries List will accumulate
// before erroring. Real tenants have hundreds to low-thousands of
// classifications; this is a sanity bound on a misbehaving server.
const maxClassifications = 100_000

// List walks every page of /api/core/classifications and returns the
// full taxonomy. Pagination mirrors FieldDefinitions.List.
func (cs *Classifications) List(ctx context.Context) ([]Classification, error) {
	headers := map[string]string{"pageSize": strconv.Itoa(listPageSize)}

	path := "/api/core/classifications"
	var out []Classification
	for {
		var page classificationPage
		if err := cs.r.getJSON(ctx, path, headers, &page); err != nil {
			return nil, fmt.Errorf("aprimo: list classifications: %w", err)
		}
		out = append(out, page.Items...)
		if len(out) > maxClassifications {
			return nil, fmt.Errorf("aprimo: list classifications: exceeded cap of %d entries", maxClassifications)
		}
		if page.Links.Next == nil {
			break
		}
		nextPath, err := relativizePath(page.Links.Next.Href)
		if err != nil {
			return nil, fmt.Errorf("aprimo: parse next link %q: %w", page.Links.Next.Href, err)
		}
		path = nextPath
	}
	return out, nil
}
