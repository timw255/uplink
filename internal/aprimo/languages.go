package aprimo

import (
	"context"
	"fmt"
	"strconv"
)

// Languages is the languages resource. Each Aprimo tenant configures
// one or more Language records; field values are localized by
// `languageId`. The Uplink Lua companion pipeline calls List once at
// startup to build a culture→id map so scripts can specify language
// values via the IETF locale tag (e.g., "en-US") rather than the
// tenant-specific GUID.
type Languages struct {
	r *requester
}

// Language is the slice of the upstream object Uplink uses. The full
// schema also carries `isEnabledForUI`, audit timestamps, etc. that
// we don't decode — they pass by unread.
type Language struct {
	ID                 string `json:"id"`
	Culture            string `json:"culture"`
	Name               string `json:"name"`
	IsEnabledForFields bool   `json:"isEnabledForFields"`
}

type languagePage struct {
	Items []Language `json:"items"`
	Links struct {
		Next *struct {
			Href string `json:"href"`
		} `json:"next"`
	} `json:"_links"`
}

// maxLanguages caps the per-call accumulation. Real tenants have at
// most a few dozen languages; this is a sanity bound on a misbehaving
// server, not an operator-visible limit.
const maxLanguages = 1_000

// List walks every page of /api/core/languages and returns the full
// set. Pagination mirrors FieldDefinitions.List — page through HAL
// `_links.next` until none remains.
func (l *Languages) List(ctx context.Context) ([]Language, error) {
	headers := map[string]string{"pageSize": strconv.Itoa(listPageSize)}

	path := "/api/core/languages"
	var out []Language
	for {
		var page languagePage
		if err := l.r.getJSON(ctx, path, headers, &page); err != nil {
			return nil, fmt.Errorf("aprimo: list languages: %w", err)
		}
		out = append(out, page.Items...)
		if len(out) > maxLanguages {
			return nil, fmt.Errorf("aprimo: list languages: exceeded cap of %d entries", maxLanguages)
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
