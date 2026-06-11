package aprimo

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// FieldDefinitions is the field-definitions resource. Aprimo orgs
// configure their metadata schema via FieldDefinition records; the
// Uplink Lua companion pipeline calls List once at startup to build
// a name→id map so scripts can reference fields by their display
// name rather than the tenant-specific GUID.
type FieldDefinitions struct {
	r *requester
}

// Field DataType constants. These are the canonical lowercase
// discriminator values. Aprimo's API emits dataType in PascalCase
// (e.g. "TextList", "OptionList"); normalizeDataType lowercases each
// value at the decode boundary so every consumer can compare against
// these constants directly.
const (
	DataTypeSingleLineText    = "singlelinetext"
	DataTypeMultiLineText     = "multilinetext"
	DataTypeHTML              = "html"
	DataTypeRichContent       = "richcontent"
	DataTypeNumeric           = "numeric"
	DataTypeDate              = "date"
	DataTypeDateTime          = "datetime"
	DataTypeTime              = "time"
	DataTypeDuration          = "duration"
	DataTypeJSON              = "json"
	DataTypeOptionList        = "optionlist"
	DataTypeClassificationList = "classificationlist"
	DataTypeTextList          = "textlist"
	DataTypeRecordList        = "recordlist"
	DataTypeRecordLink        = "recordlink"
	DataTypeUserList          = "userlist"
	DataTypeUserGroupList     = "usergrouplist"
	DataTypeLanguageList      = "languagelist"
	DataTypeHyperlinkList     = "hyperlinklist"
)

// FieldDefinition is the slice of the upstream object Uplink uses.
// Aprimo returns many additional properties (type-specific defaults,
// localized labels, etc.) that we don't decode — they're allowed to
// pass by unread thanks to the encoding/json default.
//
// DataType is Aprimo's discriminated-union tag. Lowercase string values
// confirmed against aprimo-js' FieldDefinition.ts:
//
//	singlelinetext, multilinetext, html, richcontent
//	numeric, date, datetime, time, duration
//	json
//	optionlist, classificationlist, textlist
//	recordlist, recordlink
//	userlist, usergrouplist, languagelist
//	hyperlinklist
//
// The resolver in internal/connectors/aprimo dispatches per DataType
// to produce the right wire shape for record Create/Update calls.
type FieldDefinition struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// fieldDefinitionPage is the HAL paged envelope we deserialize. The
// upstream schema has many other fields (page, pageSize, totalCount)
// we don't currently need; `next` is what drives pagination.
type fieldDefinitionPage struct {
	Items []FieldDefinition `json:"items"`
	Links struct {
		Next *struct {
			Href string `json:"href"`
		} `json:"next"`
	} `json:"_links"`
}

// maxFieldDefinitions caps how many entries List will accumulate
// before returning an error. Sized for real-world tenants (typically
// a few hundred field defs); if a tenant legitimately exceeds it,
// raise the cap rather than letting a misbehaving server force the
// daemon to OOM at startup.
const maxFieldDefinitions = 10_000

// OptionListItem is one entry in an OptionListFieldDefinition's
// items array. Aprimo's API returns these embedded inside the field
// definition when fetched via GetByID (the bulk List endpoint omits
// them by default).
type OptionListItem struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// FieldDefinitionDetail is the fuller view returned by GetByID. It
// includes type-specific embedded payloads like option list items.
type FieldDefinitionDetail struct {
	FieldDefinition
	// OptionListItems is populated when DataType == "optionlist".
	OptionListItems []OptionListItem `json:"items,omitempty"`
}

// normalizeDataType folds Aprimo's PascalCase dataType discriminator
// (e.g. "TextList") to the canonical lowercase form used by the
// DataType* constants. Applied at every decode boundary so downstream
// comparisons need not worry about server-side casing.
func normalizeDataType(dt string) string {
	return strings.ToLower(dt)
}

// GetByID fetches the full definition of a single field by its
// GUID. Used by the resolver to pull `items` out of OptionList
// field definitions for name→id resolution.
func (fd *FieldDefinitions) GetByID(ctx context.Context, id string) (*FieldDefinitionDetail, error) {
	var out FieldDefinitionDetail
	if err := fd.r.getJSON(ctx, "/api/core/fielddefinition/"+id, nil, &out); err != nil {
		return nil, fmt.Errorf("aprimo: get field definition %s: %w", id, err)
	}
	out.DataType = normalizeDataType(out.DataType)
	return &out, nil
}

// List walks every page of /api/core/fielddefinitions and returns
// the full set of {ID, Name, DataType} triples. Pagination is driven
// by the HAL `_links.next.href` returned in each page; we follow
// links until none remains.
//
// The bulk listing does NOT include type-specific embedded payloads
// (option list items, classification roots, etc.). For those, follow
// up with GetByID per field of interest.
func (fd *FieldDefinitions) List(ctx context.Context) ([]FieldDefinition, error) {
	// Aprimo's listing endpoints take page-size via HTTP header,
	// not query string — `buildHeaders` in aprimo-js mirrors this.
	headers := map[string]string{"pageSize": strconv.Itoa(200)}

	path := "/api/core/fielddefinitions"
	var out []FieldDefinition
	for {
		var page fieldDefinitionPage
		if err := fd.r.getJSON(ctx, path, headers, &page); err != nil {
			return nil, fmt.Errorf("aprimo: list field definitions: %w", err)
		}
		for i := range page.Items {
			page.Items[i].DataType = normalizeDataType(page.Items[i].DataType)
		}
		out = append(out, page.Items...)
		if len(out) > maxFieldDefinitions {
			return nil, fmt.Errorf("aprimo: list field definitions: exceeded cap of %d entries", maxFieldDefinitions)
		}
		if page.Links.Next == nil {
			break
		}
		// Aprimo emits HAL `next` links as absolute URLs. Strip the
		// scheme+host so we can hand the path+query to the existing
		// requester (which prepends baseURL). A relative href is
		// accepted as-is.
		nextPath, err := relativizePath(page.Links.Next.Href)
		if err != nil {
			return nil, fmt.Errorf("aprimo: parse next link %q: %w", page.Links.Next.Href, err)
		}
		path = nextPath
	}
	return out, nil
}

// relativizePath normalizes a HAL `_links.*.href` into the path+query
// form the requester wants. Absolute URLs have their scheme+host
// stripped; already-relative paths pass through unchanged.
func relativizePath(href string) (string, error) {
	if strings.HasPrefix(href, "/") {
		return href, nil
	}
	u, err := url.Parse(href)
	if err != nil {
		return "", err
	}
	if u.Path == "" {
		return "/", nil
	}
	if u.RawQuery == "" {
		return u.Path, nil
	}
	return u.Path + "?" + u.RawQuery, nil
}
