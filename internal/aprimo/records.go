package aprimo

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
)

// Records is the records resource. It covers what the connector's
// Write/Delete/Stat/Read paths need; broader record operations are
// added as required.
type Records struct {
	r *requester
}

// CreateRequest is the body passed to Records.Create. Only the most
// commonly populated fields are typed; extra fields can be carried by
// the open-ended Extra map (merged into the request JSON at marshal
// time).
type CreateRequest struct {
	// Status is "draft" (default), "released", or "archived".
	Status string `json:"status,omitempty"`

	// ContentType is the content-type name the record is stamped with
	// (e.g. "Asset").
	ContentType string `json:"contentType,omitempty"`

	// Files attaches an uploaded file. Use NewFilesFromUpload for the
	// canonical upload-then-attach shape.
	Files *Files `json:"files,omitempty"`

	// Fields carries field add/update/remove actions. The shape is
	// open because field definitions vary per tenant; use raw JSON.
	Fields json.RawMessage `json:"fields,omitempty"`

	// Classifications attaches classification ids.
	Classifications *ClassificationActions `json:"classifications,omitempty"`

	// Extra holds anything not modeled above. Merged into the
	// top-level JSON object via MarshalJSON.
	Extra map[string]any `json:"-"`
}

// MarshalJSON merges Extra into the structured fields so callers can
// pass arbitrary additional request properties without us having to
// pre-model every Aprimo concept.
func (r CreateRequest) MarshalJSON() ([]byte, error) {
	type alias CreateRequest
	base, err := json.Marshal(alias(r))
	if err != nil {
		return nil, err
	}
	if len(r.Extra) == 0 {
		return base, nil
	}
	return mergeJSON(base, r.Extra)
}

// UpdateRequest is the body for Records.Update. All fields are
// optional; only what's set is sent.
type UpdateRequest struct {
	Status          string                 `json:"status,omitempty"`
	Files           *Files                 `json:"files,omitempty"`
	Fields          json.RawMessage        `json:"fields,omitempty"`
	Classifications *ClassificationActions `json:"classifications,omitempty"`
	Extra           map[string]any         `json:"-"`
}

// MarshalJSON: same Extra-merging behavior as CreateRequest.
func (r UpdateRequest) MarshalJSON() ([]byte, error) {
	type alias UpdateRequest
	base, err := json.Marshal(alias(r))
	if err != nil {
		return nil, err
	}
	if len(r.Extra) == 0 {
		return base, nil
	}
	return mergeJSON(base, r.Extra)
}

// Files is the {"master": "<token>", "addOrUpdate": [...]} block.
type Files struct {
	Master      string       `json:"master,omitempty"`
	AddOrUpdate []FileAction `json:"addOrUpdate,omitempty"`
	Remove      []IDRef      `json:"remove,omitempty"`
}

// FileAction is one entry in Files.AddOrUpdate. Versions is the
// usual carrier of the upload token.
type FileAction struct {
	ID       string          `json:"id,omitempty"`
	Versions *VersionActions `json:"versions,omitempty"`
}

// VersionActions wraps add/update/remove on a file's versions.
type VersionActions struct {
	AddOrUpdate []Version `json:"addOrUpdate,omitempty"`
	Remove      []IDRef   `json:"remove,omitempty"`
}

// Version is a file version record. ID is the upload token returned
// by Uploader.UploadFile.
type Version struct {
	ID           string `json:"id"`
	FileName     string `json:"fileName,omitempty"`
	VersionLabel string `json:"versionLabel,omitempty"`
	Comment      string `json:"comment,omitempty"`
}

// ClassificationActions carries classification id add/remove.
type ClassificationActions struct {
	AddOrUpdate []IDRef `json:"addOrUpdate,omitempty"`
	Remove      []IDRef `json:"remove,omitempty"`
}

// IDRef is the universal {"id": "..."} body fragment.
type IDRef struct {
	ID string `json:"id"`
}

// NewFilesFromUpload returns the Files block for attaching a freshly
// uploaded token to a record on Create. The FileAction carries no id,
// so Aprimo creates a brand-new file on the record.
//
// For Update flows, use NewVersionFilesUpdate instead — without the
// file id Aprimo would add another sibling file alongside the existing
// master rather than appending a version to it.
func NewFilesFromUpload(token, filename string) *Files {
	return &Files{
		Master: token,
		AddOrUpdate: []FileAction{{
			Versions: &VersionActions{
				AddOrUpdate: []Version{{ID: token, FileName: filename}},
			},
		}},
	}
}

// NewVersionFilesUpdate returns the Files block for appending a new
// version to an existing master file on a record. fileID is the
// Aprimo file id (NOT the record id) — discover it via Records.MasterFile
// for the target record. The new token becomes both the record's master
// pointer and the new file version.
func NewVersionFilesUpdate(token, filename, fileID string) *Files {
	return &Files{
		Master: token,
		AddOrUpdate: []FileAction{{
			ID: fileID,
			Versions: &VersionActions{
				AddOrUpdate: []Version{{ID: token, FileName: filename}},
			},
		}},
	}
}

// CreateResponse is what /api/core/records returns on success.
type CreateResponse struct {
	ID string `json:"id"`
}

// Create posts a new record. ImmediateIndex sets the
// `set-immediateSearchIndexUpdate` header so the record is searchable
// without waiting for the next indexer pass — heavier on the server.
func (rs *Records) Create(ctx context.Context, req CreateRequest, immediateIndex bool) (*CreateResponse, error) {
	headers := indexHeader(immediateIndex)
	var out CreateResponse
	if err := rs.r.postJSON(ctx, "/api/core/records", req, &out, headers); err != nil {
		return nil, err
	}
	return &out, nil
}

// Update PUTs a partial update to a record. Only fields set on req
// are sent.
func (rs *Records) Update(ctx context.Context, id string, req UpdateRequest, immediateIndex bool) error {
	headers := indexHeader(immediateIndex)
	return rs.r.putJSON(ctx, "/api/core/record/"+url.PathEscape(id), req, nil, headers)
}

// MasterFileRef is the slice of an Aprimo masterfile resource the
// connector needs: just the file id. The masterfile endpoint returns
// far more (HAL links, audit fields, etc.); we ignore everything else.
type MasterFileRef struct {
	ID string `json:"id"`
}

// MasterFile returns the master file currently attached to a record.
// Used by the Update flow to target a new version at the right file id
// rather than creating a sibling file.
//
// Returns an error wrapping ErrNotFound when the record has no master.
func (rs *Records) MasterFile(ctx context.Context, recordID string) (MasterFileRef, error) {
	var out MasterFileRef
	if err := rs.r.getJSON(ctx,
		"/api/core/record/"+url.PathEscape(recordID)+"/masterfile",
		nil, &out); err != nil {
		return MasterFileRef{}, err
	}
	return out, nil
}

// masterFileBatchSize bounds how many record ids go into one search call.
// Aprimo's search caps results at pageSize=1000; the OR-chain expression
// itself handles far more (verified well past 2000 ids), so 1000 is the
// effective per-call batch.
const masterFileBatchSize = 1000

// searchRecordsResult is the slice of the /search/records response the
// master-file resolver reads: each hit's record id plus its embedded master
// file (present when the request carries select-Record: masterfile).
type searchRecordsResult struct {
	Items []struct {
		ID       string `json:"id"`
		Embedded struct {
			MasterFile struct {
				ID string `json:"id"`
			} `json:"masterfile"`
		} `json:"_embedded"`
	} `json:"items"`
}

// ResolveMasterFiles batch-resolves the current master file id for many
// records in one search call per 1000 ids, rather than one MasterFile GET
// per record — the difference between hundreds of calls and hundreds of
// thousands on an update-heavy import.
//
// It searches `Id = "x" OR Id = "y" OR …` with select-Record: masterfile and
// reads each hit's embedded master file. The returned map is
// recordID → masterFileID; records with no master file (or that no longer
// exist) are simply absent, and the caller should fall back to a single
// MasterFile lookup for those. Ids that aren't well-formed record ids are
// skipped so a stray value can't malform the expression.
func (rs *Records) ResolveMasterFiles(ctx context.Context, recordIDs []string) (map[string]string, error) {
	out := make(map[string]string, len(recordIDs))
	var firstErr error
	for chunk := range slices.Chunk(recordIDs, masterFileBatchSize) {
		clauses := make([]string, 0, len(chunk))
		for _, id := range chunk {
			if isRecordID(id) {
				clauses = append(clauses, `Id = "`+id+`"`)
			}
		}
		if len(clauses) == 0 {
			continue
		}
		body := map[string]any{
			"searchExpression": map[string]any{"expression": strings.Join(clauses, " OR ")},
		}
		path := fmt.Sprintf("/api/core/search/records?page=1&pageSize=%d", len(clauses))
		var resp searchRecordsResult
		if err := rs.r.postJSON(ctx, path, body, &resp, map[string]string{"select-Record": "masterfile"}); err != nil {
			// A failed chunk leaves its records out of the map; they fall back
			// to the per-record GET. Continue with the rest; stop only if the
			// run is cancelled.
			if firstErr == nil {
				firstErr = err
			}
			if ctx.Err() != nil {
				break
			}
			continue
		}
		for _, it := range resp.Items {
			if it.ID != "" && it.Embedded.MasterFile.ID != "" {
				out[it.ID] = it.Embedded.MasterFile.ID
			}
		}
	}
	return out, firstErr
}

// isRecordID reports whether s is a bare alphanumeric token (the shape of an
// Aprimo record id). Used to keep arbitrary input — anything that could
// carry a quote and break the search expression — out of the OR-chain.
func isRecordID(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		default:
			return false
		}
	}
	return true
}

// Delete permanently removes a record. Cannot be undone.
func (rs *Records) Delete(ctx context.Context, id string, immediateIndex bool) error {
	headers := indexHeader(immediateIndex)
	return rs.r.deleteJSON(ctx, "/api/core/record/"+url.PathEscape(id), headers)
}

// GetByID fetches a single record. The returned RawMessage preserves
// the full HAL response (including _embedded / _links) for callers
// that need fields beyond the typed envelope. expandSelect, if not
// empty, becomes the `select-Record` header value (e.g.
// "masterfile,fields"); see the Aprimo HAL docs for the syntax.
func (rs *Records) GetByID(ctx context.Context, id string, expandSelect string) (json.RawMessage, error) {
	var headers map[string]string
	if expandSelect != "" {
		headers = map[string]string{"select-Record": expandSelect}
	}
	var raw json.RawMessage
	if err := rs.r.getJSON(ctx, "/api/core/record/"+url.PathEscape(id), headers, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func indexHeader(immediate bool) map[string]string {
	if !immediate {
		return nil
	}
	return map[string]string{"set-immediateSearchIndexUpdate": "true"}
}

// mergeJSON merges extras into base (which must be a JSON object).
// extras overwrite base on key collision.
func mergeJSON(base []byte, extras map[string]any) ([]byte, error) {
	var merged map[string]any
	if err := json.Unmarshal(base, &merged); err != nil {
		return nil, err
	}
	if merged == nil {
		merged = make(map[string]any, len(extras))
	}
	maps.Copy(merged, extras)
	return json.Marshal(merged)
}
