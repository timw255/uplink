package aprimo

import (
	"context"
	"encoding/json"
	"net/url"
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
	for k, v := range extras {
		merged[k] = v
	}
	return json.Marshal(merged)
}
