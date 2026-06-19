package aprimo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRecordsMasterFile_ParsesID verifies that Records.MasterFile pulls
// the file id out of the canonical Aprimo HAL response shape, ignoring
// the rest of the payload (links, audit fields, watermark, etc.).
func TestRecordsMasterFile_ParsesID(t *testing.T) {
	const wantFileID = "8e4702def1d84ce39fc1b44e012d09cc"
	const wantPath = "/api/core/record/abc123/masterfile"

	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/hal+json")
		_, _ = w.Write([]byte(`{
			"_links": { "self": { "href": "https://x/y" } },
			"id": "` + wantFileID + `",
			"checkedOut": false,
			"watermarkType": "None"
		}`))
	}))
	defer srv.Close()

	r := &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}
	rs := &Records{r: r}

	ref, err := rs.MasterFile(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("MasterFile: %v", err)
	}
	if ref.ID != wantFileID {
		t.Errorf("ID = %q, want %q", ref.ID, wantFileID)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != wantPath {
		t.Errorf("path = %q, want %q", gotPath, wantPath)
	}
}

// TestResolveMasterFiles pins the wire contract reverse-engineered from the
// live tenant: a POST to /search/records with select-Record: masterfile and
// an `Id = "x" OR Id = "y"` expression, parsing each hit's embedded master
// file. Also guards that a non-record-id value can't be injected into the
// expression, and that a record with no master is simply absent.
func TestResolveMasterFiles(t *testing.T) {
	var gotMethod, gotPath, gotSelect string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotSelect = r.Header.Get("select-Record")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[
			{"id":"reca","_embedded":{"masterfile":{"id":"filea"}}},
			{"id":"recb","_embedded":{}}
		]}`))
	}))
	defer srv.Close()

	rs := &Records{r: &requester{client: srv.Client(), auth: stubAuth("tok"), baseURL: srv.URL}}

	// The third value carries a quote+space: it must be filtered out so it
	// can't malform or inject the OR-chain.
	got, err := rs.ResolveMasterFiles(context.Background(), []string{"reca", "recb", `x" OR 1=1`})
	if err != nil {
		t.Fatalf("ResolveMasterFiles: %v", err)
	}

	// Only the record with an embedded master file lands in the map.
	if len(got) != 1 || got["reca"] != "filea" {
		t.Fatalf("map = %v, want {reca:filea}", got)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/core/search/records" {
		t.Fatalf("request = %s %s, want POST /api/core/search/records", gotMethod, gotPath)
	}
	if gotSelect != "masterfile" {
		t.Fatalf("select-Record = %q, want masterfile", gotSelect)
	}
	se, _ := gotBody["searchExpression"].(map[string]any)
	if expr, _ := se["expression"].(string); expr != `Id = "reca" OR Id = "recb"` {
		t.Fatalf("expression = %q, want OR-chain of valid ids only (injection id dropped)", expr)
	}
}

// TestRecordsMasterFile_NotFound covers the case where the record has
// no master file. The error chain must match ErrNotFound so callers can
// branch (the connector falls back to a Create-shape payload).
func TestRecordsMasterFile_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"exceptionType":"Adam.Rest.Common.NoDataFoundException","exceptionMessage":"No master file."}`))
	}))
	defer srv.Close()

	r := &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}
	rs := &Records{r: r}

	_, err := rs.MasterFile(context.Background(), "abc123")
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
	var apiErr *Error
	if !asAprimoErr(err, &apiErr) || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected aprimo.Error with status 404, got %T %v", err, err)
	}
}

// TestNewVersionFilesUpdate_ShapeIsCorrect locks in the JSON shape the
// connector relies on: master pointer to the new token, addOrUpdate
// carrying the EXISTING file id with a new version. Without the file
// id, Aprimo treats the addOrUpdate as "make a new sibling file" —
// which was the bug this function exists to prevent.
func TestNewVersionFilesUpdate_ShapeIsCorrect(t *testing.T) {
	got := NewVersionFilesUpdate("tok-new", "report.pdf", "file-existing")
	body, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := []string{
		`"master":"tok-new"`,
		`"id":"file-existing"`,           // FileAction.id — the load-bearing bit
		`"addOrUpdate":[{"id":"tok-new"`, // version.id is the new token
		`"fileName":"report.pdf"`,
	}
	s := string(body)
	for _, w := range want {
		if !strings.Contains(s, w) {
			t.Errorf("missing %q in body %s", w, s)
		}
	}
}

// asAprimoErr is errors.As without importing errors at the top of the
// file (other tests in this package have the same pattern).
func asAprimoErr(err error, target **Error) bool {
	for cur := err; cur != nil; {
		if e, ok := cur.(*Error); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := cur.(unwrapper); ok {
			cur = u.Unwrap()
			continue
		}
		break
	}
	return false
}
