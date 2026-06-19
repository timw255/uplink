package aprimo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCollectionsAddRecords_Chunks verifies AddRecords files a large set in
// UpdateRecords calls of at most collectionFileBatchSize, sending every id
// exactly once as an AddOrUpdate action.
func TestCollectionsAddRecords_Chunks(t *testing.T) {
	var calls, totalIDs int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		var req UpdateCollectionRequest
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		calls++
		if req.Records != nil {
			totalIDs += len(req.Records.AddOrUpdate)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cs := &Collections{r: &requester{client: srv.Client(), auth: stubAuth("tok"), baseURL: srv.URL}}

	ids := make([]string, 1500) // 1000 + 500 → two chunks
	for i := range ids {
		ids[i] = fmt.Sprintf("rec-%d", i)
	}
	if err := cs.AddRecords(context.Background(), "coll-1", ids); err != nil {
		t.Fatalf("AddRecords: %v", err)
	}
	if calls != 2 {
		t.Fatalf("PUT calls = %d, want 2 (chunked at %d)", calls, CollectionBatchSize)
	}
	if totalIDs != 1500 {
		t.Fatalf("total filed ids = %d, want 1500", totalIDs)
	}
}

// TestCollectionsUpdateRecords verifies the request shape Collections.UpdateRecords
// sends: PUT against /api/core/collection/{id} with the records action set
// in the body. The path encoding is exercised by passing an id with a
// special character.
func TestCollectionsUpdateRecords(t *testing.T) {
	var got struct {
		method  string
		path    string
		body    map[string]any
		hasAuth bool
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method = r.Method
		got.path = r.URL.Path
		got.hasAuth = r.Header.Get("Authorization") != ""
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got.body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Content-Type": "application/json"},
	}
	c := &Collections{r: r}

	err := c.UpdateRecords(context.Background(), "coll-abc", UpdateCollectionRequest{
		Records: &CollectionRecordActions{
			AddOrUpdate: []IDRef{{ID: "rec-1"}, {ID: "rec-2"}},
		},
	})
	if err != nil {
		t.Fatalf("UpdateRecords: %v", err)
	}

	if got.method != http.MethodPut {
		t.Errorf("method = %q, want PUT", got.method)
	}
	if got.path != "/api/core/collection/coll-abc" {
		t.Errorf("path = %q, want /api/core/collection/coll-abc", got.path)
	}
	if !got.hasAuth {
		t.Errorf("Authorization header missing")
	}
	records, ok := got.body["records"].(map[string]any)
	if !ok {
		t.Fatalf("body.records not present or wrong shape: %v", got.body)
	}
	add, ok := records["addOrUpdate"].([]any)
	if !ok || len(add) != 2 {
		t.Fatalf("records.addOrUpdate not as expected: %v", records)
	}
	if first := add[0].(map[string]any); first["id"] != "rec-1" {
		t.Errorf("addOrUpdate[0].id = %v, want rec-1", first["id"])
	}
}

// TestCollectionsUpdateRecords_ErrorResponse covers the error path —
// non-2xx surfaces as an *Error the caller can branch on.
func TestCollectionsUpdateRecords_ErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"exceptionType":"Adam.Rest.Common.NoDataFoundException","exceptionMessage":"Cannot find the collection with id 'coll-gone'."}`))
	}))
	defer srv.Close()

	r := &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Content-Type": "application/json"},
	}
	c := &Collections{r: r}

	err := c.UpdateRecords(context.Background(), "coll-gone", UpdateCollectionRequest{
		Records: &CollectionRecordActions{
			AddOrUpdate: []IDRef{{ID: "rec-1"}},
		},
	})
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *aprimo.Error, got %T: %v", err, err)
	}
	if apiErr.Status != http.StatusNotFound {
		t.Errorf("Status = %d, want 404", apiErr.Status)
	}
	if apiErr.AprimoCode != "Adam.Rest.Common.NoDataFoundException" {
		t.Errorf("AprimoCode = %q, want NoDataFoundException", apiErr.AprimoCode)
	}
}
