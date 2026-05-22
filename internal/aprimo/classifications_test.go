package aprimo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClassificationsList_DecodesNamePath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/core/classifications" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/hal+json")
		_, _ = fmt.Fprintln(w, `{
			"items": [
				{"id": "cls-root",  "name": "Topics",   "namePath": "Topics"},
				{"id": "cls-sport", "name": "Sports",   "namePath": "Topics/Sports"},
				{"id": "cls-foot",  "name": "Football", "namePath": "Topics/Sports/Football"}
			],
			"_links": { "self": { "href": "/api/core/classifications" } }
		}`)
	}))
	defer srv.Close()

	cs := &Classifications{r: &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}

	out, err := cs.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3", len(out))
	}
	if out[2].NamePath != "Topics/Sports/Football" {
		t.Errorf("out[2].NamePath = %q", out[2].NamePath)
	}
	if out[2].ID != "cls-foot" {
		t.Errorf("out[2].ID = %q", out[2].ID)
	}
}
