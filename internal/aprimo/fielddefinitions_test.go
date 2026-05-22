package aprimo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestFieldDefinitionsList_SinglePage(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if r.URL.Path != "/api/core/fielddefinitions" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("pageSize") != "200" {
			t.Errorf("pageSize header = %q, want 200", r.Header.Get("pageSize"))
		}
		w.Header().Set("Content-Type", "application/hal+json")
		_, _ = fmt.Fprintln(w, `{
			"items": [
				{"id": "fld-1", "name": "Caption"},
				{"id": "fld-2", "name": "Rights Holder"}
			],
			"_links": { "self": { "href": "/api/core/fielddefinitions" } }
		}`)
	}))
	defer srv.Close()

	fd := &FieldDefinitions{r: &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}

	defs, err := fd.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("len(defs) = %d, want 2", len(defs))
	}
	if defs[0].ID != "fld-1" || defs[0].Name != "Caption" {
		t.Errorf("defs[0] = %+v", defs[0])
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Errorf("expected 1 request, got %d", hits)
	}
}

func TestFieldDefinitionsList_FollowsHALNextLinks(t *testing.T) {
	var hits int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/hal+json")
		switch n {
		case 1:
			// First page emits an absolute next URL (Aprimo's actual
			// convention). We need to strip scheme+host and re-issue.
			fmt.Fprintf(w, `{
				"items": [{"id": "a", "name": "A"}],
				"_links": { "next": { "href": "%s/api/core/fielddefinitions?page=2" } }
			}`, srv.URL)
		case 2:
			if r.URL.Path != "/api/core/fielddefinitions" || r.URL.RawQuery != "page=2" {
				t.Errorf("page-2 request = %s?%s, want /api/core/fielddefinitions?page=2", r.URL.Path, r.URL.RawQuery)
			}
			// Last page: no next link.
			_, _ = fmt.Fprintln(w, `{
				"items": [{"id": "b", "name": "B"}],
				"_links": { "self": { "href": "/api/core/fielddefinitions?page=2" } }
			}`)
		default:
			t.Errorf("unexpected request #%d to %s", n, r.URL.String())
		}
	}))
	defer srv.Close()

	fd := &FieldDefinitions{r: &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}

	defs, err := fd.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("defs = %+v, want 2", defs)
	}
	if defs[0].ID != "a" || defs[1].ID != "b" {
		t.Errorf("order wrong: %+v", defs)
	}
}

func TestFieldDefinitions_GetByID_DecodesOptionListItems(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/core/fielddefinition/fd-1" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/hal+json")
		_, _ = fmt.Fprintln(w, `{
			"id": "fd-1",
			"name": "Department",
			"dataType": "optionlist",
			"items": [
				{"id": "opt-1", "name": "Marketing"},
				{"id": "opt-2", "name": "Engineering"}
			]
		}`)
	}))
	defer srv.Close()

	fd := &FieldDefinitions{r: &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}

	got, err := fd.GetByID(context.Background(), "fd-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.DataType != "optionlist" {
		t.Errorf("DataType = %q", got.DataType)
	}
	if len(got.OptionListItems) != 2 {
		t.Fatalf("items len = %d", len(got.OptionListItems))
	}
	if got.OptionListItems[0].Name != "Marketing" {
		t.Errorf("items[0] = %+v", got.OptionListItems[0])
	}
}

func TestRelativizePath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/api/core/fielddefinitions", "/api/core/fielddefinitions"},
		{"/api/core/fielddefinitions?page=2", "/api/core/fielddefinitions?page=2"},
		{"https://x.dam.aprimo.com/api/core/fielddefinitions", "/api/core/fielddefinitions"},
		{"https://x.dam.aprimo.com/api/core/fielddefinitions?page=2&pageSize=200", "/api/core/fielddefinitions?page=2&pageSize=200"},
	}
	for _, c := range cases {
		got, err := relativizePath(c.in)
		if err != nil {
			t.Errorf("relativizePath(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("relativizePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
