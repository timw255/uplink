package aprimo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUsersList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/core/users" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/hal+json")
		_, _ = fmt.Fprintln(w, `{
			"items": [
				{"id": "u-1", "name": "tim",   "email": "tim@acme.com"},
				{"id": "u-2", "name": "alice", "email": "alice@acme.com"}
			],
			"_links": { "self": { "href": "/api/core/users" } }
		}`)
	}))
	defer srv.Close()

	u := &Users{r: &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}
	got, err := u.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0].Email != "tim@acme.com" {
		t.Fatalf("got = %+v", got)
	}
}

func TestUserGroupsList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/core/usergroups" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/hal+json")
		_, _ = fmt.Fprintln(w, `{
			"items": [
				{"id": "g-1", "name": "Editors"},
				{"id": "g-2", "name": "Reviewers"}
			],
			"_links": { "self": { "href": "/api/core/usergroups" } }
		}`)
	}))
	defer srv.Close()

	g := &UserGroups{r: &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}
	got, err := g.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[1].Name != "Reviewers" {
		t.Fatalf("got = %+v", got)
	}
}
