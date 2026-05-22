package aprimo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLanguagesList_DecodesCultureAndFlags(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/core/languages" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/hal+json")
		_, _ = fmt.Fprintln(w, `{
			"items": [
				{"id": "lang-en", "culture": "en-US", "name": "English",  "isEnabledForFields": true},
				{"id": "lang-fr", "culture": "fr-FR", "name": "French",   "isEnabledForFields": true},
				{"id": "lang-jp", "culture": "ja-JP", "name": "Japanese", "isEnabledForFields": false}
			],
			"_links": { "self": { "href": "/api/core/languages" } }
		}`)
	}))
	defer srv.Close()

	l := &Languages{r: &requester{
		client:  srv.Client(),
		auth:    stubAuth("tok"),
		baseURL: srv.URL,
		headers: map[string]string{"Accept": "application/hal+json"},
	}}

	got, err := l.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Culture != "en-US" || !got[0].IsEnabledForFields {
		t.Errorf("got[0] = %+v", got[0])
	}
	if got[2].IsEnabledForFields {
		t.Errorf("got[2].IsEnabledForFields = true, want false")
	}
}
