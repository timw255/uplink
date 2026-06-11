package aprimo

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// connectorWithResolver builds a minimal Connector wired to a stock
// test resolver. Just enough to exercise fieldsFromMeta — no HTTP
// client, no store.
func connectorWithResolver() *Connector {
	c := &Connector{name: "trial"}
	c.resolver.Store(newTestResolver())
	return c
}

// TestFieldsFromMeta_ResolvesAndWrapsList confirms the boundary:
// companion scripts emit `{name, value}` entries; this is where they
// get translated into Aprimo's `{id, localizedValues:
// [{languageId, value}]}` shape and wrapped in `addOrUpdate`. The
// resolver dispatches per-type — this test just spot-checks the
// integration shape.
func TestFieldsFromMeta_ResolvesAndWrapsList(t *testing.T) {
	c := connectorWithResolver()
	meta := map[string]any{
		"dest_fields": []any{
			map[string]any{"name": "Client", "value": "Acme"},
			map[string]any{"name": "Caption", "value": "Hello", "language": "fr-FR"},
		},
	}
	got, err := c.fieldsFromMeta(meta)
	if err != nil {
		t.Fatalf("fieldsFromMeta: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil RawMessage")
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	add, ok := decoded["addOrUpdate"].([]any)
	if !ok {
		t.Fatalf("addOrUpdate is not an array: %T (%v)", decoded["addOrUpdate"], decoded)
	}
	if len(add) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(add), add)
	}
	first := add[0].(map[string]any)
	if first["id"] != "fld-client" {
		t.Errorf("first.id = %v, want fld-client", first["id"])
	}
	firstLV := first["localizedValues"].([]any)[0].(map[string]any)
	if firstLV["languageId"] != "lang-en" {
		t.Errorf("first default language wrong: %v", firstLV["languageId"])
	}
	if firstLV["value"] != "Acme" {
		t.Errorf("first value wrong: %v", firstLV["value"])
	}
	second := add[1].(map[string]any)
	secondLV := second["localizedValues"].([]any)[0].(map[string]any)
	if secondLV["languageId"] != "lang-fr" {
		t.Errorf("explicit fr-FR not honored: %v", secondLV["languageId"])
	}
}

// TestFieldsFromMeta_UnknownNameSurfacesError verifies the resolver
// error reaches the caller — Create/Update returns the error and the
// job fails loudly instead of silently shipping a record without
// metadata.
func TestFieldsFromMeta_UnknownNameSurfacesError(t *testing.T) {
	c := connectorWithResolver()
	meta := map[string]any{
		"dest_fields": []any{
			map[string]any{"name": "NotAField", "value": "x"},
		},
	}
	_, err := c.fieldsFromMeta(meta)
	if err == nil {
		t.Fatal("expected error for unknown field name")
	}
	if !strings.Contains(err.Error(), "NotAField") {
		t.Errorf("error should quote the unknown name: %v", err)
	}
}

func TestFieldsFromMeta_EmptyListYieldsNil(t *testing.T) {
	c := connectorWithResolver()
	meta := map[string]any{"dest_fields": []any{}}
	got, err := c.fieldsFromMeta(meta)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for empty list, got: %s", string(got))
	}
}

// TestFieldsFromMeta_PassThroughPreShapedMap covers power-users who
// supply dest_fields as a fully-shaped map (e.g., for remove/clear
// operations not reachable through the {name, value} shape). The map
// passes through unwrapped — they're responsible for using id +
// localizedValues themselves.
func TestFieldsFromMeta_PassThroughPreShapedMap(t *testing.T) {
	c := connectorWithResolver()
	preShaped := map[string]any{
		"addOrUpdate": []any{
			map[string]any{
				"id":              "fld-pre",
				"localizedValues": []any{map[string]any{"languageId": "lang-en", "value": "y"}},
			},
		},
		"remove": []any{"fld-z"},
	}
	meta := map[string]any{"dest_fields": preShaped}
	got, err := c.fieldsFromMeta(meta)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil")
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded["remove"], []any{"fld-z"}) {
		t.Fatalf("remove = %v, want [fld-z]", decoded["remove"])
	}
}

func TestFieldsFromMeta_NoMetaReturnsNil(t *testing.T) {
	c := connectorWithResolver()
	got, err := c.fieldsFromMeta(nil)
	if err != nil || got != nil {
		t.Fatalf("nil meta: got=%v err=%v", got, err)
	}
	got, err = c.fieldsFromMeta(map[string]any{})
	if err != nil || got != nil {
		t.Fatalf("empty meta: got=%v err=%v", got, err)
	}
}
