package aprimo

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/timw255/uplink/internal/aprimo"
)

// newTestResolver builds a resolver populated with one of every
// dataType the production code handles. Keeps every test in this
// file consistent.
func newTestResolver() *resolver {
	return &resolver{
		fieldsByName: map[string]fieldRef{
			"caption":        {ID: "fld-caption", DataType: aprimo.DataTypeSingleLineText},
			"rights holder":  {ID: "fld-rights", DataType: aprimo.DataTypeSingleLineText},
			"client":         {ID: "fld-client", DataType: aprimo.DataTypeSingleLineText},
			"word count":     {ID: "fld-wc", DataType: aprimo.DataTypeNumeric},
			"shot date":      {ID: "fld-date", DataType: aprimo.DataTypeDate},
			"raw meta":       {ID: "fld-json", DataType: aprimo.DataTypeJSON},
			"keywords":       {ID: "fld-kw", DataType: aprimo.DataTypeTextList},
			"topics":         {ID: "fld-topics", DataType: aprimo.DataTypeClassificationList},
			"department":     {ID: "fld-dept", DataType: aprimo.DataTypeOptionList},
			"owner":          {ID: "fld-owner", DataType: aprimo.DataTypeUserList},
			"review team":    {ID: "fld-rev", DataType: aprimo.DataTypeUserGroupList},
			"related":        {ID: "fld-rel", DataType: aprimo.DataTypeRecordList},
			"links":          {ID: "fld-links", DataType: aprimo.DataTypeHyperlinkList},
			"interface lang": {ID: "fld-ilang", DataType: aprimo.DataTypeLanguageList},
		},
		languagesByCulture: map[string]string{
			"en-us": "lang-en",
			"fr-fr": "lang-fr",
			"ja-jp": "lang-jp",
		},
		languagesByName: map[string]string{
			"english":  "lang-en",
			"french":   "lang-fr",
			"japanese": "lang-jp",
		},
		defaultLanguageID: "lang-en",
		classificationsByPath: map[string]string{
			"topics":                 "cls-topics",
			"topics/sports":          "cls-sports",
			"topics/sports/football": "cls-football",
		},
		optionItemsByField: map[string]map[string]string{
			"fld-dept": {
				"marketing":   "opt-marketing",
				"engineering": "opt-engineering",
			},
		},
		usersByKey: map[string]string{
			"tim@acme.com":   "u-tim",
			"alice@acme.com": "u-alice",
			"tim":            "u-tim",
		},
		userGroupsByName: map[string]string{
			"editors":   "g-editors",
			"reviewers": "g-reviewers",
		},
	}
}

// --- scalar types ---

func TestResolver_TextScalar(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Caption", "value": "Sunset"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	lv := out[0]["localizedValues"].([]map[string]any)[0]
	if lv["value"] != "Sunset" || lv["languageId"] != "lang-en" {
		t.Errorf("lv = %+v", lv)
	}
	// No `values` key on scalar.
	if _, has := lv["values"]; has {
		t.Errorf("scalar must not produce `values`: %v", lv)
	}
}

func TestResolver_NumericFormatsAsInvariantString(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Word Count", "value": int64(1234)},
		map[string]any{"name": "Word Count", "value": 3.14},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	lvs := out[0]["localizedValues"].([]map[string]any)
	if lvs[0]["value"] != "1234" {
		t.Errorf("int → %v", lvs[0]["value"])
	}
	if lvs[1]["value"] != "3.14" {
		t.Errorf("float → %v", lvs[1]["value"])
	}
}

func TestResolver_JSONStringifiesTables(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Raw Meta", "value": map[string]any{"camera": "R5", "iso": int64(400)}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	s := out[0]["localizedValues"].([]map[string]any)[0]["value"].(string)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		t.Fatalf("not valid JSON: %v (%q)", err, s)
	}
	if parsed["camera"] != "R5" {
		t.Errorf("camera = %v", parsed["camera"])
	}
}

func TestResolver_JSONStringPassthrough(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Raw Meta", "value": `{"ok":true}`},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := out[0]["localizedValues"].([]map[string]any)[0]["value"]; got != `{"ok":true}` {
		t.Errorf("value = %v", got)
	}
}

func TestResolver_JSONStringInvalidRejected(t *testing.T) {
	r := newTestResolver()
	_, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Raw Meta", "value": "not-json"},
	})
	if err == nil || !strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("expected JSON-validation error, got %v", err)
	}
}

// --- list types ---

func TestResolver_TextList(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Keywords", "value": []any{"sunset", "mountain", "travel"}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	lv := out[0]["localizedValues"].([]map[string]any)[0]
	want := []string{"sunset", "mountain", "travel"}
	if got, _ := lv["values"].([]string); !reflect.DeepEqual(got, want) {
		t.Errorf("values = %v, want %v", lv["values"], want)
	}
	if _, has := lv["value"]; has {
		t.Errorf("list must not produce singular `value`: %v", lv)
	}
}

func TestResolver_ClassificationsByPath(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Topics", "value": []any{"Topics > Sports > Football", "Topics/Sports"}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got, _ := out[0]["localizedValues"].([]map[string]any)[0]["values"].([]string)
	want := []string{"cls-football", "cls-sports"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("classification IDs = %v, want %v", got, want)
	}
}

func TestResolver_ClassificationUnknownErrors(t *testing.T) {
	r := newTestResolver()
	_, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Topics", "value": "Topics/Nope"},
	})
	if err == nil || !strings.Contains(err.Error(), "Topics/Nope") {
		t.Fatalf("expected unknown-classification error, got %v", err)
	}
}

func TestResolver_OptionListByItemName(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Department", "value": "Marketing"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got, _ := out[0]["localizedValues"].([]map[string]any)[0]["values"].([]string)
	if !reflect.DeepEqual(got, []string{"opt-marketing"}) {
		t.Errorf("got = %v", got)
	}
}

func TestResolver_UsersByEmailOrLogin(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Owner", "value": []any{"tim@acme.com", "alice@acme.com"}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got, _ := out[0]["localizedValues"].([]map[string]any)[0]["values"].([]string)
	if !reflect.DeepEqual(got, []string{"u-tim", "u-alice"}) {
		t.Errorf("got = %v", got)
	}
}

func TestResolver_UserGroups(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Review Team", "value": []any{"Editors", "Reviewers"}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got, _ := out[0]["localizedValues"].([]map[string]any)[0]["values"].([]string)
	if !reflect.DeepEqual(got, []string{"g-editors", "g-reviewers"}) {
		t.Errorf("got = %v", got)
	}
}

func TestResolver_RecordListPassesIDsThrough(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Related", "value": []any{"rec-1", "rec-2"}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got, _ := out[0]["localizedValues"].([]map[string]any)[0]["values"].([]string)
	if !reflect.DeepEqual(got, []string{"rec-1", "rec-2"}) {
		t.Errorf("got = %v", got)
	}
}

func TestResolver_LanguageListByCultureOrName(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Interface Lang", "value": []any{"fr-FR", "Japanese"}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	got, _ := out[0]["localizedValues"].([]map[string]any)[0]["values"].([]string)
	if !reflect.DeepEqual(got, []string{"lang-fr", "lang-jp"}) {
		t.Errorf("got = %v", got)
	}
}

func TestResolver_HyperlinkList(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Links", "value": []any{
			map[string]any{"url": "https://x/y", "label": "X"},
			map[string]any{"url": "https://a/b"},
		}},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	links, _ := out[0]["localizedValues"].([]map[string]any)[0]["hyperlinks"].([]map[string]any)
	if len(links) != 2 {
		t.Fatalf("expected 2, got %d", len(links))
	}
	if links[0]["url"] != "https://x/y" || links[0]["label"] != "X" {
		t.Errorf("links[0] = %+v", links[0])
	}
	if _, has := links[1]["label"]; has {
		t.Errorf("links[1] should have no label: %+v", links[1])
	}
}

// --- multi-language + consolidation ---

func TestResolver_MultiLanguageSameFieldConsolidates(t *testing.T) {
	r := newTestResolver()
	out, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Caption", "value": "Sunset", "language": "en-US"},
		map[string]any{"name": "Caption", "value": "Coucher de soleil", "language": "fr-FR"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 consolidated entry, got %d", len(out))
	}
	lvs := out[0]["localizedValues"].([]map[string]any)
	if len(lvs) != 2 {
		t.Fatalf("expected 2 localizedValues, got %d", len(lvs))
	}
}

// --- error surfaces ---

func TestResolver_UnknownFieldName(t *testing.T) {
	r := newTestResolver()
	_, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Photographer", "value": "Alice"},
	})
	if err == nil || !strings.Contains(err.Error(), "Photographer") {
		t.Fatalf("expected unknown-field error: %v", err)
	}
}

func TestResolver_MissingValueRejected(t *testing.T) {
	r := newTestResolver()
	_, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Caption"},
	})
	if err == nil || !strings.Contains(err.Error(), "value") {
		t.Fatalf("expected missing-value error: %v", err)
	}
}

func TestResolver_NoDefaultLanguageRequiresExplicit(t *testing.T) {
	r := newTestResolver()
	r.defaultLanguageID = ""
	_, err := r.resolveFieldEntries([]any{
		map[string]any{"name": "Caption", "value": "x"},
	})
	if err == nil || !strings.Contains(err.Error(), "default_language") {
		t.Fatalf("expected default_language error: %v", err)
	}
}

// --- canonical path helper ---

func TestCanonicalPath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Topics", "topics"},
		{"Topics/Sports", "topics/sports"},
		{"Topics > Sports", "topics/sports"},
		{"Topics  >  Sports", "topics/sports"},
		{" topics / sports / football ", "topics/sports/football"},
		{"Topics>Sports>Football", "topics/sports/football"},
	}
	for _, c := range cases {
		if got := canonicalPath(c.in); got != c.want {
			t.Errorf("canonicalPath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
