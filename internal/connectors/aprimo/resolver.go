package aprimo

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/timw255/uplink/internal/aprimo"
)

// resolver translates a flat list of companion-script-produced field entries
// (`{name, value, language?}`) into the Aprimo API's required shape.
// The wire shape depends on the field's dataType — scalar types use
// `value: <coerced>`, list types use `values: [<resolved-ids-or-strings>]`,
// HyperlinkList has its own `hyperlinks: [{url,label}]` form. The
// resolver dispatches per-type and resolves any name references
// (classifications, option items, users, user groups, languages)
// against catalogs prefetched at connector Init.
type resolver struct {
	// fieldsByName: canonicalized name → fieldRef (ID + DataType).
	fieldsByName map[string]fieldRef

	// languagesByCulture: lowercase culture tag → languageId.
	languagesByCulture map[string]string
	// languagesByName: lowercase language display name → languageId.
	// Populated alongside culture so LanguageList values written by
	// script as either "en-US" or "English" both work.
	languagesByName map[string]string

	// defaultLanguageID resolved from cfg.DefaultLanguage at Init.
	defaultLanguageID string

	// Classification index: full namePath (slash-joined) → ID. Lookup
	// canonicalizes both "/" and " > " script separators to "/" and
	// lowercases.
	classificationsByPath map[string]string

	// optionItemsByField: fieldId → (lowercased item name → itemId).
	optionItemsByField map[string]map[string]string

	// usersByKey: email or login (lowercased) → userId.
	usersByKey map[string]string

	// userGroupsByName: lowercased group name → groupId.
	userGroupsByName map[string]string
}

type fieldRef struct {
	ID       string
	DataType string
}

// buildResolver fetches all catalogs from Aprimo and constructs an
// in-memory resolver. Called from Connector.Init AND periodically
// from the background refresher.
func buildResolver(ctx context.Context, api *aprimo.Client, defaultLanguage string) (*resolver, error) {
	fields, err := api.FieldDefinitions.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("prefetch field definitions: %w", err)
	}
	langs, err := api.Languages.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("prefetch languages: %w", err)
	}
	classifications, err := api.Classifications.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("prefetch classifications: %w", err)
	}
	users, err := api.Users.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("prefetch users: %w", err)
	}
	groups, err := api.UserGroups.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("prefetch user groups: %w", err)
	}

	r := &resolver{
		fieldsByName:          make(map[string]fieldRef, len(fields)),
		languagesByCulture:    make(map[string]string, len(langs)),
		languagesByName:       make(map[string]string, len(langs)),
		classificationsByPath: make(map[string]string, len(classifications)),
		optionItemsByField:    make(map[string]map[string]string),
		usersByKey:            make(map[string]string, len(users)*2),
		userGroupsByName:      make(map[string]string, len(groups)),
	}

	for _, f := range fields {
		key := canonicalFieldName(f.Name)
		if key == "" {
			continue
		}
		// Last write wins on duplicate names (Aprimo permits duplicate
		// display names; scripts can't disambiguate either).
		r.fieldsByName[key] = fieldRef{ID: f.ID, DataType: f.DataType}
	}

	for _, l := range langs {
		if c := strings.ToLower(strings.TrimSpace(l.Culture)); c != "" {
			r.languagesByCulture[c] = l.ID
		}
		if n := strings.ToLower(strings.TrimSpace(l.Name)); n != "" {
			r.languagesByName[n] = l.ID
		}
	}

	for _, c := range classifications {
		if c.NamePath == "" {
			continue
		}
		r.classificationsByPath[canonicalPath(c.NamePath)] = c.ID
	}

	for _, u := range users {
		if e := strings.ToLower(strings.TrimSpace(u.Email)); e != "" {
			r.usersByKey[e] = u.ID
		}
		if n := strings.ToLower(strings.TrimSpace(u.Name)); n != "" {
			r.usersByKey[n] = u.ID
		}
	}
	for _, g := range groups {
		if n := strings.ToLower(strings.TrimSpace(g.Name)); n != "" {
			r.userGroupsByName[n] = g.ID
		}
	}

	// OptionList items come back inline on the field listing, so build the
	// name→id maps straight from it — no per-field GetByID.
	for _, f := range fields {
		if f.DataType != aprimo.DataTypeOptionList {
			continue
		}
		items := make(map[string]string, len(f.OptionListItems))
		for _, it := range f.OptionListItems {
			if n := strings.ToLower(strings.TrimSpace(it.Name)); n != "" {
				items[n] = it.ID
			}
		}
		r.optionItemsByField[f.ID] = items
	}

	if defaultLanguage != "" {
		key := strings.ToLower(strings.TrimSpace(defaultLanguage))
		id, ok := r.languagesByCulture[key]
		if !ok {
			return nil, fmt.Errorf(
				"default_language %q is not configured on tenant; available cultures: %s",
				defaultLanguage, availableCultures(langs))
		}
		r.defaultLanguageID = id
	}
	return r, nil
}

// resolveFieldEntries walks the engine's flat field-entry list, looks
// each name up, dispatches per-type for value coercion + sub-entity
// resolution, and groups by field id so multi-language writes for the
// same field consolidate into one addOrUpdate entry. Each entry is
// keyed by `id` (the field-definition GUID), the property Aprimo's
// record API requires on every fields.addOrUpdate item.
func (r *resolver) resolveFieldEntries(entries []any) ([]map[string]any, error) {
	type acc struct {
		dataType  string
		localized []map[string]any
	}
	order := make([]string, 0, len(entries))
	byField := make(map[string]*acc, len(entries))

	for i, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("entry[%d]: expected a table, got %T", i, raw)
		}
		nameAny, ok := entry["name"]
		if !ok {
			return nil, fmt.Errorf("entry[%d]: missing required `name` field", i)
		}
		name, ok := nameAny.(string)
		if !ok || name == "" {
			return nil, fmt.Errorf("entry[%d]: `name` must be a non-empty string", i)
		}
		valueAny, ok := entry["value"]
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): missing required `value` field", i, name)
		}

		ref, ok := r.fieldsByName[canonicalFieldName(name)]
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): no Aprimo field definition matches this name "+
				"(check the field's display name in Aprimo, or wait for the next refresh_interval / restart the daemon)", i, name)
		}

		langID, err := r.resolveLanguage(entry["language"], name, i)
		if err != nil {
			return nil, err
		}

		localized, err := r.coerceValue(ref, valueAny, langID, name, i)
		if err != nil {
			return nil, err
		}

		if _, seen := byField[ref.ID]; !seen {
			byField[ref.ID] = &acc{dataType: ref.DataType}
			order = append(order, ref.ID)
		}
		byField[ref.ID].localized = append(byField[ref.ID].localized, localized)
	}

	out := make([]map[string]any, 0, len(order))
	for _, fid := range order {
		out = append(out, map[string]any{
			"id":              fid,
			"localizedValues": byField[fid].localized,
		})
	}
	return out, nil
}

// coerceValue produces the localizedValues entry for a single
// (field, value, language) triple. It dispatches on dataType to pick
// the right wire shape (`value` vs `values` vs `hyperlinks`) and to
// resolve any sub-entity name references.
func (r *resolver) coerceValue(ref fieldRef, raw any, langID, name string, idx int) (map[string]any, error) {
	switch ref.DataType {
	case aprimo.DataTypeSingleLineText, aprimo.DataTypeMultiLineText,
		aprimo.DataTypeHTML, aprimo.DataTypeRichContent:
		s, err := stringValue(raw, ref.DataType, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "value": s}, nil

	case aprimo.DataTypeNumeric:
		s, err := numericValue(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "value": s}, nil

	case aprimo.DataTypeDate, aprimo.DataTypeDateTime,
		aprimo.DataTypeTime, aprimo.DataTypeDuration:
		s, err := stringValue(raw, ref.DataType, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "value": s}, nil

	case aprimo.DataTypeJSON:
		s, err := jsonValue(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "value": s}, nil

	case aprimo.DataTypeTextList:
		list, err := stringList(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "values": list}, nil

	case aprimo.DataTypeClassificationList:
		list, err := r.classificationIDs(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "values": list}, nil

	case aprimo.DataTypeOptionList:
		list, err := r.optionItemIDs(ref.ID, raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "values": list}, nil

	case aprimo.DataTypeUserList:
		list, err := r.userIDs(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "values": list}, nil

	case aprimo.DataTypeUserGroupList:
		list, err := r.userGroupIDs(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "values": list}, nil

	case aprimo.DataTypeLanguageList:
		list, err := r.languageIDs(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "values": list}, nil

	case aprimo.DataTypeRecordList, aprimo.DataTypeRecordLink:
		// Phase 1: no record-name resolution. Operators pass IDs.
		list, err := stringList(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "values": list}, nil

	case aprimo.DataTypeHyperlinkList:
		links, err := hyperlinkList(raw, name, idx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"languageId": langID, "hyperlinks": links}, nil

	case "":
		return nil, fmt.Errorf("entry[%d] (%q): field has no dataType (catalog incomplete; restart to refresh)", idx, name)
	default:
		return nil, fmt.Errorf("entry[%d] (%q): unsupported dataType %q", idx, name, ref.DataType)
	}
}

// --- per-type coercion helpers ------------------------------------------

func stringValue(raw any, dataType, name string, idx int) (string, error) {
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("entry[%d] (%q): %s field expects a string value, got %T", idx, name, dataType, raw)
	}
	return s, nil
}

// numericValue formats a script-side number (int64 or float64) into
// the InvariantCulture decimal string Aprimo expects. A string value
// is also accepted (passed through, trimmed) for cases where the
// script formatted the number itself.
func numericValue(raw any, name string, idx int) (string, error) {
	switch v := raw.(type) {
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int:
		return strconv.Itoa(v), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case string:
		return strings.TrimSpace(v), nil
	default:
		return "", fmt.Errorf("entry[%d] (%q): numeric field expects a number or string, got %T", idx, name, raw)
	}
}

// jsonValue stringifies a script-supplied value for a JSON field.
// Aprimo stores the value as a string containing valid JSON, so a
// Lua table comes through as `{...}`-encoded JSON.
func jsonValue(raw any, name string, idx int) (string, error) {
	if s, ok := raw.(string); ok {
		// Trust the script: validate it parses as JSON, then return.
		var probe any
		if err := json.Unmarshal([]byte(s), &probe); err != nil {
			return "", fmt.Errorf("entry[%d] (%q): JSON field's string value is not valid JSON: %w", idx, name, err)
		}
		return s, nil
	}
	bs, err := json.Marshal(raw)
	if err != nil {
		return "", fmt.Errorf("entry[%d] (%q): cannot marshal value to JSON: %w", idx, name, err)
	}
	return string(bs), nil
}

// stringList coerces raw into []string. A bare string becomes a
// 1-element list (convenience for single-value list fields).
func stringList(raw any, name string, idx int) ([]string, error) {
	switch v := raw.(type) {
	case string:
		return []string{v}, nil
	case []any:
		out := make([]string, 0, len(v))
		for j, elem := range v {
			s, ok := elem.(string)
			if !ok {
				return nil, fmt.Errorf("entry[%d] (%q): list[%d] must be a string, got %T", idx, name, j, elem)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("entry[%d] (%q): expected a string or list of strings, got %T", idx, name, raw)
	}
}

func (r *resolver) classificationIDs(raw any, name string, idx int) ([]string, error) {
	names, err := stringList(raw, name, idx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		// If the script supplied a GUID-ish string we'd allow pass-through,
		// but resolving by name keeps the surface uniform. Operators with
		// IDs in hand can use the pre-shaped map escape hatch.
		id, ok := r.classificationsByPath[canonicalPath(n)]
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): unknown classification %q "+
				"(use the full namePath, e.g., \"Topics/Sports/Football\" or \"Topics > Sports > Football\")", idx, name, n)
		}
		out = append(out, id)
	}
	return out, nil
}

func (r *resolver) optionItemIDs(fieldID string, raw any, name string, idx int) ([]string, error) {
	names, err := stringList(raw, name, idx)
	if err != nil {
		return nil, err
	}
	items, ok := r.optionItemsByField[fieldID]
	if !ok {
		return nil, fmt.Errorf("entry[%d] (%q): option items not loaded for this field — daemon was likely started before the field was created", idx, name)
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		id, ok := items[strings.ToLower(strings.TrimSpace(n))]
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): unknown option-list item %q for this field", idx, name, n)
		}
		out = append(out, id)
	}
	return out, nil
}

func (r *resolver) userIDs(raw any, name string, idx int) ([]string, error) {
	names, err := stringList(raw, name, idx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		id, ok := r.usersByKey[strings.ToLower(strings.TrimSpace(n))]
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): unknown user %q (look up by email or login)", idx, name, n)
		}
		out = append(out, id)
	}
	return out, nil
}

func (r *resolver) userGroupIDs(raw any, name string, idx int) ([]string, error) {
	names, err := stringList(raw, name, idx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		id, ok := r.userGroupsByName[strings.ToLower(strings.TrimSpace(n))]
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): unknown user group %q", idx, name, n)
		}
		out = append(out, id)
	}
	return out, nil
}

func (r *resolver) languageIDs(raw any, name string, idx int) ([]string, error) {
	names, err := stringList(raw, name, idx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		key := strings.ToLower(strings.TrimSpace(n))
		id, ok := r.languagesByCulture[key]
		if !ok {
			id, ok = r.languagesByName[key]
		}
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): unknown language %q (use the IETF culture tag like \"en-US\" or the display name)", idx, name, n)
		}
		out = append(out, id)
	}
	return out, nil
}

// hyperlinkList accepts a list of `{url=..., label=...}` tables and
// produces the Aprimo `hyperlinks: [{url, label?}]` shape. A bare
// string is rejected — hyperlinks need at minimum a URL keyed under
// `url` so the operator is explicit.
func hyperlinkList(raw any, name string, idx int) ([]map[string]any, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("entry[%d] (%q): hyperlink list expects a list of {url, label} tables, got %T", idx, name, raw)
	}
	out := make([]map[string]any, 0, len(list))
	for j, elem := range list {
		m, ok := elem.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): hyperlink[%d] must be a table with `url`, got %T", idx, name, j, elem)
		}
		urlAny, ok := m["url"]
		if !ok {
			return nil, fmt.Errorf("entry[%d] (%q): hyperlink[%d] missing required `url`", idx, name, j)
		}
		url, ok := urlAny.(string)
		if !ok || url == "" {
			return nil, fmt.Errorf("entry[%d] (%q): hyperlink[%d].url must be a non-empty string", idx, name, j)
		}
		hl := map[string]any{"url": url}
		if labelAny, has := m["label"]; has {
			if label, ok := labelAny.(string); ok {
				hl["label"] = label
			}
		}
		out = append(out, hl)
	}
	return out, nil
}

// --- language resolution -------------------------------------------------

func (r *resolver) resolveLanguage(raw any, entryName string, entryIdx int) (string, error) {
	if raw == nil {
		if r.defaultLanguageID == "" {
			return "", fmt.Errorf("entry[%d] (%q): no `language` specified and the aprimo connector has no `default_language` configured", entryIdx, entryName)
		}
		return r.defaultLanguageID, nil
	}
	culture, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("entry[%d] (%q): `language` must be a string", entryIdx, entryName)
	}
	culture = strings.TrimSpace(culture)
	if culture == "" {
		if r.defaultLanguageID == "" {
			return "", fmt.Errorf("entry[%d] (%q): `language` is empty and no default_language is configured", entryIdx, entryName)
		}
		return r.defaultLanguageID, nil
	}
	id, ok := r.languagesByCulture[strings.ToLower(culture)]
	if !ok {
		return "", fmt.Errorf("entry[%d] (%q): unknown language culture %q (configure it in Aprimo's Admin → Languages and restart the daemon)", entryIdx, entryName, culture)
	}
	return id, nil
}

// --- small helpers -------------------------------------------------------

func canonicalFieldName(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// canonicalPath normalizes a classification path: trims, lowercases,
// and collapses "/" and " > " separators to a single "/". So
// "Topics / Sports / Football", "Topics > Sports > Football", and
// "topics/sports/football" all canonicalize to the same key.
func canonicalPath(s string) string {
	s = strings.TrimSpace(s)
	// Normalize " > " separator → "/".
	s = strings.ReplaceAll(s, " > ", "/")
	s = strings.ReplaceAll(s, ">", "/")
	// Collapse whitespace around slashes.
	parts := strings.Split(s, "/")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return strings.ToLower(strings.Join(parts, "/"))
}

func availableCultures(langs []aprimo.Language) string {
	cultures := make([]string, 0, len(langs))
	for _, l := range langs {
		if l.Culture != "" {
			cultures = append(cultures, l.Culture)
		}
	}
	sort.Strings(cultures)
	return strings.Join(cultures, ", ")
}

// silence unused if a later refactor stops using context.
var _ = context.Background
