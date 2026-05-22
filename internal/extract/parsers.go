package extract

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// parseJSON decodes s into a Lua-side table. Top-level arrays come back
// as Lua sequences (integer-keyed 1..N); objects come back as string-
// keyed tables. Numbers use json.Number to preserve integer-ness so
// "id":1234 doesn't round-trip through float64 and surprise scripts
// reading bigger IDs.
func parseJSON(L *lua.LState, s string) (lua.LValue, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return jsonValueToLua(L, v), nil
}

func jsonValueToLua(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(x)
	case string:
		return lua.LString(x)
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return lua.LNumber(i)
		}
		if f, err := x.Float64(); err == nil {
			return lua.LNumber(f)
		}
		return lua.LString(x.String())
	case float64:
		return lua.LNumber(x)
	case []any:
		t := L.NewTable()
		for i, elem := range x {
			t.RawSetInt(i+1, jsonValueToLua(L, elem))
		}
		return t
	case map[string]any:
		t := L.NewTable()
		for k, elem := range x {
			t.RawSetString(k, jsonValueToLua(L, elem))
		}
		return t
	default:
		return lua.LString(fmt.Sprintf("%v", x))
	}
}

// parseXML decodes s into a Lua table using a simple element-to-table
// model. The same shape encoding/xml's stdlib decoder produces with
// generic any-typed targets is unwieldy for Lua-land, so we build it
// ourselves: every element becomes a table with attributes flattened
// in (no `@attr` prefix — scripts read dc:rights as `desc["dc:rights"]`,
// which is what they intuitively want) and text-only leaves collapse
// to their string value.
//
// Repeated child elements with the same name collect into a sequence,
// so `<rdf:li>foo</rdf:li><rdf:li>bar</rdf:li>` reads back as
// `{"foo", "bar"}` rather than overwriting.
func parseXML(L *lua.LState, s string) (lua.LValue, error) {
	dec := xml.NewDecoder(strings.NewReader(s))
	// Tolerant defaults — we're reading real-world XMP, not a strict
	// schema. CharsetReader returning the input as-is is fine for the
	// metadata cases scripts will deal with.
	dec.Strict = false
	dec.CharsetReader = func(_ string, r io.Reader) (io.Reader, error) { return r, nil }

	root := L.NewTable()
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if start, ok := tok.(xml.StartElement); ok {
			name := xmlElemName(start.Name)
			child, err := readXMLElement(L, dec, start)
			if err != nil {
				return nil, err
			}
			addXMLChild(L, root, name, child)
		}
	}
	return root, nil
}

func readXMLElement(L *lua.LState, dec *xml.Decoder, start xml.StartElement) (lua.LValue, error) {
	tbl := L.NewTable()
	for _, attr := range start.Attr {
		tbl.RawSetString(xmlElemName(attr.Name), lua.LString(attr.Value))
	}
	var text strings.Builder
	hasChildren := false
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			hasChildren = true
			name := xmlElemName(t.Name)
			child, err := readXMLElement(L, dec, t)
			if err != nil {
				return nil, err
			}
			addXMLChild(L, tbl, name, child)
		case xml.CharData:
			text.Write(t)
		case xml.EndElement:
			// Leaf-with-only-text optimization: a pure-text element with
			// no attributes returns the string directly.
			trimmed := strings.TrimSpace(text.String())
			if !hasChildren && len(start.Attr) == 0 {
				return lua.LString(trimmed), nil
			}
			if trimmed != "" {
				tbl.RawSetString("#text", lua.LString(trimmed))
			}
			return tbl, nil
		}
	}
}

func xmlElemName(n xml.Name) string {
	if n.Space == "" {
		return n.Local
	}
	// Preserve the namespace-prefixed shape the XML source used. The
	// XMP examples in the README rely on `desc["dc:title"]` working
	// directly. encoding/xml resolves prefixes to full URIs by default;
	// we re-derive a prefix-style key from the URI for the well-known
	// XMP namespaces, falling back to the URI for unknowns.
	if pfx, ok := wellKnownXMLPrefix[n.Space]; ok {
		return pfx + ":" + n.Local
	}
	return n.Local
}

// Common namespaces used in XMP. Not exhaustive — operators with unusual
// schemas can still reach the element via the local name. Picked so the
// README examples ("dc:rights", "dc:creator") work out of the box.
var wellKnownXMLPrefix = map[string]string{
	"http://purl.org/dc/elements/1.1/":  "dc",
	"http://ns.adobe.com/xap/1.0/":      "xmp",
	"http://ns.adobe.com/xap/1.0/rights":"xmpRights",
	"http://ns.adobe.com/photoshop/1.0/":"photoshop",
	"http://iptc.org/std/Iptc4xmpCore/1.0/xmlns/":"Iptc4xmpCore",
	"http://www.w3.org/1999/02/22-rdf-syntax-ns#":"rdf",
	"adobe:ns:meta/":                    "x",
}

// addXMLChild attaches a child to a parent table, promoting to a
// sequence on repeat keys so XMP `rdf:li` lists round-trip cleanly.
// First occurrence: parent[name] = child. Second occurrence: replace
// with {first, child}. Third onward: Append to the existing sequence.
// We track "this slot has been promoted" by checking whether the
// existing value is a sequence-shaped table whose first element is
// what we'd have left as a plain value — but the cheaper invariant
// is to mark the slot with an integer key 1 on first promotion and
// check for that on subsequent inserts.
func addXMLChild(L *lua.LState, parent *lua.LTable, name string, child lua.LValue) {
	existing := parent.RawGetString(name)
	if existing == lua.LNil {
		parent.RawSetString(name, child)
		return
	}
	// If the existing value is already a sequence we created (has at
	// least one integer-indexed entry at key 1), just append.
	if t, ok := existing.(*lua.LTable); ok && t.RawGetInt(1) != lua.LNil {
		t.Append(child)
		return
	}
	// Promote first occurrence into a fresh sequence.
	seq := L.NewTable()
	seq.Append(existing)
	seq.Append(child)
	parent.RawSetString(name, seq)
}

// parseCSV decodes s as CSV. opts is a Lua table with optional fields:
//   - delimiter: single-char string, defaults to ","
//   - header_row: bool; if true, each row is a string-keyed table using
//     the first row as field names; otherwise rows are integer-keyed.
//   - comment: single-char string; lines starting with it are skipped.
func parseCSV(L *lua.LState, s string, opts *lua.LTable) (lua.LValue, error) {
	r := csv.NewReader(strings.NewReader(s))
	r.FieldsPerRecord = -1
	r.LazyQuotes = true

	headerRow := false
	if opts != nil {
		if d, ok := opts.RawGetString("delimiter").(lua.LString); ok && len(d) > 0 {
			runes := []rune(string(d))
			r.Comma = runes[0]
		}
		if c, ok := opts.RawGetString("comment").(lua.LString); ok && len(c) > 0 {
			runes := []rune(string(c))
			r.Comment = runes[0]
		}
		if h, ok := opts.RawGetString("header_row").(lua.LBool); ok {
			headerRow = bool(h)
		}
	}

	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	out := L.NewTable()
	if headerRow && len(rows) > 0 {
		header := rows[0]
		for i, row := range rows[1:] {
			rec := L.NewTable()
			for j, cell := range row {
				if j < len(header) {
					rec.RawSetString(header[j], lua.LString(cell))
				} else {
					rec.RawSetInt(j+1, lua.LString(cell))
				}
			}
			out.RawSetInt(i+1, rec)
		}
		return out, nil
	}
	for i, row := range rows {
		rec := L.NewTable()
		for j, cell := range row {
			rec.RawSetInt(j+1, lua.LString(cell))
		}
		out.RawSetInt(i+1, rec)
	}
	return out, nil
}
