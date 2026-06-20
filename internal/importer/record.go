// Package importer drives a one-shot bulk load of assets and metadata
// into Aprimo from a JSONL manifest — the migration counterpart to the
// streaming daemon. Each line describes one record:
//
//   - has "id", no "file"      → PATCH metadata onto the existing record
//   - has "id" and "file"      → upload "file" as a new version + metadata
//   - no "id", has "file"      → upload "file", create a record + metadata
//   - neither                  → error
//
// The importer reuses the Aprimo connector's existing Write /
// WriteMetadata paths (and the same field resolver companion and enrich
// scripts use), so a manifest's fields[] honor the identical
// {name, value, language?} contract. It never opens the daemon's store,
// data dir, or lockfile — the only durable output is Aprimo itself plus
// an optional results ledger.
package importer

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Record is one JSONL manifest line. Only the four reserved keys are
// recognized; metadata lives in Fields under the same contract the
// daemon's companion and enrich scripts produce.
type Record struct {
	// ID, when present, targets an existing Aprimo record. Without a
	// File this is a metadata-only PATCH; with a File the upload lands
	// as a new version on that record.
	ID string `json:"id,omitempty"`

	// File is the asset's path within the --source connector. Required
	// when ID is absent (there is nothing to attach the upload to
	// otherwise).
	File string `json:"file,omitempty"`

	// Status optionally overrides the record's lifecycle status
	// (draft|released|archived). Empty leaves it to the connector's
	// default_status on create, or unchanged on update.
	Status string `json:"status,omitempty"`

	// Fields carries the metadata, each entry resolved by display name
	// against the live Aprimo catalog.
	Fields []FieldEntry `json:"fields,omitempty"`
}

// FieldEntry mirrors the {name, value, language?} shape a companion or
// enrich script returns. Value is whatever JSON produced (string,
// number, bool, list, or object) — the resolver coerces it per the
// field's Aprimo data type.
type FieldEntry struct {
	Name     string `json:"name"`
	Value    any    `json:"value"`
	Language string `json:"language,omitempty"`
}

// Action classifies what the importer will do with a record.
type Action string

const (
	// ActionCreated uploads File and creates a new record.
	ActionCreated Action = "created"
	// ActionUpdated uploads File as a new version on the existing record
	// identified by ID, plus any metadata.
	ActionUpdated Action = "updated"
	// ActionMetadata PATCHes metadata onto the existing record (no upload).
	ActionMetadata Action = "metadata"
	// ActionUploaded is an intermediate ledger-only state: the file's
	// bytes were uploaded (token in hand) but the record isn't created
	// yet. It is never counted in the summary; resume reads it to skip a
	// re-upload.
	ActionUploaded Action = "uploaded"
	// ActionFiled is a ledger-only marker row: the records listed in its
	// Filed field were filed into the default collection. Never counted in
	// the summary; resume reads it to skip re-filing already-filed records.
	ActionFiled Action = "filed"
)

// validStatuses is the set Aprimo accepts for a record's lifecycle
// status, matched case-insensitively.
var validStatuses = map[string]struct{}{
	"draft": {}, "released": {}, "archived": {},
}

// normalizedStatus lowercases the record's status for the wire and for
// validation, returning "" when unset.
func (r Record) normalizedStatus() string {
	return strings.ToLower(strings.TrimSpace(r.Status))
}

// validate checks the structural rules that don't need the Aprimo
// catalog: the id-or-file requirement and a known status value. Field
// resolution is validated separately (it needs the connector).
func (r Record) validate() error {
	if r.ID == "" && r.File == "" {
		return fmt.Errorf(`record needs either "id" (to update an existing record) or "file" (to upload a new asset)`)
	}
	if s := r.normalizedStatus(); s != "" {
		if _, ok := validStatuses[s]; !ok {
			return fmt.Errorf("invalid status %q (want draft, released, or archived)", r.Status)
		}
	}
	return nil
}

// action returns the operation the record maps to. Precondition:
// validate passed, so at least one of ID/File is set.
func (r Record) action() Action {
	switch {
	case r.ID != "" && r.File != "":
		return ActionUpdated
	case r.ID != "":
		return ActionMetadata
	default:
		return ActionCreated
	}
}

// meta builds the map the Aprimo connector's Write / WriteMetadata
// consume. dest_fields uses the same []any-of-maps shape the resolver
// expects from companion scripts.
func (r Record) meta() map[string]any {
	m := make(map[string]any, 3)
	if r.ID != "" {
		m["dest_id"] = r.ID
	}
	if s := r.normalizedStatus(); s != "" {
		m["dest_status"] = s
	}
	if len(r.Fields) > 0 {
		entries := make([]any, 0, len(r.Fields))
		for _, f := range r.Fields {
			fm := map[string]any{"name": f.Name, "value": f.Value}
			if f.Language != "" {
				fm["language"] = f.Language
			}
			entries = append(entries, fm)
		}
		m["dest_fields"] = entries
	}
	return m
}

// parseLine decodes one manifest line into a Record. Unknown top-level
// keys are always rejected, so typos and raw-export columns that belong
// under fields[] fail loudly rather than getting silently dropped.
func parseLine(raw []byte) (Record, error) {
	var rec Record
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return Record{}, fmt.Errorf("%w (move export columns into a \"fields\" array)", err)
		}
		return Record{}, err
	}
	return rec, nil
}

// ScanFieldUsage streams the manifest and returns the distinct field names it
// references and whether any entry specifies a language. The import command
// passes these to the Aprimo connector so it prefetches only the catalogs the
// run's field types need. Malformed lines are tolerated here (they contribute
// nothing) — the run itself reports parse errors per line.
func ScanFieldUsage(path string) (fieldNames []string, usesLanguage bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = f.Close() }()

	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		raw := bytes.TrimSpace(sc.Bytes())
		if len(raw) == 0 {
			continue
		}
		var line struct {
			Fields []FieldEntry `json:"fields"`
		}
		if json.Unmarshal(raw, &line) != nil {
			continue
		}
		for _, fe := range line.Fields {
			if fe.Name != "" && !seen[fe.Name] {
				seen[fe.Name] = true
				fieldNames = append(fieldNames, fe.Name)
			}
			if strings.TrimSpace(fe.Language) != "" {
				usesLanguage = true
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, false, err
	}
	return fieldNames, usesLanguage, nil
}

// hashLine returns a short, stable fingerprint of a manifest line, used
// to skip already-completed lines when resuming. Whitespace is trimmed so
// trivial reformatting doesn't defeat the match.
func hashLine(raw []byte) string {
	sum := sha256.Sum256(bytes.TrimSpace(raw))
	return hex.EncodeToString(sum[:8])
}
