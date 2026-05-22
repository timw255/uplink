package aprimo

import (
	"errors"
	"net/http"
	"testing"
)

// The canonical body Aprimo returns when /api/core/records (or
// /api/core/record/{id}) receives a token referencing a non-existent
// upload. Captured from a live trial environment via the diagnostic
// probe; see project memory `project_upload_token_purge` for context.
const noDataFoundBodyTemplate = `{"exceptionType":"Adam.Rest.Common.NoDataFoundException","exceptionMessage":"Cannot find the uploaded file specified with the token 'tok-xyz'.","stackTrace":null,"innerException":null}`

func TestErrorIs_UploadTokenMissing_PositiveCase(t *testing.T) {
	err := newHTTPError(
		http.StatusNotFound,
		[]byte(noDataFoundBodyTemplate),
		"Adam.Rest.Common.NoDataFoundException",
		"Cannot find the uploaded file specified with the token 'tok-xyz'.",
		nil,
	)
	if !errors.Is(err, ErrUploadTokenMissing) {
		t.Fatalf("expected errors.Is to match ErrUploadTokenMissing, got false")
	}
	// Also matches ErrNotFound — that's expected and harmless.
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the same error to ALSO match ErrNotFound")
	}
}

func TestErrorIs_UploadTokenMissing_RecordNotFoundDoesNotMatch(t *testing.T) {
	// PUT /api/core/record/{id} for a missing record id returns the same
	// status + exceptionType, but the body refers to the record id, NOT a
	// token. Must NOT be classified as a token-missing error, or we'd
	// re-upload after every record-renamed-out-from-under-us.
	body := `{"exceptionType":"Adam.Rest.Common.NoDataFoundException","exceptionMessage":"Cannot find the record with id 'rec-gone'.","stackTrace":null,"innerException":null}`
	err := newHTTPError(
		http.StatusNotFound,
		[]byte(body),
		"Adam.Rest.Common.NoDataFoundException",
		"Cannot find the record with id 'rec-gone'.",
		nil,
	)
	if errors.Is(err, ErrUploadTokenMissing) {
		t.Fatalf("record-not-found 404 must NOT match ErrUploadTokenMissing")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound match for a generic 404")
	}
}

func TestErrorIs_UploadTokenMissing_WrongStatus(t *testing.T) {
	// A 400 with the same body shape must not match. The sentinel keys
	// off the documented status code, not the body alone, so a future
	// API change is surfaced loudly instead of silently re-classified.
	err := newHTTPError(
		http.StatusBadRequest,
		[]byte(noDataFoundBodyTemplate),
		"Adam.Rest.Common.NoDataFoundException",
		"Cannot find the uploaded file specified with the token 'tok-xyz'.",
		nil,
	)
	if errors.Is(err, ErrUploadTokenMissing) {
		t.Fatalf("non-404 must not match ErrUploadTokenMissing")
	}
}

func TestErrorIs_UploadTokenMissing_WrongExceptionType(t *testing.T) {
	body := `{"exceptionType":"Adam.Rest.SomeOtherException","exceptionMessage":"Cannot find the uploaded file specified with the token 'tok-xyz'."}`
	err := newHTTPError(
		http.StatusNotFound,
		[]byte(body),
		"Adam.Rest.SomeOtherException",
		"Cannot find the uploaded file specified with the token 'tok-xyz'.",
		nil,
	)
	if errors.Is(err, ErrUploadTokenMissing) {
		t.Fatalf("different exceptionType must not match ErrUploadTokenMissing")
	}
}
