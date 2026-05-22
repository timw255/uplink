// Package aprimo is a small Aprimo REST API client built for what the
// Aprimo connector needs: authenticated calls, segmented uploads, record
// CRUD, and enough metadata operations to drive channel transforms. It
// is not a comprehensive SDK — surface area grows only as the connector
// demands it.
package aprimo

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
)

// Error is the base type returned by every API call that failed. Callers
// branch with errors.Is / errors.As against the typed errors below.
type Error struct {
	// Message is a short human-readable description.
	Message string
	// Status is the HTTP status code if the call reached the server.
	// Zero for transport / cancellation / config errors.
	Status int
	// AprimoCode is the optional exceptionType returned in the response
	// body (e.g. "ValidationFailed"). Empty when not provided.
	AprimoCode string
	// ResponseBody is the raw response body for diagnostics. Truncated
	// to keep memory bounded.
	ResponseBody []byte
	// Cause is the underlying error, if any.
	Cause error
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	switch {
	case e.Status != 0 && e.Message != "":
		return fmt.Sprintf("aprimo: %d %s: %s", e.Status, http.StatusText(e.Status), e.Message)
	case e.Status != 0:
		return fmt.Sprintf("aprimo: %d %s", e.Status, http.StatusText(e.Status))
	case e.Message != "":
		return "aprimo: " + e.Message
	default:
		return "aprimo: error"
	}
}

// Unwrap exposes the cause so errors.Is / errors.As can traverse.
func (e *Error) Unwrap() error { return e.Cause }

// ErrNotFound / ErrUnauthorized / ErrRateLimited / ErrConflict / ErrServer
// are sentinel categories. The Aprimo connector branches on these via
// errors.Is to translate them into connector.ErrNotFound etc.
var (
	ErrNotFound     = errors.New("aprimo: not found")
	ErrUnauthorized = errors.New("aprimo: unauthorized")
	ErrForbidden    = errors.New("aprimo: forbidden")
	ErrBadRequest   = errors.New("aprimo: bad request")
	ErrConflict     = errors.New("aprimo: conflict")
	ErrValidation   = errors.New("aprimo: validation failed")
	ErrRateLimited  = errors.New("aprimo: rate limited")
	ErrServer       = errors.New("aprimo: server error")
	ErrAuth         = errors.New("aprimo: authentication failed")
	ErrUpload       = errors.New("aprimo: upload failed")

	// ErrUploadTokenMissing means the upload behind a token no longer
	// exists in the upload service. Aprimo purges unattached uploads
	// after a few days, so any token persisted across a long gap can
	// land here. Surfaces as 404 + Adam.Rest.Common.NoDataFoundException
	// with a body referencing the token; uploadTokenMissingMarker is the
	// substring used to disambiguate from "record not found" 404s.
	ErrUploadTokenMissing = errors.New("aprimo: upload token missing")
)

// uploadTokenMissingMarker is the substring Aprimo includes in the body
// of a 404 NoDataFoundException when the upload token references nothing
// on disk. Used to distinguish "the upload is gone" from other 404s
// (e.g. "the record id is gone") on endpoints where both are possible.
const uploadTokenMissingMarker = "specified with the token"

const aprimoCodeNoDataFound = "Adam.Rest.Common.NoDataFoundException"

// Is supports errors.Is by walking the standard sentinel categories.
// The Status field drives most categories; AprimoCode is a fallback.
func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrNotFound:
		return e.Status == http.StatusNotFound
	case ErrUnauthorized:
		return e.Status == http.StatusUnauthorized
	case ErrForbidden:
		return e.Status == http.StatusForbidden
	case ErrBadRequest:
		return e.Status == http.StatusBadRequest
	case ErrConflict:
		return e.Status == http.StatusConflict
	case ErrValidation:
		return e.Status == http.StatusUnprocessableEntity
	case ErrRateLimited:
		return e.Status == http.StatusTooManyRequests
	case ErrServer:
		return e.Status >= 500 && e.Status < 600
	case ErrUploadTokenMissing:
		// 404 + the specific NoDataFoundException + a body that
		// references the token. The body check disambiguates against
		// a "record not found" 404 on PUT /record/{id}, which is the
		// same status and same exceptionType but is about the record
		// id in the URL, not the upload token in the body.
		if e.Status != http.StatusNotFound {
			return false
		}
		if e.AprimoCode != aprimoCodeNoDataFound {
			return false
		}
		return bytes.Contains(e.ResponseBody, []byte(uploadTokenMissingMarker))
	}
	return false
}

// newHTTPError builds an *Error from an HTTP response with a body slice
// already drained. The status is what's reported to callers; the typed
// categories above derive from it.
func newHTTPError(status int, body []byte, aprimoCode, message string, cause error) *Error {
	if message == "" {
		message = http.StatusText(status)
	}
	const maxBody = 8 << 10 // 8 KiB
	if len(body) > maxBody {
		body = append([]byte(nil), body[:maxBody]...)
	} else if body != nil {
		body = append([]byte(nil), body...)
	}
	return &Error{
		Message:      message,
		Status:       status,
		AprimoCode:   aprimoCode,
		ResponseBody: body,
		Cause:        cause,
	}
}

// newTransportError builds an *Error for failures that never reached an
// HTTP response (DNS, ECONNREFUSED, ctx cancellation, etc.).
func newTransportError(message string, cause error) *Error {
	return &Error{Message: message, Cause: cause}
}
