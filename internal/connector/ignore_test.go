package connector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// readOnlyFakeConnector implements only Connector.Read; every other
// method panics if invoked. LoadIgnoreMatcher only calls Read, so this
// surface is sufficient for testing it.
type readOnlyFakeConnector struct {
	body []byte
	err  error
}

func (f *readOnlyFakeConnector) Name() string { return "fake" }
func (f *readOnlyFakeConnector) Init(context.Context) error {
	return nil
}
func (f *readOnlyFakeConnector) Close() error { return nil }
func (f *readOnlyFakeConnector) List(context.Context, string) ([]Entry, error) {
	panic("List should not be called by LoadIgnoreMatcher")
}
func (f *readOnlyFakeConnector) Stat(context.Context, string) (Entry, error) {
	panic("Stat should not be called by LoadIgnoreMatcher")
}
func (f *readOnlyFakeConnector) Read(_ context.Context, path string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.body)), nil
}
func (f *readOnlyFakeConnector) OpenRange(context.Context, string, int64, int64) (io.ReadCloser, error) {
	panic("OpenRange should not be called")
}
func (f *readOnlyFakeConnector) Write(context.Context, string, SegmentSource, map[string]any) (Entry, error) {
	panic("Write should not be called")
}
func (f *readOnlyFakeConnector) Delete(context.Context, string) error {
	panic("Delete should not be called")
}
func (f *readOnlyFakeConnector) Move(context.Context, string, string) error {
	panic("Move should not be called")
}
func (f *readOnlyFakeConnector) Reconcile(context.Context, StateStore, ProgressFunc) (ReconcileResult, error) {
	panic("Reconcile should not be called")
}
func (f *readOnlyFakeConnector) Walk(context.Context, string, func(Entry) error) error {
	panic("Walk should not be called")
}

func TestLoadIgnoreMatcher_AbsentFileReturnsNil(t *testing.T) {
	fake := &readOnlyFakeConnector{err: ErrNotFound}
	got, err := LoadIgnoreMatcher(context.Background(), fake)
	if err != nil {
		t.Fatalf("expected nil error for ErrNotFound, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil matcher for absent file, got %v", got)
	}
}

func TestLoadIgnoreMatcher_PresentFileCompiles(t *testing.T) {
	fake := &readOnlyFakeConnector{body: []byte("*.tmp\n# a comment\nbuild/\n")}
	m, err := LoadIgnoreMatcher(context.Background(), fake)
	if err != nil {
		t.Fatalf("LoadIgnoreMatcher: %v", err)
	}
	if m == nil {
		t.Fatal("expected a non-nil matcher")
	}
	if m.PatternCount() == 0 {
		t.Fatal("matcher has no patterns; expected at least the *.tmp rule")
	}
	if !m.ShouldIgnore("scratch.tmp") {
		t.Error("matcher did not ignore scratch.tmp")
	}
	if m.ShouldIgnore("scratch.txt") {
		t.Error("matcher should NOT ignore scratch.txt")
	}
}

func TestLoadIgnoreMatcher_EmptyFileIsHarmless(t *testing.T) {
	fake := &readOnlyFakeConnector{body: []byte("")}
	m, err := LoadIgnoreMatcher(context.Background(), fake)
	if err != nil {
		t.Fatalf("LoadIgnoreMatcher: %v", err)
	}
	if m == nil {
		t.Fatal("expected an empty-but-non-nil matcher for an empty file")
	}
	if m.PatternCount() != 0 {
		t.Errorf("empty file should produce zero patterns, got %d", m.PatternCount())
	}
}

func TestLoadIgnoreMatcher_NonNotFoundErrorPropagates(t *testing.T) {
	want := errors.New("network is unreachable")
	fake := &readOnlyFakeConnector{err: want}
	_, err := LoadIgnoreMatcher(context.Background(), fake)
	if err == nil {
		t.Fatal("expected propagation of non-NotFound read error")
	}
	if !errors.Is(err, want) {
		t.Errorf("expected error chain to include %v, got %v", want, err)
	}
	if !strings.Contains(err.Error(), ".uplinkignore") {
		t.Errorf("error message should reference the ignore filename, got %q", err.Error())
	}
}
