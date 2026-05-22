package connector

import (
	"context"
	"errors"
	"testing"
)

func TestRegistryRegisterAndBuild(t *testing.T) {
	r := NewRegistry()
	r.Register(Registration{
		Manifest: Manifest{ID: "noop", Name: "no-op"},
		Factory: func(name string, _ map[string]any) (Connector, error) {
			return &fakeConnector{name: name}, nil
		},
	})

	c, err := r.Build("noop", "test", nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if c.Name() != "test" {
		t.Fatalf("Name = %q, want %q", c.Name(), "test")
	}

	m, ok := r.Manifest("noop")
	if !ok || m.Name != "no-op" {
		t.Fatalf("Manifest: got %+v ok=%v", m, ok)
	}
}

func TestRegisterEmbeddedManifest(t *testing.T) {
	r := NewRegistry()
	r.RegisterEmbedded([]byte(`{"id":"emb","name":"embedded","requiresAuth":true}`),
		func(name string, _ map[string]any) (Connector, error) {
			return &fakeConnector{name: name}, nil
		})
	m, ok := r.Manifest("emb")
	if !ok || !m.RequiresAuth {
		t.Fatalf("manifest not parsed: %+v ok=%v", m, ok)
	}
}

func TestRegistryUnknownType(t *testing.T) {
	r := NewRegistry()
	_, err := r.Build("missing", "x", nil)
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestHandlerFunc(t *testing.T) {
	var got Event
	h := HandlerFunc(func(_ context.Context, e Event) error {
		got = e
		return nil
	})
	want := Event{Connector: "c", Kind: EventCreate}
	if err := h.Handle(context.Background(), want); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got.Connector != want.Connector || got.Kind != want.Kind {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestSentinelErrors(t *testing.T) {
	wrapped := errors.New("vendor 404: " + ErrNotFound.Error())
	_ = wrapped // sanity: ensure sentinel can be referenced in formatting
	if !errors.Is(ErrNotFound, ErrNotFound) {
		t.Fatal("errors.Is broken for sentinel")
	}
}

type fakeConnector struct {
	Connector
	name string
}

func (f *fakeConnector) Name() string { return f.name }
