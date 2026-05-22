package connector

// Manifest is the static, type-level descriptor for a connector kind. It
// is the Go counterpart of the source.json file each connector package
// embeds via go:embed.
//
// Same struct backs both: a JSON unmarshal of source.json on disk
// produces the same shape as the embedded value, so UI components and
// packagers that read source.json see identical identity (id, name,
// auth, optional UI hooks) without duplication.
type Manifest struct {
	// ID is the stable, machine-readable identifier (e.g. "localfs",
	// "s3", "aprimo"). It is also the value used in YAML configs to
	// reference this connector kind.
	ID string `json:"id"`

	// Name is a short human-readable label.
	Name string `json:"name"`

	// RequiresAuth declares whether the connector needs interactive
	// auth (OAuth, login) before it can run. Uplink uses this to
	// surface "needs setup" status; non-interactive deployments still
	// fulfill auth via env vars or secrets.
	RequiresAuth bool `json:"requiresAuth,omitempty"`

	// Connect names a UI component (relative to the connector's
	// directory) that walks a user through authentication. Empty for
	// connectors with no interactive auth.
	Connect string `json:"connect,omitempty"`

	// Settings names a UI component for the connector's settings
	// screen. Empty if there is nothing to configure beyond the
	// fields enumerated in YAML.
	Settings string `json:"settings,omitempty"`

	// Dependencies enumerates external runtime modules the connector
	// needs. Compiled-in Go connectors leave this empty; the field
	// exists so packagers consuming source.json directly can react.
	Dependencies []string `json:"dependencies,omitempty"`

	// Translations is an opaque blob of locale -> messages, loaded
	// from a sibling translations.json when present. Uplink does not
	// inspect it; it is preserved for UI consumers.
	Translations map[string]any `json:"translations,omitempty"`
}
