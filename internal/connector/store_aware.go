package connector

// StoreAware is the optional interface for connectors that need
// access to the Uplink store (marker files, sync_log inserts, etc).
// The Pool calls UseStore on connectors that implement it after Init.
//
// The interface is intentionally `any` so connector packages can
// type-assert their concrete store dependency without creating an
// import cycle here. The Pool passes the real *store.Store.
type StoreAware interface {
	UseStore(store any)
}
