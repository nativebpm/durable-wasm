package durable

import "github.com/nativebpm/durable-wasm/host/durable"

// Re-export key types and functions from the host/durable package
// to simplify imports for consumers of this module.

// SnapshotStore abstracts the storage backend for linear memory snapshots.
type SnapshotStore = durable.SnapshotStore

// FileSnapshotStore implements SnapshotStore using the local file system.
type FileSnapshotStore = durable.FileSnapshotStore

// Engine coordinates execution, compilation, and snapshotting of WASM modules.
type Engine = durable.Engine

// Session tracks the dynamic execution state of a running WASM instance.
type Session = durable.Session

// NewEngine creates a new reusable WASM Durable Execution Engine.
func NewEngine(wasmPath string, store SnapshotStore) (*Engine, error) {
	return durable.NewEngine(wasmPath, store)
}
