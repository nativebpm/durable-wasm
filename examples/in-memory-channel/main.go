package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nativebpm/wasman"
)

func main() {
	// 1. Locate the precompiled WASM module.
	// We use the "dirty_page_oplog.wasm" compiled test module from the package testdata.
	wasmPath := filepath.Join("..", "..", "testdata", "dirty_page_oplog.wasm")
	if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
		// Try fallback if running from root directory
		wasmPath = filepath.Join("connectors", "wasman", "testdata", "dirty_page_oplog.wasm")
		if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
			fmt.Println("Error: dirty_page_oplog.wasm not found. Please compile it first using 'make build-testdata'")
			os.Exit(1)
		}
	}

	// 2. Define the in-memory API Handler callback.
	// The guest WASM logic calls host APIs (such as DB writes, system logs, or HTTP requests).
	// By specifying WithApiHandler, all API calls are routed directly to this Go function closure
	// inside the same address space, bypassing the HTTP network stack entirely.
	apiHandler := func(apiName string, request []byte) ([]byte, error) {
		fmt.Printf("[HOST CALLBACK] API called: %q with request payload: %q\n", apiName, string(request))
		// Return a mock response payload
		return []byte(fmt.Sprintf("mock-response-for-%s", string(request))), nil
	}

	// 3. Configure and execute the WASM session using the Fluent Runner API.
	// The Runner builder manages SnapshotStore initialization, engine building,
	// and execution configuration in a single unified chain with sticky error handling.
	ctx := context.Background()
	instanceID := "in-memory-session-demo"
	store := wasman.NewMemorySnapshotStore()

	fmt.Printf("[HOST] Starting execution of WASM session %q...\n", instanceID)
	crashed, err := wasman.NewRunner().
		WithContext(ctx).
		WithWasmPath(wasmPath).
		WithStore(store).
		WithSessionID(instanceID).
		WithEntrypoint("run_test").
		WithApiHandler(apiHandler).
		Run()

	if err != nil {
		fmt.Printf("Execution failed: %v (crashed: %v)\n", err, crashed)
		os.Exit(1)
	}

	fmt.Println("[HOST] WASM execution completed successfully!")

	// 4. Demonstrate State Resumption / Durability.
	// Since checkpoint() was called inside the WASM core (dirty_page_oplog.go),
	// the engine automatically saved full and delta memory snapshots to the store.
	fmt.Println("[HOST] Loading saved session metadata from store to verify checkpointing...")
	meta, err := store.LoadMetadata(instanceID)
	if err != nil {
		fmt.Printf("Failed to load metadata: %v\n", err)
		os.Exit(1)
	}

	if meta != nil {
		fmt.Printf("[HOST] Saved Session state verified. Current DB version counter: %d\n", meta.Version)
	}
}
