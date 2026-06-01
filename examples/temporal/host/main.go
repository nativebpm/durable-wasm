package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/nativebpm/durable-wasm"
)

const (
	instanceID   = "temporal-activity-tx"
	serverAddr   = "localhost:18085"
	snapshotFile = "temporal-activity-tx.bin"
	dbFile       = "database_temporal.json"
)

func main() {
	fmt.Println("[HOST] Starting Temporal Durable Activity Execution Example...")

	// 1. Clean up old files
	_ = os.Remove(snapshotFile)
	_ = os.Remove(dbFile)

	// 2. Start local Mock HTTP Server to mock external billing, CRM, and DB API endpoints
	mockServer := startMockServer(serverAddr)
	defer mockServer.Shutdown(context.Background())

	// Give the server a small moment to bind to the port
	time.Sleep(100 * time.Millisecond)

	// 3. Initialize the Reusable Durable WASM Engine
	wasmPath := filepath.Join("..", "worker", "worker.wasm")
	store := &durable.FileSnapshotStore{Dir: "."}
	
	engine, err := durable.NewEngine(wasmPath, store)
	if err != nil {
		fmt.Printf("[HOST ERROR] Failed to initialize engine: %v\n", err)
		os.Exit(1)
	}

	// 4. RUN 1: Execute with simulated crash on the first checkpoint (Step 0)
	fmt.Println("\n--- RUN 1: Starting Temporal Activity with Simulated Crash ---")
	crashed, err := engine.Execute(instanceID, "run", serverAddr, true)
	if err != nil {
		if crashed {
			fmt.Printf("[HOST] Activity successfully suspended/crashed: %v\n", err)
		} else {
			fmt.Printf("[HOST ERROR] Execution failed: %v\n", err)
			os.Exit(1)
		}
	}

	// Verify snapshot exists
	if _, err := os.Stat(snapshotFile); os.IsNotExist(err) {
		fmt.Println("[HOST ERROR] Snapshot was not created on checkpoint!")
		os.Exit(1)
	}
	fmt.Println("[HOST] Verified that snapshot file was written to disk.")

	// 5. RUN 2: Restore from checkpoint and resume execution
	fmt.Println("\n--- RUN 2: Restoring Activity State from Snapshot and Resuming execution ---")
	crashed, err = engine.Execute(instanceID, "run", serverAddr, false)
	if err != nil {
		fmt.Printf("[HOST ERROR] Resumed execution failed: %v\n", err)
		os.Exit(1)
	}

	if crashed {
		fmt.Println("[HOST ERROR] Resumed execution crashed unexpectedly!")
		os.Exit(1)
	}

	// 6. Verify Database Persistence (Hybrid approach validation)
	fmt.Println("\n--- HYBRID APPROACH VALIDATION ---")
	dbBytes, err := os.ReadFile(dbFile)
	if err != nil {
		fmt.Printf("[HOST ERROR] Final database record not found: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[HOST] Read from persistent DB (%s): %s\n", dbFile, string(dbBytes))

	// Clean up snapshot since the transaction is completed (we no longer need workflow memory)
	_ = os.Remove(snapshotFile)
	if _, err := os.Stat(snapshotFile); os.IsNotExist(err) {
		fmt.Println("[HOST] Workflow memory snapshot successfully cleaned up from disk (Transaction Completed).")
	}

	// Clean up database_temporal.json
	_ = os.Remove(dbFile)
	
	fmt.Println("\n[HOST] Temporal Activity example completed successfully.")
	os.Exit(0)
}

func startMockServer(addr string) *http.Server {
	mux := http.NewServeMux()

	// Download route (used for reading calculation parameters)
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// Returns base rate and multiplier
		paramsResponse := `{"base_rate":1.5,"multiplier":8.0}`
		_, _ = w.Write([]byte(paramsResponse))
	})

	// Upload route (used for sending request body and final results)
	var uploadCount int
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		uploadCount++
		w.WriteHeader(http.StatusOK)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("[MOCK SERVICES ERROR] Failed to read upload: %v\n", err)
			return
		}

		if uploadCount == 1 {
			fmt.Printf("[MOCK TEMPORAL SERVICE] Received param request query: %s\n", string(body))
		} else if uploadCount == 2 {
			fmt.Printf("[MOCK DATABASE API] Received final calculation result to persist: %s\n", string(body))
			_ = os.WriteFile(dbFile, body, 0644)
		}
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			fmt.Printf("[MOCK SERVICES ERROR] Failed to listen: %v\n", err)
			return
		}
		if err := server.Serve(l); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[MOCK SERVICES ERROR] Serve error: %v\n", err)
		}
	}()

	fmt.Printf("[MOCK SERVICES] Listening on http://%s\n", addr)
	return server
}
