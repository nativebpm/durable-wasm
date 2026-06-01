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
	instanceID   = "csv-worker-instance"
	serverAddr   = "localhost:18082"
	snapshotFile = "csv-worker-instance.bin"
)

func main() {
	fmt.Println("[HOST] Starting CSV-to-JSON Pipeline Durable Execution Example...")

	// 1. Clean up old snapshots
	_ = os.Remove(snapshotFile)

	// 2. Start local Mock HTTP Server to mock external REST calls
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
		fmt.Println("[HOST ERROR] Make sure worker.wasm is compiled by running 'make build'")
		os.Exit(1)
	}

	// 4. RUN 1: Execute with simulated crash on the first checkpoint (Step 0)
	fmt.Println("\n--- RUN 1: Executing WASM CSV pipeline with simulated crash ---")
	crashed, err := engine.Execute(instanceID, "run", serverAddr, true)
	if err != nil {
		if crashed {
			fmt.Printf("[HOST] Execution successfully suspended/crashed: %v\n", err)
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

	// 5. RUN 2: Restore from checkpoint and resume CSV processing to completion
	fmt.Println("\n--- RUN 2: Restoring from snapshot and processing CSV stream ---")
	crashed, err = engine.Execute(instanceID, "run", serverAddr, false)
	if err != nil {
		fmt.Printf("[HOST ERROR] Resumed execution failed: %v\n", err)
		os.Exit(1)
	}

	if crashed {
		fmt.Println("[HOST ERROR] Resumed execution crashed unexpectedly!")
		os.Exit(1)
	}

	// 6. Final Clean up
	_ = os.Remove(snapshotFile)
	fmt.Println("\n[HOST] Durable WASM CSV pipeline example completed successfully.")
	os.Exit(0)
}

func startMockServer(addr string) *http.Server {
	mux := http.NewServeMux()

	// Download route: Streams mock CSV data
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)

		csvData := `id,name,email,amount
1,Alice Johnson,alice@example.com,120.50
2,Bob Smith,bob-invalid-email,250.00
3,Charlie Brown,charlie@example.com,invalid_amount_field
4,David Miller,david@example.com,450.00
5,Eve Adams,eve@example.com,90.25
`
		_, _ = w.Write([]byte(csvData))
	})

	// Upload route: Receives JSON data and displays it
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		fmt.Println("[MOCK SERVER] Received transformed JSON stream:")
		
		// Copy upload request body to host standard output to see the streamed JSON records
		_, err := io.Copy(os.Stdout, r.Body)
		if err != nil {
			fmt.Printf("[MOCK SERVER ERROR] Failed to read request body: %v\n", err)
		}
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			fmt.Printf("[MOCK SERVER ERROR] Failed to listen: %v\n", err)
			return
		}
		if err := server.Serve(l); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[MOCK SERVER ERROR] Serve error: %v\n", err)
		}
	}()

	fmt.Printf("[MOCK SERVER] Listening on http://%s\n", addr)
	return server
}
