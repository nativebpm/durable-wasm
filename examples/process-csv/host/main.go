package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/nativebpm/wasman"
)

const (
	instanceID   = "csv-worker-instance"
	serverAddr   = "localhost:18082"
	snapshotsDir = "snapshots"
)

func main() {
	slog.Info("[HOST] Starting CSV-to-JSON Pipeline Durable Execution Example")

	// 1. Start local Mock HTTP Server to mock external REST calls
	mockServer := startMockServer(serverAddr)
	defer mockServer.Shutdown(context.Background())

	// Give the server a small moment to bind to the port
	time.Sleep(100 * time.Millisecond)

	// 2. Initialize the Reusable Durable WASM Engine with File store
	wasmPath := filepath.Join("..", "worker", "worker.wasm")
	_ = os.RemoveAll(snapshotsDir)
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		slog.Error("[HOST] Failed to create snapshots directory", "error", err)
		os.Exit(1)
	}
	store := &wasman.FileSnapshotStore{Dir: snapshotsDir}

	// Clear any leftover snapshot from previous runs in the database
	_ = store.Delete(instanceID)

	// 3. RUN 1: Execute with simulated crash on the first checkpoint (Step 0)
	slog.Info("[HOST] RUN 1: Executing WASM CSV pipeline with simulated crash")
	crashed, err := wasman.NewTestRunner().
		WithWasmPath(wasmPath).
		WithStore(store).
		WithSessionID(instanceID).
		WithServer(serverAddr).
		WithCrash(true).
		Run()
	if err != nil {
		if crashed {
			slog.Info("[HOST] Execution successfully suspended/crashed", "error", err)
		} else {
			slog.Error("[HOST] Execution failed", "error", err)
			os.Exit(1)
		}
	}

	// Verify snapshot exists in File store
	_, err = store.Load(instanceID)
	if err != nil {
		slog.Error("[HOST] Snapshot was not found in File store", "error", err)
		os.Exit(1)
	}
	slog.Info("[HOST] Verified that snapshot was successfully written to File store")

	// 4. RUN 2: Restore from checkpoint and resume CSV processing to completion
	slog.Info("[HOST] RUN 2: Restoring from snapshot and processing CSV stream")
	crashed, err = wasman.NewTestRunner().
		WithWasmPath(wasmPath).
		WithStore(store).
		WithSessionID(instanceID).
		WithServer(serverAddr).
		WithCrash(false).
		Run()
	if err != nil {
		slog.Error("[HOST] Resumed execution failed", "error", err)
		os.Exit(1)
	}

	if crashed {
		slog.Error("[HOST] Resumed execution crashed unexpectedly!")
		os.Exit(1)
	}

	// 5. Final Clean up
	_ = store.Delete(instanceID)
	slog.Info("[HOST] Durable WASM CSV pipeline example completed successfully")
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

		slog.Info("[MOCK SERVER] Received transformed JSON stream")

		// Copy upload request body to host standard output to see the streamed JSON records
		_, err := io.Copy(os.Stdout, r.Body)
		if err != nil {
			slog.Error("[MOCK SERVER] Failed to read request body", "error", err)
		}
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Error("[MOCK SERVER] Failed to listen", "error", err)
			return
		}
		if err := server.Serve(l); err != nil && err != http.ErrServerClosed {
			slog.Error("[MOCK SERVER] Serve error", "error", err)
		}
	}()

	slog.Info("[MOCK SERVER] Listening", "addr", "http://"+addr)
	return server
}
