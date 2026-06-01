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
	instanceID   = "worker-instance-42"
	serverAddr   = "localhost:18080"
	snapshotFile = "worker-instance-42.bin"
)

func main() {
	fmt.Println("[HOST] Starting Reusable Durable WASM Execution Orchestrator...")

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
		fmt.Println("[HOST ERROR] Make sure worker.wasm is compiled by running 'make build-worker'")
		os.Exit(1)
	}

	// 4. RUN 1: Execute with simulated crash on the first checkpoint
	fmt.Println("\n--- RUN 1: Executing WASM from scratch with simulated crash ---")
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

	// 5. RUN 2: Restore from checkpoint and resume execution
	fmt.Println("\n--- RUN 2: Restoring from snapshot and completing execution ---")
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
	fmt.Println("\n[HOST] Durable WASM Execution demonstration complete.")
	os.Exit(0)
}

func startMockServer(addr string) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)

		// Stream 40KB of lowercase text
		line := []byte("durable execution engine base on webassembly and tinygo stream processing test line.\n")
		for i := 0; i < 500; i++ {
			_, _ = w.Write(line)
		}
	})

	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		totalBytes := 0
		allUppercase := true

		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				totalBytes += n
				for i := 0; i < n; i++ {
					if buf[i] >= 'a' && buf[i] <= 'z' {
						allUppercase = false
					}
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		fmt.Printf("[MOCK SERVER] Received total %d bytes. All Uppercase validation: %t\n", totalBytes, allUppercase)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
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
