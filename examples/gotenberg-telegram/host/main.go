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
	instanceID   = "gotenberg-telegram-pipeline"
	serverAddr   = "localhost:18084"
	snapshotFile = "gotenberg-telegram-pipeline.bin"
)

func main() {
	fmt.Println("[HOST] Starting Gotenberg-Telegram Durable Pipeline Example...")

	// 1. Clean up old snapshots
	_ = os.Remove(snapshotFile)

	// 2. Start local Mock HTTP Server to mock Telegram and Gotenberg APIs
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
	fmt.Println("\n--- RUN 1: Starting Gotenberg-Telegram workflow with Simulated Crash ---")
	crashed, err := engine.Execute(instanceID, "run", serverAddr, true)
	if err != nil {
		if crashed {
			fmt.Printf("[HOST] Orchestrator successfully suspended/crashed: %v\n", err)
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
	fmt.Println("\n--- RUN 2: Restoring from Snapshot and completing execution ---")
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
	fmt.Println("\n[HOST] Gotenberg-Telegram example completed successfully.")
	os.Exit(0)
}

func startMockServer(addr string) *http.Server {
	mux := http.NewServeMux()

	// Download route
	// First download: Telegram DOCX file.
	// Second download: Gotenberg converted PDF response.
	var downloadCount int
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		downloadCount++
		w.WriteHeader(http.StatusOK)

		if downloadCount == 1 {
			w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
			_, _ = w.Write([]byte("[MOCK DOCX INVOICE FILE CONTENTS]"))
		} else {
			w.Header().Set("Content-Type", "application/pdf")
			_, _ = w.Write([]byte("[MOCK GENERATED PDF INVOICE CONTENTS]"))
		}
	})

	// Upload route
	// First upload: Upload DOCX to Gotenberg.
	// Second upload: Upload PDF to Telegram user.
	var uploadCount int
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		uploadCount++
		w.WriteHeader(http.StatusOK)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("[MOCK SERVER ERROR] Failed to read upload: %v\n", err)
			return
		}

		if uploadCount == 1 {
			fmt.Printf("[MOCK GOTENBERG SERVICE] Received DOCX file for conversion: %s\n", string(body))
		} else {
			fmt.Printf("[MOCK TELEGRAM BOT API] Sending PDF back to chat 77665544: %s\n", string(body))
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
