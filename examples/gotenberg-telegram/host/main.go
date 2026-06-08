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
	instanceID   = "gotenberg-telegram-pipeline"
	serverAddr   = "localhost:18084"
	snapshotsDir = "snapshots"
)

func main() {
	slog.Info("[HOST] Starting Gotenberg-Telegram Durable Pipeline Example")

	// 1. Start local Mock HTTP Server to mock Telegram and Gotenberg APIs
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
	slog.Info("[HOST] RUN 1: Starting Gotenberg-Telegram workflow with Simulated Crash")
	crashed, err := wasman.NewTestRunner().
		WithWasmPath(wasmPath).
		WithStore(store).
		WithSessionID(instanceID).
		WithServer(serverAddr).
		WithCrash(true).
		Run()
	if err != nil {
		if crashed {
			slog.Info("[HOST] Orchestrator successfully suspended/crashed", "error", err)
		} else {
			slog.Error("[HOST] Execution failed", "error", err)
			os.Exit(1)
		}
	}

	// Verify snapshot exists in SQLite database
	_, err = store.Load(instanceID)
	if err != nil {
		slog.Error("[HOST] Snapshot was not found in SQLite", "error", err)
		os.Exit(1)
	}
	slog.Info("[HOST] Verified that snapshot was successfully written to SQLite database")

	// 4. RUN 2: Restore from checkpoint and resume execution
	slog.Info("[HOST] RUN 2: Restoring from Snapshot and completing execution")
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
	slog.Info("[HOST] Gotenberg-Telegram example completed successfully")
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
			slog.Error("[MOCK SERVER] Failed to read upload", "error", err)
			return
		}

		if uploadCount == 1 {
			slog.Info("[MOCK GOTENBERG SERVICE] Received DOCX file for conversion", "body", string(body))
		} else {
			slog.Info("[MOCK TELEGRAM BOT API] Sending PDF back to chat 77665544", "body", string(body))
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
