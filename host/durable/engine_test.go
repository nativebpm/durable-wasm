package durable

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurableExecutionLifecycle(t *testing.T) {
	instanceID := "test-worker-instance"
	snapshotFile := "./test-worker-instance.bin"
	serverAddr := "localhost:18081"

	// 1. Clean up old snapshots
	_ = os.Remove(snapshotFile)
	defer os.Remove(snapshotFile)

	// 2. Start mock HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello from durable test stream!"))
	})

	var receivedBytes []byte
	var uploadErr error
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		receivedBytes, uploadErr = io.ReadAll(r.Body)
		if uploadErr != nil {
			http.Error(w, uploadErr.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", serverAddr)
	require.NoError(t, err)

	go func() {
		_ = server.Serve(ln)
	}()
	defer server.Shutdown(context.Background())

	// Give the server a small moment to start
	time.Sleep(50 * time.Millisecond)

	// 3. Initialize engine
	wasmPath := filepath.Join("..", "..", "worker", "worker.wasm")
	store := &FileSnapshotStore{Dir: "."}
	
	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err, "Failed to compile WASM module. Make sure worker.wasm is built.")

	// 4. RUN 1: Execute with simulated crash
	crashed, err := engine.Execute(instanceID, "run", serverAddr, true)
	require.Error(t, err)
	assert.True(t, crashed, "Expected run 1 to crash at checkpoint")

	// Verify snapshot exists
	_, err = os.Stat(snapshotFile)
	require.NoError(t, err, "Snapshot bin file should have been written to disk")

	// 5. RUN 2: Restore from checkpoint and run to completion
	crashed, err = engine.Execute(instanceID, "run", serverAddr, false)
	require.NoError(t, err, "Run 2 should complete without errors")
	assert.False(t, crashed, "Run 2 should not crash")

	// 6. Verify processed output
	expectedOutput := "HELLO FROM DURABLE TEST STREAM!"
	assert.Equal(t, expectedOutput, string(receivedBytes), "Data processed by WASM worker should be converted to uppercase")
}
