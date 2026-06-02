package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/nativebpm/durable-wasm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


func TestCSVProcessPipeline_Success_With_Retry(t *testing.T) {


	// 2. Start mock REST API services using httptest
	var receivedBytes []byte
	var uploadCalled bool

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/download" {
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
			return
		}

		if r.URL.Path == "/upload" {
			uploadCalled = true
			var err error
			receivedBytes, err = io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"OK"}`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer testServer.Close()

	// Extract host and port from test server URL
	// E.g., http://127.0.0.1:51234 -> 127.0.0.1:51234
	srvAddr := testServer.Listener.Addr().String()

	// 3. Initialize File Snapshot Store
	_ = os.RemoveAll("snapshots_test")
	err := os.MkdirAll("snapshots_test", 0755)
	require.NoError(t, err)
	store := &durable.FileSnapshotStore{Dir: "snapshots_test"}
	defer os.RemoveAll("snapshots_test")

	// 4. Initialize Durable WASM Engine
	wasmPath := "../worker/worker.wasm"
	engine, err := durable.NewEngine(wasmPath, store)
	require.NoError(t, err)

	// 5. RUN 1: Execute with simulated crash
	crashed, err := engine.Execute(context.Background(), instanceID, "run", srvAddr, true)
	require.Error(t, err)
	assert.True(t, crashed, "First run should crash")

	// Verify snapshot exists
	snapshot, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.NotEmpty(t, snapshot)

	// 6. RUN 2: Restore from snapshot
	crashed, err = engine.Execute(context.Background(), instanceID, "run", srvAddr, false)
	require.NoError(t, err)
	assert.False(t, crashed)

	// Verify upload was called and received correct transformed records
	assert.True(t, uploadCalled)

	resultStr := string(receivedBytes)
	// Alice and David and Eve are valid. Bob (invalid email) and Charlie (invalid amount) should be skipped/marked invalid
	assert.Contains(t, resultStr, `"name":"Alice Johnson"`)
	assert.Contains(t, resultStr, `"name":"David Miller"`)
	assert.Contains(t, resultStr, `"name":"Eve Adams"`)

	// Check that snapshot was deleted in final main.go logic?
	// The host/main.go deletes it manually at the end. Let's do it too
	_ = store.Delete(instanceID)
	_, err = store.Load(instanceID)
	assert.Error(t, err)
}
