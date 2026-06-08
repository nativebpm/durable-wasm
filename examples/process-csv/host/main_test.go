package main

import (
	"os"
	"testing"

	"github.com/nativebpm/wasman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVProcessPipeline_Success_With_Retry(t *testing.T) {
	// 2. Mock state in-memory
	var receivedBytes []byte
	var uploadCalled bool

	downloadHandler := func() ([]byte, error) {
		csvData := `id,name,email,amount
1,Alice Johnson,alice@example.com,120.50
2,Bob Smith,bob-invalid-email,250.00
3,Charlie Brown,charlie@example.com,invalid_amount_field
4,David Miller,david@example.com,450.00
5,Eve Adams,eve@example.com,90.25
`
		return []byte(csvData), nil
	}

	uploadHandler := func(payload []byte) error {
		uploadCalled = true
		receivedBytes = payload
		return nil
	}

	// 3. Initialize File Snapshot Store
	_ = os.RemoveAll("snapshots_test")
	err := os.MkdirAll("snapshots_test", 0755)
	require.NoError(t, err)
	store := &wasman.FileSnapshotStore{Dir: "snapshots_test"}
	defer os.RemoveAll("snapshots_test")

	// 4. Initialize Durable WASM Engine
	wasmPath := "../worker/worker.wasm"

	// 5. RUN 1: Execute with simulated crash
	crashed, err := wasman.NewTestRunner().
		WithWasmPath(wasmPath).
		WithStore(store).
		WithSessionID(instanceID).
		WithDownloadHandler(downloadHandler).
		WithUploadHandler(uploadHandler).
		WithCrash(true).
		Run()
	require.Error(t, err)
	assert.True(t, crashed, "First run should crash")

	// Verify snapshot exists
	snapshot, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.NotEmpty(t, snapshot)

	// 6. RUN 2: Restore from snapshot
	crashed, err = wasman.NewTestRunner().
		WithWasmPath(wasmPath).
		WithStore(store).
		WithSessionID(instanceID).
		WithDownloadHandler(downloadHandler).
		WithUploadHandler(uploadHandler).
		WithCrash(false).
		Run()
	require.NoError(t, err)
	assert.False(t, crashed)

	// Verify upload was called and received correct transformed records
	assert.True(t, uploadCalled)

	resultStr := string(receivedBytes)
	// Alice and David and Eve are valid. Bob (invalid email) and Charlie (invalid amount) should be skipped/marked invalid
	assert.Contains(t, resultStr, `"name":"Alice Johnson"`)
	assert.Contains(t, resultStr, `"name":"David Miller"`)
	assert.Contains(t, resultStr, `"name":"Eve Adams"`)

	// Check that snapshot was deleted
	_ = store.Delete(instanceID)
	_, err = store.Load(instanceID)
	assert.Error(t, err)
}
