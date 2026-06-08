package main

import (
	"os"
	"sync/atomic"
	"testing"

	"github.com/nativebpm/wasman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGotenbergTelegramPipeline_Success_With_Retry(t *testing.T) {
	// 2. Mock state in-memory
	var downloadCount int32
	var uploadCount int32

	var receivedDocxBytes []byte
	var receivedPdfBytes []byte

	downloadHandler := func() ([]byte, error) {
		count := atomic.AddInt32(&downloadCount, 1)
		if count == 1 {
			return []byte("[MOCK DOCX INVOICE FILE CONTENTS]"), nil
		}
		return []byte("[MOCK GENERATED PDF INVOICE CONTENTS]"), nil
	}

	uploadHandler := func(payload []byte) error {
		count := atomic.AddInt32(&uploadCount, 1)
		if count == 1 {
			receivedDocxBytes = payload
		} else {
			receivedPdfBytes = payload
		}
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

	// Verify upload count and content
	assert.Equal(t, int32(2), atomic.LoadInt32(&uploadCount))
	assert.Contains(t, string(receivedDocxBytes), "[MOCK DOCX INVOICE FILE CONTENTS]")
	assert.Contains(t, string(receivedPdfBytes), "[MOCK GENERATED PDF INVOICE CONTENTS]")

	// Cleanup snapshot
	_ = store.Delete(instanceID)
}
