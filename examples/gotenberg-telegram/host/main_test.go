package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/nativebpm/durable-wasm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGotenbergTelegramPipeline_Success_With_Retry(t *testing.T) {


	// 2. Start mock REST API services using httptest
	var downloadCount int32
	var uploadCount int32

	var receivedDocxBytes []byte
	var receivedPdfBytes []byte

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/download" {
			count := atomic.AddInt32(&downloadCount, 1)
			w.WriteHeader(http.StatusOK)

			if count == 1 {
				w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
				_, _ = w.Write([]byte("[MOCK DOCX INVOICE FILE CONTENTS]"))
			} else {
				w.Header().Set("Content-Type", "application/pdf")
				_, _ = w.Write([]byte("[MOCK GENERATED PDF INVOICE CONTENTS]"))
			}
			return
		}

		if r.URL.Path == "/upload" {
			count := atomic.AddInt32(&uploadCount, 1)
			w.WriteHeader(http.StatusOK)

			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if count == 1 {
				receivedDocxBytes = body
			} else {
				receivedPdfBytes = body
			}
			_, _ = w.Write([]byte(`{"status":"OK"}`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer testServer.Close()

	// Extract host and port from test server URL
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
	crashed, err := engine.Session(instanceID).
		WithServer(srvAddr).
		WithCrash(true).
		Run(context.Background())
	require.Error(t, err)
	assert.True(t, crashed, "First run should crash")

	// Verify snapshot exists
	snapshot, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.NotEmpty(t, snapshot)

	// 6. RUN 2: Restore from snapshot
	crashed, err = engine.Session(instanceID).
		WithServer(srvAddr).
		WithCrash(false).
		Run(context.Background())
	require.NoError(t, err)
	assert.False(t, crashed)

	// Verify upload count and content
	assert.Equal(t, int32(2), atomic.LoadInt32(&uploadCount))
	assert.Contains(t, string(receivedDocxBytes), "[MOCK DOCX INVOICE FILE CONTENTS]")
	assert.Contains(t, string(receivedPdfBytes), "[MOCK GENERATED PDF INVOICE CONTENTS]")

	// Cleanup snapshot
	_ = store.Delete(instanceID)
}
