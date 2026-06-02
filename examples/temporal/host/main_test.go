package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	localTemporal "github.com/nativebpm/temporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func TestDurableWasmWorkflow_RealTemporalServer(t *testing.T) {
	// 1. Clean up database files
	_ = os.Remove(dbFile)
	_ = os.Remove(sqliteDBFile)
	defer func() {
		_ = os.Remove(dbFile)
		_ = os.Remove(sqliteDBFile)
	}()

	// 2. Start mock HTTP server
	mockServer := startMockServer(serverAddr)
	defer mockServer.Shutdown(context.Background())

	// Give mock server time to start
	time.Sleep(100 * time.Millisecond)

	// 3. Connect to real Temporal Server
	cfg := localTemporal.LoadFromEnv()
	cfg.TaskQueue = "durable-wasm-test-queue-" + uuid.New().String()

	c, err := localTemporal.NewClient(cfg)
	require.NoError(t, err, "Temporal server must be running on %s", cfg.HostPort)
	defer c.Close()

	// 4. Start real Worker in background
	w := worker.New(c.RawClient(), cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(DurableWasmWorkflow)
	w.RegisterActivity(ExecuteDurableWasmActivity)

	err = w.Start()
	require.NoError(t, err)
	defer w.Stop()

	// 5. Run Workflow
	workflowID := "durable-wasm-test-workflow-" + uuid.New().String()
	instanceID := "temporal-test-activity-tx-" + uuid.New().String()

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: cfg.TaskQueue,
	}, DurableWasmWorkflow, instanceID, serverAddr)
	require.NoError(t, err)

	// 6. Wait for workflow to finish
	var result string
	err = run.Get(context.Background(), &result)
	require.NoError(t, err)

	// 7. Verify final result format
	assert.Contains(t, result, `"completed":true`)
	assert.Contains(t, result, `"result_value":1800`)
	assert.Contains(t, result, `"activity_id":"ACT-TEMP-4455"`)
}
