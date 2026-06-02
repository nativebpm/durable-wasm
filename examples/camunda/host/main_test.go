package main

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nativebpm/camunda"
	"github.com/nativebpm/durable-wasm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCamundaWasmOrchestration_RealCamundaServer(t *testing.T) {
	// 1. Cleanup files
	_ = os.Remove(dbFile)
	_ = os.RemoveAll("snapshots_test")
	defer func() {
		_ = os.Remove(dbFile)
		_ = os.RemoveAll("snapshots_test")
	}()

	// 2. Start mock REST API services
	mockServices := startMockServer(serverAddr)
	defer mockServices.Shutdown(context.Background())

	// Give the mock services a moment to start
	time.Sleep(100 * time.Millisecond)

	// 3. Initialize Camunda Client pointed to real Camunda server
	client, err := camunda.NewClient(camundaURL, "durable-wasm-worker")
	require.NoError(t, err, "Camunda server must be running on %s", camundaURL)

	// 4. Deploy the BPMN process definition to Camunda
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = deployProcess(ctx, client)
	require.NoError(t, err)

	// 5. Initialize Durable Engine
	wasmPath := "../worker/worker.wasm"
	_ = os.RemoveAll("snapshots_test")
	err = os.MkdirAll("snapshots_test", 0755)
	require.NoError(t, err)
	store := &durable.FileSnapshotStore{Dir: "snapshots_test"}
	defer os.RemoveAll("snapshots_test")

	engine, err := durable.NewEngine(wasmPath, store)
	require.NoError(t, err)

	// 6. Create Camunda Worker
	w := camunda.NewWorker(client, nil)
	w.SetMaxTasks(1)
	w.SetPollInterval(100 * time.Millisecond)
	w.SetAsyncResponseTimeout(0) // Disable long polling for fast test execution

	// Register Task Handler for topic "durable-wasm-task"
	uniqueKey := "order-test-" + uuid.NewString()

	w.RegisterHandler("durable-wasm-task", camunda.TaskHandlerFunc(
		func(ctx context.Context, c *camunda.Client, task camunda.ExternalTask, complete camunda.CompleteFunc, fail camunda.FailFunc) error {

			// Only process our specific test instance task
			if task.BusinessKey != uniqueKey {
				return nil
			}

			_, err := store.Load(uniqueKey)
			hasSnapshot := err == nil

			shouldCrash := !hasSnapshot

			crashed, err := engine.Session(uniqueKey).
				WithServer(serverAddr).
				WithCrash(shouldCrash).
				Run(ctx)
			if err != nil {
				if crashed {
					slog.Info("[WORKER HANDLER] Reporting task failure to Camunda...", "task_id", task.ID)
					errFail := fail("simulated_host_crash", "WASM state snapshotted", 1, 1000)
					if errFail != nil {
						slog.Error("[WORKER HANDLER] Failed to report task failure to Camunda", "error", errFail)
					} else {
						slog.Info("[WORKER HANDLER] Reported task failure successfully!")
					}
					return nil
				}
				slog.Error("[WORKER HANDLER] WASM Engine execution failed", "error", err)
				_ = fail(err.Error(), "execution error", 0, 0)
				return nil
			}

			slog.Info("[WORKER HANDLER] WASM Engine execution completed successfully. Completing Camunda task...")
			err = complete().Execute()
			if err != nil {
				slog.Error("[WORKER HANDLER] Failed to complete task in Camunda", "error", err)
				return err
			}

			_ = store.Delete(uniqueKey)
			slog.Info("[WORKER HANDLER] Cleaned up temporary WASM snapshot from store.")
			return nil
		},
	), 60000, []string{})

	// Start worker in background
	go func() {
		w.Start(ctx)
	}()

	// Start process instance in Camunda
	_, err = client.StartProcessInstance(ctx, "DurableWasmCamundaProcess", uniqueKey, nil)
	require.NoError(t, err)

	// 7. Wait and verify database output (up to 15 seconds)
	require.Eventually(t, func() bool {
		_, err := os.Stat(dbFile)
		return err == nil
	}, 15*time.Second, 100*time.Millisecond)

	// Verify persistence file content
	dbBytes, err := os.ReadFile(dbFile)
	require.NoError(t, err)
	assert.Contains(t, string(dbBytes), `"status":"processed"`)

	// Snapshot should be cleaned up
	assert.Eventually(t, func() bool {
		_, err = store.Load(uniqueKey)
		return err != nil
	}, 3*time.Second, 50*time.Millisecond, "Snapshot should be deleted")
}

