package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	localTemporal "github.com/nativebpm/temporal"
	"github.com/nativebpm/wasman"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	sqliteDBFile = "snapshots.db"
	dbFile       = "database_temporal.json"
	serverAddr   = "localhost:18085"
)

// GreetWorkflow coordinates execution of the greeting process.
func DurableWasmWorkflow(ctx workflow.Context, instanceID string, serverAddr string) (string, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 1.0,
			MaximumAttempts:    2, // Attempt 1 fails, Attempt 2 succeeds
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	var result string
	err := workflow.ExecuteActivity(ctx, ExecuteDurableWasmActivity, instanceID, serverAddr).Get(ctx, &result)
	return result, err
}

func ExecuteDurableWasmActivity(ctx context.Context, instanceID string, serverAddr string) (string, error) {
	info := activity.GetInfo(ctx)
	attempt := info.Attempt

	slog.Info("[HOST ACTIVITY] Executing Durable WASM Activity", "attempt", attempt, "instance_id", instanceID)

	wasmPath := filepath.Join("..", "worker", "worker.wasm")
	snapshotsDir := "snapshots"
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create snapshots directory: %w", err)
	}
	store := &wasman.FileSnapshotStore{Dir: snapshotsDir}

	// First attempt will simulate crash, second attempt will recover
	simulateCrash := (attempt == 1)
	if simulateCrash {
		slog.Info("[HOST ACTIVITY] Attempt 1: Running WASM worker with simulated crash")
		// Clean up any old snapshots before starting first run
		_ = store.Delete(instanceID)
	} else {
		slog.Info("[HOST ACTIVITY] Attempt 2: Resuming WASM worker from snapshot")
	}

	crashed, err := wasman.NewTestRunner().
		WithWasmPath(wasmPath).
		WithStore(store).
		WithSessionID(instanceID).
		WithServer(serverAddr).
		WithCrash(simulateCrash).
		Run()
	if err != nil {
		if crashed {
			slog.Info("[HOST ACTIVITY] WASM worker suspended/crashed as expected", "error", err)
			return "", fmt.Errorf("simulated crash: %w", err)
		}
		return "", fmt.Errorf("wasm execution failed: %w", err)
	}

	// Read persistence validation database output
	dbBytes, err := os.ReadFile(dbFile)
	if err != nil {
		return "", fmt.Errorf("final database record not found: %w", err)
	}

	// Clean up database snapshot since transaction is completed
	_ = store.Delete(instanceID)

	return string(dbBytes), nil
}

func main() {
	slog.Info("[HOST] Starting Temporal Durable Activity Execution Example")

	// 1. Clean up old files
	_ = os.Remove(dbFile)
	_ = os.Remove(sqliteDBFile)

	// 2. Start local Mock HTTP Server to mock external billing, CRM, and DB API endpoints
	mockServer := startMockServer(serverAddr)
	defer mockServer.Shutdown(context.Background())

	// Give the server a small moment to bind to the port
	time.Sleep(100 * time.Millisecond)

	// 3. Connect to Temporal Server
	cfg := localTemporal.LoadFromEnv()
	cfg.TaskQueue = "wasman-temporal-queue"

	slog.Info("[HOST] Connecting to Temporal Server...", "host_port", cfg.HostPort, "namespace", cfg.Namespace)
	c, err := localTemporal.NewClient(cfg)
	if err != nil {
		slog.Error("[HOST] Failed to create Temporal client", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	// 4. Initialize and start Temporal Worker in background
	w := worker.New(c.RawClient(), cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(DurableWasmWorkflow)
	w.RegisterActivity(ExecuteDurableWasmActivity)

	slog.Info("[HOST] Starting Temporal Worker...")
	if err := w.Start(); err != nil {
		slog.Error("[HOST] Failed to start Temporal worker", "error", err)
		os.Exit(1)
	}
	defer w.Stop()

	// 5. Run Temporal Workflow
	workflowID := "wasman-workflow-" + uuid.New().String()
	instanceID := "temporal-activity-tx"

	slog.Info("[HOST] Starting Workflow execution", "workflow_id", workflowID)
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: cfg.TaskQueue,
	}, DurableWasmWorkflow, instanceID, serverAddr)
	if err != nil {
		slog.Error("[HOST] Failed to start workflow", "error", err)
		os.Exit(1)
	}

	// 6. Wait for workflow to finish
	var result string
	err = run.Get(context.Background(), &result)
	if err != nil {
		slog.Error("[HOST] Workflow failed", "error", err)
		os.Exit(1)
	}

	slog.Info("[HOST] HYBRID APPROACH VALIDATION")
	slog.Info("[HOST] Read from persistent DB (via Temporal Workflow)", "content", result)

	// Clean up database_temporal.json
	_ = os.Remove(dbFile)
	_ = os.Remove(sqliteDBFile)

	slog.Info("[HOST] Temporal Activity example completed successfully!")
}

func startMockServer(addr string) *http.Server {
	mux := http.NewServeMux()

	// Download route (used for reading calculation parameters)
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// Returns base rate and multiplier
		paramsResponse := `{"base_rate":1.5,"multiplier":8.0}`
		_, _ = w.Write([]byte(paramsResponse))
	})

	// Upload route (used for sending request body and final results)
	var uploadCount int
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		uploadCount++
		w.WriteHeader(http.StatusOK)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("[MOCK SERVICES] Failed to read upload", "error", err)
			return
		}

		if uploadCount == 1 {
			slog.Info("[MOCK TEMPORAL SERVICE] Received param request query", "body", string(body))
		} else if uploadCount == 2 {
			slog.Info("[MOCK DATABASE API] Received final calculation result to persist", "body", string(body))
			_ = os.WriteFile(dbFile, body, 0644)
		}
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Error("[MOCK SERVICES] Failed to listen", "error", err)
			return
		}
		if err := server.Serve(l); err != nil && err != http.ErrServerClosed {
			slog.Error("[MOCK SERVICES] Serve error", "error", err)
		}
	}()

	slog.Info("[MOCK SERVICES] Listening", "addr", "http://"+addr)
	return server
}
