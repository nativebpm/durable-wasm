package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nativebpm/camunda"
	"github.com/nativebpm/durable-wasm"
)

const (
	serverAddr   = "localhost:18086"
	dbFile       = "database_camunda.json"
	snapshotsDir = "snapshots"
	camundaURL   = "http://localhost:8080"
)

func main() {
	slog.Info("[HOST] Starting Camunda-WASM Real Orchestration Example")

	// 1. Clean up old files
	_ = os.Remove(dbFile)

	// 2. Start local Mock HTTP Server to mock external inventory, payment, and DB endpoints
	mockServer := startMockServer(serverAddr)
	defer mockServer.Shutdown(context.Background())

	// Give the server a small moment to bind to the port
	time.Sleep(100 * time.Millisecond)

	// 3. Initialize Camunda Client
	client, err := camunda.NewClient(camundaURL, "durable-wasm-worker")
	if err != nil {
		slog.Error("[HOST] Failed to initialize Camunda client", "error", err)
		os.Exit(1)
	}

	// 4. Deploy the BPMN process definition to Camunda
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := deployProcess(ctx, client); err != nil {
		slog.Error("[HOST] Failed to deploy process", "error", err)
		os.Exit(1)
	}

	// 5. Initialize the Reusable Durable WASM Engine with File store
	_ = os.RemoveAll(snapshotsDir)
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		slog.Error("[HOST] Failed to create snapshots directory", "error", err)
		os.Exit(1)
	}
	wasmPath := filepath.Join("..", "worker", "worker.wasm")
	store := &durable.FileSnapshotStore{Dir: snapshotsDir}

	engine, err := durable.NewEngine(wasmPath, store)
	if err != nil {
		slog.Error("[HOST] Failed to initialize WASM engine", "error", err)
		os.Exit(1)
	}

	// 6. Create and configure Camunda Worker
	w := camunda.NewWorker(client, nil)
	w.SetMaxTasks(1)
	w.SetPollInterval(1 * time.Second)

	// Register Task Handler for topic "durable-wasm-task"
	w.RegisterHandler("durable-wasm-task", camunda.TaskHandlerFunc(
		func(ctx context.Context, c *camunda.Client, task camunda.ExternalTask, complete camunda.CompleteFunc, fail camunda.FailFunc) error {
			businessKey := task.BusinessKey
			if businessKey == "" {
				businessKey = task.ID
			}

			slog.Info("[WORKER HANDLER] Received task from Camunda", "task_id", task.ID, "business_key", businessKey)

			_, err := store.Load(businessKey)
			hasSnapshot := err == nil

			var shouldCrash bool
			if !hasSnapshot {
				slog.Info("[WORKER HANDLER] No snapshot found. This is RUN 1 (Fresh start). Enabling crash simulation.")
				shouldCrash = true
			} else {
				slog.Info("[WORKER HANDLER] Snapshot found! This is RUN 2 (Restoration). Resuming WASM execution.")
				shouldCrash = false
			}

			crashed, err := engine.Execute(ctx, businessKey, "run", serverAddr, shouldCrash)
			if err != nil {
				if crashed {
					slog.Warn("[WORKER HANDLER] WASM Engine execution suspended due to simulated crash", "error", err)
					// Fail task in Camunda with 1 retry and 1-second timeout to trigger a retry
					slog.Info("[WORKER HANDLER] Reporting task failure to Camunda for simulated crash...")
					_ = fail("simulated_host_crash", "WASM state snapshotted, waiting for restoration", 1, 1000)
					return nil
				}
				slog.Error("[WORKER HANDLER] WASM Engine execution failed", "error", err)
				_ = fail(err.Error(), "execution error", 0, 0)
				return nil
			}

			slog.Info("[WORKER HANDLER] WASM Engine execution completed successfully. Completing Camunda task...")

			// Complete task in Camunda
			err = complete().Execute()
			if err != nil {
				slog.Error("[WORKER HANDLER] Failed to complete Camunda task", "error", err)
				return err
			}

			// Clean up snapshot using the store interface
			_ = store.Delete(businessKey)
			slog.Info("[WORKER HANDLER] Cleaned up temporary WASM snapshot from store.")
			return nil
		},
	), 60000, []string{})

	// 7. Start the Worker in a background goroutine
	go func() {
		slog.Info("[HOST] Starting Camunda External Task Worker")
		w.Start(ctx)
	}()

	// 8. Start a process instance in Camunda with unique businessKey
	uniqueKey := "order-" + uuid.NewString()
	slog.Info("[HOST] Starting Camunda process instance", "business_key", uniqueKey)
	processInstanceID, err := client.StartProcessInstance(ctx, "DurableWasmCamundaProcess", uniqueKey, nil)
	if err != nil {
		slog.Error("[HOST] Failed to start process instance", "error", err)
		os.Exit(1)
	}
	slog.Info("[HOST] Process instance started successfully", "instance_id", processInstanceID)

	// 9. Wait and verify database output
	slog.Info("[HOST] Waiting for the database file to be written...")

	// Wait up to 30 seconds
	for i := 0; i < 300; i++ {
		time.Sleep(100 * time.Millisecond)
		if _, err := os.Stat(dbFile); err == nil {
			break
		}
	}

	// 10. Verify Database Persistence (Hybrid approach validation)
	slog.Info("[HOST] HYBRID APPROACH VALIDATION")
	dbBytes, err := os.ReadFile(dbFile)
	if err != nil {
		slog.Error("[HOST] Final database record not found", "error", err)
		os.Exit(1)
	}
	slog.Info("[HOST] Read from persistent DB", "file", dbFile, "content", string(dbBytes))

	// Clean up database_camunda.json
	_ = os.Remove(dbFile)

	slog.Info("[HOST] Camunda-WASM Real Orchestration example completed successfully")
	cancel()
	time.Sleep(500 * time.Millisecond) // Give worker a moment to shut down
	os.Exit(0)
}

func deployProcess(ctx context.Context, client *camunda.Client) error {
	file, err := os.Open("../bpmn/process.bpmn")
	if err != nil {
		return err
	}
	defer file.Close()

	deploymentID, err := client.DeployProcess(ctx, "durable-wasm-camunda-deployment", file, file.Name())
	if err != nil {
		return err
	}

	slog.Info("[HOST] Deployed BPMN process to Camunda", "deployment_id", deploymentID)
	return nil
}

func startMockServer(addr string) *http.Server {
	mux := http.NewServeMux()

	var downloadCount int32

	// Download route (used for reading responses)
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		count := atomic.AddInt32(&downloadCount, 1)

		var responseBody string
		if count == 1 {
			// First download request corresponds to Inventory Check response
			responseBody = `{"status":"available"}`
		} else {
			// Second download request corresponds to Payment Capture response
			responseBody = `{"status":"success"}`
		}

		_, _ = w.Write([]byte(responseBody))
	})

	// Upload route (used for sending requests)
	var uploadCount int32
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&uploadCount, 1)
		w.WriteHeader(http.StatusOK)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("[MOCK SERVICES] Failed to read upload", "error", err)
			return
		}

		if count == 1 {
			slog.Info("[MOCK INVENTORY SERVICE] Received check request", "body", string(body))
		} else if count == 2 {
			slog.Info("[MOCK PAYMENT SERVICE] Received capture request", "body", string(body))
		} else if count == 3 {
			slog.Info("[MOCK DATABASE API] Received final order record to persist", "body", string(body))
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
