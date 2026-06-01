package main

import (
	"context"
	"fmt"
	"io"
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
	camundaURL   = "http://localhost:8080"
)

func main() {
	fmt.Println("[HOST] Starting Camunda-WASM Real Orchestration Example...")

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
		fmt.Printf("[HOST ERROR] Failed to initialize Camunda client: %v\n", err)
		os.Exit(1)
	}

	// 4. Deploy the BPMN process definition to Camunda
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := deployProcess(ctx, client); err != nil {
		fmt.Printf("[HOST ERROR] Failed to deploy process: %v\n", err)
		os.Exit(1)
	}

	// 5. Initialize the Reusable Durable WASM Engine
	wasmPath := filepath.Join("..", "worker", "worker.wasm")
	store := &durable.FileSnapshotStore{Dir: "."}
	
	engine, err := durable.NewEngine(wasmPath, store)
	if err != nil {
		fmt.Printf("[HOST ERROR] Failed to initialize WASM engine: %v\n", err)
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

			fmt.Printf("\n--- [WORKER HANDLER] Received task from Camunda! TaskID=%s, BusinessKey=%s ---\n", task.ID, businessKey)

			snapshotPath := filepath.Join(".", businessKey+".bin")
			_, err := os.Stat(snapshotPath)
			hasSnapshot := !os.IsNotExist(err)

			var shouldCrash bool
			if !hasSnapshot {
				fmt.Println("[WORKER HANDLER] No snapshot found. This is RUN 1 (Fresh start). Enabling crash simulation.")
				shouldCrash = true
			} else {
				fmt.Println("[WORKER HANDLER] Snapshot found! This is RUN 2 (Restoration). Resuming WASM execution.")
				shouldCrash = false
			}

			crashed, err := engine.Execute(businessKey, "run", serverAddr, shouldCrash)
			if err != nil {
				if crashed {
					fmt.Printf("[WORKER HANDLER] WASM Engine execution suspended due to simulated crash: %v\n", err)
					// Fail task in Camunda with 1 retry and 1-second timeout to trigger a retry
					fmt.Println("[WORKER HANDLER] Reporting task failure to Camunda for simulated crash...")
					_ = fail("simulated_host_crash", "WASM state snapshotted, waiting for restoration", 1, 1000)
					return nil
				}
				fmt.Printf("[WORKER HANDLER ERROR] WASM Engine execution failed: %v\n", err)
				_ = fail(err.Error(), "execution error", 0, 0)
				return nil
			}

			fmt.Println("[WORKER HANDLER] WASM Engine execution completed successfully. Completing Camunda task...")
			
			// Complete task in Camunda
			err = complete().Execute()
			if err != nil {
				fmt.Printf("[WORKER HANDLER ERROR] Failed to complete Camunda task: %v\n", err)
				return err
			}

			// Clean up snapshot file
			_ = os.Remove(snapshotPath)
			fmt.Println("[WORKER HANDLER] Cleaned up temporary WASM snapshot from disk.")
			return nil
		},
	), 60000, []string{})

	// 7. Start the Worker in a background goroutine
	go func() {
		fmt.Println("[HOST] Starting Camunda External Task Worker...")
		w.Start(ctx)
	}()

	// 8. Start a process instance in Camunda with unique businessKey
	uniqueKey := "order-" + uuid.NewString()
	fmt.Printf("\n[HOST] Starting Camunda process instance with BusinessKey='%s'...\n", uniqueKey)
	processInstanceID, err := client.StartProcessInstance(ctx, "DurableWasmCamundaProcess", uniqueKey, nil)
	if err != nil {
		fmt.Printf("[HOST ERROR] Failed to start process instance: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[HOST] Process instance started successfully. ID=%s\n", processInstanceID)

	// 9. Wait and verify database output
	fmt.Println("[HOST] Waiting for the database file to be written...")
	
	// Wait up to 30 seconds
	for i := 0; i < 300; i++ {
		time.Sleep(100 * time.Millisecond)
		if _, err := os.Stat(dbFile); err == nil {
			break
		}
	}

	// 10. Verify Database Persistence (Hybrid approach validation)
	fmt.Println("\n--- HYBRID APPROACH VALIDATION ---")
	dbBytes, err := os.ReadFile(dbFile)
	if err != nil {
		fmt.Printf("[HOST ERROR] Final database record not found: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[HOST] Read from persistent DB (%s): %s\n", dbFile, string(dbBytes))

	// Clean up database_camunda.json
	_ = os.Remove(dbFile)
	
	fmt.Println("\n[HOST] Camunda-WASM Real Orchestration example completed successfully.")
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

	fmt.Printf("[HOST] Deployed BPMN process to Camunda. DeploymentID=%s\n", deploymentID)
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
			fmt.Printf("[MOCK SERVICES ERROR] Failed to read upload: %v\n", err)
			return
		}

		if count == 1 {
			fmt.Printf("[MOCK INVENTORY SERVICE] Received check request: %s\n", string(body))
		} else if count == 2 {
			fmt.Printf("[MOCK PAYMENT SERVICE] Received capture request: %s\n", string(body))
		} else if count == 3 {
			fmt.Printf("[MOCK DATABASE API] Received final order record to persist: %s\n", string(body))
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
			fmt.Printf("[MOCK SERVICES ERROR] Failed to listen: %v\n", err)
			return
		}
		if err := server.Serve(l); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[MOCK SERVICES ERROR] Serve error: %v\n", err)
		}
	}()

	fmt.Printf("[MOCK SERVICES] Listening on http://%s\n", addr)
	return server
}
