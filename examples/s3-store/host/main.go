package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nativebpm/durable-wasm"
)

const (
	instanceID = "s3-demo-instance"
	serverAddr = "localhost:18080"
)

func main() {
	slog.Info("[HOST] Starting Native S3 Durable WASM Execution Orchestrator")

	// 1. Read S3 configuration from environment or use local MinIO defaults
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		bucket = "durable-wasm-demo"
	}
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	}
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
	}

	ctx := context.Background()

	// 2. Initialize S3 snapshot store
	slog.Info("[HOST] Connecting to S3/MinIO", "endpoint", endpoint, "bucket", bucket)
	store, err := durable.NewS3SnapshotStore(ctx, bucket, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.Region = "us-east-1"
		o.UsePathStyle = true
	})
	if err != nil {
		slog.Error("[HOST] Failed to initialize S3 store", "error", err)
		os.Exit(1)
	}

	// 3. Automatically create bucket if it doesn't exist (helpful for local MinIO setup)
	_, err = store.Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		slog.Info("[HOST] Bucket creation skipped (already exists or permission restriction)", "bucket", bucket)
	} else {
		slog.Info("[HOST] Created S3 bucket", "bucket", bucket)
	}

	// 4. Start local Mock HTTP Server to mock external REST calls
	mockServer := startMockServer(serverAddr)
	defer mockServer.Shutdown(ctx)

	// Give the server a small moment to bind to the port
	time.Sleep(100 * time.Millisecond)

	// 5. Initialize the Reusable Durable WASM Engine with S3 store
	wasmPath := os.Getenv("WASM_PATH")
	if wasmPath == "" {
		wasmPath = filepath.Join("..", "worker", "worker.wasm")
	}

	// Clear any leftover snapshot from previous runs
	_ = store.Delete(instanceID)

	engine, err := durable.NewEngine(wasmPath, store)
	if err != nil {
		slog.Error("[HOST] Failed to initialize engine", "error", err)
		slog.Error("[HOST] Make sure worker.wasm is compiled by running 'make build-worker'")
		os.Exit(1)
	}

	// 6. RUN 1: Execute with simulated crash on the first checkpoint
	slog.Info("[HOST] RUN 1: Executing WASM from scratch with simulated crash")
		crashed, err := engine.Session(instanceID).
		WithServer(serverAddr).
		WithCrash(true).
		Run(ctx)
	if err != nil {
		if crashed {
			slog.Info("[HOST] Execution successfully suspended/crashed", "error", err)
		} else {
			slog.Error("[HOST] Execution failed", "error", err)
			os.Exit(1)
		}
	}

	// Verify snapshot exists in S3
	snapshot, err := store.Load(instanceID)
	if err != nil || len(snapshot) == 0 {
		slog.Error("[HOST] Snapshot was not found in S3", "error", err)
		os.Exit(1)
	}
	slog.Info("[HOST] Verified that snapshot was successfully written to S3 bucket")

	// 7. RUN 2: Restore from checkpoint and resume execution
	slog.Info("[HOST] RUN 2: Restoring from S3 snapshot and completing execution")
	crashed, err = engine.Session(instanceID).
		WithServer(serverAddr).
		WithCrash(false).
		Run(ctx)
	if err != nil {
		slog.Error("[HOST] Resumed execution failed", "error", err)
		os.Exit(1)
	}

	if crashed {
		slog.Error("[HOST] Resumed execution crashed unexpectedly!")
		os.Exit(1)
	}

	slog.Info("[HOST] Durable WASM Execution on S3 Store completed successfully!")
	os.Exit(0)
}

func startMockServer(addr string) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)

		// Stream 40KB of lowercase text
		line := []byte("durable execution engine based on webassembly and tinygo native S3 store test line.\n")
		for i := 0; i < 500; i++ {
			_, _ = w.Write(line)
		}
	})

	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		totalBytes := 0
		allUppercase := true

		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				totalBytes += n
				for i := 0; i < n; i++ {
					if buf[i] >= 'a' && buf[i] <= 'z' {
						allUppercase = false
					}
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		slog.Info("[MOCK SERVER] Received payload", "bytes", totalBytes, "all_uppercase", allUppercase)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Error("[MOCK SERVER] Failed to listen", "error", err)
			return
		}
		if err := server.Serve(l); err != nil && err != http.ErrServerClosed {
			slog.Error("[MOCK SERVER] Serve error", "error", err)
		}
	}()

	slog.Info("[MOCK SERVER] Listening", "addr", "http://"+addr)
	return server
}
