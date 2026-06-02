# Wasman: Durable WebAssembly (WASM) Execution Engine

A robust, highly-reusable, and lightweight Durable Execution Engine built on Go, WebAssembly (WASM), and Wasmtime. It provides fault-tolerant execution of custom business logic inside a secure sandbox with automatic memory snapshotting, failure recovery, and memory-efficient streaming.

## Features
- **Durable Execution**: Pauses execution at checkpoints, snapshots the WASM linear memory, and restores state seamlessly after crashes or restarts.
- **$O(1)$ RAM Stream-first HTTP**: Transfers arbitrary stream data (CSV, files, binary payloads) in 4KB chunks directly into/from WASM linear memory via `io.Pipe`, guaranteeing constant memory usage regardless of payload size.
- **WASM Sandbox**: Executes custom code inside a secure virtual machine sandbox via `wasmtime-go`.
- **Simple Reusable API**: Simplifies client imports by exposing all key host interfaces (`Engine`, `Session`, `SnapshotStore`) at the module root level.
- **Robust Failure Resilience**: Avoids resource leaks with reusable context-aware HTTP connections and explicit Wasmtime store cleanup.

---

## Project Structure
- `wasman.go`: Engine compilation, initialization, and core execution cycle.
- `types.go`: Core structures, interfaces, configurations, and errors.
- `execution.go`: Fluent builder API for running execution sessions.
- `session.go`: Stream-first I/O handling for host downloads/uploads.
- `s3_store.go`: Implementation of `SnapshotStore` using S3-compatible object storage (utilizing native If-Match/If-None-Match ETag OCC).
- `FileSnapshotStore` (defined in `fs_store.go`): Implementation of `SnapshotStore` using local files (ideal for local debugging and dev environments).
- `examples/`: Real-world orchestration use cases:
  - `camunda/`: Service task orchestration using Camunda 7 External Tasks with simulated crash recovery.
  - `temporal/`: Long-running Math/CRM activity run in a simulated Temporal execution environment with checkpointing.
  - `process-csv/`: High-performance CSV processing and mapping using $O(1)$ RAM streaming.
  - `gotenberg-telegram/`: Streams a document from Telegram Bot API, converts it to PDF using Gotenberg, and streams it back.
  - `s3-store/`: Baseline demonstration of memory snapshotting, restoration, and native OCC using S3SnapshotStore.

---

## Getting Started

### Prerequisites
- **Go**: v1.26+
- **TinyGo**: For compiling the WASM workers.
- **Docker**: For running external test components (Camunda, Temporal).

### Running Tests
To run automated unit tests for the core engine:
```bash
go test -v ./...
```

---

## Execution Examples

### 1. Simple Stream & Crash Demonstration (CSV processing)
Runs the host orchestrator using mock streaming endpoints. It executes the worker, triggers a simulated crash, writes the snapshots to local files, restarts, restores memory, and runs to completion.
```bash
make -C wasman run
```

### 2. Camunda 7 Service Task Integration
Simulates a real Camunda External Task processing order validation and payment capture.
1. Make sure your local Camunda 7 docker container is running (bound to port `8080`).
2. Run the example:
```bash
make -C wasman run-camunda-example
```

### 3. Temporal Activity Checkpoint demo
Runs a mock Temporal Activity runner demonstrating step-by-step progress tracking, heartbeating, and recovery from failures.
```bash
make -C wasman run-temporal-example
```

### 4. CSV-to-JSON Pipeline ($O(1)$ RAM)
Streams a large mock CSV file, parses/validates columns in WASM, transforms to JSON, and posts the results back to the endpoint chunk-by-chunk.
```bash
make -C wasman run-csv-example
```

### 5. Document Processing Pipeline (Gotenberg & Telegram)
Streams a document template, renders it, converts to PDF via Gotenberg API, and uploads it back.
```bash
make -C wasman run-gotenberg-telegram-example
```

### 6. S3 Native Snapshot Store demo
Runs a baseline memory snapshotting and restoration orchestrator saving modules and progress directly to S3/MinIO.
```bash
make -C wasman run-s3-store-example
```

---

## API Usage Example

```go
package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/nativebpm/wasman"
)
func main() {
	// 1. Initialize snapshot store (File or S3-compatible store)
	// store, err := wasman.NewS3SnapshotStore(ctx, "my-bucket")
	store := &wasman.FileSnapshotStore{Dir: "snapshots"}

	// 2. Load and compile the TinyGo compiled worker.wasm
	engine, err := wasman.NewEngine("worker.wasm", store)
	if err != nil {
		panic(err)
	}

	// 3. Execute the module. 
	// If a snapshot exists under "my-session-id", memory is restored automatically.
	ctx := context.Background()
	crashed, err := engine.Session("my-session-id").
		WithEntrypoint("run").
		WithServer("localhost:8080").
		WithCrash(false).
		Run(ctx)
	if err != nil {
		if crashed {
			fmt.Println("Execution suspended at checkpoint.")
		} else {
			fmt.Printf("Execution failed: %v\n", err)
		}
	} else {
		fmt.Println("Execution finished successfully!")
	}
}
```
