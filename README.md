# Wasman: Durable WebAssembly (WASM) Execution Engine

A robust, highly-reusable, and lightweight **Durable Execution Engine** built on Go, WebAssembly (WASM), and the `wazero` runtime. It provides fault-tolerant, stateful execution of custom business logic inside a secure sandbox with automatic memory snapshotting, failure recovery, and memory-efficient streaming, completely free of CGO and glibc dependencies.

---

## The Durable Execution Philosophy

Modern distributed architectures often require executing long-running or multi-step business logic (such as workflows, integrations, and orchestrations) that must survive infrastructure failures. Traditional approaches rely on complex database state machines or heavy external orchestrators.

**Wasman** addresses this by leveraging WebAssembly's sandboxed linear memory:
1. **Host-Guest Isolation**: The guest business logic is compiled into a `.wasm` module (compiled via TinyGo/Go) and executed inside the pure-Go `wazero` runtime.
2. **Stateless Host, Stateful Storage**: The execution host runs the virtual machine sandboxes. It remains stateless. All state (linear memory snapshots, execution logs) is persisted in S3 or local file snapshot stores.
3. **Black Box API**: Developers using the platform interact only with high-level client APIs (generated via Protobuf/GRPC or client SDKs). The underlying complexity of WebAssembly, snapshotting, and transaction control is entirely hidden.

```
       [ Client / API Request ] (StartProcess / CompleteTask)
                 │
                 ▼
       ┌──────────────────┐
       │  WorkflowEngine  │ (Host Orchestrator)
       └─────────┬────────┘
                 │
      ┌──────────┴──────────┐
      ▼                     ▼
┌───────────┐         ┌───────────┐
│ bpmn_vm   │ (WASM)  │  worker   │ (WASM Business Logic)
│ Interpreter         │  Executor │
└─────┬─────┘         └─────┬─────┘
      │                     │
      └──────────┬──────────┘
                 │ (State & Memory Checkpoints)
                 ▼
       ┌──────────────────┐
       │  Snapshot Store  │ (Gzip-compressed Snapshots & Deltas)
       └─────────┬────────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
   [ S3 Storage ]   [ File Storage ]
```

---

## Technical Features & Architectural Design

### 1. Transparent Storage Compression
Checkpointing large WebAssembly modules generates snapshots of their linear memory (typically multiples of 64KB pages). To prevent S3/disk space bloat under high throughput:
- **Gzip Compression**: Snapshots, page deltas, and oplogs are transparently compressed using standard gzip format.
- **Sniffing & Backwards Compatibility**: Reads dynamically check for the gzip magic bytes (`0x1f 0x8b`). If present, the data is decompressed in-flight. If absent, it reads the legacy uncompressed data, ensuring full backwards compatibility.

### 2. $O(1)$ RAM Stream-first HTTP
For high-performance data processing (e.g., streaming files, large JSON/CSV payloads):
- Data is transferred directly to/from WASM linear memory in 4KB chunks using `io.Pipe`.
- This guarantees constant memory footprint ($O(1)$ RAM) regardless of payload size, avoiding heap exhaustion and high GC pause times.

### 3. Page-Level Delta Snapshots
Instead of writing a full multi-megabyte memory snapshot on every single checkpoint:
- **Hashing**: Wasman uses FNV-64a to hash individual 64KB memory pages.
- **Deltas**: On subsequent checkpoints, it only writes pages that have actually been modified (dirty pages), drastically reducing I/O latency.

### 4. Optimistic Concurrency Control (OCC)
In high-concurrency environments where multiple orchestrator nodes might receive step execution triggers for the same process instance:
- **S3 ETag Headers**: The S3 storage client uses native HTTP `If-Match` headers.
- **State Integrity**: If another node has updated the snapshot in the meantime, the write fails with an OCC conflict, preventing state corruption.

---

## Defeated Corner Cases (Failure & Crash Recovery)

Wasman guarantees durable execution by checkpointing and restoring state across node crashes:

### Scenario: Server Crash During Execution
1. **Before Step**: The VM starts executing a process. It hits a checkpoint (e.g., before an external API call or a User Task wait state).
2. **Checkpointing**:
   - The engine halts execution.
   - It captures the current state, writing a `Full Snapshot` or `Delta Snapshot` to S3.
   - It logs the expected step transition.
3. **Crash**: The host server crashes (e.g., hardware failure, OOM, or manual redeployment).
4. **Resumption**:
   - Another node receives the request to resume.
   - It reads the metadata, loads the compiled WASM binary, and pulls the compressed snapshot.
   - It restores the linear memory of the WASM VM to the exact page-level state of the last checkpoint.
   - It replays the execution logs (Oplog) to restore transient state and resumes execution seamlessly.

---

## Directory Structure

- [wasman.go](file:///Users/user/github.com/nativebpm/wasman/wasman.go): WASM compilation, runtime setup, and engine execution loops.
- [compress.go](file:///Users/user/github.com/nativebpm/wasman/compress.go): Transparent Gzip compression utilities.
- [fs_store.go](file:///Users/user/github.com/nativebpm/wasman/fs_store.go): Local file-system snapshot store with optional compression.
- [s3_store.go](file:///Users/user/github.com/nativebpm/wasman/s3_store.go): S3-compatible object snapshot store with OCC.
- [types.go](file:///Users/user/github.com/nativebpm/wasman/types.go): Common structures, interfaces, configurations, and error mappings.
- [examples/](file:///Users/user/github.com/nativebpm/wasman/examples/):
  - [process-csv/](file:///Users/user/github.com/nativebpm/wasman/examples/process-csv/): High-throughput CSV mapping with simulated crash recovery and $O(1)$ RAM usage.
  - [camunda/](file:///Users/user/github.com/nativebpm/wasman/examples/camunda/): Integration with Camunda 7 External Tasks.
  - [temporal/](file:///Users/user/github.com/nativebpm/wasman/examples/temporal/): CRM/Math activities in a simulated Temporal environment.
  - [gotenberg-telegram/](file:///Users/user/github.com/nativebpm/wasman/examples/gotenberg-telegram/): Streaming PDF generation bot integration.
  - [s3-store/](file:///Users/user/github.com/nativebpm/wasman/examples/s3-store/): Direct S3/MinIO snapshotting baseline demonstration.

---

## Getting Started

### Running Tests
To run unit and integration tests for the core engine:
```bash
go test -v .
```

### Running the CSV Crash Demonstration
The `process-csv` example demonstrates a complete crash-and-restore cycle:
1. Compile the WASM worker:
   ```bash
   make build-worker
   ```
2. Run the CSV pipeline:
   ```bash
   make run-csv-example
   ```
This will:
- Start a mock HTTP server.
- Initiate execution of the CSV pipeline.
- Simulate a host crash on the first checkpoint.
- Verify the compressed snapshot is written to disk.
- Restore the memory from the snapshot and complete the execution successfully.

---

## API Usage Example

```go
package main

import (
	"context"
	"fmt"
	"github.com/nativebpm/wasman"
)

func main() {
	ctx := context.Background()

	// 1. Initialize snapshot store with compression enabled
	store := &wasman.FileSnapshotStore{
		Dir:         "snapshots",
		Compression: true,
	}

	// 2. Load and compile the WASM module
	engine, err := wasman.NewEngine("worker.wasm", store)
	if err != nil {
		panic(err)
	}

	// 3. Execute session.
	// If a snapshot exists under this session ID, memory is restored automatically.
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
		fmt.Println("Execution completed successfully!")
	}
}
```
