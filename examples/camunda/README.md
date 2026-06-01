# Camunda 7 External Task - Durable WASM Example

This example demonstrates how to run fault-tolerant, multi-step business logic inside a WebAssembly (WASM) sandbox as a Camunda 7 External Task Worker.

## The Problem It Solves
When orchestrating microservices using Camunda External Tasks, workers often perform sequential steps (e.g., Step 1: Check Inventory -> Step 2: Charge Payment -> Step 3: Update Order DB). 

If the worker crashes or the host loses power mid-execution:
1. **Inconsistency / Double Operations**: Re-running the task from the beginning might execute Step 1 and Step 2 again, causing duplicate billing or double inventory deductions.
2. **Complex State Tracking**: Developers usually have to write complex, ad-hoc state check logic at the start of each task handler to see what has already been done.

## The Durable WASM Solution
Our WebAssembly engine executes the task inside an isolated sandbox. At each business checkpoint (e.g., after checking inventory, or after capturing payment), the worker calls the host function `checkpoint()` which freezes the WASM linear memory and serializes it to disk.

When a crash occurs:
1. The host process halts, but the task is not completed in Camunda.
2. When the task is retried (or another worker instance fetches it), the engine detects an existing memory snapshot for this `BusinessKey` / `TaskID`.
3. It initializes a clean WASM instance, restores the memory state instantly from the snapshot, and resumes execution **exactly** from the last saved state-machine step, skipping previously executed external calls.
4. Once completed, it reports success to Camunda and cleans up the snapshot.

---

## Architecture
- **TinyGo Worker** (`worker/`): Implements the state machine (Steps 0 to 4) and compiles to `worker.wasm`. It imports host functions for checkpointing and HTTP streaming.
- **Go Host** (`host/`): Polls Camunda REST API for tasks on the `durable-wasm-task` topic, implements host callbacks (network mock APIs, snapshot loading/saving), and manages the Wasmtime runtime.

---

## How to Run

### 1. Start Camunda 7
Ensure a local Camunda 7 server is running on port `8080`. You can start it via Docker:
```bash
docker run -d --name camunda-test -p 8080:8080 camunda/camunda-bpm-platform:latest
```

### 2. Compile and Run the Example
From this directory, run:
```bash
make run
```
This command will:
1. Compile the TinyGo worker into `worker/worker.wasm`.
2. Deploy the [process.bpmn](file:///Users/user/github.com/nativebpm/durable-wasm/examples/camunda/bpmn/process.bpmn) definition to Camunda.
3. Start a process instance in Camunda.
4. Launch the External Task worker, which will fetch the task and:
   - Run **Step 0**, save a checkpoint, and **simulate a host crash**.
   - Fail the task in Camunda with 1 retry remaining.
   - Fetch the task again (simulating recovery), load the checkpoint, restore memory, and execute **Step 1 (Inventory Check)**, **Step 2 (Payment Capture)**, **Step 3 (DB Save)**, and **Step 4 (Completion)**.
   - Complete the task in Camunda.
