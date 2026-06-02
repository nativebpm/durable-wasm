# Temporal Durable Activity - WASM Example

This example demonstrates how to run heavy, long-running, or multi-step operations inside a WebAssembly (WASM) sandbox as a real Temporal Activity, preserving calculation progress across restarts using Temporal's Retry Policy.

## The Problem It Solves
Temporal Activities are designed to be idempotent and retried from scratch when a failure occurs. However, in scenarios involving:
1. **Heavy Computations**: Re-running hours of data analysis or scientific math calculations from step 0 wastes massive CPU cycles.
2. **Sequential Third-Party Integrations**: Re-executing already completed API requests during retries can overload target systems or require complex idempotency keys.

## The Durable WASM Solution
Our WASM execution engine allows the activity code to yield execution at critical checkpoints. When the activity worker crashes, the host stores a progress snapshot. When Temporal retries the activity:
1. The engine checks if a snapshot exists for this Activity Execution ID in SQLite database.
2. It restores the WASM memory state from the snapshot.
3. The activity continues execution **exactly from the step it paused**, retaining local variables and processing state.
4. Final business results are committed to the database only on success, conforming to the hybrid data pattern (keeping active working state in snapshots and final state in the DB).

---

## Architecture
- **TinyGo Worker** (`worker/`): A step-based calculator.
  - **Step 0**: Initializes execution.
  - **Step 1**: Downloads calculation parameters (base rate, multiplier) from a mock endpoint.
  - **Step 2**: Executes calculations and increments progress trackers.
  - **Step 3**: Saves final calculated output to the database.
- **Go Host** (`host/`): A real Temporal Activity Runner & Starter.
  - Connects to the local Temporal Server.
  - Registers the `DurableWasmWorkflow` and `ExecuteDurableWasmActivity`.
  - On the first attempt (`Attempt == 1`), the activity simulates a host crash during execution, saving the WASM snapshot in `snapshots.db` and returning a failure to Temporal.
  - On the second attempt (`Attempt == 2`), Temporal automatically schedules a retry. The activity loads the saved WASM memory snapshot from `snapshots.db`, resumes execution from Step 1, and successfully completes the activity.

---

## How to Run

1. **Start Temporal Dev Server** in background:
   ```bash
   make start-temporal
   ```

2. **Run the Example**:
   ```bash
   make run
   ```
   This command will:
   - Build the TinyGo worker into `worker/worker.wasm`.
   - Start a mock server representing external parameter services.
   - Boot the Go host, which registers the Temporal Worker and starts the workflow.
   - Run the workflow:
     - **Attempt 1**: Simulates crash and fails.
     - **Attempt 2**: Recovers state from SQLite and completes successfully.

3. **Stop Temporal Dev Server**:
   ```bash
   make stop-temporal
   ```
