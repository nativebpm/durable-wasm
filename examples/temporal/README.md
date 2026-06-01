# Temporal Durable Activity - WASM Example

This example demonstrates how to run heavy, long-running, or multi-step operations inside a WebAssembly (WASM) sandbox as a Temporal Activity, preserving calculation progress across restarts.

## The Problem It Solves
Temporal Activities are designed to be idempotent and retried from scratch when a failure occurs. However, in scenarios involving:
1. **Heavy Computations**: Re-running hours of data analysis or scientific math calculations from step 0 wastes massive CPU cycles.
2. **Sequential Third-Party Integrations**: Re-executing already completed API requests during retries can overload target systems or require complex idempotency keys.

## The Durable WASM Solution
Our WASM execution engine allows the activity code to yield execution at critical checkpoints. When the activity worker crashes, the host stores a progress snapshot. When Temporal retries the activity:
1. The engine checks if a snapshot exists for this Activity Execution ID.
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
- **Go Host** (`host/`): A mock Temporal Activity Runner that handles initialization, simulates a crash on step 0, checkpoints memory to `temporal-activity-tx.bin`, restarts, restores the memory, and executes to completion.

---

## How to Run

From this directory, run:
```bash
make run
```
This command will:
1. Build the TinyGo worker into `worker/worker.wasm`.
2. Start a mock server representing external parameter services and database endpoints.
3. Run the Go host which:
   - Starts execution, saves a checkpoint, and **simulates a crash** (Run 1).
   - Reloads from the checkpoint, restores memory, and runs to completion (Run 2).
   - Verifies the final calculation output in `database_temporal.json` and cleans up snapshots.
