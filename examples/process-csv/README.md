# O(1) Memory CSV-to-JSON Processing Pipeline

This example demonstrates how to process large streams of data (e.g. CSV to JSON mapping) inside a WebAssembly (WASM) sandbox with strict $O(1)$ constant memory consumption and crash resilience.

## The Problem It Solves
When microservices process large uploads (like 500MB CSV files):
1. **Out of Memory (OOM) Risks**: Loading the entire file or large arrays of structures into memory can trigger OOM errors and crash the container.
2. **High Memory Overhead**: Even streaming parsers in Go can accumulate memory garbage if not designed carefully.
3. **Mid-stream Failures**: If the parser crashes halfway through a large file, restarting it from scratch means reprocessing all records.

## The Durable WASM Solution
1. **$O(1)$ Memory Consumption**: The TinyGo worker allocates a fixed 4KB memory buffer. It pulls CSV data chunk-by-chunk from the host using the `stream_data` host function, transforms it, and immediately pushes JSON output chunks back to the network. The memory allocation remains constant at $O(1)$ regardless of whether the file is 10 Kilobytes or 10 Gigabytes.
2. **Crash Resilience**: During streaming, the worker records the parsing state (current line index, processed bytes). If a crash happens, the host saves the memory checkpoint. Upon restart, the engine restores the parsing state and continues processing the stream from the last successfully read block.

---

## How to Run

From this directory, run:
```bash
make run
```
This command will:
1. Build the TinyGo worker into `worker/worker.wasm`.
2. Start a mock server that serves mock CSV records and receives JSON uploads.
3. Run the Go host which:
   - Starts streaming the CSV, processes a chunk, and **simulates a crash** (Run 1).
   - Reloads the state, restores the exact line and buffer state, and streams the remaining records to the JSON upload server (Run 2).
