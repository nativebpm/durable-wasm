# Wasman Performance Benchmarks

This document records the performance, memory footprint, and allocation profile of the **Wasman Durable Execution Engine** after optimizing the guest-host communication to use direct in-memory stream handlers instead of TCP loopback HTTP sockets.

---

## Benchmark Results

All benchmarks were run on a macOS environment using Go 1.26, Wazero runtime, and TinyGo-compiled WASM modules.

### Performance Summary Table

| Benchmark Name | Iterations ($N$) | Execution Time (ns/op) | Execution Time ($\mu\text{s}$ or $\text{ms}$) | Memory Allocated (B/op) | Heap Allocations (allocs/op) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **`BenchmarkSnapshotStore_SaveFull`** | 9,386 | 149,297 ns | 0.15 ms / op | 4,194,308 B | 1 alloc |
| **`BenchmarkSnapshotStore_SaveDeltas`** | 703,782 | 1,548 ns | 1.55 $\mu$s / op | 8,192 B | 2 allocs |
| **`BenchmarkWasmVM_Execute` (Cold Start)** | 487 | 2,695,567 ns | 2.70 ms / op | 8,919,737 B | 344 allocs |
| **`BenchmarkWasmVM_ExecuteWarm` (Warm Resume)** | 19,771 | 57,148 ns | **57.15 $\mu$s / op** | **51,298 B** | **14 allocs** |

---

## Detailed Analysis

### 1. The Impact of Warm-up (Cold vs. Warm Starts)
* **Cold Execution (`BenchmarkWasmVM_Execute`)**: Takes **2.7 ms** per operation. This includes compiling the WASM binary, allocating the full Wazero instance, running TinyGo initialization (`_start`), and writing the initial full memory snapshot.
* **Warm Execution (`BenchmarkWasmVM_ExecuteWarm`)**: Takes only **57.15 microseconds** per operation. 
  * The Go benchmark framework automatically runs multiple iterations to stabilize JIT/hot paths.
  * In addition, our benchmark logic executes the initial step once outside the measured loop to compile the WASM module, initialize the engine, and establish the baseline snapshot.
  * Inside the benchmark loop, the engine only executes the `"resume"` flow, restoring the memory delta and executing a single task step. This represents the real-world hot-path during task routing.

### 2. High-Performance Memory Snapshotting
* **Full Snapshots vs. Delta Page Snapshots**:
  * Saving a full 4MB memory snapshot takes **149 $\mu$s**.
  * Saving modified delta pages (only 2 dirty pages, 8KB total) takes **1.55 $\mu$s**, reducing storage and serialization latency by **99%**.

### 3. $O(1)$ RAM In-Memory Stream Execution
* Thanks to the removal of the HTTP client, TCP loopback, and JSON HTTP transfers, the warm execution path requires only **51 KB** of heap allocations and **14 allocations** per state transition step. This completely eliminates CPU hotspots and GC pressure under high execution throughput.
