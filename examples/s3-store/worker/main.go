//go:build wasm

package main

import (
	"unsafe"
)

// Global state variables.
// In WebAssembly, global variables reside in the linear memory.
// When we snapshot and restore the linear memory, the values of these variables
// are automatically preserved and restored.
var (
	step           int32 = 0
	processedBytes int32 = 0
)

// Host function imports.
// In TinyGo, we use the `//go:wasmimport` compiler directive to import host functions.

//go:wasmimport env checkpoint
func checkpoint()

// direction: 0 for reading from network, 1 for writing to network.
// ptr: memory address pointing to the start of the buffer.
// length: size of the buffer/data to transfer.
// Returns the number of bytes read/written, or a negative value on error.
//
//go:wasmimport env stream_data
func stream_data(direction int32, ptr uint32, length uint32) int32

// The entrypoint function invoked by the Go host.
// It uses a state machine to support resumption from checkpoints.
//
//export run
func run() int32 {
	for {
		switch step {
		case 0:
			println("[WASM WORKER] Step 0: Starting initialization...")
			// Transition to step 1 before making a checkpoint.
			// When restored, execution will resume from step 1 because the 'step'
			// variable will be loaded with value 1 from the memory snapshot.
			step = 1
			println("[WASM WORKER] Step 0 completed. Initiating checkpoint.")
			checkpoint()

		case 1:
			println("[WASM WORKER] Step 1: Processing data stream...")

			// Allocate a 4KB buffer on the WASM heap.
			// This represents O(1) memory consumption since the buffer size is constant.
			buf := make([]byte, 4096)
			// Get the raw memory address of the first byte of the slice.
			ptr := uint32(uintptr(unsafe.Pointer(&buf[0])))

			for {
				// Read a chunk from the network stream into the buffer.
				bytesRead := stream_data(0, ptr, uint32(len(buf)))
				if bytesRead < 0 {
					println("[WASM WORKER] Error: failed to read from network stream")
					return -1
				}
				if bytesRead == 0 {
					// EOF reached.
					println("[WASM WORKER] Stream EOF. All data received.")
					break
				}

				// Transform the data in-place (converting lowercase to uppercase).
				for i := int32(0); i < bytesRead; i++ {
					if buf[i] >= 'a' && buf[i] <= 'z' {
						buf[i] = buf[i] - 'a' + 'A'
					}
				}

				// Write the transformed chunk back to the network stream.
				bytesWritten := stream_data(1, ptr, uint32(bytesRead))
				if bytesWritten != bytesRead {
					println("[WASM WORKER] Error: mismatch in bytes written to network stream")
					return -1
				}

				processedBytes += bytesRead
			}

			// Signal EOF to the output network stream.
			stream_data(1, ptr, 0)

			// Transition to step 2 and trigger a checkpoint.
			step = 2
			println("[WASM WORKER] Step 1 completed. Initiating checkpoint.")
			checkpoint()

		case 2:
			println("[WASM WORKER] Step 2: Finalizing business process...")
			println("[WASM WORKER] Total bytes processed and transformed:", processedBytes)
			step = 3
			println("[WASM WORKER] Step 2 completed. Initiating final checkpoint.")
			checkpoint()

		case 3:
			println("[WASM WORKER] Execution already completed.")
			return 1
		}
	}
}

// Main function is required for TinyGo target=wasi compilation but is unused by our host.
func main() {}
