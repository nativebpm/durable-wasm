//go:build wasm

package main

import (
	"fmt"
	"io"

	"github.com/nativebpm/durable-wasm"
)

// Global state variables are automatically preserved by memory snapshotting.
var (
	processedBytes int32 = 0
)

//export run
func run() int32 {
	return durable.NewWorkflow().
		Step(initialize).
		Step(processStream).
		Step(finalizeWorkflow).
		Run()
}

func main() {}

func initialize() error {
	println("[WASM WORKER] Step 0: Starting initialization...")
	println("[WASM WORKER] Step 0 completed.")
	return nil
}

func processStream() error {
	println("[WASM WORKER] Step 1: Processing data stream...")

	// Allocate a 4KB buffer for streaming.
	buf := make([]byte, 4096)

	for {
		// Read chunk from host input stream
		n, err := durable.Reader.Read(buf)
		if err == io.EOF {
			println("[WASM WORKER] Stream EOF. All data received.")
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from network stream: %w", err)
		}

		// Transform the chunk in-place (lowercase to uppercase)
		for i := 0; i < n; i++ {
			if buf[i] >= 'a' && buf[i] <= 'z' {
				buf[i] = buf[i] - 'a' + 'A'
			}
		}

		// Write transformed chunk back to host output stream
		wn, err := durable.Writer.Write(buf[:n])
		if err != nil {
			return fmt.Errorf("failed to write to network stream: %w", err)
		}
		if wn != n {
			return fmt.Errorf("mismatch in bytes written: expected %d, got %d", n, wn)
		}

		processedBytes += int32(n)
	}

	// Signal EOF on output stream
	return durable.Writer.Close()
}

func finalizeWorkflow() error {
	println("[WASM WORKER] Step 2: Finalizing business process...")
	fmt.Printf("[WASM WORKER] Total bytes processed and transformed: %d\n", processedBytes)
	return nil
}
