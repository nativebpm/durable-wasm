//go:build wasm

package main

import (
	"fmt"
	"io"

	"github.com/nativebpm/wasman/runner"
)

// State holds the workflow state.
// All fields are automatically preserved during checkpoints by memory snapshotting.
type State struct {
	ProcessedBytes int32
}

var state = &State{
	ProcessedBytes: 0,
}

//export run
func run() int32 {
	return runner.NewWorkflow().
		Step(state.initialize).
		Step(state.processStream).
		Step(state.finalizeWorkflow).
		Run()
}

func main() {}

func (s *State) initialize() error {
	println("[WASM WORKER] Step 0: Starting initialization...")
	println("[WASM WORKER] Step 0 completed.")
	return nil
}

func (s *State) processStream() error {
	println("[WASM WORKER] Step 1: Processing data stream...")

	// Allocate a 4KB buffer for streaming.
	buf := make([]byte, 4096)

	for {
		// Read chunk from host input stream
		n, err := runner.Reader.Read(buf)
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
		wn, err := runner.Writer.Write(buf[:n])
		if err != nil {
			return fmt.Errorf("failed to write to network stream: %w", err)
		}
		if wn != n {
			return fmt.Errorf("mismatch in bytes written: expected %d, got %d", n, wn)
		}

		s.ProcessedBytes += int32(n)
	}

	// Signal EOF on output stream
	return runner.Writer.Close()
}

func (s *State) finalizeWorkflow() error {
	println("[WASM WORKER] Step 2: Finalizing business process...")
	fmt.Printf("[WASM WORKER] Total bytes processed and transformed: %d\n", s.ProcessedBytes)
	return nil
}
