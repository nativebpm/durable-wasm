//go:build wasm

package main

import (
	"github.com/nativebpm/wasman/runner"
)

//export run_test
func runTest() int32 {
	// Call time 1
	_ = runner.GetTime().UnixNano()

	// First checkpoint
	runner.Checkpoint()

	// Call time 2
	_ = runner.GetTime().UnixNano()

	// Second checkpoint
	runner.Checkpoint()

	return 0
}

func main() {}
