//go:build wasm

package main

import (
	"github.com/nativebpm/wasman/runner"
)

//export run_test
func runTest() int32 {
	// Checkpoint 1
	runner.Checkpoint()

	// Call trigger_race API to increment version in DB behind our back
	_, _ = runner.Call("trigger_race").Send()

	// Checkpoint 2 (Should fail due to OCC mismatch)
	runner.Checkpoint()

	return 0
}

func main() {}
