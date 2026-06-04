//go:build wasm

package main

import (
	"github.com/nativebpm/wasman/runner"
)

//export run_test
func runTest() int32 {
	runner.Checkpoint()
	return 0
}

func main() {}
