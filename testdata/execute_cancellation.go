//go:build wasm

package main

import (
	"github.com/nativebpm/wasman/runner"
)

//export run_test
func runTest() int32 {
	// Call API that triggers a long operation or cancellation
	_, _ = runner.Call("long_call").Send()
	return 0
}

func main() {}
