//go:build wasm

package main

import (
	"unsafe"

	"github.com/nativebpm/wasman/runner"
)

//export run_test
func runTest() int32 {
	ptr := (*int32)(unsafe.Pointer(uintptr(8)))
	val := *ptr

	if val == 0 {
		*ptr = 10
		runner.Checkpoint()
	}
	if val == 10 {
		*ptr = 20
		runner.Checkpoint()
	}
	if val == 20 {
		*ptr = 30
		runner.Checkpoint()
	}
	if val == 30 {
		*ptr = 40
		runner.Checkpoint()
	}
	if val == 40 {
		*ptr = 50
		runner.Checkpoint()
	}

	return 0
}

func main() {}
