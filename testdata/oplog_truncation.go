//go:build wasm

package main

import (
	"unsafe"

	"github.com/nativebpm/wasman/runner"
)

//export run_test
func runTest() int32 {
	ptr := (*int32)(unsafe.Pointer(uintptr(200)))
	val := *ptr

	// Call API 1
	_, _ = runner.Call("test_api").WithPayload([]byte("hello")).Send()
	if val == 0 {
		*ptr = 10
		runner.Checkpoint()
	}

	// Call API 2
	_, _ = runner.Call("test_api").WithPayload([]byte("hello")).Send()
	if val == 10 {
		*ptr = 20
		runner.Checkpoint()
	}

	// Call API 3
	_, _ = runner.Call("test_api").WithPayload([]byte("hello")).Send()
	if val == 20 {
		*ptr = 30
		runner.Checkpoint()
	}

	// Call API 4
	_, _ = runner.Call("test_api").WithPayload([]byte("hello")).Send()
	if val == 30 {
		*ptr = 40
		runner.Checkpoint()
	}

	// Call API 5
	_, _ = runner.Call("test_api").WithPayload([]byte("hello")).Send()
	if val == 40 {
		*ptr = 50
		runner.Checkpoint()
	}

	return 0
}

func main() {}
