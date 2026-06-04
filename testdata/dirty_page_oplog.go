//go:build wasm

package main

import (
	"unsafe"

	"github.com/nativebpm/wasman/runner"
)

//export run_test
func runTest() int32 {
	// Call test_api with payload "hello"
	_, _ = runner.Call("test_api").WithPayload([]byte("hello")).Send()

	// First checkpoint (Crash point 1)
	runner.Checkpoint()

	// Modify memory at offset 70000 to trigger dirty-page tracking (page 17)
	ptr := (*int32)(unsafe.Pointer(uintptr(70000)))
	*ptr = 42

	// Call test_api with payload "world"
	_, _ = runner.Call("test_api").WithPayload([]byte("world")).Send()

	// Second checkpoint
	runner.Checkpoint()

	return 0
}

func main() {}
