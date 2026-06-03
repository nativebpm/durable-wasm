package testdata

import (
	_ "embed"
)

//go:embed dirty_page_oplog.wasm
var DirtyPageOplogWasm []byte

//go:embed host_get_time.wasm
var HostGetTimeWasm []byte

//go:embed multi_checkpoint.wasm
var MultiCheckpointWasm []byte

//go:embed hash_mismatch_1.wasm
var HashMismatchWasm1 []byte

//go:embed hash_mismatch_2.wasm
var HashMismatchWasm2 []byte

//go:embed concurrent_execution.wasm
var ConcurrentExecutionWasm []byte

//go:embed oplog_truncation.wasm
var OplogTruncationWasm []byte

//go:embed execute_cancellation.wasm
var ExecuteCancellationWasm []byte

//go:embed storage_error_injection.wasm
var StorageErrorInjectionWasm []byte

//go:embed soak_stress.wasm
var SoakStressWasm []byte

//go:embed multi_version_1.wasm
var MultiVersionWasm1 []byte

//go:embed multi_version_2.wasm
var MultiVersionWasm2 []byte
