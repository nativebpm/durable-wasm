package testdata

import (
	_ "embed"
)

//go:embed dirty_page_oplog.wat
var DirtyPageOplogWat string

//go:embed host_get_time.wat
var HostGetTimeWat string

//go:embed multi_checkpoint.wat
var MultiCheckpointWat string

//go:embed hash_mismatch_1.wat
var HashMismatchWat1 string

//go:embed hash_mismatch_2.wat
var HashMismatchWat2 string

//go:embed concurrent_execution.wat
var ConcurrentExecutionWat string

//go:embed oplog_truncation.wat
var OplogTruncationWat string

//go:embed execute_cancellation.wat
var ExecuteCancellationWat string

//go:embed storage_error_injection.wat
var StorageErrorInjectionWat string

//go:embed soak_stress.wat
var SoakStressWat string

//go:embed multi_version_1.wat
var MultiVersionWat1 string

//go:embed multi_version_2.wat
var MultiVersionWat2 string
