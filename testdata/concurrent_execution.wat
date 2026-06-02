(module
  (import "env" "host_call_api" (func $host_call_api (param i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "checkpoint" (func $checkpoint))
  (memory (export "memory") 1)
  (data (i32.const 0) "trigger_race")
  (func (export "run_test")
    (call $checkpoint) ;; Checkpoint 1

    ;; Call trigger_race API to increment version in DB behind our back
    (call $host_call_api (i32.const 0) (i32.const 12) (i32.const 0) (i32.const 0) (i32.const 100) (i32.const 10))
    drop

    (call $checkpoint) ;; Checkpoint 2 (Should fail due to OCC)
  )
)
