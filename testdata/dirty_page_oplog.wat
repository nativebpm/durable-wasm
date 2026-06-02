(module
  (import "env" "checkpoint" (func $checkpoint))
  (import "env" "host_call_api" (func $host_call_api (param i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 2)
  (data (i32.const 0) "test_api")
  (data (i32.const 16) "hello")
  (data (i32.const 100) "world")
  (func (export "run_test")
    ;; Call test_api with payload "hello" -> outputs to offset 32
    (call $host_call_api
      (i32.const 0)   ;; apiNamePtr
      (i32.const 8)   ;; apiNameLen
      (i32.const 16)  ;; reqPtr
      (i32.const 5)   ;; reqLen
      (i32.const 32)  ;; respPtr
      (i32.const 64)  ;; respMaxLen
    )
    drop

    ;; First checkpoint (Crash point 1)
    (call $checkpoint)

    ;; Modify memory in the 2nd page (offset 70000) to trigger dirty-page tracking
    (i32.store (i32.const 70000) (i32.const 42))

    ;; Call test_api with payload "world" -> outputs to offset 200
    (call $host_call_api
      (i32.const 0)   ;; apiNamePtr
      (i32.const 8)   ;; apiNameLen
      (i32.const 100) ;; reqPtr
      (i32.const 5)   ;; reqLen
      (i32.const 200) ;; respPtr
      (i32.const 64)  ;; respMaxLen
    )
    drop

    ;; Second checkpoint
    (call $checkpoint)
  )
)
