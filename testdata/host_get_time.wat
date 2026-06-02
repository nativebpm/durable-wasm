(module
  (import "env" "host_get_time" (func $host_get_time (result i64)))
  (import "env" "checkpoint" (func $checkpoint))
  (memory (export "memory") 1)
  (func (export "run_test")
    ;; Call time 1
    (i64.store (i32.const 0) (call $host_get_time))

    ;; First checkpoint
    (call $checkpoint)

    ;; Call time 2
    (i64.store (i32.const 8) (call $host_get_time))

    ;; Second checkpoint
    (call $checkpoint)
  )
)
