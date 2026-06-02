(module
  (import "env" "host_call_api" (func $host_call_api (param i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "long_call")
  (func (export "run_test")
    (call $host_call_api (i32.const 0) (i32.const 9) (i32.const 0) (i32.const 0) (i32.const 100) (i32.const 10))
    drop
  )
)
