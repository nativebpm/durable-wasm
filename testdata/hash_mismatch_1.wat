(module
  (import "env" "checkpoint" (func $checkpoint))
  (memory (export "memory") 1)
  (func (export "run_test")
    (i32.store (i32.const 0) (i32.const 100))
    (call $checkpoint)
  )
)
