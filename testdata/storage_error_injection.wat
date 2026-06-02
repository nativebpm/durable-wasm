(module
  (import "env" "checkpoint" (func $checkpoint))
  (memory (export "memory") 1)
  (func (export "run_test")
    (call $checkpoint)
  )
)
