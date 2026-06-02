(module
  (import "env" "checkpoint" (func $checkpoint))
  (memory (export "memory") 1)
  (func (export "run_test")
    (local $val i32)
    ;; Read value from offset 0
    (local.set $val (i32.load (i32.const 0)))

    ;; If val == 0 (First execution)
    (if (i32.eq (local.get $val) (i32.const 0))
      (then
        (i32.store (i32.const 0) (i32.const 10))
        (call $checkpoint)
      )
    )

    ;; If val == 10
    (if (i32.eq (local.get $val) (i32.const 10))
      (then
        (i32.store (i32.const 0) (i32.const 20))
        (call $checkpoint)
      )
    )

    ;; If val == 20
    (if (i32.eq (local.get $val) (i32.const 20))
      (then
        (i32.store (i32.const 0) (i32.const 30))
        (call $checkpoint)
      )
    )

    ;; If val == 30
    (if (i32.eq (local.get $val) (i32.const 30))
      (then
        (i32.store (i32.const 0) (i32.const 40))
        (call $checkpoint)
      )
    )

    ;; If val == 40
    (if (i32.eq (local.get $val) (i32.const 40))
      (then
        (i32.store (i32.const 0) (i32.const 50))
        (call $checkpoint)
      )
    )
  )
)
