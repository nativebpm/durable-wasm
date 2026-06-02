(module
  (import "env" "host_call_api" (func $host_call_api (param i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "checkpoint" (func $checkpoint))
  (memory (export "memory") 1)
  (data (i32.const 0) "test_api")
  (data (i32.const 16) "hello")
  (func (export "run_test")
    (local $val i32)
    (local.set $val (i32.load (i32.const 200)))

    ;; Call API 1
    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
    drop
    ;; If val == 0
    (if (i32.eq (local.get $val) (i32.const 0))
      (then
        (i32.store (i32.const 200) (i32.const 10))
        (call $checkpoint)
      )
    )

    ;; Call API 2
    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
    drop
    ;; If val == 10
    (if (i32.eq (local.get $val) (i32.const 10))
      (then
        (i32.store (i32.const 200) (i32.const 20))
        (call $checkpoint)
      )
    )

    ;; Call API 3
    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
    drop
    ;; If val == 20
    (if (i32.eq (local.get $val) (i32.const 20))
      (then
        (i32.store (i32.const 200) (i32.const 30))
        (call $checkpoint)
      )
    )

    ;; Call API 4
    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
    drop
    ;; If val == 30
    (if (i32.eq (local.get $val) (i32.const 30))
      (then
        (i32.store (i32.const 200) (i32.const 40))
        (call $checkpoint)
      )
    )

    ;; Call API 5
    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
    drop
    ;; If val == 40
    (if (i32.eq (local.get $val) (i32.const 40))
      (then
        (i32.store (i32.const 200) (i32.const 50))
        (call $checkpoint)
      )
    )
  )
)
