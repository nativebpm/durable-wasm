module github.com/nativebpm/durable-wasm/examples/camunda/host

go 1.26

require (
	github.com/google/uuid v1.6.0
	github.com/nativebpm/camunda v0.0.0
	github.com/nativebpm/durable-wasm v0.0.0
)

require (
	github.com/bytecodealliance/wasmtime-go/v20 v20.0.0 // indirect
	github.com/nativebpm/httpstream v0.0.3 // indirect
	github.com/sequinstream/sequin-go v0.2.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
)

replace (
	github.com/nativebpm/camunda => ../../../../camunda
	github.com/nativebpm/durable-wasm => ../../../
)
