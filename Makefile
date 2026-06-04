.PHONY: all build build-worker build-host run clean test

all: build

build: build-worker build-host build-testdata

build-worker:
	@echo "Building WASM workers for all examples..."
	tinygo build -o examples/process-csv/worker/worker.wasm -target=wasi examples/process-csv/worker/main.go
	tinygo build -o examples/temporal/worker/worker.wasm -target=wasi examples/temporal/worker/main.go
	tinygo build -o examples/camunda/worker/worker.wasm -target=wasi examples/camunda/worker/main.go
	tinygo build -o examples/gotenberg-telegram/worker/worker.wasm -target=wasi examples/gotenberg-telegram/worker/main.go
	tinygo build -o examples/s3-store/worker/worker.wasm -target=wasi examples/s3-store/worker/main.go

build-testdata:
	@echo "Building WASM testdata from Go sources..."
	tinygo build -o testdata/dirty_page_oplog.wasm -target=wasi testdata/dirty_page_oplog.go
	tinygo build -o testdata/host_get_time.wasm -target=wasi testdata/host_get_time.go
	tinygo build -o testdata/concurrent_execution.wasm -target=wasi testdata/concurrent_execution.go
	tinygo build -o testdata/execute_cancellation.wasm -target=wasi testdata/execute_cancellation.go
	tinygo build -o testdata/hash_mismatch_1.wasm -target=wasi testdata/hash_mismatch_1.go
	tinygo build -o testdata/hash_mismatch_2.wasm -target=wasi testdata/hash_mismatch_2.go
	tinygo build -o testdata/multi_checkpoint.wasm -target=wasi testdata/multi_checkpoint.go
	tinygo build -o testdata/oplog_truncation.wasm -target=wasi testdata/oplog_truncation.go
	tinygo build -o testdata/soak_stress.wasm -target=wasi testdata/soak_stress.go
	tinygo build -o testdata/storage_error_injection.wasm -target=wasi testdata/storage_error_injection.go
	tinygo build -o testdata/multi_version_1.wasm -target=wasi testdata/multi_version_1.go
	tinygo build -o testdata/multi_version_2.wasm -target=wasi testdata/multi_version_2.go


build-host:
	@echo "Building Go host orchestrator for CSV example..."
	cd examples/process-csv/host && go build -o host main.go

run:
	make -C examples/process-csv run

test: build-worker build-testdata
	@echo "Running tests..."
	go test -v ./...

clean:
	@echo "Cleaning artifacts..."
	rm -f examples/process-csv/worker/worker.wasm
	rm -f examples/temporal/worker/worker.wasm
	rm -f examples/camunda/worker/worker.wasm
	rm -f examples/gotenberg-telegram/worker/worker.wasm
	rm -f examples/s3-store/worker/worker.wasm
	rm -f testdata/*.wasm
	make -C examples/process-csv clean
	make -C examples/temporal clean
	make -C examples/camunda clean
	make -C examples/gotenberg-telegram clean
	make -C examples/s3-store clean

run-csv-example:
	make -C examples/process-csv run

run-temporal-example:
	make -C examples/temporal run

run-camunda-example:
	make -C examples/camunda run

run-gotenberg-telegram-example:
	make -C examples/gotenberg-telegram run

run-s3-store-example:
	make -C examples/s3-store run

run-s3-store-docker:
	make -C examples/s3-store run-docker
