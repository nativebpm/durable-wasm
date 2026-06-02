.PHONY: all build build-worker build-host run clean test docker-build

all: build

build: build-worker build-host
build-worker:
	@echo "Building WASM workers for all examples..."
	tinygo build -o examples/durable-s3/worker/worker.wasm -target=wasi examples/durable-s3/worker/main.go
	tinygo build -o examples/process-csv/worker/worker.wasm -target=wasi examples/process-csv/worker/main.go
	tinygo build -o examples/temporal/worker/worker.wasm -target=wasi examples/temporal/worker/main.go
	tinygo build -o examples/camunda/worker/worker.wasm -target=wasi examples/camunda/worker/main.go
	tinygo build -o examples/gotenberg-telegram/worker/worker.wasm -target=wasi examples/gotenberg-telegram/worker/main.go
	tinygo build -o examples/s3-store/worker/worker.wasm -target=wasi examples/s3-store/worker/main.go

build-host:
	@echo "Building Go host orchestrator..."
	cd examples/durable-s3/host && go build -o host main.go

run:
	make -C examples/durable-s3 run

test: build-worker
	@echo "Running tests..."
	go test -v ./...
clean:
	@echo "Cleaning artifacts..."
	rm -f examples/durable-s3/worker/worker.wasm examples/durable-s3/host/host
	rm -f examples/process-csv/worker/worker.wasm
	rm -f examples/temporal/worker/worker.wasm
	rm -f examples/camunda/worker/worker.wasm
	rm -f examples/gotenberg-telegram/worker/worker.wasm
	rm -f examples/s3-store/worker/worker.wasm
	make -C examples/durable-s3 clean
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

run-durable-s3-docker:
	make -C examples/durable-s3 run-docker

docker-build:

	@echo "Building scratch-based Docker image..."
	docker build -t wasm-durable-host -f examples/durable-s3/host/Dockerfile .

